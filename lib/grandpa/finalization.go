// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package grandpa

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ChainSafe/gossamer/dot/telemetry"
)

type ephemeralService interface {
	Start() error
	Stop() error
}

type finalizationHandler struct {
	servicesLock       sync.Mutex
	finalizationEngine ephemeralService
	votingRound        ephemeralService

	newServices    func() (engine, voting ephemeralService)
	timeoutStop    time.Duration
	initiateRound  func() error
	observableErrs chan error
	stopCh         chan struct{}
	handlerDone    chan struct{}
}

func newFinalizationHandler(service *Service) *finalizationHandler {
	// builder is a constructor function which takes care to instantiate
	// and return the services needed to finalize a round, those services
	// are ephemeral services with a lifetime of a round
	builder := func() (engine, voting ephemeralService) {
		finalizationEngine := newFinalizationEngine(service)
		votingRound := newHandleVotingRound(service, finalizationEngine.actionCh)
		return finalizationEngine, votingRound
	}

	return &finalizationHandler{
		newServices: builder,

		timeoutStop:    5 * time.Second,
		initiateRound:  service.initiateRound,
		observableErrs: make(chan error),
		stopCh:         make(chan struct{}),
		handlerDone:    make(chan struct{}),
	}
}

func (fh *finalizationHandler) Start() (errsCh <-chan error, err error) {
	go fh.start()
	return fh.observableErrs, nil
}

func (fh *finalizationHandler) start() {
	defer close(fh.handlerDone)

	for {
		select {
		case <-fh.stopCh:
			return
		default:
		}

		err := fh.initiateRound()
		if err != nil {
			fh.observableErrs <- fmt.Errorf("initiating round: %w", err)
			return
		}

		err = fh.waitServices()
		if err != nil {
			fh.observableErrs <- err
			return
		}
	}
}

func (fh *finalizationHandler) stop() (err error) {
	fh.servicesLock.Lock()
	defer fh.servicesLock.Unlock()

	stopWg := new(sync.WaitGroup)
	stopWg.Add(2)

	errs := [2]error{}
	go func() {
		defer stopWg.Done()
		errs[0] = fh.finalizationEngine.Stop()
	}()

	go func() {
		defer stopWg.Done()
		errs[1] = fh.votingRound.Stop()
	}()

	stopWg.Wait()

	if errs[0] != nil {
		return errs[0]
	}

	if errs[1] != nil {
		return errs[1]
	}

	return nil
}

func (fh *finalizationHandler) Stop() (err error) {
	close(fh.stopCh)

	err = fh.stop()
	select {
	case <-fh.handlerDone:
	case <-time.After(fh.timeoutStop):
		return errTimeoutWhileStoping
	}

	close(fh.observableErrs)
	return err
}

// waitServices will start the services and wait until they complete or
func (fh *finalizationHandler) waitServices() error {
	fh.servicesLock.Lock()
	fh.finalizationEngine, fh.votingRound = fh.newServices()
	fh.servicesLock.Unlock()

	finalizationEngineErr := make(chan error)
	go func() {
		defer close(finalizationEngineErr)
		err := fh.finalizationEngine.Start()
		if err != nil {
			finalizationEngineErr <- err
		}
	}()

	votingRoundErr := make(chan error)
	go func() {
		defer close(votingRoundErr)
		err := fh.votingRound.Start()
		if err != nil {
			votingRoundErr <- err
		}
	}()

	for {
		select {
		case <-fh.stopCh:
			return nil

		case err, ok := <-votingRoundErr:
			if !ok {
				votingRoundErr = nil
			}

			stopErr := fh.stop()
			if stopErr != nil {
				logger.Infof("stopping finalisation handler: %s", stopErr)
			}
			return err

		case err, ok := <-finalizationEngineErr:
			if !ok {
				finalizationEngineErr = nil
			}

			stopErr := fh.stop()
			if stopErr != nil {
				logger.Infof("stopping finalisation handler: %s", stopErr)
			}

			return err

		default:
			finish := votingRoundErr == nil && finalizationEngineErr == nil
			if finish {
				return nil
			}
		}
	}
}

var errTimeoutWhileStoping = errors.New("timeout while stopping")

type handleVotingRound struct {
	grandpaService       *Service
	timeoutStop          time.Duration
	finalizationEngineCh <-chan engineAction
	stopCh               chan struct{}
	engineDone           chan struct{}
}

func newHandleVotingRound(service *Service, finalizationEngineCh <-chan engineAction) *handleVotingRound {
	return &handleVotingRound{
		timeoutStop:          5 * time.Second,
		grandpaService:       service,
		stopCh:               make(chan struct{}),
		engineDone:           make(chan struct{}),
		finalizationEngineCh: finalizationEngineCh,
	}
}

func (h *handleVotingRound) Stop() (err error) {
	if h.stopCh == nil {
		return nil
	}

	close(h.stopCh)
	select {
	case <-h.engineDone:
	case <-time.After(h.timeoutStop):
		return errTimeoutWhileStoping
	}

	h.stopCh = nil
	return nil
}

// playGrandpaRound executes a round of GRANDPA
// at the end of this round, a block will be finalised.
// TODO(test): stopping handleVotingRound first and then stopping
// finalizationEngine might cause a write in a non-reading unbuff channel
// blocking the finalizationEngine to stop and triggering the stop timeout timer
func (h *handleVotingRound) Start() (err error) {
	defer close(h.engineDone)
	start := time.Now()

	logger.Debugf("starting round %d with set id %d",
		h.grandpaService.state.round, h.grandpaService.state.setID)

	for {
		select {
		case <-h.stopCh:
			return nil

		case action, ok := <-h.finalizationEngineCh:
			if !ok {
				return nil
			}

			switch action {
			case determinePrevote:
				isPrimary, err := h.grandpaService.handleIsPrimary()
				if err != nil {
					return fmt.Errorf("handling primary: %w", err)
				}

				// broadcast pre-vote
				preVote, err := h.grandpaService.determinePreVote()
				if err != nil {
					return fmt.Errorf("determining pre-vote: %w", err)
				}

				signedpreVote, prevoteMessage, err :=
					h.grandpaService.createSignedVoteAndVoteMessage(preVote, prevote)
				if err != nil {
					return fmt.Errorf("creating signed vote: %w", err)
				}

				if !isPrimary {
					h.grandpaService.prevotes.Store(h.grandpaService.publicKeyBytes(), signedpreVote)
				}

				logger.Warnf("sending pre-vote message: {%v}", prevoteMessage)
				h.grandpaService.sendPrevoteMessage(prevoteMessage)

			case determinePrecommit:
				preComit, err := h.grandpaService.determinePreCommit()
				if err != nil {
					return fmt.Errorf("determining pre-commit: %w", err)
				}

				signedpreComit, precommitMessage, err :=
					h.grandpaService.createSignedVoteAndVoteMessage(preComit, precommit)
				if err != nil {
					return fmt.Errorf("creating signed vote: %w", err)
				}

				logger.Warnf("sending pre-commit message: {%v}", precommitMessage)
				h.grandpaService.precommits.Store(h.grandpaService.publicKeyBytes(), signedpreComit)
				h.grandpaService.sendPrecommitMessage(precommitMessage)

			case finalize:
				commitMessage, err := h.grandpaService.newCommitMessage(
					h.grandpaService.head, h.grandpaService.state.round, h.grandpaService.state.setID)
				if err != nil {
					return fmt.Errorf("creating commit message: %w", err)
				}

				commitConsensusMessage, err := commitMessage.ToConsensusMessage()
				if err != nil {
					return fmt.Errorf("transforming commit into consensus message: %w", err)
				}

				logger.Debugf("sending commit message: %v", commitMessage)

				h.grandpaService.network.GossipMessage(commitConsensusMessage)
				h.grandpaService.telemetry.SendMessage(telemetry.NewAfgFinalizedBlocksUpTo(
					h.grandpaService.head.Hash(),
					fmt.Sprint(h.grandpaService.head.Number),
				))

				logger.Debugf("round completed in %s", time.Since(start))
				return nil

			case alreadyFinalized:
				logger.Debugf("round completed in %s", time.Since(start))
				return nil
			}
		}
	}
}

// actions that should take place accordingly to votes the
// finalisation engine knows about
type engineAction byte

const (
	determinePrevote engineAction = iota
	determinePrecommit
	alreadyFinalized
	finalize
)

type finalizationEngine struct {
	grandpaService *Service

	timeoutStop time.Duration
	stopCh      chan struct{}
	engineDone  chan struct{}
	actionCh    chan engineAction
}

func newFinalizationEngine(service *Service) *finalizationEngine {
	return &finalizationEngine{
		grandpaService: service,
		timeoutStop:    5 * time.Second,
		actionCh:       make(chan engineAction),
		stopCh:         make(chan struct{}),
		engineDone:     make(chan struct{}),
	}
}

func (f *finalizationEngine) Stop() (err error) {
	if f.stopCh == nil {
		return nil
	}

	close(f.stopCh)
	select {
	case <-f.engineDone:
	case <-time.After(f.timeoutStop):
		return errTimeoutWhileStoping
	}

	f.stopCh = nil
	close(f.actionCh)
	return nil
}

func (f *finalizationEngine) Start() (err error) {
	defer close(f.engineDone)

	err = f.defineRoundVotes()
	if err != nil {
		return fmt.Errorf("defining round votes: %w", err)
	}

	err = f.finalizeRound()
	if err != nil {
		return fmt.Errorf("finalising round: %w", err)
	}

	return nil
}

func (f *finalizationEngine) defineRoundVotes() error {
	gossipInterval := f.grandpaService.interval
	determinePrevoteTimer := time.NewTimer(gossipInterval * 2)
	determinePrecommitTimer := time.NewTimer(gossipInterval * 4)

	var precommited bool = false

	for !precommited {
		select {
		case <-f.stopCh:
			if !determinePrevoteTimer.Stop() {
				<-determinePrevoteTimer.C
			}

			if !determinePrecommitTimer.Stop() {
				<-determinePrecommitTimer.C
			}

			return nil

		case <-determinePrevoteTimer.C:
			alreadyCompletable, err := f.grandpaService.checkRoundCompletable()
			if err != nil {
				return fmt.Errorf("checking round is completable: %w", err)
			}

			if alreadyCompletable {
				f.actionCh <- alreadyFinalized
				return nil
			}

			f.actionCh <- determinePrevote

		case <-determinePrecommitTimer.C:
			alreadyCompletable, err := f.grandpaService.checkRoundCompletable()
			if err != nil {
				return fmt.Errorf("checking round is completable: %w", err)
			}

			if alreadyCompletable {
				f.actionCh <- alreadyFinalized
				return nil
			}

			prevoteGrandpaGhost, err := f.grandpaService.getPreVotedBlock()
			if err != nil {
				return fmt.Errorf("getting grandpa ghost: %w", err)
			}

			total, err := f.grandpaService.getTotalVotesForBlock(prevoteGrandpaGhost.Hash, prevote)
			if err != nil {
				return fmt.Errorf("getting grandpa ghost: %w", err)
			}

			if total <= f.grandpaService.state.threshold() {
				determinePrecommitTimer.Reset(gossipInterval * 4)
				break
			}

			latestFinalizedHash := f.grandpaService.head.Hash()
			isDescendant, err := f.grandpaService.blockState.IsDescendantOf(
				latestFinalizedHash, prevoteGrandpaGhost.Hash)
			if err != nil {
				return fmt.Errorf("checking grandpa ghost ancestry: %w", err)
			}

			if !isDescendant {
				panic("block with supermajority does not belong to the latest finalized block chain")
			}

			f.actionCh <- determinePrecommit
			precommited = true
		}
	}

	return nil
}

func (f *finalizationEngine) finalizeRound() error {
	gossipInterval := f.grandpaService.interval
	attemptFinalizationTicker := time.NewTicker(gossipInterval / 2)
	defer attemptFinalizationTicker.Stop()

	for {
		completable, err := f.grandpaService.checkRoundCompletable()
		if err != nil {
			return fmt.Errorf("checking round is completable: %w", err)
		}

		if completable {
			f.actionCh <- alreadyFinalized
			return nil
		}

		finalizable, err := f.grandpaService.attemptToFinalize()
		if err != nil {
			return fmt.Errorf("attempting to finalize: %w", err)
		}

		if finalizable {
			f.actionCh <- finalize
			return nil
		}

		select {
		case <-f.stopCh:
			return nil
		case <-attemptFinalizationTicker.C:
		}
	}
}
