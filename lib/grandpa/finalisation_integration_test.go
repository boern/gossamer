// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

//go:build integration

package grandpa

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_finalisationHandler_Stop_ShouldHalt_Services(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		wantErr    error
		errString  string
		newHandler func(*gomock.Controller) *finalisationHandler
	}{
		"halt_ephemeral_services_after_calling_stop": {
			// when we start the finalisation handler we instantiate
			// and call the Run method from each ephemeral services
			// (votingHandler, finalisationEngine) since they are mocked
			// they will wait until the Stop method being called to release
			// the blocking channel and return from the function
			newHandler: func(ctrl *gomock.Controller) *finalisationHandler {
				builder := func() (engine ephemeralService, voting ephemeralService) {
					engineStopCh := make(chan struct{})
					mockEngine := NewMockephemeralService(ctrl)

					mockEngine.EXPECT().Run().DoAndReturn(func() error {
						<-engineStopCh
						return nil
					}).MaxTimes(1)
					mockEngine.EXPECT().Stop().DoAndReturn(func() error {
						close(engineStopCh)
						return nil
					})

					votingStopCh := make(chan struct{})
					mockVoting := NewMockephemeralService(ctrl)
					mockVoting.EXPECT().Run().DoAndReturn(func() error {
						<-votingStopCh
						return nil
					}).MaxTimes(1)
					mockVoting.EXPECT().Stop().DoAndReturn(func() error {
						close(votingStopCh)
						return nil
					})
					return mockEngine, mockVoting
				}

				return &finalisationHandler{
					newServices: builder,
					// mocked initiate round function
					initiateRound: func() error { return nil },
					stopCh:        make(chan struct{}),
					handlerDone:   make(chan struct{}),
					firstRun:      true,
				}
			},
		},
		"halt_fails_to_stop_one_ephemeral_service": {
			wantErr:   errServicesStopFailed,
			errString: "services stop failed: cannot stop finalisation engine test",
			newHandler: func(ctrl *gomock.Controller) *finalisationHandler {
				builder := func() (engine ephemeralService, voting ephemeralService) {
					engineStopCh := make(chan struct{})
					mockEngine := NewMockephemeralService(ctrl)

					mockEngine.EXPECT().Run().DoAndReturn(func() error {
						<-engineStopCh
						return nil
					}).MaxTimes(1)
					mockEngine.EXPECT().Stop().DoAndReturn(func() error {
						close(engineStopCh)
						return errors.New("cannot stop finalisation engine test")
					})

					votingStopCh := make(chan struct{})
					mockVoting := NewMockephemeralService(ctrl)
					mockVoting.EXPECT().Run().DoAndReturn(func() error {
						<-votingStopCh
						return nil
					}).MaxTimes(1)
					mockVoting.EXPECT().Stop().DoAndReturn(func() error {
						close(votingStopCh)
						return nil
					})

					return mockEngine, mockVoting
				}

				return &finalisationHandler{
					newServices: builder,
					// mocked initiate round function
					initiateRound: func() error { return nil },
					stopCh:        make(chan struct{}),
					handlerDone:   make(chan struct{}),
					firstRun:      true,
				}
			},
		},
		"halt_fails_to_stop_both_ephemeral_service": {
			wantErr: errServicesStopFailed,
			errString: "services stop failed: cannot stop finalisation engine test; " +
				"cannot stop voting handler test",
			newHandler: func(ctrl *gomock.Controller) *finalisationHandler {
				builder := func() (engine ephemeralService, voting ephemeralService) {
					engineStopCh := make(chan struct{})
					mockEngine := NewMockephemeralService(ctrl)

					mockEngine.EXPECT().Run().DoAndReturn(func() error {
						<-engineStopCh
						return nil
					}).MaxTimes(1)
					mockEngine.EXPECT().Stop().DoAndReturn(func() error {
						close(engineStopCh)
						return errors.New("cannot stop finalisation engine test")
					})

					votingStopCh := make(chan struct{})
					mockVoting := NewMockephemeralService(ctrl)
					mockVoting.EXPECT().Run().DoAndReturn(func() error {
						<-votingStopCh
						return nil
					}).MaxTimes(1)
					mockVoting.EXPECT().Stop().DoAndReturn(func() error {
						close(votingStopCh)
						return errors.New("cannot stop voting handler test")
					})

					return mockEngine, mockVoting
				}

				return &finalisationHandler{
					newServices: builder,
					// mocked initiate round function
					initiateRound: func() error { return nil },
					stopCh:        make(chan struct{}),
					handlerDone:   make(chan struct{}),
					firstRun:      true,
				}
			},
		},
	}

	for tname, tt := range testcases {
		tt := tt
		t.Run(tname, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			handler := tt.newHandler(ctrl)

			errorCh, err := handler.Start()
			require.NoError(t, err)

			err = handler.Stop()
			require.ErrorIs(t, err, tt.wantErr)
			if tt.errString != "" {
				require.EqualError(t, err, tt.errString)
			}

			// since we are stopping the finalisation handler we expect
			// the errorCh to be closed without any error
			err, ok := <-errorCh
			require.Falsef(t, ok,
				"expected channel to be closed, got an unexpected error: %s", err)
		})
	}
}
