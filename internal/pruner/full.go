// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ChainSafe/chaindb"
	"github.com/ChainSafe/gossamer/internal/log"
	"github.com/ChainSafe/gossamer/lib/common"
	"github.com/ChainSafe/gossamer/pkg/scale"
)

// FullNode stores state trie diff and allows online state trie pruning
type FullNode struct {
	logger                          log.LeveledLogger
	deathList                       []deathRow
	storageDB                       chaindb.Database
	journalDB                       chaindb.Database
	deletedMerkleValueToBlockNumber map[string]uint32
	// pendingNumber is the block number to be pruned.
	// Initial value is set to 1 and is incremented after every block pruning.
	pendingNumber uint32
	retainBlocks  uint32
	sync.RWMutex
}

// NewFullNode creates a Pruner for full node.
func NewFullNode(journalDB, storageDB chaindb.Database, retainBlocks uint32, l log.LeveledLogger) (*FullNode, error) {
	p := &FullNode{
		deathList:                       make([]deathRow, 0),
		deletedMerkleValueToBlockNumber: make(map[string]uint32),
		storageDB:                       storageDB,
		journalDB:                       journalDB,
		retainBlocks:                    retainBlocks,
		logger:                          l,
	}

	blockNum, err := p.getLastPrunedIndex()
	if err != nil {
		return nil, err
	}

	p.logger.Debugf("last pruned block is %d", blockNum)
	blockNum++

	p.pendingNumber = blockNum

	err = p.loadDeathList()
	if err != nil {
		return nil, err
	}

	go p.start()

	return p, nil
}

// StoreJournalRecord stores journal record into DB and add deathRow into deathList
func (p *FullNode) StoreJournalRecord(deletedMerkleValues, insertedMerkleValues map[string]struct{},
	blockHash common.Hash, blockNum uint32) error {
	jr := newJournalRecord(blockHash, insertedMerkleValues, deletedMerkleValues)

	key := &journalKey{blockNum, blockHash}
	err := p.storeJournal(key, jr)
	if err != nil {
		return fmt.Errorf("failed to store journal record for %d: %w", blockNum, err)
	}

	p.logger.Debugf("journal record stored for block number %d", blockNum)
	p.addDeathRow(jr, blockNum)
	return nil
}

func (p *FullNode) addDeathRow(jr *journalRecord, blockNum uint32) {
	if blockNum == 0 {
		return
	}

	p.Lock()
	defer p.Unlock()

	// The block is already pruned.
	if blockNum < p.pendingNumber {
		return
	}

	p.processInsertedKeys(jr.insertedMerkleValues, jr.blockHash)

	// add deleted keys from journal to death index
	deletedMerkleValueToBlockNumber := make(map[string]uint32, len(jr.deletedMerkleValues))
	for k := range jr.deletedMerkleValues {
		p.deletedMerkleValueToBlockNumber[k] = blockNum
		deletedMerkleValueToBlockNumber[k] = blockNum
	}

	blockIndex := blockNum - p.pendingNumber
	idx := blockIndex - uint32(len(p.deathList))
	for {
		p.deathList = append(p.deathList, deathRow{})
		if idx == 0 {
			break
		}
		idx--
	}

	record := &deathRecord{
		blockHash:                       jr.blockHash,
		deletedMerkleValueToBlockNumber: deletedMerkleValueToBlockNumber,
	}

	// add deathRow to deathList
	p.deathList[blockIndex] = append(p.deathList[blockIndex], record)
}

// Remove re-inserted keys
func (p *FullNode) processInsertedKeys(insertedMerkleValues map[string]struct{}, blockHash common.Hash) {
	for k := range insertedMerkleValues {
		num, ok := p.deletedMerkleValueToBlockNumber[k]
		if !ok {
			continue
		}
		records := p.deathList[num-p.pendingNumber]
		for _, v := range records {
			if v.blockHash == blockHash {
				delete(v.deletedMerkleValueToBlockNumber, k)
			}
		}
		delete(p.deletedMerkleValueToBlockNumber, k)
	}
}

func (p *FullNode) start() {
	p.logger.Debug("pruning started")

	var canPrune bool
	checkPruning := func() {
		p.Lock()
		defer p.Unlock()
		if uint32(len(p.deathList)) <= p.retainBlocks {
			canPrune = false
			return
		}
		canPrune = true

		// pop first element from death list
		row := p.deathList[0]
		blockNum := p.pendingNumber

		p.logger.Debugf("pruning block number %d", blockNum)

		sdbBatch := p.storageDB.NewBatch()
		for _, record := range row {
			err := p.deleteKeys(sdbBatch, record.deletedMerkleValueToBlockNumber)
			if err != nil {
				p.logger.Warnf("failed to prune keys for block number %d: %s", blockNum, err)
				sdbBatch.Reset()
				return
			}

			for k := range record.deletedMerkleValueToBlockNumber {
				delete(p.deletedMerkleValueToBlockNumber, k)
			}
		}

		if err := sdbBatch.Flush(); err != nil {
			p.logger.Warnf("failed to prune keys for block number %d: %s", blockNum, err)
			return
		}

		err := p.storeLastPrunedIndex(blockNum)
		if err != nil {
			p.logger.Warnf("failed to store last pruned index for block number %d: %s", blockNum, err)
			return
		}

		p.deathList = p.deathList[1:]
		p.pendingNumber++

		jdbBatch := p.journalDB.NewBatch()
		for _, record := range row {
			jk := &journalKey{blockNum, record.blockHash}
			err = p.deleteJournalRecord(jdbBatch, jk)
			if err != nil {
				p.logger.Warnf("failed to delete journal record for block number %d: %s", blockNum, err)
				jdbBatch.Reset()
				return
			}
		}

		if err = jdbBatch.Flush(); err != nil {
			p.logger.Warnf("failed to flush delete journal record for block number %d: %s", blockNum, err)
			return
		}
		p.logger.Debugf("pruned block number %d", blockNum)
	}

	for {
		checkPruning()
		// Don't sleep if we have data to prune.
		if !canPrune {
			time.Sleep(pruneInterval)
		}
	}
}

func (p *FullNode) storeJournal(key *journalKey, jr *journalRecord) error {
	encKey, err := scale.Marshal(*key)
	if err != nil {
		return fmt.Errorf("failed to encode journal key block num %d: %w", key.blockNum, err)
	}

	encRecord, err := scale.Marshal(*jr)
	if err != nil {
		return fmt.Errorf("failed to encode journal record block num %d: %w", key.blockNum, err)
	}

	err = p.journalDB.Put(encKey, encRecord)
	if err != nil {
		return err
	}

	return nil
}

// loadDeathList loads deathList and deletedMerkleValueToBlockNumber from journalRecord.
func (p *FullNode) loadDeathList() error {
	itr := p.journalDB.NewIterator()
	defer itr.Release()

	for itr.Next() {
		key := &journalKey{}
		err := scale.Unmarshal(itr.Key(), key)
		if err != nil {
			return fmt.Errorf("failed to decode journal key %w", err)
		}

		val := itr.Value()

		jr := &journalRecord{}
		err = scale.Unmarshal(val, jr)
		if err != nil {
			return fmt.Errorf("failed to decode journal record block num %d : %w", key.blockNum, err)
		}

		p.addDeathRow(jr, key.blockNum)
	}
	return nil
}

func (*FullNode) deleteJournalRecord(b chaindb.Batch, key *journalKey) error {
	encKey, err := scale.Marshal(*key)
	if err != nil {
		return err
	}

	err = b.Del(encKey)
	if err != nil {
		return err
	}

	return nil
}

func (p *FullNode) storeLastPrunedIndex(blockNum uint32) error {
	encNum, err := scale.Marshal(blockNum)
	if err != nil {
		return err
	}

	err = p.journalDB.Put([]byte(lastPrunedKey), encNum)
	if err != nil {
		return err
	}

	return nil
}

func (p *FullNode) getLastPrunedIndex() (blockNumber uint32, err error) {
	val, err := p.journalDB.Get([]byte(lastPrunedKey))
	if errors.Is(err, chaindb.ErrKeyNotFound) {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	err = scale.Unmarshal(val, &blockNumber)
	if err != nil {
		return 0, err
	}

	return blockNumber, nil
}

func (*FullNode) deleteKeys(b chaindb.Batch,
	deletedMerkleValueToBlockNumber map[string]uint32) error {
	for merkleValue := range deletedMerkleValueToBlockNumber {
		err := b.Del([]byte(merkleValue))
		if err != nil {
			return err
		}
	}

	return nil
}
