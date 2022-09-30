// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ChainSafe/chaindb"
	"github.com/ChainSafe/gossamer/lib/common"
	"github.com/ChainSafe/gossamer/pkg/scale"
)

// FullNode prunes unneeded database keys for blocks older than the current
// block minus the number of blocks to retain specified.
// It keeps track through a journal database of the trie changes for every block
// in order to determine what can be pruned and what should be kept.
type FullNode struct {
	// Configuration
	retainBlocks uint32

	// Dependency injected
	logger    Logger
	storageDB ChainDBNewBatcher
	journalDB JournalDB

	// Internal state
	deathList                       [][]deathRecord
	deletedMerkleValueToBlockNumber map[string]uint32
	nextBlockNumberToPrune          uint32
	// mutex protects the in memory data members since StoreJournalRecord
	// is called in lib/babe `epochHandler`'s `run` method which is run
	// in its own goroutine.
	mutex sync.RWMutex
}

type deathRecord struct {
	blockHash                       common.Hash
	deletedMerkleValueToBlockNumber map[string]uint32
}

type journalKey struct {
	blockNumber uint32
	blockHash   common.Hash
}

type journalRecord struct {
	// blockHash is the hash of the block for which the inserted
	// and deleted Merkle values sets are corresponding to.
	blockHash common.Hash
	// insertedMerkleValues is the set of Merkle values of the trie nodes
	// inserted in the trie for the block.
	insertedMerkleValues map[string]struct{}
	// deletedMerkleValues is the set of Merkle values of the trie nodes
	// removed from the trie for the block.
	deletedMerkleValues map[string]struct{}
}

// NewFullNode creates a Pruner for full node.
func NewFullNode(journalDB JournalDB, storageDB ChainDBNewBatcher, retainBlocks uint32,
	logger Logger) (pruner *FullNode, err error) {
	lastPrunedBlockNumber, err := getLastPrunedBlockNumber(journalDB)
	if err != nil {
		return nil, fmt.Errorf("getting last pruned block number: %w", err)
	}
	logger.Debugf("last pruned block number: %d", lastPrunedBlockNumber)

	pruner = &FullNode{
		deletedMerkleValueToBlockNumber: make(map[string]uint32),
		storageDB:                       storageDB,
		journalDB:                       journalDB,
		retainBlocks:                    retainBlocks,
		nextBlockNumberToPrune:          1 + lastPrunedBlockNumber,
		logger:                          logger,
	}

	err = pruner.loadDeathList()
	if err != nil {
		return nil, fmt.Errorf("loading death list: %w", err)
	}

	return pruner, nil
}

// StoreJournalRecord stores journal record into DB and add deathRow into deathList
func (p *FullNode) StoreJournalRecord(deletedMerkleValues, insertedMerkleValues map[string]struct{},
	blockHash common.Hash, blockNumber uint32) (err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	blockIsAlreadyPruned := blockNumber < p.nextBlockNumberToPrune
	if blockIsAlreadyPruned {
		panic(fmt.Sprintf("block number %d is already pruned, last block number pruned was %d",
			blockNumber, p.nextBlockNumberToPrune))
	}

	journalDBBatch := p.journalDB.NewBatch()

	for uint32(len(p.deathList)) > p.retainBlocks {
		blockNumberToPrune := p.nextBlockNumberToPrune
		err := p.prune(journalDBBatch)
		if err != nil {
			journalDBBatch.Reset()
			return fmt.Errorf("pruning block number %d: %w", blockNumberToPrune, err)
		}
		p.logger.Debugf("pruned block number %d", blockNumberToPrune)
	}

	key := journalKey{
		blockNumber: blockNumber,
		blockHash:   blockHash,
	}
	record := journalRecord{
		blockHash:            blockHash,
		insertedMerkleValues: insertedMerkleValues,
		deletedMerkleValues:  deletedMerkleValues,
	}
	err = p.storeJournal(journalDBBatch, key, record)
	if err != nil {
		journalDBBatch.Reset()
		return fmt.Errorf("storing journal record for block number %d: %w", blockNumber, err)
	}

	err = journalDBBatch.Flush()
	if err != nil {
		return fmt.Errorf("flushing journal database batch: %w", err)
	}

	p.logger.Debugf("journal record stored for block number %d", blockNumber)
	p.addDeathRow(blockNumber, record)
	return nil
}

func (p *FullNode) addDeathRow(blockNumber uint32, journalRecord journalRecord) {
	p.processInsertedKeys(journalRecord.insertedMerkleValues, journalRecord.blockHash)

	deletedMerkleValueToBlockNumber := make(map[string]uint32, len(journalRecord.deletedMerkleValues))
	for k := range journalRecord.deletedMerkleValues {
		p.deletedMerkleValueToBlockNumber[k] = blockNumber
		deletedMerkleValueToBlockNumber[k] = blockNumber
	}

	blockIndex := blockNumber - p.nextBlockNumberToPrune
	elementsToAdd := blockIndex - uint32(len(p.deathList))
	extraDeathList := make([][]deathRecord, elementsToAdd)
	p.deathList = append(p.deathList, extraDeathList...)

	record := deathRecord{
		blockHash:                       journalRecord.blockHash,
		deletedMerkleValueToBlockNumber: deletedMerkleValueToBlockNumber,
	}

	p.deathList[blockIndex] = append(p.deathList[blockIndex], record)
}

// Remove re-inserted keys
func (p *FullNode) processInsertedKeys(insertedMerkleValues map[string]struct{}, blockHash common.Hash) {
	for insertedKey := range insertedMerkleValues {
		blockNumber, ok := p.deletedMerkleValueToBlockNumber[insertedKey]
		if !ok {
			continue
		}

		deathListIndex := blockNumber - p.nextBlockNumberToPrune
		deathRow := p.deathList[deathListIndex]
		for _, record := range deathRow {
			if record.blockHash.Equal(blockHash) {
				delete(record.deletedMerkleValueToBlockNumber, insertedKey)
			}
		}
		delete(p.deletedMerkleValueToBlockNumber, insertedKey)
	}
}

func (p *FullNode) prune(journalDBBatch PutDeleter) (err error) {
	row := p.deathList[0]

	storageBatch := p.storageDB.NewBatch()
	err = pruneStorage(row, storageBatch)
	if err != nil {
		storageBatch.Reset()
		return fmt.Errorf("pruning storage: %w", err)
	}

	err = pruneJournal(row, p.nextBlockNumberToPrune, journalDBBatch)
	if err != nil {
		storageBatch.Reset()
		return fmt.Errorf("pruning journal: %w", err)
	}

	err = storeLastPrunedBlockNumber(journalDBBatch, p.nextBlockNumberToPrune)
	if err != nil {
		storageBatch.Reset()
		return fmt.Errorf("storing last pruned block number: %w", err)
	}

	err = storageBatch.Flush()
	if err != nil {
		return fmt.Errorf("flushing storage database batch: %w", err)
	}

	// Update in memory state
	p.deathList = p.deathList[1:]
	p.nextBlockNumberToPrune++

	return nil
}

func pruneStorage(row []deathRecord, batch Deleter) (err error) {
	for _, record := range row {
		for deletedMerkleValue := range record.deletedMerkleValueToBlockNumber {
			err = batch.Del([]byte(deletedMerkleValue))
			if err != nil {
				return fmt.Errorf("deleting database key: %w", err)
			}
		}
	}
	return nil
}

func pruneJournal(row []deathRecord, blockNumber uint32, batch Deleter) (err error) {
	for _, record := range row {
		key := journalKey{
			blockNumber: blockNumber,
			blockHash:   record.blockHash,
		}

		encodedKey, err := scale.Marshal(key)
		if err != nil {
			return fmt.Errorf("scale encoding journal key: %w", err)
		}

		err = batch.Del(encodedKey)
		if err != nil {
			return fmt.Errorf("deleting key from batch: %w", err)
		}
	}
	return nil
}

func (p *FullNode) storeJournal(batch Putter, key journalKey, record journalRecord) (err error) {
	scaleEncodedKey, err := scale.Marshal(key)
	if err != nil {
		return fmt.Errorf("scale encoding journal key: %w", err)
	}

	scaleEncodedRecord, err := scale.Marshal(record)
	if err != nil {
		return fmt.Errorf("scale encoding journal record: %w", err)
	}

	err = batch.Put(scaleEncodedKey, scaleEncodedRecord)
	if err != nil {
		return fmt.Errorf("inserting record in journal database: %w", err)
	}

	return nil
}

// loadDeathList loads deathList and deletedMerkleValueToBlockNumber from journalRecord.
func (p *FullNode) loadDeathList() (err error) {
	dbIterator := p.journalDB.NewIterator()
	defer dbIterator.Release()

	for dbIterator.Next() {
		scaleEncodedKey := dbIterator.Key()
		var key journalKey
		err := scale.Unmarshal(scaleEncodedKey, &key)
		if err != nil {
			return fmt.Errorf("scale decoding journal key: %w", err)
		}

		scaleEncodedRecord := dbIterator.Value()

		var record journalRecord
		err = scale.Unmarshal(scaleEncodedRecord, &record)
		if err != nil {
			return fmt.Errorf("scale decoding journal record for key %#v: %w", key, err)
		}

		blockIsAlreadyPruned := key.blockNumber < p.nextBlockNumberToPrune
		if blockIsAlreadyPruned {
			// TODO delete the record from database
			continue
		}

		p.addDeathRow(key.blockNumber, record)
	}

	return nil
}

const (
	lastPrunedKey = "last_pruned"
)

func storeLastPrunedBlockNumber(batch Putter, blockNumber uint32) error {
	encodedBlockNumber, err := scale.Marshal(blockNumber)
	if err != nil {
		return fmt.Errorf("encoding block number: %w", err)
	}

	err = batch.Put([]byte(lastPrunedKey), encodedBlockNumber)
	if err != nil {
		return fmt.Errorf("storing last pruned block number: %w", err)
	}

	return nil
}

func getLastPrunedBlockNumber(journalDB Getter) (blockNumber uint32, err error) {
	encodedBlockNumber, err := journalDB.Get([]byte(lastPrunedKey))
	if err != nil {
		if errors.Is(err, chaindb.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("getting last pruned from database: %w", err)
	}

	err = scale.Unmarshal(encodedBlockNumber, &blockNumber)
	if err != nil {
		return 0, fmt.Errorf("decoding block number: %w", err)
	}

	return blockNumber, nil
}
