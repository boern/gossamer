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

const (
	lastPrunedKey               = "last_pruned"
	highestBlockNumberKey       = "highest_block_number"
	deletedMerkleValueKeyPrefix = "deleted_"
	blockNumberToHashPrefix     = "block_number_to_hash_"
)

// FullNode prunes unneeded database keys for blocks older than the current
// block minus the number of blocks to retain specified.
// It keeps track through a journal database of the trie changes for every block
// in order to determine what can be pruned and what should be kept.
type FullNode struct {
	// Configuration
	retainBlocks uint32

	// Dependency injected
	logger          Logger
	storageDatabase ChainDBNewBatcher
	journalDatabase JournalDatabase
	blockState      BlockState

	// Internal state
	// nextBlockNumberToPrune is the next block number to prune.
	// It is updated on disk but cached in memory as this field.
	nextBlockNumberToPrune uint32
	// highestBlockNumber is the highest block number stored in the journal.
	// It is updated on disk but cached in memory as this field.
	highestBlockNumber uint32
	// mutex protects the in memory data members since StoreJournalRecord
	// is called in lib/babe `epochHandler`'s `run` method which is run
	// in its own goroutine.
	mutex sync.RWMutex
}

type journalKey struct {
	BlockNumber uint32
	BlockHash   common.Hash
}

type journalRecord struct {
	// InsertedMerkleValues is the set of Merkle values of the trie nodes
	// inserted in the trie for the block.
	InsertedMerkleValues map[string]struct{}
	// DeletedMerkleValues is the set of Merkle values of the trie nodes
	// removed from the trie for the block.
	DeletedMerkleValues map[string]struct{}
}

// NewFullNode creates a full node pruner.
func NewFullNode(journalDB JournalDatabase, storageDB ChainDBNewBatcher, retainBlocks uint32,
	blockState BlockState, logger Logger) (pruner *FullNode, err error) {
	highestBlockNumber, err := getBlockNumberFromKey(journalDB, []byte(highestBlockNumberKey))
	if err != nil {
		return nil, fmt.Errorf("getting highest block number: %w", err)
	}
	logger.Debugf("highest block number stored in journal: %d", highestBlockNumber)

	lastPrunedBlockNumber, err := getBlockNumberFromKey(journalDB, []byte(lastPrunedKey))
	if err != nil {
		return nil, fmt.Errorf("getting last pruned block number: %w", err)
	}
	logger.Debugf("last pruned block number: %d", lastPrunedBlockNumber)
	nextBlockNumberToPrune := lastPrunedBlockNumber + 1

	pruner = &FullNode{
		storageDatabase:        storageDB,
		journalDatabase:        journalDB,
		blockState:             blockState,
		retainBlocks:           retainBlocks,
		nextBlockNumberToPrune: nextBlockNumberToPrune,
		highestBlockNumber:     highestBlockNumber,
		logger:                 logger,
	}

	// Prune all block numbers necessary, if for example the
	// user lowers the retainBlocks parameter.
	journalDBBatch := journalDB.NewBatch()
	err = pruner.pruneAll(journalDBBatch)
	if err != nil {
		journalDBBatch.Reset()
		return nil, fmt.Errorf("pruning: %w", err)
	}
	err = journalDBBatch.Flush()
	if err != nil {
		return nil, fmt.Errorf("flushing journal database batch: %w", err)
	}

	return pruner, nil
}

// StoreJournalRecord stores the trie deltas impacting the storage database for a particular
// block hash. It prunes all block numbers falling off the window of block numbers to keep,
// before inserting the new record. It is thread safe to call.
func (p *FullNode) StoreJournalRecord(deletedMerkleValues, insertedMerkleValues map[string]struct{},
	blockHash common.Hash, blockNumber uint32) (err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	blockIsAlreadyPruned := blockNumber < p.nextBlockNumberToPrune
	if blockIsAlreadyPruned {
		panic(fmt.Sprintf("block number %d is already pruned, last block number pruned was %d",
			blockNumber, p.nextBlockNumberToPrune))
	}

	// Delist re-inserted keys from being pruned.
	// WARNING: this must be before the pruning to avoid
	// pruning still needed database keys.
	journalDBBatch := p.journalDatabase.NewBatch()
	err = p.handleInsertedKeys(insertedMerkleValues, blockNumber,
		blockHash, journalDBBatch)
	if err != nil {
		journalDBBatch.Reset()
		return fmt.Errorf("handling inserted keys: %w", err)
	}

	err = journalDBBatch.Flush()
	if err != nil {
		return fmt.Errorf("flushing re-inserted keys updates to journal database: %w", err)
	}

	journalDBBatch = p.journalDatabase.NewBatch()

	// Update highest block number only in memory so `pruneAll` can use it,
	// prune and flush the deletions in the journal and storage databases.
	if blockNumber > p.highestBlockNumber {
		p.highestBlockNumber = blockNumber
	}

	// Prune before inserting a new journal record
	err = p.pruneAll(journalDBBatch)
	if err != nil {
		journalDBBatch.Reset()
		return fmt.Errorf("pruning database: %w", err)
	}

	if blockNumber > p.highestBlockNumber {
		err = storeBlockNumberAtKey(journalDBBatch, []byte(highestBlockNumberKey), blockNumber)
		if err != nil {
			journalDBBatch.Reset()
			return fmt.Errorf("storing highest block number in journal database: %w", err)
		}
	}

	// Note we store block number <-> block hashes in the database
	// so we can pick up the block hashes after a program restart
	// using the stored last pruned block number and stored highest
	// block number encountered.
	err = appendBlockHash(blockNumber, blockHash, p.journalDatabase, journalDBBatch)
	if err != nil {
		journalDBBatch.Reset()
		return fmt.Errorf("recording block hash in journal database: %w", err)
	}

	record := journalRecord{
		InsertedMerkleValues: insertedMerkleValues,
		DeletedMerkleValues:  deletedMerkleValues,
	}
	err = storeJournalRecord(journalDBBatch, blockNumber, blockHash, record)
	if err != nil {
		journalDBBatch.Reset()
		return fmt.Errorf("storing journal record for block number %d: %w", blockNumber, err)
	}

	err = journalDBBatch.Flush()
	if err != nil {
		return fmt.Errorf("flushing journal database batch: %w", err)
	}

	p.logger.Debugf("journal record stored for block number %d", blockNumber)
	return nil
}

func (p *FullNode) handleInsertedKeys(insertedMerkleValues map[string]struct{},
	blockNumber uint32, blockHash common.Hash, journalDBBatch Putter) (err error) {
	for insertedMerkleValue := range insertedMerkleValues {
		err = p.handleInsertedKey(insertedMerkleValue, blockNumber, blockHash, journalDBBatch)
		if err != nil {
			return fmt.Errorf("handling inserted key 0x%x: %w",
				[]byte(insertedMerkleValue), err)
		}
	}

	return nil
}

func (p *FullNode) handleInsertedKey(insertedMerkleValue string, blockNumber uint32,
	blockHash common.Hash, journalDBBatch Putter) (err error) {
	// Try to find if Merkle value was deleted in another block before
	// since we no longer want to prune it, as it was re-inserted.
	journalKeyDeletedAt, err := p.journalDatabase.Get([]byte(deletedMerkleValueKeyPrefix + insertedMerkleValue))
	merkleValueDeletedInAnotherBlock := errors.Is(err, chaindb.ErrKeyNotFound)
	if !merkleValueDeletedInAnotherBlock {
		return nil
	} else if err != nil {
		return fmt.Errorf("getting journal key for Merkle value from journal database: %w", err)
	}

	var key journalKey
	err = scale.Unmarshal(journalKeyDeletedAt, &key)
	if err != nil {
		return fmt.Errorf("decoding journal key: %w", err)
	}

	deletedInUncleBlock := key.BlockNumber >= blockNumber
	if deletedInUncleBlock {
		return nil
	}

	isDescendant, err := p.blockState.IsDescendantOf(key.BlockHash, blockHash)
	if err != nil {
		return fmt.Errorf("checking if block %s is descendant of block %s: %w",
			key.BlockHash, blockHash, err)
	}
	deletedInUncleBlock = !isDescendant
	if deletedInUncleBlock {
		return nil
	}

	// Remove Merkle value from the deleted set of the block it was deleted in.
	encodedJournalRecord, err := p.journalDatabase.Get(journalKeyDeletedAt)
	if err != nil {
		return fmt.Errorf("getting record from journal database: %w", err)
	}

	var record journalRecord
	err = scale.Unmarshal(encodedJournalRecord, &record)
	if err != nil {
		return fmt.Errorf("decoding journal record: %w", err)
	}

	delete(record.DeletedMerkleValues, insertedMerkleValue)

	encodedJournalRecord, err = scale.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding updated journal record: %w", err)
	}

	err = journalDBBatch.Put(journalKeyDeletedAt, encodedJournalRecord)
	if err != nil {
		return fmt.Errorf("putting updated journal record in journal database batch: %w", err)
	}

	return nil
}

func (p *FullNode) pruneAll(journalDBBatch PutDeleter) (err error) {
	if p.highestBlockNumber-p.nextBlockNumberToPrune <= p.retainBlocks {
		return nil
	}

	storageBatch := p.storageDatabase.NewBatch()
	blockNumberToPrune := p.nextBlockNumberToPrune
	for p.highestBlockNumber-blockNumberToPrune > p.retainBlocks {
		err := prune(blockNumberToPrune, p.journalDatabase, journalDBBatch, storageBatch)
		if err != nil {
			storageBatch.Reset()
			return fmt.Errorf("pruning block number %d: %w", blockNumberToPrune, err)
		}
		blockNumberToPrune++
	}

	lastBlockNumberPruned := blockNumberToPrune - 1

	err = storeBlockNumberAtKey(journalDBBatch, []byte(lastPrunedKey), lastBlockNumberPruned)
	if err != nil {
		storageBatch.Reset()
		return fmt.Errorf("writing last pruned block number to journal database batch: %w", err)
	}

	err = storageBatch.Flush()
	if err != nil {
		return fmt.Errorf("flushing storage database batch: %w", err)
	}

	p.logger.Debugf("pruned block numbers [%d..%d]", p.nextBlockNumberToPrune, lastBlockNumberPruned)
	p.nextBlockNumberToPrune = blockNumberToPrune

	return nil
}

func prune(blockNumberToPrune uint32, journalDB Getter, journalDBBatch Deleter,
	storageBatch Deleter) (err error) {
	blockHashes, err := loadBlockHashes(blockNumberToPrune, journalDB)
	if err != nil {
		return fmt.Errorf("loading block hashes for block number to prune: %w", err)
	}

	err = pruneStorage(blockNumberToPrune, blockHashes,
		journalDB, storageBatch)
	if err != nil {
		return fmt.Errorf("pruning storage: %w", err)
	}

	err = pruneJournal(blockNumberToPrune, blockHashes, journalDBBatch)
	if err != nil {
		return fmt.Errorf("pruning journal: %w", err)
	}

	return nil
}

func pruneStorage(blockNumber uint32, blockHashes []common.Hash,
	journalDB Getter, batch Deleter) (err error) {
	for _, blockHash := range blockHashes {
		record, err := getJournalRecord(journalDB, blockNumber, blockHash)
		if err != nil {
			return fmt.Errorf("getting journal record: %w", err)
		}

		for deletedMerkleValue := range record.DeletedMerkleValues {
			err = batch.Del([]byte(deletedMerkleValue))
			if err != nil {
				return fmt.Errorf("deleting key from batch: %w", err)
			}
		}
	}
	return nil
}

func pruneJournal(blockNumber uint32, blockHashes []common.Hash,
	batch Deleter) (err error) {
	err = pruneBlockHashes(blockNumber, batch)
	if err != nil {
		return fmt.Errorf("pruning block hashes: %w", err)
	}

	for _, blockHash := range blockHashes {
		key := journalKey{
			BlockNumber: blockNumber,
			BlockHash:   blockHash,
		}
		encodedKey, err := scale.Marshal(key)
		if err != nil {
			return fmt.Errorf("scale encoding journal key: %w", err)
		}

		err = batch.Del(encodedKey)
		if err != nil {
			return fmt.Errorf("deleting journal key from batch: %w", err)
		}
	}
	return nil
}

func storeJournalRecord(batch Putter, blockNumber uint32, blockHash common.Hash,
	record journalRecord) (err error) {
	key := journalKey{
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
	}
	encodedKey, err := scale.Marshal(key)
	if err != nil {
		return fmt.Errorf("scale encoding journal key: %w", err)
	}

	for deletedMerkleValue := range record.DeletedMerkleValues {
		// We store the block hash + block number for each deleted node hash
		// so a node hash can quickly be checked for from the journal database
		// when running `handleInsertedKey`.
		databaseKey := []byte(deletedMerkleValueKeyPrefix + deletedMerkleValue)
		err = batch.Put(databaseKey, encodedKey)
		if err != nil {
			return fmt.Errorf("putting journal key in database batch: %w", err)
		}
	}

	encodedRecord, err := scale.Marshal(record)
	if err != nil {
		return fmt.Errorf("scale encoding journal record: %w", err)
	}

	// We store the journal record (block hash + deleted node hashes + inserted node hashes)
	// in the journal database at the key (block hash + block number)
	err = batch.Put(encodedKey, encodedRecord)
	if err != nil {
		return fmt.Errorf("putting journal record in database batch: %w", err)
	}

	return nil
}

func getJournalRecord(database Getter, blockNumber uint32,
	blockHash common.Hash) (record journalRecord, err error) {
	key := journalKey{
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
	}
	encodedKey, err := scale.Marshal(key)
	if err != nil {
		return record, fmt.Errorf("scale encoding key: %w", err)
	}

	encodedRecord, err := database.Get(encodedKey)
	if err != nil {
		return record, fmt.Errorf("getting from database: %w", err)
	}

	err = scale.Unmarshal(encodedRecord, &record)
	if err != nil {
		return record, fmt.Errorf("scale decoding journal record: %w", err)
	}

	return record, nil
}

func storeBlockNumberAtKey(batch Putter, key []byte, blockNumber uint32) error {
	encodedBlockNumber, err := scale.Marshal(blockNumber)
	if err != nil {
		return fmt.Errorf("encoding block number: %w", err)
	}

	err = batch.Put(key, encodedBlockNumber)
	if err != nil {
		return fmt.Errorf("putting block number %d: %w", blockNumber, err)
	}

	return nil
}

// getBlockNumberFromKey obtains the block number from the database at the given key.
// If the key is not found, the block number `0` is returned without error.
func getBlockNumberFromKey(database Getter, key []byte) (blockNumber uint32, err error) {
	encodedBlockNumber, err := database.Get(key)
	if err != nil {
		if errors.Is(err, chaindb.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("getting block number from database: %w", err)
	}

	err = scale.Unmarshal(encodedBlockNumber, &blockNumber)
	if err != nil {
		return 0, fmt.Errorf("decoding block number: %w", err)
	}

	return blockNumber, nil
}

func loadBlockHashes(blockNumber uint32, journalDB Getter) (blockHashes []common.Hash, err error) {
	keyString := blockNumberToHashPrefix + fmt.Sprint(blockNumber)
	key := []byte(keyString)
	encodedBlockHashes, err := journalDB.Get(key)
	if err != nil {
		return nil, fmt.Errorf("getting block hashes for block number %d: %w", blockNumber, err)
	}

	// Note the reason we don't use scale is to append a hash to existing hashes without
	// having to scale decode and scale encode.
	numberOfBlockHashes := len(encodedBlockHashes) / common.HashLength
	blockHashes = make([]common.Hash, numberOfBlockHashes)
	for i := 0; i < numberOfBlockHashes; i++ {
		startIndex := i * common.HashLength
		endIndex := startIndex + common.HashLength
		blockHashes[i] = common.NewHash(encodedBlockHashes[startIndex:endIndex])
	}

	return blockHashes, nil
}

func appendBlockHash(blockNumber uint32, blockHash common.Hash, journalDB Getter,
	batch Putter) (err error) {
	keyString := blockNumberToHashPrefix + fmt.Sprint(blockNumber)
	key := []byte(keyString)
	encodedBlockHashes, err := journalDB.Get(key)
	if err != nil && !errors.Is(err, chaindb.ErrKeyNotFound) {
		return fmt.Errorf("getting block hashes for block number %d: %w", blockNumber, err)
	}

	encodedBlockHashes = append(encodedBlockHashes, blockHash.ToBytes()...)

	err = batch.Put(key, encodedBlockHashes)
	if err != nil {
		return fmt.Errorf("putting block hashes for block number %d: %w", blockNumber, err)
	}

	return nil
}

func pruneBlockHashes(blockNumber uint32, batch Deleter) (err error) {
	keyString := blockNumberToHashPrefix + fmt.Sprint(blockNumber)
	key := []byte(keyString)
	err = batch.Del(key)
	if err != nil {
		return fmt.Errorf("deleting block hashes for block number %d from database: %w",
			blockNumber, err)
	}
	return nil
}
