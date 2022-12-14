// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/ChainSafe/gossamer/internal/database"
	"github.com/ChainSafe/gossamer/lib/common"
	"github.com/ChainSafe/gossamer/pkg/scale"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scaleMarshal(t *testing.T, x any) (b []byte) {
	t.Helper()

	b, err := scale.Marshal(x)
	require.NoError(t, err)

	return b
}

func Test_FullNode_pruneAll(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		pruner                 *FullNode
		journalDatabaseBuilder func(ctrl *gomock.Controller) JournalDatabase
		storageDatabaseBuilder func(ctrl *gomock.Controller) NewWriteBatcher
		loggerBuilder          func(ctrl *gomock.Controller) Logger
		journalBatchBuilder    func(ctrl *gomock.Controller) SetDeleter
		errWrapped             error
		errMessage             string
		expectedPruner         *FullNode
	}{
		"not enough blocks to prune": {
			pruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 1,
				highestBlockNumber:     3,
			},
			expectedPruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 1,
				highestBlockNumber:     3,
			},
		},
		"prune block error": {
			pruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 1,
				highestBlockNumber:     4,
			},
			journalDatabaseBuilder: func(ctrl *gomock.Controller) JournalDatabase {
				journalDatabase := NewMockJournalDatabase(ctrl)
				journalDatabase.EXPECT().Get([]byte("block_number_to_hash_1")).Return(nil, errTest)
				return journalDatabase
			},
			storageDatabaseBuilder: func(ctrl *gomock.Controller) NewWriteBatcher {
				storageDatabase := NewMockJournalDatabase(ctrl)
				batch := NewMockWriteBatch(ctrl)
				storageDatabase.EXPECT().NewWriteBatch().Return(batch)
				batch.EXPECT().Cancel()
				return storageDatabase
			},
			expectedPruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 1,
				highestBlockNumber:     4,
			},
			errWrapped: errTest,
			errMessage: "pruning block number 1: " +
				"loading block hashes for block number to prune: " +
				"getting block hashes for block number 1: test error",
		},
		"store last block number pruned error": {
			pruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 1,
				highestBlockNumber:     4,
			},
			journalDatabaseBuilder: func(ctrl *gomock.Controller) JournalDatabase {
				database := NewMockJournalDatabase(ctrl)
				blockHashes := common.Hash{2}.ToBytes()
				database.EXPECT().Get([]byte("block_number_to_hash_1")).Return(blockHashes, nil)

				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				record := journalRecord{
					DeletedNodeHashes: map[common.Hash]struct{}{{3}: {}},
				}
				encodedRecord := scaleMarshal(t, record)
				database.EXPECT().Get(encodedKey).Return(encodedRecord, nil)

				return database
			},
			storageDatabaseBuilder: func(ctrl *gomock.Controller) NewWriteBatcher {
				storageDatabase := NewMockJournalDatabase(ctrl)
				batch := NewMockWriteBatch(ctrl)
				storageDatabase.EXPECT().NewWriteBatch().Return(batch)
				batch.EXPECT().Delete(common.Hash{3}.ToBytes()).Return(nil)
				batch.EXPECT().Cancel()
				return storageDatabase
			},
			journalBatchBuilder: func(ctrl *gomock.Controller) SetDeleter {
				batch := NewMockSetDeleter(ctrl)

				batch.EXPECT().Delete([]byte("block_number_to_hash_1")).Return(nil)

				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				batch.EXPECT().Delete(encodedKey).Return(nil)

				encodedLastBlockNumberPruned := scaleMarshal(t, uint32(1))
				batch.EXPECT().Set([]byte("last_pruned"), encodedLastBlockNumberPruned).
					Return(errTest)
				return batch
			},
			expectedPruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 1,
				highestBlockNumber:     4,
			},
			errWrapped: errTest,
			errMessage: "writing last pruned block number to journal database batch: " +
				"putting block number 1: test error",
		},
		"storage batch flush error": {
			pruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 1,
				highestBlockNumber:     4,
			},
			journalDatabaseBuilder: func(ctrl *gomock.Controller) JournalDatabase {
				database := NewMockJournalDatabase(ctrl)
				blockHashes := common.Hash{2}.ToBytes()
				database.EXPECT().Get([]byte("block_number_to_hash_1")).Return(blockHashes, nil)

				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				record := journalRecord{
					DeletedNodeHashes: map[common.Hash]struct{}{{3}: {}},
				}
				encodedRecord := scaleMarshal(t, record)
				database.EXPECT().Get(encodedKey).Return(encodedRecord, nil)

				return database
			},
			storageDatabaseBuilder: func(ctrl *gomock.Controller) NewWriteBatcher {
				storageDatabase := NewMockJournalDatabase(ctrl)
				batch := NewMockWriteBatch(ctrl)
				storageDatabase.EXPECT().NewWriteBatch().Return(batch)
				batch.EXPECT().Delete(common.Hash{3}.ToBytes()).Return(nil)
				batch.EXPECT().Flush().Return(errTest)
				return storageDatabase
			},
			journalBatchBuilder: func(ctrl *gomock.Controller) SetDeleter {
				batch := NewMockSetDeleter(ctrl)

				batch.EXPECT().Delete([]byte("block_number_to_hash_1")).Return(nil)

				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				batch.EXPECT().Delete(encodedKey).Return(nil)

				encodedLastBlockNumberPruned := scaleMarshal(t, uint32(1))
				batch.EXPECT().Set([]byte("last_pruned"), encodedLastBlockNumberPruned).
					Return(nil)
				return batch
			},
			expectedPruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 1,
				highestBlockNumber:     4,
			},
			errWrapped: errTest,
			errMessage: "flushing storage database batch: test error",
		},
		"success": {
			pruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 1,
				highestBlockNumber:     4,
			},
			journalDatabaseBuilder: func(ctrl *gomock.Controller) JournalDatabase {
				database := NewMockJournalDatabase(ctrl)
				blockHashes := common.Hash{2}.ToBytes()
				database.EXPECT().Get([]byte("block_number_to_hash_1")).Return(blockHashes, nil)

				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				record := journalRecord{
					DeletedNodeHashes: map[common.Hash]struct{}{{3}: {}},
				}
				encodedRecord := scaleMarshal(t, record)
				database.EXPECT().Get(encodedKey).Return(encodedRecord, nil)

				return database
			},
			storageDatabaseBuilder: func(ctrl *gomock.Controller) NewWriteBatcher {
				storageDatabase := NewMockJournalDatabase(ctrl)
				batch := NewMockWriteBatch(ctrl)
				storageDatabase.EXPECT().NewWriteBatch().Return(batch)
				batch.EXPECT().Delete(common.Hash{3}.ToBytes()).Return(nil)
				batch.EXPECT().Flush().Return(nil)
				return storageDatabase
			},
			loggerBuilder: func(ctrl *gomock.Controller) Logger {
				logger := NewMockLogger(ctrl)
				logger.EXPECT().Debugf("pruned block numbers [%d..%d]", uint32(1), uint32(1))
				return logger
			},
			journalBatchBuilder: func(ctrl *gomock.Controller) SetDeleter {
				batch := NewMockSetDeleter(ctrl)

				batch.EXPECT().Delete([]byte("block_number_to_hash_1")).Return(nil)

				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				batch.EXPECT().Delete(encodedKey).Return(nil)

				encodedLastBlockNumberPruned := scaleMarshal(t, uint32(1))
				batch.EXPECT().Set([]byte("last_pruned"), encodedLastBlockNumberPruned).
					Return(nil)
				return batch
			},
			expectedPruner: &FullNode{
				retainBlocks:           2,
				nextBlockNumberToPrune: 2,
				highestBlockNumber:     4,
			},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			if testCase.journalDatabaseBuilder != nil {
				testCase.pruner.journalDatabase = testCase.journalDatabaseBuilder(ctrl)
				testCase.expectedPruner.journalDatabase = testCase.pruner.journalDatabase
			}

			if testCase.storageDatabaseBuilder != nil {
				testCase.pruner.storageDatabase = testCase.storageDatabaseBuilder(ctrl)
				testCase.expectedPruner.storageDatabase = testCase.pruner.storageDatabase
			}

			if testCase.loggerBuilder != nil {
				testCase.pruner.logger = testCase.loggerBuilder(ctrl)
				testCase.expectedPruner.logger = testCase.pruner.logger
			}

			var journalBatch SetDeleter
			if testCase.journalBatchBuilder != nil {
				journalBatch = testCase.journalBatchBuilder(ctrl)
			}

			err := testCase.pruner.pruneAll(journalBatch)

			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
			assert.Equal(t, testCase.expectedPruner, testCase.pruner)
		})
	}
}

func Test_prune(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		blockNumberToPrune  uint32
		journalDBBuilder    func(ctrl *gomock.Controller) Getter
		journalBatchBuilder func(ctrl *gomock.Controller) Deleter
		storageBatchBuilder func(ctrl *gomock.Controller) Deleter
		errWrapped          error
		errMessage          string
	}{
		"load block hashes error": {
			blockNumberToPrune: 1,
			journalDBBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				database.EXPECT().Get([]byte("block_number_to_hash_1")).Return(nil, errTest)
				return database
			},
			journalBatchBuilder: func(_ *gomock.Controller) Deleter { return nil },
			storageBatchBuilder: func(_ *gomock.Controller) Deleter { return nil },
			errWrapped:          errTest,
			errMessage: "loading block hashes for block number to prune: " +
				"getting block hashes for block number 1: test error",
		},
		"prune storage error": {
			blockNumberToPrune: 1,
			journalDBBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				blockHashes := common.Hash{2}.ToBytes()
				database.EXPECT().Get([]byte("block_number_to_hash_1")).Return(blockHashes, nil)
				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				database.EXPECT().Get(encodedKey).Return(nil, errTest)
				return database
			},
			journalBatchBuilder: func(_ *gomock.Controller) Deleter { return nil },
			storageBatchBuilder: func(_ *gomock.Controller) Deleter { return nil },
			errWrapped:          errTest,
			errMessage: "pruning storage: getting journal record: " +
				"getting from database: test error",
		},
		"prune journal error": {
			blockNumberToPrune: 1,
			journalDBBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				blockHashes := common.Hash{2}.ToBytes()
				database.EXPECT().Get([]byte("block_number_to_hash_1")).Return(blockHashes, nil)
				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				record := journalRecord{
					DeletedNodeHashes: map[common.Hash]struct{}{{3}: {}},
				}
				encodedRecord := scaleMarshal(t, record)
				database.EXPECT().Get(encodedKey).Return(encodedRecord, nil)
				return database
			},
			journalBatchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				batch.EXPECT().Delete([]byte("block_number_to_hash_1")).Return(errTest)
				return batch
			},
			storageBatchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				batch.EXPECT().Delete(common.Hash{3}.ToBytes()).Return(nil)
				return batch
			},
			errWrapped: errTest,
			errMessage: "pruning journal: pruning block hashes: " +
				"deleting block hashes for block number 1 from database: " +
				"test error",
		},
		"success": {
			blockNumberToPrune: 1,
			journalDBBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				blockHashes := common.Hash{2}.ToBytes()
				database.EXPECT().Get([]byte("block_number_to_hash_1")).Return(blockHashes, nil)
				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				record := journalRecord{
					DeletedNodeHashes: map[common.Hash]struct{}{{3}: {}},
				}
				encodedRecord := scaleMarshal(t, record)
				database.EXPECT().Get(encodedKey).Return(encodedRecord, nil)
				return database
			},
			journalBatchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				batch.EXPECT().Delete([]byte("block_number_to_hash_1")).Return(nil)
				key := journalKey{BlockNumber: 1, BlockHash: common.Hash{2}}
				encodedKey := scaleMarshal(t, key)
				batch.EXPECT().Delete(encodedKey).Return(nil)
				return batch
			},
			storageBatchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				batch.EXPECT().Delete(common.Hash{3}.ToBytes()).Return(nil)
				return batch
			},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			journalDB := testCase.journalDBBuilder(ctrl)
			journalBatch := testCase.journalBatchBuilder(ctrl)
			storageBatch := testCase.storageBatchBuilder(ctrl)
			err := prune(testCase.blockNumberToPrune, journalDB, journalBatch, storageBatch)

			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}

func Test_pruneStorage(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		blockNumber     uint32
		blockHashes     []common.Hash
		databaseBuilder func(ctrl *gomock.Controller) Getter
		batchBuilder    func(ctrl *gomock.Controller) Deleter
		errWrapped      error
		errMessage      string
	}{
		"get journal record error": {
			blockNumber: 10,
			blockHashes: []common.Hash{{1}},
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				key := journalKey{BlockNumber: 10, BlockHash: common.Hash{1}}
				encodedKey := scaleMarshal(t, key)
				database.EXPECT().Get(encodedKey).Return(nil, errTest)
				return database
			},
			batchBuilder: func(ctrl *gomock.Controller) Deleter { return nil },
			errWrapped:   errTest,
			errMessage:   "getting journal record: getting from database: test error",
		},
		"node hash deletion error": {
			blockNumber: 10,
			blockHashes: []common.Hash{{1}},
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				key := journalKey{BlockNumber: 10, BlockHash: common.Hash{1}}
				encodedKey := scaleMarshal(t, key)
				record := journalRecord{DeletedNodeHashes: map[common.Hash]struct{}{
					{3}: {},
				}}
				encodedRecord := scaleMarshal(t, record)
				database.EXPECT().Get(encodedKey).Return(encodedRecord, nil)
				return database
			},
			batchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				batch.EXPECT().Delete(common.Hash{3}.ToBytes()).Return(errTest)
				return batch
			},
			errWrapped: errTest,
			errMessage: "deleting key from batch: test error",
		},
		"success": {
			blockNumber: 10,
			blockHashes: []common.Hash{{1}, {2}},
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)

				key1 := journalKey{BlockNumber: 10, BlockHash: common.Hash{1}}
				encodedKey1 := scaleMarshal(t, key1)
				record1 := journalRecord{DeletedNodeHashes: map[common.Hash]struct{}{{11}: {}, {12}: {}}}
				encodedRecord1 := scaleMarshal(t, record1)
				database.EXPECT().Get(encodedKey1).Return(encodedRecord1, nil)

				key2 := journalKey{BlockNumber: 10, BlockHash: common.Hash{2}}
				encodedKey2 := scaleMarshal(t, key2)
				record2 := journalRecord{DeletedNodeHashes: map[common.Hash]struct{}{{13}: {}}}
				encodedRecord2 := scaleMarshal(t, record2)
				database.EXPECT().Get(encodedKey2).Return(encodedRecord2, nil)

				return database
			},
			batchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				batch.EXPECT().Delete(common.Hash{11}.ToBytes()).Return(nil)
				batch.EXPECT().Delete(common.Hash{12}.ToBytes()).Return(nil)
				batch.EXPECT().Delete(common.Hash{13}.ToBytes()).Return(nil)
				return batch
			},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			database := testCase.databaseBuilder(ctrl)
			batch := testCase.batchBuilder(ctrl)
			err := pruneStorage(testCase.blockNumber,
				testCase.blockHashes, database, batch)

			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}

func Test_pruneJournal(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		blockNumber  uint32
		blockHashes  []common.Hash
		batchBuilder func(ctrl *gomock.Controller) Deleter
		errWrapped   error
		errMessage   string
	}{
		"prune block hashes error": {
			batchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				batch.EXPECT().Delete([]byte("block_number_to_hash_10")).Return(errTest)
				return batch
			},
			blockNumber: 10,
			errWrapped:  errTest,
			errMessage: "pruning block hashes: " +
				"deleting block hashes for block number 10 from database: " +
				"test error",
		},
		"delete journal key error": {
			batchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				batch.EXPECT().Delete([]byte("block_number_to_hash_10")).Return(nil)
				encodedKey := scaleMarshal(t, journalKey{BlockNumber: 10, BlockHash: common.Hash{1}})
				batch.EXPECT().Delete(encodedKey).Return(errTest)
				return batch
			},
			blockNumber: 10,
			blockHashes: []common.Hash{{1}},
			errWrapped:  errTest,
			errMessage: "deleting journal key from batch: " +
				"test error",
		},
		"success": {
			batchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				batch.EXPECT().Delete([]byte("block_number_to_hash_10")).Return(nil)
				encodedKeyA := scaleMarshal(t, journalKey{BlockNumber: 10, BlockHash: common.Hash{1}})
				batch.EXPECT().Delete(encodedKeyA).Return(nil)
				encodedKeyB := scaleMarshal(t, journalKey{BlockNumber: 10, BlockHash: common.Hash{2}})
				batch.EXPECT().Delete(encodedKeyB).Return(nil)
				return batch
			},
			blockNumber: 10,
			blockHashes: []common.Hash{{1}, {2}},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			batch := testCase.batchBuilder(ctrl)
			err := pruneJournal(testCase.blockNumber, testCase.blockHashes, batch)

			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}

func Test_storeJournalRecord(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		batchBuilder func(ctrl *gomock.Controller) Setter
		blockNumber  uint32
		blockHash    common.Hash
		record       journalRecord
		errWrapped   error
		errMessage   string
	}{
		"deleted node hash put error": {
			batchBuilder: func(ctrl *gomock.Controller) Setter {
				database := NewMockSetDeleter(ctrl)
				databaseKey := makeDeletedKey(common.Hash{3})
				encodedKey := scaleMarshal(t, journalKey{BlockNumber: 1, BlockHash: common.Hash{2}})
				database.EXPECT().Set(databaseKey, encodedKey).Return(errTest)
				return database
			},
			blockNumber: 1,
			blockHash:   common.Hash{2},
			record:      journalRecord{DeletedNodeHashes: map[common.Hash]struct{}{{3}: {}}},
			errWrapped:  errTest,
			errMessage:  "putting journal key in database batch: test error",
		},
		"encoded record put error": {
			batchBuilder: func(ctrl *gomock.Controller) Setter {
				database := NewMockSetDeleter(ctrl)
				databaseKey := makeDeletedKey(common.Hash{3})
				encodedKey := scaleMarshal(t, journalKey{BlockNumber: 1, BlockHash: common.Hash{2}})
				database.EXPECT().Set(databaseKey, encodedKey).Return(nil)
				encodedRecord := scaleMarshal(t, journalRecord{
					DeletedNodeHashes:  map[common.Hash]struct{}{{3}: {}},
					InsertedNodeHashes: map[common.Hash]struct{}{{4}: {}},
				})
				database.EXPECT().Set(encodedKey, encodedRecord).Return(errTest)
				return database
			},
			blockNumber: 1,
			blockHash:   common.Hash{2},
			record: journalRecord{
				DeletedNodeHashes:  map[common.Hash]struct{}{{3}: {}},
				InsertedNodeHashes: map[common.Hash]struct{}{{4}: {}},
			},
			errWrapped: errTest,
			errMessage: "putting journal record in database batch: test error",
		},
		"success": {
			batchBuilder: func(ctrl *gomock.Controller) Setter {
				database := NewMockSetDeleter(ctrl)
				databaseKey := makeDeletedKey(common.Hash{3})
				encodedKey := scaleMarshal(t, journalKey{BlockNumber: 1, BlockHash: common.Hash{2}})
				database.EXPECT().Set(databaseKey, encodedKey).Return(nil)
				encodedRecord := scaleMarshal(t, journalRecord{
					DeletedNodeHashes:  map[common.Hash]struct{}{{3}: {}},
					InsertedNodeHashes: map[common.Hash]struct{}{{4}: {}},
				})
				database.EXPECT().Set(encodedKey, encodedRecord).Return(nil)
				return database
			},
			blockNumber: 1,
			blockHash:   common.Hash{2},
			record: journalRecord{
				DeletedNodeHashes:  map[common.Hash]struct{}{{3}: {}},
				InsertedNodeHashes: map[common.Hash]struct{}{{4}: {}},
			},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			batch := testCase.batchBuilder(ctrl)
			err := storeJournalRecord(batch, testCase.blockNumber, testCase.blockHash, testCase.record)

			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}

func Test_getJournalRecord(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		databaseBuilder func(ctrl *gomock.Controller) Getter
		blockNumber     uint32
		blockHash       common.Hash
		record          journalRecord
		errWrapped      error
		errMessage      string
	}{
		"get error": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				expectedKey := scaleMarshal(t, journalKey{BlockNumber: 1, BlockHash: common.Hash{2}})
				database.EXPECT().Get(expectedKey).Return(nil, errTest)
				return database
			},
			blockNumber: 1,
			blockHash:   common.Hash{2},
			errWrapped:  errTest,
			errMessage:  "getting from database: test error",
		},
		"scale decoding error": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				expectedKey := scaleMarshal(t, journalKey{BlockNumber: 1, BlockHash: common.Hash{2}})
				database.EXPECT().Get(expectedKey).Return([]byte{99}, nil)
				return database
			},
			blockNumber: 1,
			blockHash:   common.Hash{2},
			errWrapped:  io.EOF,
			errMessage: "scale decoding journal record: decoding struct: " +
				"unmarshalling field at index 0: decoding length: reading bytes: EOF",
		},
		"success": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				expectedKey := scaleMarshal(t, journalKey{BlockNumber: 1, BlockHash: common.Hash{2}})
				returnedValue := scaleMarshal(t, journalRecord{
					InsertedNodeHashes: map[common.Hash]struct{}{{1}: {}, {2}: {}},
					DeletedNodeHashes:  map[common.Hash]struct{}{{2}: {}, {3}: {}},
				})
				database.EXPECT().Get(expectedKey).Return(returnedValue, nil)
				return database
			},
			blockNumber: 1,
			blockHash:   common.Hash{2},
			record: journalRecord{
				InsertedNodeHashes: map[common.Hash]struct{}{{1}: {}, {2}: {}},
				DeletedNodeHashes:  map[common.Hash]struct{}{{2}: {}, {3}: {}},
			},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			database := testCase.databaseBuilder(ctrl)
			record, err := getJournalRecord(database,
				testCase.blockNumber, testCase.blockHash)

			assert.Equal(t, testCase.record, record)
			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}

func Test_storeBlockNumberAtKey(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		batchBuilder func(ctrl *gomock.Controller) Setter
		key          []byte
		blockNumber  uint32
		errWrapped   error
		errMessage   string
	}{
		"put error": {
			batchBuilder: func(ctrl *gomock.Controller) Setter {
				database := NewMockSetDeleter(ctrl)
				expectedKey := []byte("key")
				expectedValue := scaleMarshal(t, uint32(1))
				database.EXPECT().Set(expectedKey, expectedValue).Return(errTest)
				return database
			},
			key:         []byte("key"),
			blockNumber: 1,
			errWrapped:  errTest,
			errMessage:  "putting block number 1: test error",
		},
		"success": {
			batchBuilder: func(ctrl *gomock.Controller) Setter {
				database := NewMockSetDeleter(ctrl)
				expectedKey := []byte("key")
				expectedValue := scaleMarshal(t, uint32(1))
				database.EXPECT().Set(expectedKey, expectedValue).Return(nil)
				return database
			},
			key:         []byte("key"),
			blockNumber: 1,
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			batch := testCase.batchBuilder(ctrl)
			err := storeBlockNumberAtKey(batch, testCase.key, testCase.blockNumber)

			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}

func Test_getBlockNumberFromKey(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		databaseBuilder func(ctrl *gomock.Controller) Getter
		key             []byte
		blockNumber     uint32
		errWrapped      error
		errMessage      string
	}{
		"get error": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				expectedKey := []byte("key")
				database.EXPECT().Get(expectedKey).Return(nil, errTest)
				return database
			},
			key:        []byte("key"),
			errWrapped: errTest,
			errMessage: "getting block number from database: test error",
		},
		"key not found": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				journalDatabase := NewMockJournalDatabase(ctrl)
				expectedKey := []byte("key")
				journalDatabase.EXPECT().Get(expectedKey).Return(nil, database.ErrKeyNotFound)
				return journalDatabase
			},
			key: []byte("key"),
		},
		"decoding error": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				expectedKey := []byte("key")
				database.EXPECT().Get(expectedKey).Return([]byte{}, nil)
				return database
			},
			key:        []byte("key"),
			errWrapped: io.EOF,
			errMessage: "decoding block number: EOF",
		},
		"success": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				expectedKey := []byte("key")
				returnedValue := scaleMarshal(t, uint32(1))
				database.EXPECT().Get(expectedKey).Return(returnedValue, nil)
				return database
			},
			key:         []byte("key"),
			blockNumber: 1,
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			database := testCase.databaseBuilder(ctrl)
			blockNumber, err := getBlockNumberFromKey(database, testCase.key)

			assert.Equal(t, testCase.blockNumber, blockNumber)
			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}

func Test_loadBlockHashes(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		databaseBuilder func(ctrl *gomock.Controller) Getter
		blockNumber     uint32
		blockHashes     []common.Hash
		errWrapped      error
		errMessage      string
	}{
		"get from database error": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				database.EXPECT().Get(databaseKey).Return(nil, errTest)
				return database
			},
			blockNumber: 10,
			errWrapped:  errTest,
			errMessage:  "getting block hashes for block number 10: test error",
		},
		"single block hash": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				database.EXPECT().Get(databaseKey).Return(common.Hash{2}.ToBytes(), nil)
				return database
			},
			blockNumber: 10,
			blockHashes: []common.Hash{{2}},
		},
		"multiple block hashes": {
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				databaseValue := bytes.Join([][]byte{
					common.Hash{2}.ToBytes(), common.Hash{3}.ToBytes(),
				}, nil)
				database.EXPECT().Get(databaseKey).
					Return(databaseValue, nil)
				return database
			},
			blockNumber: 10,
			blockHashes: []common.Hash{{2}, {3}},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			database := testCase.databaseBuilder(ctrl)
			blockHashes, err := loadBlockHashes(testCase.blockNumber, database)

			assert.Equal(t, testCase.blockHashes, blockHashes)
			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}

func Test_appendBlockHashes(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		blockNumber     uint32
		blockHash       common.Hash
		databaseBuilder func(ctrl *gomock.Controller) Getter
		batchBuilder    func(ctrl *gomock.Controller) Setter
		errWrapped      error
		errMessage      string
	}{
		"get from database error": {
			blockNumber: 10,
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				database.EXPECT().Get(databaseKey).Return(nil, errTest)
				return database
			},
			batchBuilder: func(ctrl *gomock.Controller) Setter {
				return nil
			},
			errWrapped: errTest,
			errMessage: "getting block hashes for block number 10: test error",
		},
		"key not found": {
			blockNumber: 10,
			blockHash:   common.Hash{2},
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				journalDB := NewMockJournalDatabase(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				journalDB.EXPECT().Get(databaseKey).Return(nil, database.ErrKeyNotFound)
				return journalDB
			},
			batchBuilder: func(ctrl *gomock.Controller) Setter {
				batch := NewMockSetDeleter(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				databaseValue := common.Hash{2}.ToBytes()
				batch.EXPECT().Set(databaseKey, databaseValue).Return(nil)
				return batch
			},
		},
		"put error": {
			blockNumber: 10,
			blockHash:   common.Hash{2},
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				database.EXPECT().Get(databaseKey).Return(nil, nil)
				return database
			},
			batchBuilder: func(ctrl *gomock.Controller) Setter {
				batch := NewMockSetDeleter(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				databaseValue := common.Hash{2}.ToBytes()
				batch.EXPECT().Set(databaseKey, databaseValue).Return(errTest)
				return batch
			},
			errWrapped: errTest,
			errMessage: "putting block hashes for block number 10: test error",
		},
		"append to existing block hashes": {
			blockNumber: 10,
			blockHash:   common.Hash{2},
			databaseBuilder: func(ctrl *gomock.Controller) Getter {
				database := NewMockJournalDatabase(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				databaseValue := bytes.Join([][]byte{
					common.Hash{1}.ToBytes(), common.Hash{3}.ToBytes(),
				}, nil)
				database.EXPECT().Get(databaseKey).Return(databaseValue, nil)
				return database
			},
			batchBuilder: func(ctrl *gomock.Controller) Setter {
				batch := NewMockSetDeleter(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				databaseValue := bytes.Join([][]byte{
					common.Hash{1}.ToBytes(), common.Hash{3}.ToBytes(), common.Hash{2}.ToBytes(),
				}, nil)
				batch.EXPECT().Set(databaseKey, databaseValue).Return(nil)
				return batch
			},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			database := testCase.databaseBuilder(ctrl)
			batch := testCase.batchBuilder(ctrl)
			err := appendBlockHash(testCase.blockNumber, testCase.blockHash, database, batch)

			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}

func Test_pruneBlockHashes(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	testCases := map[string]struct {
		blockNumber  uint32
		batchBuilder func(ctrl *gomock.Controller) Deleter
		errWrapped   error
		errMessage   string
	}{
		"delete from batch error": {
			blockNumber: 10,
			batchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				batch.EXPECT().Delete(databaseKey).Return(errTest)
				return batch
			},
			errWrapped: errTest,
			errMessage: "deleting block hashes for block number 10 from database: test error",
		},
		"success": {
			blockNumber: 10,
			batchBuilder: func(ctrl *gomock.Controller) Deleter {
				batch := NewMockSetDeleter(ctrl)
				databaseKey := []byte("block_number_to_hash_10")
				batch.EXPECT().Delete(databaseKey).Return(nil)
				return batch
			},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			batch := testCase.batchBuilder(ctrl)
			err := pruneBlockHashes(testCase.blockNumber, batch)

			assert.ErrorIs(t, err, testCase.errWrapped)
			if testCase.errWrapped != nil {
				assert.EqualError(t, err, testCase.errMessage)
			}
		})
	}
}
