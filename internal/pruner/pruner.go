// Copyright 2021 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

import (
	"time"

	"github.com/ChainSafe/gossamer/lib/common"
)

const (
	lastPrunedKey = "last_pruned"
	pruneInterval = time.Second
)

type deathRecord struct {
	blockHash                       common.Hash
	deletedMerkleValueToBlockNumber map[string]uint32 // Mapping from deleted key hash to block number.
}

type deathRow []*deathRecord

type journalRecord struct {
	// blockHash of the block corresponding to journal record
	blockHash common.Hash
	// Merkle values of nodes inserted in the state trie of the block
	insertedMerkleValues map[string]struct{}
	// Merkle values of nodes deleted from the state trie of the block
	deletedMerkleValues map[string]struct{}
}

type journalKey struct {
	blockNum  uint32
	blockHash common.Hash
}

func newJournalRecord(hash common.Hash, insertedMerkleValues,
	deletedMerkleValues map[string]struct{}) *journalRecord {
	return &journalRecord{
		blockHash:            hash,
		insertedMerkleValues: insertedMerkleValues,
		deletedMerkleValues:  deletedMerkleValues,
	}
}
