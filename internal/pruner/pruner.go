// Copyright 2021 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

import (
	"time"

	"github.com/ChainSafe/gossamer/lib/common"
)

const (
	journalPrefix = "journal"
	lastPrunedKey = "last_pruned"
	pruneInterval = time.Second
)

const (
	// Full pruner mode.
	Full = Mode("full")
	// Archive pruner mode.
	Archive = Mode("archive")
)

// Mode online pruning mode of historical state tries
type Mode string

// IsValid checks whether the pruning mode is valid
func (p Mode) IsValid() bool {
	switch p {
	case Full:
		return true
	case Archive:
		return true
	default:
		return false
	}
}

// Config holds state trie pruning mode and retained blocks
type Config struct {
	Mode           Mode
	RetainedBlocks uint32
}

// Pruner is implemented by FullNode and ArchiveNode.
type Pruner interface {
	StoreJournalRecord(deletedMerkleValues, insertedMerkleValues map[string]struct{},
		blockHash common.Hash, blockNum int64) error
}

type deathRecord struct {
	blockHash                       common.Hash
	deletedMerkleValueToBlockNumber map[string]int64
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
	blockNum  int64
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
