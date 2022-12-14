// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

import (
	"github.com/ChainSafe/gossamer/internal/database"
	"github.com/ChainSafe/gossamer/lib/common"
)

// Logger is the logger for the online pruner.
type Logger interface {
	Debug(s string)
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// NewWriteBatcher is the new batcher interface.
type NewWriteBatcher interface {
	NewWriteBatch() (writeBatch database.WriteBatch)
}

// JournalDatabase is the interface for the journal database.
type JournalDatabase interface {
	NewWriteBatcher
	Getter
}

// GetterSetter combines the Getter and Setter interfaces.
type GetterSetter interface {
	Getter
	Setter
}

// Getter is the database getter interface.
type Getter interface {
	Get(key []byte) (value []byte, err error)
}

// SetDeleter combines the Setter and Deleter interfaces.
type SetDeleter interface {
	Setter
	Deleter
}

// Setter puts a key value in the database and returns an error.
type Setter interface {
	Set(key, value []byte) error
}

// Deleter deletes a key and returns an error.
type Deleter interface {
	Delete(key []byte) error
}

// BlockState is the block state interface to determine
// if a block is the descendant of another block.
type BlockState interface {
	IsDescendantOf(parent, child common.Hash) (bool, error)
}
