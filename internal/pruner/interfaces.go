// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

import "github.com/ChainSafe/chaindb"

// Logger is the logger for the online pruner.
type Logger interface {
	Debug(s string)
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// ChainDBNewBatcher is the chaindb new batcher interface.
type ChainDBNewBatcher interface {
	NewBatch() chaindb.Batch
}

// JournalDB is the chaindb interface for the journal database.
type JournalDB interface {
	ChainDBNewBatcher
	Getter
	NewIterator() chaindb.Iterator
}

// Getter is the database getter interface.
type Getter interface {
	Get(key []byte) (value []byte, err error)
}

type PutDeleter interface {
	Putter
	Deleter
}

// Putter puts a key value in the database and returns an error.
type Putter interface {
	Put(key, value []byte) error
}

// Deleter deletes a key and returns an error.
type Deleter interface {
	Del(key []byte) error
}
