// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

//go:generate mockgen -package=$GOPACKAGE -destination=mocks_test.go . Getter,Putter,Deleter,JournalDatabase,ChainDBNewBatcher,PutDeleter,Logger
//go:generate mockgen -package=$GOPACKAGE -destination=mocks_chaindb_test.go github.com/ChainSafe/chaindb Batch
