// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

//go:generate mockgen -package=$GOPACKAGE -destination=mocks_test.go . JournalDatabase,SetDeleter,Logger
//go:generate mockgen -package=$GOPACKAGE -destination=mocks_database_test.go github.com/ChainSafe/gossamer/internal/database WriteBatch
