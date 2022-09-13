// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package state

import "github.com/ChainSafe/gossamer/lib/common"

type Pruner interface {
	StoreJournalRecord(deletedMerkleValues, insertedMerkleValues map[string]struct{},
		blockHash common.Hash, blockNum int64) error
}
