// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package state

import "github.com/ChainSafe/gossamer/lib/common"

type Pruner interface {
	StoreJournalRecord(deletedNodeHashes, insertedNodeHashes map[common.Hash]struct{},
		blockHash common.Hash, blockNumber uint32) error
}
