// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package pruner

import "github.com/ChainSafe/gossamer/lib/common"

// ArchiveNode is a no-op since we don't prune nodes in archive mode.
type ArchiveNode struct{}

// NewArchiveNode returns a new archive mode pruner (no-op).
func NewArchiveNode() *ArchiveNode {
	return &ArchiveNode{}
}

// StoreJournalRecord for archive node doesn't do anything.
func (*ArchiveNode) StoreJournalRecord(_, _ map[string]struct{}, _ common.Hash, _ uint32) (_ error) {
	return nil
}
