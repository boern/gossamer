// Copyright 2022 ChainSafe Systems (ON)
// SPDX-License-Identifier: LGPL-3.0-only

package node

type variant struct {
	bits byte
	mask byte
}

// Node variants
// See https://spec.polkadot.network/#defn-node-header
var (
	leafVariant = variant{ // leaf 01
		bits: 0b0100_0000,
		mask: 0b1100_0000,
	}
	branchVariant = variant{ // branch 10
		bits: 0b1000_0000,
		mask: 0b1100_0000,
	}
	branchWithValueVariant = variant{ // branch 11
		bits: 0b1100_0000,
		mask: 0b1100_0000,
	}
)

// partialKeyLengthHeaderMask returns the partial key length
// header bit mask corresponding to the variant header bit mask.
// For example for the leaf variant with variant mask 1100_000,
// the partial key length header mask returned is 0011_1111.
func (v variant) partialKeyLengthHeaderMask() byte {
	return ^v.mask
}
