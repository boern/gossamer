package types

import "github.com/ChainSafe/gossamer/pkg/scale"

// NewEquivocation returns a new VaryingDataType to represent a grandpa equivocation
func NewGrandpaEquivocation() scale.VaryingDataType {
	return scale.MustNewVaryingDataType(ChangesTrieRootDigest{}, PreRuntimeDigest{}, ConsensusDigest{}, SealDigest{})
}
