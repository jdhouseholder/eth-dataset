package ethdataset

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/crypto"
)

func getNibble(data []byte, i int) byte {
	if i%2 == 0 {
		// Even index: upper nibble
		return data[i/2] >> 4
	} else {
		// Odd index: lower nibble
		return data[i/2] & 0x0F
	}
}

type SparseBucketMapping struct {
	BucketId uint64
	RowId    uint64
	ProofId  uint64
}

func Hash(domainSeparator string, parentHash []byte, addressHash []byte, level uint64) []byte {
	nibble := getNibble(addressHash, int(level))
	return crypto.Keccak256(append(append([]byte(domainSeparator), parentHash...), nibble))
}

func computeCompressionFactor(node *MPTNode) int {
	switch {
	case node.IsLeafNode():
		return len(node.Leaf.Key)
	case node.IsExtensionNode():
		key := len(node.Extension.Key)
		switch {
		case node.Extension.IsEmbedded():
			return key + computeCompressionFactor(node.Extension.Embedded)
		default:
			return key

		}

	case node.IsBranchNode():
		return 1
	default:
		panic("unreachable")
	}
}

func BucketsForMPTKey3(
	domainSeparator string,
	addressHash []byte,
	proofIds []uint64,
	nodes []*MPTNode,
	numBuckets uint64,
	rowsPerBucket uint64,
) []SparseBucketMapping {
	remainingBuckets := make([]uint64, numBuckets)
	for i := uint64(0); i < numBuckets; i++ {
		remainingBuckets[i] = uint64(i)
	}

	var bucketPath []SparseBucketMapping

	hash := make([]byte, 32)
	level := uint64(0)

	nTreeTop := uint64(3)
	for i := uint64(0); i < nTreeTop; i++ {
		hash = Hash(domainSeparator, hash, addressHash, level)
		level += 1
	}

	for i := nTreeTop; i < uint64(len(proofIds)); i++ {
		compressionFactor := computeCompressionFactor(nodes[i])
		for j := 0; j < compressionFactor; j++ {
			hash = Hash(domainSeparator, hash, addressHash, level)
			level += 1
		}

		bucketIndex := binary.LittleEndian.Uint64(hash[:8]) % (numBuckets - i)
		bucketId := remainingBuckets[bucketIndex]
		rowId := binary.LittleEndian.Uint64(hash[8:16]) % rowsPerBucket
		remainingBuckets[bucketIndex] = remainingBuckets[numBuckets-i-1]

		bucketPath = append(bucketPath, SparseBucketMapping{BucketId: bucketId, RowId: rowId, ProofId: proofIds[i]})
	}

	return bucketPath
}
