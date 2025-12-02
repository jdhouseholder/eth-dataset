package ethdataset

import (
	"encoding/binary"

	"github.com/holiman/uint256"
)

type SlimAccount struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     []byte
	CodeHash []byte
}

type SerializedAccount struct {
	// 8 bytes
	Nonce uint64
	// 32 bytes
	Balance uint256.Int
	// 32 bytes
	Root [32]byte
	// 32 bytes
	CodeHash [32]byte
	// 8 bytes
	CodeId uint64
	// 512 bytes
	ProofIds [64]uint64
}

// 8 + 32 + 32 + 32 + 8 + 512 = 624

// Define constants for field sizes and offsets to make the code clear and maintainable.
const (
	// Field Sizes
	nonceSize    = 8
	balanceSize  = 32
	rootSize     = 32
	codeHashSize = 32
	codeIdSize   = 8
	maxProofIds  = 64
	proofIdSize  = 8
	proofIdsSize = maxProofIds * proofIdSize

	// Total size of the serialized data
	TotalSerializedSize = nonceSize + balanceSize + rootSize + codeHashSize + codeIdSize + proofIdsSize // 624 bytes

	// Field Offsets
	nonceOffset    = 0
	balanceOffset  = nonceOffset + nonceSize       // 8
	rootOffset     = balanceOffset + balanceSize   // 40
	codeHashOffset = rootOffset + rootSize         // 72
	codeIdOffset   = codeHashOffset + codeHashSize // 104
	proofIdsOffset = codeIdOffset + codeIdSize     // 112
)

// SerializeAccount packs account data into a single, fixed-size byte slice.
// The layout is as follows:
// [0:8]      - Nonce (uint64)
// [8:40]     - Balance (uint256)
// [40:72]    - Root ([32]byte)
// [72:104]   - CodeHash ([32]byte)
// [104:112]  - CodeId (uint64)
// [112:624]  - ProofIds ([64]uint64)

func SerializeAccount(slimAccount SlimAccount, codeId uint64, proofIds []uint64) []byte {
	buf := make([]byte, TotalSerializedSize)

	binary.BigEndian.PutUint64(buf[nonceOffset:balanceOffset], slimAccount.Nonce)

	binary.BigEndian.PutUint64(buf[nonceOffset:balanceOffset], slimAccount.Nonce)

	// 3. Serialize Balance (*uint256.Int)
	// If Balance is nil, it represents 0, and the buffer is already zeroed.
	if slimAccount.Balance != nil {
		// Use Bytes32() to get a fixed [32]byte array representation.
		balanceBytes := slimAccount.Balance.Bytes32()
		copy(buf[balanceOffset:rootOffset], balanceBytes[:])
	}

	copy(buf[rootOffset:codeHashOffset], slimAccount.Root)
	copy(buf[codeHashOffset:codeIdOffset], slimAccount.CodeHash)
	binary.BigEndian.PutUint64(buf[codeIdOffset:proofIdsOffset], codeId)

	for i := 0; i < len(proofIds); i++ {
		offset := proofIdsOffset + (i * proofIdSize)
		binary.BigEndian.PutUint64(buf[offset:offset+proofIdSize], proofIds[i])
	}

	return buf
}
