package ethdataset

import (
	"encoding/binary"
	"log"
	"unsafe"

	"github.com/cockroachdb/pebble"
)

func uint64SliceToBytesUnsafe(data []uint64) []byte {
	if len(data) == 0 {
		return nil
	}

	return unsafe.Slice((*byte)(unsafe.Pointer(&data[0])), len(data)*8)
}

func bytesToUint64(data []byte) []uint64 {
	n := len(data) / 8
	if n == 0 {
		return nil // or return []uint64{}
	}
	out := make([]uint64, n)
	for i := 0; i < n; i++ {
		off := i * 8
		out[i] = binary.LittleEndian.Uint64(data[off : off+8])
	}
	return out
}

type AccountToProof struct {
	db *pebble.DB
}

func NewAccountToProof(path string) *AccountToProof {
	return &AccountToProof{
		db: openPebbleDB(path, "accountToProof"),
	}
}

func (atp *AccountToProof) Save(addressHash []byte, proofIds []uint64) {
	if err := atp.db.Set(addressHash, uint64SliceToBytesUnsafe(proofIds), pebble.NoSync); err != nil {
		log.Fatal(err)
	}
}

func (atp *AccountToProof) Get(addressHash []byte) []uint64 {
	b, c, err := atp.db.Get(addressHash)
	if err != nil {
		log.Fatal(err)
	}

	dst := make([]byte, len(b))
	copy(dst, b)

	if err := c.Close(); err != nil {
		log.Fatal(err)
	}

	return bytesToUint64(dst)
}

func (atp *AccountToProof) Close() {
	atp.db.Close()
}
