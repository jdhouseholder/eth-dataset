package ethdataset

import "encoding/binary"

func uint64ToKey(u uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, u)
	return key
}
