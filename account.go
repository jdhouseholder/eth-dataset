package ethdataset

import (
	"log"

	"github.com/cockroachdb/pebble"
)

type AccountTable struct {
	DB *pebble.DB
}

func NewAccountTable(path string) *AccountTable {
	return &AccountTable{
		DB: openPebbleDB(path, "accounts"),
	}
}

func (at *AccountTable) Save(addressHash []byte, accountBytes []byte) {
	if err := at.DB.Set(addressHash, accountBytes, pebble.NoSync); err != nil {
		log.Fatal(err)
	}
}

func (at *AccountTable) Get(addressHash []byte) []byte {
	b, c, err := at.DB.Get(addressHash)
	if err != nil {
		log.Fatal(err)
	}

	dst := make([]byte, len(b))
	copy(dst, b)

	if err := c.Close(); err != nil {
		log.Fatal(err)
	}

	return dst
}

func (at *AccountTable) Close() {
	at.DB.Close()
}
