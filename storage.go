package ethdataset

import (
	"log"

	"github.com/cockroachdb/pebble"
)

type StorageTable struct {
	DB *pebble.DB
}

func NewStorageTable(path string) *StorageTable {
	return &StorageTable{
		DB: openPebbleDB(path, "storage"),
	}
}

func (t *StorageTable) Save(key, code []byte) {
	if err := t.DB.Set(key, code, pebble.NoSync); err != nil {
		log.Fatal(err)
	}
}

func (t *StorageTable) Close() {
	if err := t.DB.Close(); err != nil {
		log.Fatal(err)
	}
}
