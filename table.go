package ethdataset

import (
	"errors"
	"log"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

func openPebbleDB(path, name string) *pebble.DB {
	db, err := pebble.Open(filepath.Join(path, name), FastUnsafeOptions())
	if err != nil {
		log.Fatal(err)
	}
	return db
}

type Table struct {
	DB *pebble.DB
}

func NewTable(path, name string) *Table {
	return &Table{
		DB: openPebbleDB(path, name),
	}
}

func (t *Table) Set(key, value []byte) {
	if err := t.DB.Set(key, value, pebble.NoSync); err != nil {
		log.Fatal(err)
	}
}

func (t *Table) Get(key []byte) []byte {
	b, c, err := t.DB.Get(key)
	if err != nil {
		log.Fatal(err)
	}

	out := make([]byte, len(b))
	copy(out, b)

	if err := c.Close(); err != nil {
		log.Fatal(err)
	}

	return out
}

func (t *Table) MaybeGet(key []byte) []byte {
	b, c, err := t.DB.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		if c != nil {
			c.Close()
		}
		return nil
	}

	if err != nil {
		log.Fatal(err)
	}

	out := make([]byte, len(b))
	copy(out, b)

	if err := c.Close(); err != nil {
		log.Fatal(err)
	}

	return out
}

func (t *Table) Contains(key []byte) bool {
	_, closer, err := t.DB.Get(key)
	if closer != nil {
		closer.Close()
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return false
	}
	if err != nil {
		log.Fatal(err)
	}
	return true
}

func (t *Table) Close() {
	if err := t.DB.Close(); err != nil {
		log.Fatal(err)
	}
}
