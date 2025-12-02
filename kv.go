package ethdataset

import (
	"io"
	"log"
	"os"

	"github.com/golang/snappy"
)

type ColumnFile struct {
	File *os.File
	io.WriteCloser
	io.Reader
}

func NewColumnFile(path string) *ColumnFile {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return &ColumnFile{
		File:        file,
		WriteCloser: snappy.NewBufferedWriter(file),
		Reader:      snappy.NewReader(file),
	}
}

type KeyValue struct {
	Key   *ColumnFile
	Value *ColumnFile
}

func NewKeyValue(path string) *KeyValue {
	return &KeyValue{
		Key:   NewColumnFile(path + "_key.bin.sz"),
		Value: NewColumnFile(path + "_value.bin.sz"),
	}
}

func (kv *KeyValue) Write(key, value []byte) (int, error) {
	if _, err := kv.Key.Write(key); err != nil {
		log.Fatal(err)
	}
	if _, err := kv.Value.Write(value); err != nil {
		log.Fatal(err)
	}
	return 0, nil
}

func (kv *KeyValue) Close() error {
	if err := kv.Key.Close(); err != nil {
		log.Fatal(err)
	}
	if err := kv.Value.Close(); err != nil {
		log.Fatal(err)
	}
	return nil
}

func (kv *KeyValue) Iter(keySize, valueSize uint64) Iter {
	end := uint64(320_000_000)

	return Iter{
		kv:        kv,
		keySize:   keySize,
		valueSize: valueSize,
		end:       end,
	}
}

type Iter struct {
	kv        *KeyValue
	offset    uint64
	keySize   uint64
	valueSize uint64

	end uint64

	currentKey   []byte
	currentValue []byte
}

func (i *Iter) Next() bool {
	i.offset += 1

	if i.offset >= i.end {
		return false
	}

	key := make([]byte, i.keySize)
	_, err := io.ReadFull(i.kv.Key.Reader, key)
	if err != nil {
		log.Fatal(err)
	}
	i.currentKey = key

	value := make([]byte, i.valueSize)
	_, err = io.ReadFull(i.kv.Value.Reader, value)
	if err != nil {
		log.Fatal(err)
	}
	i.currentValue = value

	return true
}

func (i *Iter) Key() []byte {
	return i.currentKey
}

func (i *Iter) Value() []byte {
	return i.currentValue
}
