package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"bytes"
	"errors"
	"unsafe"
)

// Iterator provides a way to seek to specific keys and iterate through
// the keyspace from that point, as well as access the values of those keys.
//
// For example:
//
//      it := db.NewIterator(readOpts)
//      defer it.Release()
//
//      it.Seek([]byte("foo"))
//		for ; it.Valid(); it.Next() {
//          fmt.Printf("Key: %v Value: %v\n", it.Key().Data(), it.Value().Data())
// 		}
//
//      if err := it.Err(); err != nil {
//          return err
//      }
//
type Iterator struct {
	c *C.rocksdb_iterator_t
}

// newNativeIterator creates a Iterator object.
func newNativeIterator(c *C.rocksdb_iterator_t) *Iterator {
	return &Iterator{c}
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (i *Iterator) Valid() bool {
	return C.rocksdb_iter_valid(i.c) != 0
}

// ValidForPrefix returns false only when an Iterator has iterated past the
// first or the last key in the database or the specified prefix.
func (i *Iterator) ValidForPrefix(prefix []byte) bool {
	return C.rocksdb_iter_valid(i.c) != 0 && bytes.HasPrefix(i.Key().Data(), prefix)
}

// Key returns the key the iterator currently holds.
func (i *Iterator) Key() *Slice {
	var cLen C.size_t
	cKey := C.rocksdb_iter_key(i.c, &cLen)
	if cKey == nil {
		return nil
	}
	return &Slice{cKey, cLen, true}
}

// Value returns the value in the database the iterator currently holds.
func (i *Iterator) Value() *Slice {
	var cLen C.size_t
	cVal := C.rocksdb_iter_value(i.c, &cLen)
	if cVal == nil {
		return nil
	}
	return &Slice{cVal, cLen, true}
}

// Next moves the iterator to the next sequential key in the database.
func (i *Iterator) Next() {
	C.rocksdb_iter_next(i.c)
}

// Prev moves the iterator to the previous sequential key in the database.
func (i *Iterator) Prev() {
	C.rocksdb_iter_prev(i.c)
}

// SeekToFirst moves the iterator to the first key in the database.
func (i *Iterator) SeekToFirst() {
	C.rocksdb_iter_seek_to_first(i.c)
}

// SeekToLast moves the iterator to the last key in the database.
func (i *Iterator) SeekToLast() {
	C.rocksdb_iter_seek_to_last(i.c)
}

// Seek moves the iterator to the position greater than or equal to the key.
func (i *Iterator) Seek(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_iter_seek(i.c, cKey, C.size_t(len(key)))
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (i *Iterator) Err() error {
	var cErr *C.char
	C.rocksdb_iter_get_error(i.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Release closes the iterator.
func (i *Iterator) Release() {
	C.rocksdb_iter_destroy(i.c)
	i.c = nil
}
