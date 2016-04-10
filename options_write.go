package gorocksdb

// #include "rocksdb/c.h"
import "C"

// WriteOptions represent all of the available options when writing to a
// database.
type WriteOptions struct {
	c *C.rocksdb_writeoptions_t
}

// NewDefaultWriteOptions creates a default WriteOptions object.
func NewDefaultWriteOptions() *WriteOptions {
	return newNativeWriteOptions(C.rocksdb_writeoptions_create())
}

// newNativeWriteOptions creates a WriteOptions object.
func newNativeWriteOptions(c *C.rocksdb_writeoptions_t) *WriteOptions {
	return &WriteOptions{c}
}

// SetSync sets the sync mode. If true, the write will be flushed
// from the operating system buffer cache before the write is considered complete.
// If this flag is true, writes will be slower.
// Default: false
func (o *WriteOptions) SetSync(value bool) {
	C.rocksdb_writeoptions_set_sync(o.c, boolToChar(value))
}

// DisableWAL sets whether WAL should be active or not.
// If true, writes will not first go to the write ahead log,
// and the write may got lost after a crash.
// Default: false
func (o *WriteOptions) DisableWAL(value bool) {
	C.rocksdb_writeoptions_disable_WAL(o.c, C.int(btoi(value)))
}

// Destroy deallocates the WriteOptions object.
func (o *WriteOptions) Destroy() {
	C.rocksdb_writeoptions_destroy(o.c)
	o.c = nil
}
