package gorocksdb

// #include "rocksdb/c.h"
import "C"

// FlushOptions represent all of the available options when manual flushing the
// database.
type FlushOptions struct {
	c *C.rocksdb_flushoptions_t
}

// NewDefaultFlushOptions creates a default FlushOptions object.
func NewDefaultFlushOptions() *FlushOptions {
	return newNativeFlushOptions(C.rocksdb_flushoptions_create())
}

// newNativeFlushOptions creates a FlushOptions object.
func newNativeFlushOptions(c *C.rocksdb_flushoptions_t) *FlushOptions {
	return &FlushOptions{c}
}

// SetWait specify if the flush will wait until the flush is done.
// Default: true
func (o *FlushOptions) SetWait(value bool) {
	C.rocksdb_flushoptions_set_wait(o.c, boolToChar(value))
}

// Destroy deallocates the FlushOptions object.
func (o *FlushOptions) Destroy() {
	C.rocksdb_flushoptions_destroy(o.c)
	o.c = nil
}
