package gorocksdb

// #include "rocksdb/c.h"
import "C"

// ReadTier controls fetching of data during a read request.
// An application can issue a read request (via Get/Iterators) and specify
// if that read should process data that ALREADY resides on a specified cache
// level. For example, if an application specifies BlockCacheTier then the
// Get call will process data that is already processed in the memtable or
// the block cache. It will not page in data from the OS cache or data that
// resides in storage.
type ReadTier uint

const (
	// ReadAllTier reads data in memtable, block cache, OS cache or storage.
	ReadAllTier = ReadTier(0)
	// BlockCacheTier reads data in memtable or block cache.
	BlockCacheTier = ReadTier(1)
)

// ReadOptions represent all of the available options when reading from a
// database.
type ReadOptions struct {
	c *C.rocksdb_readoptions_t
}

// NewReadOptions creates a default ReadOptions object.
func NewReadOptions() *ReadOptions {
	return newNativeReadOptions(C.rocksdb_readoptions_create())
}

// newNativeReadOptions creates a ReadOptions object.
func newNativeReadOptions(c *C.rocksdb_readoptions_t) *ReadOptions {
	return &ReadOptions{c}
}

// SetVerifyChecksums speciy if all data read from underlying storage will be
// verified against corresponding checksums.
// Default: false
func (o *ReadOptions) SetVerifyChecksums(value bool) {
	C.rocksdb_readoptions_set_verify_checksums(o.c, boolToChar(value))
}

// SetFillCache specify whether the "data block"/"index block"/"filter block"
// read for this iteration should be cached in memory?
// Callers may wish to set this field to false for bulk scans.
// Default: true
func (o *ReadOptions) SetFillCache(value bool) {
	C.rocksdb_readoptions_set_fill_cache(o.c, boolToChar(value))
}

// SetSnapshot sets the snapshot which should be used for the read.
// The snapshot must belong to the DB that is being read and must
// not have been released.
// Default: nil
func (o *ReadOptions) SetSnapshot(snap *Snapshot) {
	C.rocksdb_readoptions_set_snapshot(o.c, snap.c)
}

// SetReadTier specify if this read request should process data that ALREADY
// resides on a particular cache. If the required data is not
// found at the specified cache, then Status::Incomplete is returned.
// Default: ReadAllTier
func (o *ReadOptions) SetReadTier(value ReadTier) {
	C.rocksdb_readoptions_set_read_tier(o.c, C.int(value))
}

// SetTailing specify if to create a tailing iterator.
// A special iterator that has a view of the complete database
// (i.e. it can also be used to read newly added data) and
// is optimized for sequential reads. It will return records
// that were inserted into the database after the creation of the iterator.
// Default: false
func (o *ReadOptions) SetTailing(value bool) {
	C.rocksdb_readoptions_set_tailing(o.c, boolToChar(value))
}

// Release deallocates the ReadOptions object.
func (o *ReadOptions) Release() {
	C.rocksdb_readoptions_destroy(o.c)
	o.c = nil
}
