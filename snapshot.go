package gorocksdb

// #include "rocksdb/c.h"
import "C"

// Snapshot provides a consistent view of read operations in a DB.
type Snapshot struct {
	c   *C.rocksdb_snapshot_t
	cDB *C.rocksdb_t
}

// newNativeSnapshot creates a Snapshot object.
func newNativeSnapshot(c *C.rocksdb_snapshot_t, cDB *C.rocksdb_t) *Snapshot {
	return &Snapshot{c, cDB}
}

// Release removes the snapshot from the database's list of snapshots.
func (s *Snapshot) Release() {
	C.rocksdb_release_snapshot(s.cDB, s.c)
	s.c, s.cDB = nil, nil
}
