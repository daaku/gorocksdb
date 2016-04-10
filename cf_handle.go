package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

// CF represents a handle to a ColumnFamily.
type CF struct {
	c *C.rocksdb_column_family_handle_t
}

// newNativeCF creates a CF object.
func newNativeCF(c *C.rocksdb_column_family_handle_t) *CF {
	return &CF{c}
}

// Destroy calls the destructor of the underlying column family handle.
func (h *CF) Destroy() {
	C.rocksdb_column_family_handle_destroy(h.c)
}
