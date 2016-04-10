package gorocksdb

// #include "rocksdb/c.h"
import "C"

// Env is a system call environment used by a database.
type Env struct {
	c *C.rocksdb_env_t
}

// NewDefaultEnv creates a default environment.
func NewDefaultEnv() *Env {
	return newNativeEnv(C.rocksdb_create_default_env())
}

// NewMemEnv creates a memory backed environment.
func NewMemEnv() *Env {
	return newNativeEnv(C.rocksdb_create_mem_env())
}

// newNativeEnv creates a Environment object.
func newNativeEnv(c *C.rocksdb_env_t) *Env {
	return &Env{c}
}

// SetBackgroundThreads sets the number of background worker threads
// of a specific thread pool for this environment.
// 'LOW' is the default pool.
// Default: 1
func (e *Env) SetBackgroundThreads(n int) {
	C.rocksdb_env_set_background_threads(e.c, C.int(n))
}

// SetHighPriorityBackgroundThreads sets the size of the high priority
// thread pool that can be used to prevent compactions from stalling
// memtable flushes.
func (e *Env) SetHighPriorityBackgroundThreads(n int) {
	C.rocksdb_env_set_high_priority_background_threads(e.c, C.int(n))
}

// Release deallocates the Env object.
func (e *Env) Release() {
	C.rocksdb_env_destroy(e.c)
	e.c = nil
}
