package gorocksdb

// #include "rocksdb/c.h"
import "C"

// Cache is a cache used to store data read from data in memory.
type Cache struct {
	c *C.rocksdb_cache_t
}

// NewLRUCache creates a new LRU Cache object with the capacity given.
func NewLRUCache(capacity int) *Cache {
	return newNativeCache(C.rocksdb_cache_create_lru(C.size_t(capacity)))
}

// newNativeCache creates a Cache object.
func newNativeCache(c *C.rocksdb_cache_t) *Cache {
	return &Cache{c}
}

// Release deallocates the Cache object.
func (c *Cache) Release() {
	C.rocksdb_cache_destroy(c.c)
	c.c = nil
}
