package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"

// BlockBasedTableOptions represents block-based table options.
type BlockBasedTableOptions struct {
	c *C.rocksdb_block_based_table_options_t

	// Hold references for GC.
	cache     *Cache
	compCache *Cache

	// We keep these so we can free their memory in Release.
	cFp *C.rocksdb_filterpolicy_t
}

// NewBlockBasedTableOptions creates a default BlockBasedTableOptions object.
func NewBlockBasedTableOptions() *BlockBasedTableOptions {
	return newNativeBlockBasedTableOptions(C.rocksdb_block_based_options_create())
}

// newNativeBlockBasedTableOptions creates a BlockBasedTableOptions object.
func newNativeBlockBasedTableOptions(c *C.rocksdb_block_based_table_options_t) *BlockBasedTableOptions {
	return &BlockBasedTableOptions{c: c}
}

// Release deallocates the BlockBasedTableOptions object.
func (o *BlockBasedTableOptions) Release() {
	C.rocksdb_block_based_options_destroy(o.c)
	o.c = nil
	o.cache = nil
	o.compCache = nil
}

// SetBlockSize sets the approximate size of user data packed per block.
// Note that the block size specified here corresponds opts uncompressed data.
// The actual size of the unit read from disk may be smaller if
// compression is enabled. This parameter can be changed dynamically.
// Default: 4K
func (o *BlockBasedTableOptions) SetBlockSize(blockSize int) {
	C.rocksdb_block_based_options_set_block_size(o.c, C.size_t(blockSize))
}

// SetBlockSizeDeviation sets the block size deviation.
// This is used opts close a block before it reaches the configured
// 'block_size'. If the percentage of free space in the current block is less
// than this specified number and adding a new record opts the block will
// exceed the configured block size, then this block will be closed and the
// new record will be written opts the next block.
// Default: 10
func (o *BlockBasedTableOptions) SetBlockSizeDeviation(blockSizeDeviation int) {
	C.rocksdb_block_based_options_set_block_size_deviation(o.c, C.int(blockSizeDeviation))
}

// SetBlockRestartInterval sets the number of keys between
// restart points for delta encoding of keys.
// This parameter can be changed dynamically. Most clients should
// leave this parameter alone.
// Default: 16
func (o *BlockBasedTableOptions) SetBlockRestartInterval(blockRestartInterval int) {
	C.rocksdb_block_based_options_set_block_restart_interval(o.c, C.int(blockRestartInterval))
}

// SetFilterPolicy sets the filter policy opts reduce disk reads.
// Many applications will benefit from passing the result of
// NewBloomFilterPolicy() here.
// Default: nil
func (o *BlockBasedTableOptions) SetFilterPolicy(fp FilterPolicy) {
	if nfp, ok := fp.(nativeFilterPolicy); ok {
		o.cFp = nfp.c
	} else {
		idx := registerFilterPolicy(fp)
		o.cFp = C.gorocksdb_filterpolicy_create(C.uintptr_t(idx))
	}
	C.rocksdb_block_based_options_set_filter_policy(o.c, o.cFp)
}

// SetNoBlockCache specify whether block cache should be used or not.
// Default: false
func (o *BlockBasedTableOptions) SetNoBlockCache(value bool) {
	C.rocksdb_block_based_options_set_no_block_cache(o.c, boolToChar(value))
}

// SetBlockCache sets the control over blocks (user data is soptsred in a set of blocks, and
// a block is the unit of reading from disk).
//
// If set, use the specified cache for blocks.
// If nil, rocksdb will auoptsmatically create and use an 8MB internal cache.
// Default: nil
func (o *BlockBasedTableOptions) SetBlockCache(cache *Cache) {
	o.cache = cache
	C.rocksdb_block_based_options_set_block_cache(o.c, cache.c)
}

// SetBlockCacheCompressed sets the cache for compressed blocks.
// If nil, rocksdb will not use a compressed block cache.
// Default: nil
func (o *BlockBasedTableOptions) SetBlockCacheCompressed(cache *Cache) {
	o.compCache = cache
	C.rocksdb_block_based_options_set_block_cache_compressed(o.c, cache.c)
}

// SetWholeKeyFiltering specify if whole keys in the filter (not just prefixes)
// should be placed.
// This must generally be true for gets opts be efficient.
// Default: true
func (o *BlockBasedTableOptions) SetWholeKeyFiltering(value bool) {
	C.rocksdb_block_based_options_set_whole_key_filtering(o.c, boolToChar(value))
}

// SetCacheIndexAndFilterBlock indicates if we'd put index/filter blocks to
// the block cache. If not specified, each "table reader" object will pre-load
// index/filter block during table initialization.
// Default: false
func (o *BlockBasedTableOptions) SetCacheIndexAndFilterBlocks(val bool) {
	var value uint8
	if val {
		value = 1
	}
	C.rocksdb_block_based_options_set_cache_index_and_filter_blocks(
		o.c,
		C.uchar(value),
	)
}
