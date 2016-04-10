package gorocksdb_test

import (
	"fmt"

	"github.com/daaku/gorocksdb"
)

func Example() {
	// Most APIs in RocksDB use a pattern of taking an "options" object to
	// control various aspects of the API. These are the options for opening the
	// DB.
	opts := gorocksdb.NewOptions()
	defer opts.Release()
	opts.SetCreateIfMissing(true)

	// Note the use of the "Release" method everywhere. Since we're using a C API
	// underneath, we need to manually release memory. Using defer works best,
	// and remember to first check for errors.
	db, err := gorocksdb.OpenDB(opts, "/tmp/gorocksdb-example")
	if err != nil {
		panic(err)
	}
	defer db.Release()

	wo := gorocksdb.NewWriteOptions()
	defer wo.Release()

	err = db.Put(wo, []byte("foo"), []byte("bar"))
	if err != nil {
		panic(err)
	}

	ro := gorocksdb.NewReadOptions()
	defer ro.Release()

	value, err := db.Get(ro, []byte("foo"))
	if err != nil {
		panic(err)
	}
	defer value.Release()

	fmt.Printf("%s", value.Data())

	// Output: bar
}
