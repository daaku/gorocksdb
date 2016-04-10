package gorocksdb

import (
	"io/ioutil"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestCFOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestCFOpen")
	ensure.Nil(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err := OpenDBCFs(opts, dir, givenNames, []*Options{opts, opts})
	ensure.Nil(t, err)
	defer db.Release()
	ensure.DeepEqual(t, len(cfh), 2)
	cfh[0].Release()
	cfh[1].Release()

	actualNames, err := ListCFs(opts, dir)
	ensure.Nil(t, err)
	ensure.SameElements(t, actualNames, givenNames)
}

func TestCFCreateDrop(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestCFCreate")
	ensure.Nil(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, err := OpenDB(opts, dir)
	ensure.Nil(t, err)
	defer db.Release()
	cf, err := db.CreateCF(opts, "guide")
	ensure.Nil(t, err)
	defer cf.Release()

	actualNames, err := ListCFs(opts, dir)
	ensure.Nil(t, err)
	ensure.SameElements(t, actualNames, []string{"default", "guide"})

	ensure.Nil(t, db.DropCF(cf))

	actualNames, err = ListCFs(opts, dir)
	ensure.Nil(t, err)
	ensure.SameElements(t, actualNames, []string{"default"})
}

func TestCFBatchPutGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestCFPutGet")
	ensure.Nil(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err := OpenDBCFs(opts, dir, givenNames, []*Options{opts, opts})
	ensure.Nil(t, err)
	defer db.Release()
	ensure.DeepEqual(t, len(cfh), 2)
	defer cfh[0].Release()
	defer cfh[1].Release()

	wo := NewDefaultWriteOptions()
	defer wo.Release()
	ro := NewDefaultReadOptions()
	defer ro.Release()

	givenKey0 := []byte("hello0")
	givenVal0 := []byte("world0")
	givenKey1 := []byte("hello1")
	givenVal1 := []byte("world1")

	b0 := NewWriteBatch()
	defer b0.Release()
	b0.PutCF(cfh[0], givenKey0, givenVal0)
	ensure.Nil(t, db.Write(wo, b0))
	actualVal0, err := db.GetCF(ro, cfh[0], givenKey0)
	defer actualVal0.Release()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal0.Data(), givenVal0)

	b1 := NewWriteBatch()
	defer b1.Release()
	b1.PutCF(cfh[1], givenKey1, givenVal1)
	ensure.Nil(t, db.Write(wo, b1))
	actualVal1, err := db.GetCF(ro, cfh[1], givenKey1)
	defer actualVal1.Release()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal1.Data(), givenVal1)

	actualVal, err := db.GetCF(ro, cfh[0], givenKey1)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)
	actualVal, err = db.GetCF(ro, cfh[1], givenKey0)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)
}

func TestCFPutGetDelete(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestCFPutGet")
	ensure.Nil(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err := OpenDBCFs(opts, dir, givenNames, []*Options{opts, opts})
	ensure.Nil(t, err)
	defer db.Release()
	ensure.DeepEqual(t, len(cfh), 2)
	defer cfh[0].Release()
	defer cfh[1].Release()

	wo := NewDefaultWriteOptions()
	defer wo.Release()
	ro := NewDefaultReadOptions()
	defer ro.Release()

	givenKey0 := []byte("hello0")
	givenVal0 := []byte("world0")
	givenKey1 := []byte("hello1")
	givenVal1 := []byte("world1")

	ensure.Nil(t, db.PutCF(wo, cfh[0], givenKey0, givenVal0))
	actualVal0, err := db.GetCF(ro, cfh[0], givenKey0)
	defer actualVal0.Release()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal0.Data(), givenVal0)

	ensure.Nil(t, db.PutCF(wo, cfh[1], givenKey1, givenVal1))
	actualVal1, err := db.GetCF(ro, cfh[1], givenKey1)
	defer actualVal1.Release()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal1.Data(), givenVal1)

	actualVal, err := db.GetCF(ro, cfh[0], givenKey1)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)
	actualVal, err = db.GetCF(ro, cfh[1], givenKey0)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)

	ensure.Nil(t, db.DeleteCF(wo, cfh[0], givenKey0))
	actualVal, err = db.GetCF(ro, cfh[0], givenKey0)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)
}
