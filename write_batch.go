package gorocksdb

// #include "rocksdb/c.h"
import "C"
import "io"

// WriteBatch is a batching of Puts, Merges and Deletes.
type WriteBatch struct {
	c *C.rocksdb_writebatch_t
}

// NewWriteBatch create a WriteBatch object.
func NewWriteBatch() *WriteBatch {
	return newNativeWriteBatch(C.rocksdb_writebatch_create())
}

// newNativeWriteBatch create a WriteBatch object.
func newNativeWriteBatch(c *C.rocksdb_writebatch_t) *WriteBatch {
	return &WriteBatch{c}
}

// WriteBatchFrom creates a write batch from a serialized WriteBatch.
func WriteBatchFrom(data []byte) *WriteBatch {
	return newNativeWriteBatch(C.rocksdb_writebatch_create_from(byteToChar(data), C.size_t(len(data))))
}

// Put queues a key-value pair.
func (w *WriteBatch) Put(key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_put(w.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// PutCF queues a key-value pair in a column family.
func (w *WriteBatch) PutCF(cf *CF, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_put_cf(w.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Merge queues a merge of "value" with the existing value of "key".
func (w *WriteBatch) Merge(key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_merge(w.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// MergeCF queues a merge of "value" with the existing value of "key" in a
// column family.
func (w *WriteBatch) MergeCF(cf *CF, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_merge_cf(w.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Delete queues a deletion of the data at key.
func (w *WriteBatch) Delete(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_delete(w.c, cKey, C.size_t(len(key)))
}

// DeleteCF queues a deletion of the data at key in a column family.
func (w *WriteBatch) DeleteCF(cf *CF, key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_delete_cf(w.c, cf.c, cKey, C.size_t(len(key)))
}

// Data returns the serialized version of this batch.
func (w *WriteBatch) Data() []byte {
	var cSize C.size_t
	cValue := C.rocksdb_writebatch_data(w.c, &cSize)
	return charToByte(cValue, cSize)
}

// Count returns the number of updates in the batch.
func (w *WriteBatch) Count() int {
	return int(C.rocksdb_writebatch_count(w.c))
}

// NewIterator returns a iterator to iterate over the records in the batch.
func (w *WriteBatch) NewIterator() *WriteBatchIterator {
	data := w.Data()
	if len(data) < 8+4 {
		return &WriteBatchIterator{}
	}
	return &WriteBatchIterator{data: data[12:]}
}

// Clear removes all the enqueued Put and Deletes.
func (w *WriteBatch) Clear() {
	C.rocksdb_writebatch_clear(w.c)
}

// Destroy deallocates the WriteBatch object.
func (w *WriteBatch) Destroy() {
	C.rocksdb_writebatch_destroy(w.c)
	w.c = nil
}

// WriteBatchRecordType describes the type of a batch record.
type WriteBatchRecordType byte

// Types of batch records.
const (
	WriteBatchRecordTypeDeletion WriteBatchRecordType = 0x0
	WriteBatchRecordTypeValue    WriteBatchRecordType = 0x1
	WriteBatchRecordTypeMerge    WriteBatchRecordType = 0x2
	WriteBatchRecordTypeLogData  WriteBatchRecordType = 0x3
)

// WriteBatchRecord represents a record inside a WriteBatch.
type WriteBatchRecord struct {
	Key   []byte
	Value []byte
	Type  WriteBatchRecordType
}

// WriteBatchIterator represents a iterator to iterator over records.
type WriteBatchIterator struct {
	data   []byte
	record WriteBatchRecord
	err    error
}

// Next returns the next record.
// Returns false if no further record exists.
func (i *WriteBatchIterator) Next() bool {
	if i.err != nil || len(i.data) == 0 {
		return false
	}
	// reset the current record
	i.record.Key = nil
	i.record.Value = nil

	// parse the record type
	recordType := WriteBatchRecordType(i.data[0])
	i.record.Type = recordType
	i.data = i.data[1:]

	// parse the key
	x, n := i.decodeVarint(i.data)
	if n == 0 {
		i.err = io.ErrShortBuffer
		return false
	}
	k := n + int(x)
	i.record.Key = i.data[n:k]
	i.data = i.data[k:]

	// parse the data
	if recordType == WriteBatchRecordTypeValue || recordType == WriteBatchRecordTypeMerge {
		x, n := i.decodeVarint(i.data)
		if n == 0 {
			i.err = io.ErrShortBuffer
			return false
		}
		k := n + int(x)
		i.record.Value = i.data[n:k]
		i.data = i.data[k:]
	}
	return true
}

// Record returns the current record.
func (i *WriteBatchIterator) Record() *WriteBatchRecord {
	return &i.record
}

// Error returns the error if the iteration is failed.
func (i *WriteBatchIterator) Error() error {
	return i.err
}

func (i *WriteBatchIterator) decodeVarint(buf []byte) (x uint64, n int) {
	// x, n already 0
	for shift := uint(0); shift < 64; shift += 7 {
		if n >= len(buf) {
			return 0, 0
		}
		b := uint64(buf[n])
		n++
		x |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			return x, n
		}
	}
	// The number is too large to represent in a 64-bit value.
	return 0, 0
}
