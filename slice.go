package gorocksdb

// #include <stdlib.h>
import "C"
import "unsafe"

// Slice is used as a wrapper for non-copy values
type Slice struct {
	data  *C.char
	size  C.size_t
	freed bool
}

// newSlice returns a slice with the given data.
func newSlice(data *C.char, size C.size_t) *Slice {
	return &Slice{data, size, false}
}

// Data returns the data of the slice.
func (s *Slice) Data() []byte {
	return charToByte(s.data, s.size)
}

// Size returns the size of the data.
func (s *Slice) Size() int {
	return int(s.size)
}

// Release frees the slice data.
func (s *Slice) Release() {
	if !s.freed {
		C.free(unsafe.Pointer(s.data))
		s.freed = true
	}
}
