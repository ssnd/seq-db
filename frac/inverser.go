package frac

import (
	"unsafe"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/seq"
)

type inverser struct {
	buf       *bytespool.Buffer
	values    []uint32
	inversion []int
}

func newInverser(values []uint32, minMID, maxMID seq.MID, mids []uint64) *inverser {
	// skip greater than maxMID
	l := 0
	for l < len(values) && mids[values[l]] > uint64(maxMID) {
		l++
	}

	// skip less than minMID
	r := len(values) - 1
	for r >= 0 && mids[values[r]] < uint64(minMID) {
		r--
	}

	if r < l {
		// we should never end up here
		panic("can't search on empty fraction")
	}

	// build inverse map
	values = values[l : r+1]
	buf, inversion := getSlice(len(mids))
	for i, v := range values {
		inversion[v] = i + 1
	}

	return &inverser{
		buf:       buf,
		values:    values,
		inversion: inversion,
	}
}

func (is *inverser) Len() int {
	// Explanation of "plus one" in len:
	//
	// 1. Inverser responsible for a mapping, for example:
	// 1 2 3 4 5 - external lids
	// 6 5 3 2 1 - internal lids
	//
	// 2. But we consider that there is unused virtual zero item:
	//   0     1 2 3 4 5 - external lids
	// unused  6 5 3 2 1 - internal lids
	//
	// 3. Actual representation in values field is:
	// 0 1 2 3 4 - keys
	// 6 5 3 2 1 - values
	//
	// Thats why we need plus one to len(values),
	// accounting virtual zero lid
	return len(is.values) + 1
}

func (is *inverser) Inverse(k uint32) (int, bool) {
	if int(k) >= len(is.inversion) {
		return 0, false
	}
	v := is.inversion[k]
	return v, v > 0
}

func (is *inverser) Revert(i uint32) uint32 {
	return is.values[i-1]
}

func (is *inverser) Release() {
	bytespool.Release(is.buf)
	is.buf = nil
}

func getSlice(size int) (*bytespool.Buffer, []int) {
	const sizeOfInt = unsafe.Sizeof(int(0))
	buf := bytespool.Acquire(size * int(sizeOfInt))
	s := unsafe.Slice((*int)(unsafe.Pointer(unsafe.SliceData(buf.B))), size)
	clear(s)
	return buf, s
}
