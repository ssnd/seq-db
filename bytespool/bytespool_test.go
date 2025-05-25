//go:build !race

package bytespool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReleaseByIndex(t *testing.T) {
	test := func(ln, expectedCapacity, expectedReleaseIndex int) {
		t.Helper()
		pool := New()

		a := pool.Acquire(ln)
		require.Equal(t, expectedCapacity, cap(a.B))
		pool.Release(a)

		if expectedReleaseIndex == -1 {
			b := pool.Acquire(ln)
			if a == b {
				t.Fatalf("buffers pointer must not be the same")
			}
		} else {
			b := pool.pools[expectedReleaseIndex].Get().(*Buffer)
			if a != b {
				t.Fatalf("buffers pointer must be the same")
			}
		}
	}

	test(0, 256, 0)
	test(1, 256, 0)
	test(16, 256, 0)
	test(17, 256, 0)
	test(31, 256, 0)
	test(32, 256, 0)
	test(255, 256, 0)
	test(256, 256, 0)

	test(257, 512, 1)
	test(300, 512, 1)
	test(511, 512, 1)
	test(512, 512, 1)

	test(513, 1024, 2)
	test(888, 1024, 2)
	test(1023, 1024, 2)
	test(1024, 1024, 2)
	test(1024, 1024, 2)

	test(1025, 2048, 3)
	test(1555, 2048, 3)
	test(2048, 2048, 3)

	test(maxCapacity-1, maxCapacity, pools-1)
	test(maxCapacity, maxCapacity, pools-1)
	test(maxCapacity+1, maxCapacity+1, -1)

	test(1<<30, 1<<30, 22)
	test(1<<30-42, 1<<30, 22)

	test(1<<31, 1<<31, 23)
	test(1<<31-1, 1<<31, 23)
	test((1<<30)+1, 1<<31, 23)
}

func TestReleaseWithUnexpectedCapacity(t *testing.T) {
	test := func(capacity, expectedReleaseIndex int) {
		t.Helper()
		pool := New()
		expected := &Buffer{B: make([]byte, 0, capacity)}
		pool.Release(expected)
		got := pool.pools[expectedReleaseIndex].Get().(*Buffer)
		if expected != got {
			t.Fatalf("buffers pointer must be the same")
		}
	}

	test(0, 0)
	test(255, 0)
	test(257, 0)
	test(300, 0)
	test(511, 0)

	test(513, 1)
	test(550, 1)
	test(1000, 1)
	test(1023, 1)

	test(1024, 2)
	test(1025, 2)

	test((1<<30)-1, 21)
	test(1<<30, 22)
	test((1<<30)+1, 22)
	test(1<<31, 23)
}
