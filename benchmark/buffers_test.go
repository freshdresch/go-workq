package benchmark

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBuffers(t *testing.T) {
	value := &item{value: 1}
	for _, size := range []int{1, 32} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			t.Run("ptr", func(t *testing.T) { checkNewBuffers(t, size, value) })
			t.Run("int64", func(t *testing.T) { checkNewBuffers(t, size, int64(1)) })
			t.Run("bytes3", func(t *testing.T) { checkNewBuffers(t, size, [3]byte{1, 2, 3}) })
		})
	}
}

// checkNewBuffers checks shape, zeroing, independence, and that no two occupied buffers from
// two helper calls touch a common 64- or 128-byte line.
func checkNewBuffers[T comparable](t *testing.T, size int, value T) {
	t.Helper()
	const count = 24
	calls := [2][][]T{newBuffers[T](count, size), newBuffers[T](count, size)}
	var zero T
	var buffers [][]T
	for _, call := range calls {
		require.Len(t, call, count)
		buffers = append(buffers, call...)
		for _, buffer := range call {
			require.Len(t, buffer, size)
			assert.Equal(t, size, cap(buffer))
			for _, element := range buffer {
				assert.Equal(t, zero, element)
			}
		}
	}

	for i, buffer := range buffers {
		for j := range buffer {
			buffer[j] = value
		}
		for j, other := range buffers {
			expected := zero
			if j <= i {
				expected = value
			}
			for _, element := range other {
				require.Equal(t, expected, element, "write to buffer %d affected buffer %d", i, j)
			}
		}
	}

	elementSize := unsafe.Sizeof(zero)
	for _, width := range []uintptr{64, 128} {
		for i, buffer := range buffers {
			first := uintptr(unsafe.Pointer(unsafe.SliceData(buffer)))
			last := first + uintptr(len(buffer))*elementSize - 1
			for j := range i {
				otherFirst := uintptr(unsafe.Pointer(unsafe.SliceData(buffers[j])))
				otherLast := otherFirst + uintptr(len(buffers[j]))*elementSize - 1
				assert.True(
					t,
					last/width < otherFirst/width || otherLast/width < first/width,
					"buffers %d and %d share an occupied %d-byte line", i, j, width,
				)
			}
		}
	}
	runtime.KeepAlive(calls)
}

func TestNewBuffersEmptyAndZeroSized(t *testing.T) {
	assert.Empty(t, newBuffers[int64](0, 1))
	assert.Empty(t, newBuffers[int64](0, -1))
	for _, buffer := range newBuffers[int64](3, 0) {
		assert.Empty(t, buffer)
		assert.Zero(t, cap(buffer))
	}
	for _, buffer := range newBuffers[struct{}](3, 4) {
		assert.Len(t, buffer, 4)
		assert.Equal(t, 4, cap(buffer))
	}
}
