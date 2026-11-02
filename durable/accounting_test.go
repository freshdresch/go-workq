package durable

import (
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func expectedAllocation(size int) Bytes {
	if size == 0 {
		return 0
	}
	return Bytes(2 * ((size + 7) / 8) * 8)
}

func expectedSubmit(entries []Entry) Bytes {
	charge := Bytes(256) + expectedAllocation(64*len(entries)) +
		expectedAllocation(len(entries)) + expectedAllocation(16*len(entries))
	payload, largestCodec := 0, 0
	for _, entry := range entries {
		payload += len(entry.Payload)
		charge += expectedAllocation(len(entry.Codec))
		largestCodec = max(largestCodec, len(entry.Codec))
	}
	return charge + expectedAllocation(payload) + expectedAllocation(largestCodec)
}

func TestAllocationBoundaries(t *testing.T) {
	t.Parallel()
	for _, size := range []int{
		0, 1, 7, 8, 9, 511, 512, 513, 8191, 8192, 8193,
		16383, 16384, 16385, 32759, 32760, 32761, 32768, 32769,
		(2 << 20) - 1, 2 << 20, (2 << 20) + 1,
	} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			charge, err := allocationCharge(Bytes(size))
			require.NoError(t, err)
			assert.Equal(t, expectedAllocation(size), charge)
			assert.GreaterOrEqual(t, charge, Bytes(size))
		})
	}
	for _, size := range []Bytes{math.MaxUint64, math.MaxUint64 - 7, Bytes(math.MaxInt) + 1} {
		_, err := allocationCharge(size)
		assert.ErrorIs(t, err, ErrInvalidInput)
	}
	_, err := checkedAdd(math.MaxUint64, 1)
	assert.ErrorIs(t, err, ErrInvalidInput)
	_, err = checkedMultiply(math.MaxUint64, 2)
	assert.ErrorIs(t, err, ErrInvalidInput)
	_, err = submitCharge(math.MaxUint64, 0, 0, 0)
	assert.ErrorIs(t, err, ErrInvalidInput)
	_, err = submitCharge(0, 0, math.MaxUint64, 0)
	assert.ErrorIs(t, err, ErrInvalidInput)
	_, err = lookupCharge(math.MaxUint64)
	assert.ErrorIs(t, err, ErrInvalidInput)
	policy := testPolicy()
	policy.MaxPayload = math.MaxUint64
	_, err = maximumSubmitCharge(policy, BatchLimits{SubmitItems: 2, SubmitEncoded: math.MaxUint64})
	assert.ErrorIs(t, err, ErrInvalidInput)
}

func TestCommandAllocationLayouts(t *testing.T) {
	t.Parallel()
	assert.LessOrEqual(t, unsafe.Sizeof(Entry{}), uintptr(64))
	assert.LessOrEqual(t, unsafe.Sizeof(ID{}), uintptr(24))
	assert.LessOrEqual(t, unsafe.Sizeof(Status{}), uintptr(40))
	assert.Equal(t, uintptr(1), unsafe.Sizeof(false))
	assert.Equal(t, uintptr(16), unsafe.Sizeof([16]byte{}))
	assert.Less(t, unsafe.Sizeof(Queue{})+128*unsafe.Sizeof(command{}), uintptr(1<<20))
	queue := &Queue{config: testConfig(t)}
	for _, codecSize := range []int{0, 1, 7, 8, 9, 63, 64} {
		for _, count := range []int{0, 1, 256} {
			entries := make([]Entry, count)
			for index := range entries {
				entries[index] = Entry{ID: ID{Sequence: uint64(index)},
					Codec: strings.Repeat("x", codecSize), Payload: make([]byte, index%17)}
			}
			allocation, err := queue.validateSubmit(entries)
			require.NoError(t, err)
			assert.Equal(t, expectedSubmit(entries), allocation.charge)
			copies := copyEntries(entries, allocation.payload)
			assert.Len(t, copies, len(entries))
			for index := range copies {
				assert.Equal(t, entries[index], copies[index])
			}
			for _, entry := range copies {
				assert.Equal(t, len(entry.Payload), cap(entry.Payload))
			}
		}
	}
	// This arena approaches the encoded-work ceiling with maximum-count mixed records.
	entries := make([]Entry, 256)
	for index := range entries {
		entries[index] = Entry{ID: ID{Sequence: uint64(index)},
			Payload: make([]byte, 8000+index%17), Codec: "x"}
	}
	allocation, err := queue.validateSubmit(entries)
	require.NoError(t, err)
	assert.Equal(t, expectedSubmit(entries), allocation.charge)
	for _, count := range []int{0, 1, 7, 8, 9, 255, 256} {
		charge, err := lookupCharge(Bytes(count))
		require.NoError(t, err)
		assert.Equal(t, Bytes(256)+expectedAllocation(24*count)+expectedAllocation(40*count), charge)
	}
}

// Fixed iterations and retained escaping outputs measure allocation classes without
// automatic benchmark calibration, a forced GC, or a heap-size oracle.
func TestActualAllocationCharges(t *testing.T) {
	const iterations = 64
	t.Run("arena8193", func(t *testing.T) {
		outputs := make([][]byte, iterations)
		allocated := allocationProbe(func(index int) { outputs[index] = make([]byte, 8193) })
		assert.LessOrEqual(t, allocated, uint64(expectedAllocation(8193)))
		t.Logf("arena: %d bytes/op, allowance %d", allocated, expectedAllocation(8193))
		runtime.KeepAlive(outputs)
	})
	t.Run("pointerEntryArray", func(t *testing.T) {
		outputs := make([][]Entry, iterations)
		allocated := allocationProbe(func(index int) { outputs[index] = make([]Entry, 256) })
		assert.LessOrEqual(t, allocated, uint64(expectedAllocation(64*256)))
		t.Logf("Entry array: %d bytes/op, allowance %d", allocated, expectedAllocation(64*256))
		runtime.KeepAlive(outputs)
	})
	t.Run("mixedCopy", func(t *testing.T) {
		entries := []Entry{
			{Codec: "x", Payload: make([]byte, 8193)},
			{Codec: strings.Repeat("c", 63), Payload: make([]byte, 9)},
			{Codec: "seven77", Payload: make([]byte, 511)},
		}
		outputs := make([][]Entry, iterations)
		allocated := allocationProbe(func(index int) { outputs[index] = copyEntries(entries, 8713) })
		allowance := expectedSubmit(entries)
		assert.LessOrEqual(t, allocated, uint64(allowance))
		t.Logf("mixed copy: %d bytes/op, reservation %d", allocated, allowance)
		runtime.KeepAlive(outputs)
	})
}

func allocationProbe(allocate func(int)) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for index := range 64 {
		allocate(index)
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc + 63) / 64
}
