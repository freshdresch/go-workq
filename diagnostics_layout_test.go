package workq

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsLayout(t *testing.T) {
	var counters diagnosticsCounterShard
	var timing diagnosticsTimingShard
	producerPayload := unsafe.Sizeof(counters.producer.diagnosticsSideCounters)
	consumerPayload := producerPayload + 2*unsafe.Sizeof(atomicSizeHistogram{})
	timingPayload := unsafe.Offsetof(timing.producer.parkSelectDuration) +
		unsafe.Sizeof(timing.producer.parkSelectDuration)
	assert.GreaterOrEqual(
		t,
		unsafe.Offsetof(counters.consumer)-producerPayload,
		uintptr(128),
	)
	assert.GreaterOrEqual(
		t,
		unsafe.Sizeof(counters)-unsafe.Offsetof(counters.consumer)-consumerPayload,
		uintptr(128),
	)
	assert.GreaterOrEqual(
		t,
		unsafe.Offsetof(timing.consumer)-timingPayload,
		uintptr(128),
	)
	assert.GreaterOrEqual(
		t,
		unsafe.Sizeof(timing)-unsafe.Offsetof(timing.consumer)-timingPayload,
		uintptr(128),
	)
	assert.Zero(t, unsafe.Sizeof(counters.producer)%128)
	assert.Zero(t, unsafe.Sizeof(counters.consumer)%128)
	assert.Zero(t, unsafe.Sizeof(timing.producer)%128)
	t.Logf(
		"counter shard=%d timing shard=%d descriptor=%d Queue=%d",
		unsafe.Sizeof(counters),
		unsafe.Sizeof(timing),
		unsafe.Sizeof(diagnostics{}),
		unsafe.Sizeof(Queue[int]{}),
	)
	if runtime.GOARCH != "amd64" {
		return
	}
	assert.Equal(t, uintptr(8), unsafe.Sizeof(uintptr(0)))
	assert.Equal(t, uintptr(8), unsafe.Sizeof(int(0)))
	assert.Equal(t, uintptr(8), unsafe.Sizeof(counters.producer.waitStarts))
	assert.Equal(t, uintptr(24), unsafe.Sizeof([]int{}))
	assert.Equal(t, uintptr(24), unsafe.Sizeof(WaitStrategy{}))
	assert.Equal(t, uintptr(517*8), producerPayload)
	assert.Equal(t, uintptr(641*8), consumerPayload)
	assert.Equal(t, uintptr(4352), unsafe.Sizeof(counters.producer))
	assert.Equal(t, uintptr(5376), unsafe.Sizeof(counters.consumer))
	assert.Equal(t, uintptr(9728), unsafe.Sizeof(counters))
	assert.Equal(t, uintptr(3968), unsafe.Sizeof(timing.producer))
	assert.Equal(t, uintptr(7936), unsafe.Sizeof(timing))
	assert.Equal(t, uintptr(72), unsafe.Sizeof(diagnostics{}))
	var queue Queue[int]
	assert.Equal(t, uintptr(816), unsafe.Sizeof(queue))
	assert.Equal(t, uintptr(32), unsafe.Offsetof(queue.consumerWakeCap))
	assert.Equal(t, uintptr(36), unsafe.Offsetof(queue.capacity))
	assert.Equal(t, uintptr(40), unsafe.Offsetof(queue.consumerWait))
	assert.Equal(t, uintptr(88), unsafe.Offsetof(queue.diagnostics))
	assert.Equal(t, uintptr(256), unsafe.Offsetof(queue.consumers))
	assert.Equal(t, uintptr(600), unsafe.Offsetof(queue.producers))
}

func TestDiagnosticsLayoutAllocationOverflow(t *testing.T) {
	for _, mode := range []DiagnosticsMode{DiagnosticsCounters, DiagnosticsTiming} {
		cfg := DiagnosticsConfig{Mode: mode, Shards: int(^uint(0) >> 1)}
		_, ok := diagnosticsAllocationSize(cfg)
		assert.False(t, ok)
		stride := unsafe.Sizeof(diagnosticsCounterShard{})
		if mode == DiagnosticsTiming {
			stride += unsafe.Sizeof(diagnosticsTimingShard{})
		}
		maxInt := uintptr(^uint(0) >> 1)
		limit := (maxInt - unsafe.Sizeof(diagnostics{})) / stride
		cfg.Shards = int(limit)
		size, ok := diagnosticsAllocationSize(cfg)
		require.True(t, ok)
		assert.LessOrEqual(t, size, maxInt)
		cfg.Shards++
		_, ok = diagnosticsAllocationSize(cfg)
		assert.False(t, ok)
	}
}
