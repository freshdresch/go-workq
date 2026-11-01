package diagnostics

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	diagnosticvalues "github.com/freshdresch/go-data-structures/workq/diagnostics"
)

func TestDiagnosticsLayout(t *testing.T) {
	var counters CounterShard
	var timing TimingShard
	producerPayload := unsafe.Sizeof(counters.Producer.SideCounters)
	consumerPayload := producerPayload + 2*unsafe.Sizeof(AtomicSizeHistogram{})
	timingPayload := unsafe.Offsetof(timing.Producer.ParkSelectDuration) +
		unsafe.Sizeof(timing.Producer.ParkSelectDuration)
	assert.GreaterOrEqual(
		t,
		unsafe.Offsetof(counters.Consumer)-producerPayload,
		uintptr(128),
	)
	assert.GreaterOrEqual(
		t,
		unsafe.Sizeof(counters)-unsafe.Offsetof(counters.Consumer)-consumerPayload,
		uintptr(128),
	)
	assert.GreaterOrEqual(
		t,
		unsafe.Offsetof(timing.Consumer)-timingPayload,
		uintptr(128),
	)
	assert.GreaterOrEqual(
		t,
		unsafe.Sizeof(timing)-unsafe.Offsetof(timing.Consumer)-timingPayload,
		uintptr(128),
	)
	assert.Zero(t, unsafe.Sizeof(counters.Producer)%128)
	assert.Zero(t, unsafe.Sizeof(counters.Consumer)%128)
	assert.Zero(t, unsafe.Sizeof(timing.Producer)%128)
	t.Logf(
		"counter shard=%d timing shard=%d descriptor=%d",
		unsafe.Sizeof(counters),
		unsafe.Sizeof(timing),
		unsafe.Sizeof(Storage{}),
	)
	if runtime.GOARCH != "amd64" {
		return
	}
	assert.Equal(t, uintptr(8), unsafe.Sizeof(uintptr(0)))
	assert.Equal(t, uintptr(8), unsafe.Sizeof(int(0)))
	assert.Equal(t, uintptr(8), unsafe.Sizeof(counters.Producer.WaitStarts))
	assert.Equal(t, uintptr(24), unsafe.Sizeof([]int{}))
	assert.Equal(t, uintptr(517*8), producerPayload)
	assert.Equal(t, uintptr(641*8), consumerPayload)
	assert.Equal(t, uintptr(4352), unsafe.Sizeof(counters.Producer))
	assert.Equal(t, uintptr(5376), unsafe.Sizeof(counters.Consumer))
	assert.Equal(t, uintptr(9728), unsafe.Sizeof(counters))
	assert.Equal(t, uintptr(3968), unsafe.Sizeof(timing.Producer))
	assert.Equal(t, uintptr(7936), unsafe.Sizeof(timing))
	assert.Equal(t, uintptr(80), unsafe.Sizeof(Storage{}))
}

func TestDiagnosticsLayoutAllocationOverflow(t *testing.T) {
	for _, mode := range []diagnosticvalues.DiagnosticsMode{
		diagnosticvalues.DiagnosticsCounters, diagnosticvalues.DiagnosticsTiming,
	} {
		cfg := diagnosticvalues.DiagnosticsConfig{Mode: mode, Shards: int(^uint(0) >> 1)}
		_, ok := AllocationSize(cfg)
		assert.False(t, ok)
		stride := unsafe.Sizeof(CounterShard{})
		if mode == diagnosticvalues.DiagnosticsTiming {
			stride += unsafe.Sizeof(TimingShard{})
		}
		maxInt := uintptr(^uint(0) >> 1)
		limit := (maxInt - unsafe.Sizeof(Storage{})) / stride
		cfg.Shards = int(limit)
		size, ok := AllocationSize(cfg)
		require.True(t, ok)
		assert.LessOrEqual(t, size, maxInt)
		cfg.Shards++
		_, ok = AllocationSize(cfg)
		assert.False(t, ok)
	}
}

func TestDiagnosticsStorageInitialization(t *testing.T) {
	for _, mode := range []diagnosticvalues.DiagnosticsMode{
		diagnosticvalues.DiagnosticsCounters, diagnosticvalues.DiagnosticsTiming,
	} {
		for _, shards := range []int{1, 16, 256, 512} {
			cfg := diagnosticvalues.DiagnosticsConfig{Mode: mode, Shards: shards}
			storage := New(cfg)
			for i := range storage.Counters {
				shard := &storage.Counters[i]
				for _, side := range []*SideCounters{
					&shard.Producer.SideCounters,
					&shard.Consumer.SideCounters,
				} {
					assert.Equal(t, ^uint64(0), side.RequestedSize.Min.Load())
					assert.Equal(t, ^uint64(0), side.MovedSize.Min.Load())
					for j := range side.Capacity {
						assert.Equal(t, ^uint64(0), side.Capacity[j].Min.Load())
					}
				}
				assert.Equal(t, ^uint64(0), shard.Consumer.GroupTarget.Min.Load())
				assert.Equal(t, ^uint64(0), shard.Consumer.GroupActualSize.Min.Load())
			}
			for i := range storage.Timing {
				shard := &storage.Timing[i]
				for _, side := range []*SideTiming{&shard.Producer, &shard.Consumer} {
					for j := range side.WaitDuration {
						assert.Equal(t, ^uint64(0), side.WaitDuration[j].Min.Load())
					}
					for j := range side.ParkSelectDuration {
						assert.Equal(t, ^uint64(0), side.ParkSelectDuration[j].Min.Load())
					}
				}
			}
			stride := unsafe.Sizeof(CounterShard{})
			if mode == diagnosticvalues.DiagnosticsTiming {
				stride += unsafe.Sizeof(TimingShard{})
			}
			size, ok := AllocationSize(cfg)
			require.True(t, ok)
			assert.Equal(t, unsafe.Sizeof(Storage{})+uintptr(cfg.Shards)*stride, size)
		}
	}
}
