package workq

import (
	"runtime"
	"testing"
	"unsafe"

	internaldiagnostics "github.com/freshdresch/go-data-structures/workq/internal/diagnostics"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsConfigAllocationPowers(t *testing.T) {
	for _, mode := range []DiagnosticsMode{DiagnosticsCounters, DiagnosticsTiming} {
		stride := unsafe.Sizeof(internaldiagnostics.CounterShard{})
		if mode == DiagnosticsTiming {
			stride += unsafe.Sizeof(internaldiagnostics.TimingShard{})
		}
		maxInt := uintptr(^uint(0) >> 1)
		limit := (maxInt - unsafe.Sizeof(diagnostics{})) / stride
		largestAllowed := 1
		for uintptr(largestAllowed) <= limit/2 {
			largestAllowed *= 2
		}
		cfg := DiagnosticsConfig{Mode: mode, Shards: largestAllowed}
		resolved, err := resolveDiagnostics(cfg)
		require.NoError(t, err)
		assert.Equal(t, largestAllowed, resolved.Shards)
		size, ok := diagnosticsAllocationSize(cfg)
		require.True(t, ok)
		assert.LessOrEqual(t, size, maxInt)

		largestIntPower := int((maxInt + 1) / 2)
		for _, shards := range []int{largestAllowed * 2, largestIntPower} {
			cfg.Shards = shards
			_, ok = diagnosticsAllocationSize(cfg)
			assert.False(t, ok)
			_, err = resolveDiagnostics(cfg)
			require.ErrorIs(t, err, ErrInvalidDiagnostics)
			queue, err := New[int](3, WithDiagnostics(cfg))
			assert.Nil(t, queue)
			require.ErrorIs(t, err, ErrInvalidDiagnostics)
			assert.NotSame(t, ErrInvalidDiagnostics, err)
			assert.ErrorContains(t, err, "diagnostics allocation size overflows int")
		}
	}
}
func TestDiagnosticsQueueLayout(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		return
	}
	assert.Equal(t, uintptr(24), unsafe.Sizeof(WaitStrategy{}))
	assert.Equal(t, uintptr(80), unsafe.Sizeof(diagnostics{}))
	assert.Equal(t, uintptr(16), unsafe.Sizeof(DiagnosticsEndpoint[int]{}))
	var queue Queue[int]
	assert.Equal(t, uintptr(816), unsafe.Sizeof(queue))
	assert.Equal(t, uintptr(32), unsafe.Offsetof(queue.consumerWakeCap))
	assert.Equal(t, uintptr(36), unsafe.Offsetof(queue.capacity))
	assert.Equal(t, uintptr(40), unsafe.Offsetof(queue.consumerWait))
	assert.Equal(t, uintptr(88), unsafe.Offsetof(queue.diagnostics))
	assert.Equal(t, uintptr(256), unsafe.Offsetof(queue.consumers))
	assert.Equal(t, uintptr(600), unsafe.Offsetof(queue.producers))
}
