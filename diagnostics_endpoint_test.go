package workq

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsEndpointAssignment(t *testing.T) {
	for _, mode := range []DiagnosticsMode{DiagnosticsCounters, DiagnosticsTiming} {
		for _, shards := range []int{1, 4, 512} {
			t.Run(fmt.Sprintf("mode=%d/shards=%d", mode, shards), func(t *testing.T) {
				queue, err := New[int](2, WithDiagnostics(DiagnosticsConfig{
					Mode: mode, Shards: shards,
				}))
				require.NoError(t, err)
				before := queue.Diagnostics()
				first := queue.NewDiagnosticsEndpoint()
				assert.Same(t, queue, first.queue)
				assert.Zero(t, first.shard)
				copied := first
				assert.Equal(t, first, copied)
				assert.Equal(t, uint64(1), queue.diagnostics.creationTicket.Load())
				for i := 1; i < shards; i++ {
					endpoint := queue.NewDiagnosticsEndpoint()
					assert.Same(t, queue, endpoint.queue)
					assert.Equal(t, i, endpoint.shard)
				}
				alias := queue.NewDiagnosticsEndpoint()
				assert.Equal(t, first, alias)
				for i := 1; i <= shards; i++ {
					endpoint := queue.NewDiagnosticsEndpoint()
					assert.Equal(t, i%shards, endpoint.shard)
				}
				assert.Equal(t, alias, copied)
				assert.Equal(t, before, queue.Diagnostics())
				queue.Close()
				afterClose := queue.Diagnostics()
				closed := queue.NewDiagnosticsEndpoint()
				assert.Same(t, queue, closed.queue)
				assert.Equal(t, 1%shards, closed.shard)
				assert.Equal(t, afterClose, queue.Diagnostics())
			})
		}
	}
}

func TestDiagnosticsEndpointDisabledAssignment(t *testing.T) {
	queue, err := New[int](2)
	require.NoError(t, err)
	before := queue.Diagnostics()
	for range 3 {
		endpoint := queue.NewDiagnosticsEndpoint()
		assert.Same(t, queue, endpoint.queue)
		assert.Zero(t, endpoint.shard)
	}
	queue.Close()
	assert.Same(t, queue, queue.NewDiagnosticsEndpoint().queue)
	assert.Nil(t, queue.diagnostics)
	assert.Equal(t, before, queue.Diagnostics())
}

func TestDiagnosticsEndpointConcurrentAssignment(t *testing.T) {
	for _, shards := range []int{1, 16} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			queue, err := New[int](2, WithDiagnostics(DiagnosticsConfig{
				Mode: DiagnosticsTiming, Shards: shards,
			}))
			require.NoError(t, err)
			before := queue.Diagnostics()
			endpoints := make([]DiagnosticsEndpoint[int], shards*4)
			var workers sync.WaitGroup
			start := make(chan struct{})
			for i := range endpoints {
				workers.Go(func() {
					<-start
					endpoints[i] = queue.NewDiagnosticsEndpoint()
				})
			}
			close(start)
			workers.Wait()
			counts := make([]int, shards)
			for _, endpoint := range endpoints {
				assert.Same(t, queue, endpoint.queue)
				require.GreaterOrEqual(t, endpoint.shard, 0)
				require.Less(t, endpoint.shard, shards)
				counts[endpoint.shard]++
			}
			for _, count := range counts {
				assert.Equal(t, 4, count)
			}
			assert.Equal(t, uint64(len(endpoints)), queue.diagnostics.creationTicket.Load())
			assert.Equal(t, before, queue.Diagnostics())
		})
	}
}

func TestDiagnosticsEndpointTicketWrap(t *testing.T) {
	queue, err := New[int](2, WithDiagnostics(DiagnosticsConfig{
		Mode: DiagnosticsCounters, Shards: 4,
	}))
	require.NoError(t, err)
	queue.diagnostics.creationTicket.Store(^uint64(0) - 1)
	for _, want := range []int{2, 3, 0, 1, 2, 3} {
		assert.Equal(t, want, queue.NewDiagnosticsEndpoint().shard)
	}
	assert.Equal(t, uint64(4), queue.diagnostics.creationTicket.Load())
}

func TestDiagnosticsEndpointMask(t *testing.T) {
	maxInt := uint64(^uint(0) >> 1)
	for _, ticket := range []uint64{0, 1, maxInt, maxInt + 1, maxInt + 2, ^uint64(0)} {
		assert.Zero(t, diagnosticsShardIndex(ticket, 1))
		assert.Equal(t, int(ticket%16), diagnosticsShardIndex(ticket, 16))
	}
	if maxInt > 1<<32 {
		shards := uint64(1) << 33
		want := uint64(1)<<32 + 7
		assert.Equal(t, int(want), diagnosticsShardIndex(shards+want, int(shards)))
		assert.Equal(t, int(shards-1), diagnosticsShardIndex(^uint64(0), int(shards)))
	}
}

func TestDiagnosticsEndpointProducerWideAlias(t *testing.T) {
	for _, mode := range []DiagnosticsMode{DiagnosticsCounters, DiagnosticsTiming} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			queue, err := New[int](8, WithDiagnostics(DiagnosticsConfig{
				Mode: mode, Shards: 512,
			}))
			require.NoError(t, err)
			for range 300 {
				queue.NewDiagnosticsEndpoint()
			}
			endpoint := queue.NewDiagnosticsEndpoint()
			require.Equal(t, 300, endpoint.shard)
			for range 511 {
				queue.NewDiagnosticsEndpoint()
			}
			alias := queue.NewDiagnosticsEndpoint()
			require.Equal(t, endpoint, alias)
			for _, binding := range []DiagnosticsEndpoint[int]{endpoint, alias} {
				require.NoError(t, binding.Push(context.Background(), 1))
				require.NoError(t, binding.TryPush(2))
				moved, err := binding.PushBatch(context.Background(), []int{3})
				require.NoError(t, err)
				require.Equal(t, 1, moved)
				moved, err = binding.TryPushBatch([]int{4})
				require.NoError(t, err)
				require.Equal(t, 1, moved)
			}
			for index := range queue.diagnostics.counters {
				want := uint64(0)
				if index == 300 {
					want = 8
				}
				counters := &queue.diagnostics.counters[index].producer
				assert.Equal(t, want, counters.attempts[PhaseInitial].Load())
				assert.Equal(t, want, counters.movedSize.sum.Load())
			}
			snapshot := queue.Diagnostics().Producer
			assert.Equal(t, uint64(8), snapshot.Attempts[PhaseInitial])
			assert.Equal(t, uint64(8), snapshot.RequestedSize.Count)
			assert.Equal(t, uint64(8), snapshot.MovedSize.Sum)
			dst := make([]int, 8)
			moved, err := queue.TryReceive(dst)
			require.NoError(t, err)
			require.Equal(t, 8, moved)
			assert.Equal(t, []int{1, 2, 3, 4, 1, 2, 3, 4}, dst)
		})
	}
}
