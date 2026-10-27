package workq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring"
)

// These tests pin assumptions A1 and A4 of the design on each engine as New builds it. Each
// test sets the engine's cached view of the other side, either to nothing or to a partial
// count, before the other side moves. A zero-length burst or a burst that asks for more than
// the cache shows must then reload and report the fresh count.

const engineTestCapacity = 64

func newTestEngine(t *testing.T, options []Option) (*Queue[int], engine[int]) {
	t.Helper()
	queue, err := New[int](engineTestCapacity, options...)
	require.NoError(t, err)
	return queue, queue.engine
}

func enqueueAll(t *testing.T, eng engine[int], first int, count int) {
	t.Helper()
	for value := first; value < first+count; value++ {
		require.NoError(t, eng.Enqueue(value), "enqueue %d", value)
	}
}

func dequeueAll(t *testing.T, eng engine[int], first int, count int) {
	t.Helper()
	for want := first; want < first+count; want++ {
		value, err := eng.Dequeue()
		require.NoError(t, err, "dequeue %d", want)
		require.Equal(t, want, value)
	}
}

func TestEngineZeroLengthBurstIsFresh(t *testing.T) {
	const moved = 5
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue, eng := newTestEngine(t, engineCase.options)

			dequeued, available := eng.DequeueBurst(nil)
			require.Equal(t, 0, dequeued)
			require.Equal(t, 0, available)
			enqueueAll(t, eng, 0, moved)
			dequeued, available = eng.DequeueBurst(nil)
			assert.Equal(t, 0, dequeued, "zero-length dequeue moved items")
			assert.Equal(t, moved, available, "available after %d pushes", moved)

			enqueueAll(t, eng, moved, engineTestCapacity-moved)
			enqueued, free := eng.EnqueueBurst(nil)
			require.Equal(t, 0, enqueued)
			require.Equal(t, 0, free)
			dequeueAll(t, eng, 0, moved)
			enqueued, free = eng.EnqueueBurst(nil)
			assert.Equal(t, 0, enqueued, "zero-length enqueue moved items")
			assert.Equal(t, moved, free, "free after %d were taken", moved)

			assertIdleQueue(t, queue)
		})
	}
}

// shortBurstCase sets how many items or free slots the engine's cache shows before the other
// side moves the rest.
type shortBurstCase struct {
	name   string
	cached int
}

func TestEngineShortBurstMovesEverything(t *testing.T) {
	const moved = 5
	const asked = 8
	cacheCases := []shortBurstCase{
		{name: "empty cache", cached: 0},
		{name: "stale partial cache", cached: 2},
	}
	for _, engineCase := range engineCases() {
		for _, cacheCase := range cacheCases {
			t.Run(engineCase.name+"/"+cacheCase.name, func(t *testing.T) {
				queue, eng := newTestEngine(t, engineCase.options)

				enqueueAll(t, eng, 0, cacheCase.cached)
				_, available := eng.DequeueBurst(nil)
				require.Equal(t, cacheCase.cached, available)
				enqueueAll(t, eng, cacheCase.cached, moved-cacheCase.cached)
				dst := make([]int, asked)
				dequeued, available := eng.DequeueBurst(dst)
				assert.Equal(t, moved, dequeued, "dequeued from %d available", moved)
				assert.Equal(t, 0, available, "available after taking everything")
				assert.Equal(t, []int{0, 1, 2, 3, 4}, dst[:dequeued])

				enqueueAll(t, eng, moved, engineTestCapacity)
				dequeueAll(t, eng, moved, cacheCase.cached)
				_, free := eng.EnqueueBurst(nil)
				require.Equal(t, cacheCase.cached, free)
				dequeueAll(t, eng, moved+cacheCase.cached, moved-cacheCase.cached)
				items := []int{100, 101, 102, 103, 104, 105, 106, 107}
				require.Len(t, items, asked)
				enqueued, free := eng.EnqueueBurst(items)
				assert.Equal(t, moved, enqueued, "enqueued into %d free", moved)
				assert.Equal(t, 0, free, "free after filling the queue")

				dequeueAll(t, eng, 2*moved, engineTestCapacity-moved)
				dequeueAll(t, eng, 100, moved)
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestEngineDequeueAfterFailedDequeue(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue, eng := newTestEngine(t, engineCase.options)

			_, err := eng.Dequeue()
			require.ErrorIs(t, err, ring.ErrEmpty)
			enqueueAll(t, eng, 0, 3)
			value, err := eng.Dequeue()
			assert.NoError(t, err, "dequeue after pushes that followed a failed dequeue")
			assert.Equal(t, 0, value)

			assertIdleQueue(t, queue)
		})
	}
}
