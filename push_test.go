package workq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryPushFull(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			for item := range tryTestCapacity {
				require.NoError(t, queue.TryPush(item), "push %d", item)
			}

			assert.Same(t, ErrFull, queue.TryPush(tryTestCapacity))

			assert.Equal(t, sequence(0, tryTestCapacity), receiveAll(t, queue))
			requireIdleQueue(t, queue)
		})
	}
}

func TestTryPushBatchPartial(t *testing.T) {
	const alreadyQueued = tryTestCapacity - 4
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(sequence(0, alreadyQueued))
			require.NoError(t, err)
			require.Equal(t, alreadyQueued, pushed)

			pushed, err = queue.TryPushBatch(sequence(alreadyQueued, 10))
			assert.Same(t, ErrFull, err)
			assert.Equal(t, 4, pushed)

			assert.Equal(t, sequence(0, tryTestCapacity), receiveAll(t, queue))
			requireIdleQueue(t, queue)
		})
	}
}

func TestTryPushBatchLongerThanMaxBatch(t *testing.T) {
	const batchLength = 40
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			require.Less(t, queue.maxPushBatch, batchLength)

			pushed, err := queue.TryPushBatch(sequence(0, batchLength))
			assert.NoError(t, err)
			assert.Equal(t, batchLength, pushed)

			assert.Equal(t, sequence(0, batchLength), receiveAll(t, queue))
			requireIdleQueue(t, queue)
		})
	}
}

func TestTryPushBatchZeroLength(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(nil)
			assert.NoError(t, err, "open")
			assert.Zero(t, pushed, "open")

			queue.Close()
			pushed, err = queue.TryPushBatch([]int{})
			assert.Same(t, ErrClosed, err, "closed")
			assert.Zero(t, pushed, "closed")
			requireIdleQueue(t, queue)
		})
	}
}

func TestTryPushAfterClose(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			queue.Close()

			assert.Same(t, ErrClosed, queue.TryPush(1))
			pushed, err := queue.TryPushBatch([]int{1, 2})
			assert.Same(t, ErrClosed, err)
			assert.Zero(t, pushed)

			received, err := queue.TryReceive(make([]int, 1))
			assert.Same(t, ErrClosed, err, "nothing was pushed after Close")
			assert.Zero(t, received)
			requireIdleQueue(t, queue)
		})
	}
}

func TestTryPushWakesParkedConsumer(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			consumer := parkConsumer(t, &queue.consumers)

			require.NoError(t, queue.TryPush(1))

			requireToken(t, consumer)
			queue.consumers.leavePolling(mustNotPeek(t))
			queue.consumers.putWaiter(consumer)
			assert.Equal(t, []int{1}, receiveAll(t, queue))
			requireIdleQueue(t, queue)
		})
	}
}

func TestTryPushAllocs(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			var pushErr error
			// AllocsPerRun makes one extra warm-up run, so the runs fill the queue exactly.
			allocs := testing.AllocsPerRun(tryTestCapacity-1, func() {
				if err := queue.TryPush(1); err != nil {
					pushErr = err
				}
			})

			require.NoError(t, pushErr)
			assert.Zero(t, allocs)
			assert.Len(t, receiveAll(t, queue), tryTestCapacity)
			requireIdleQueue(t, queue)
		})
	}
}

func TestTryPushClosedDuringPush(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake)
	fake.setHooks(func(call engineCall) {
		if call == (engineCall{kind: callEnqueue, count: 1}) {
			queue.Close()
		}
	}, nil)

	assert.Same(t, ErrClosedDuringPush, queue.TryPush(1))

	assert.Equal(t, []int{1}, fake.takeRaw(1), "the item was pushed")
	requireIdleQueue(t, queue)
}

func TestTryPushBatchClosedDuringPush(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake)
	fake.setHooks(func(call engineCall) {
		if call == (engineCall{kind: callEnqueueBurst, count: 1}) {
			queue.Close()
		}
	}, nil)

	pushed, err := queue.TryPushBatch([]int{1, 2, 3})

	assert.Same(t, ErrClosedDuringPush, err)
	assert.Equal(t, 3, pushed)
	assert.Equal(t, []int{1, 2, 3}, fake.takeRaw(3), "the items were pushed")
	requireIdleQueue(t, queue)
}

func TestTryPushBatchWakesParkedConsumer(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			consumer := parkConsumer(t, &queue.consumers)

			pushed, err := queue.TryPushBatch([]int{1, 2})
			require.NoError(t, err)
			require.Equal(t, 2, pushed)

			requireToken(t, consumer)
			queue.consumers.leavePolling(mustNotPeek(t))
			queue.consumers.putWaiter(consumer)
			assert.Equal(t, []int{1, 2}, receiveAll(t, queue))
			requireIdleQueue(t, queue)
		})
	}
}

// TestTryPushBatchWakesAfterFirstChunk checks that the wake comes right after the first chunk
// moves, not only once the whole batch is in.
func TestTryPushBatchWakesAfterFirstChunk(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake, WithMaxPushBatch(2))
	consumer := parkConsumer(t, &queue.consumers)
	fake.setHooks(func(call engineCall) {
		if call == (engineCall{kind: callEnqueueBurst, count: 2}) {
			assert.Len(t, consumer.token, 1, "consumer token before the second chunk")
		}
	}, nil)

	pushed, err := queue.TryPushBatch(sequence(0, 5))

	require.NoError(t, err)
	assert.Equal(t, 5, pushed)
	assert.Equal(t, 3, fake.callCount(callEnqueueBurst))
	requireToken(t, consumer)
	queue.consumers.leavePolling(mustNotPeek(t))
	queue.consumers.putWaiter(consumer)
	assert.Equal(t, sequence(0, 5), fake.takeRaw(5))
	requireIdleQueue(t, queue)
}

func TestTryPushBatchClosedAfterFirstChunk(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake, WithMaxPushBatch(2))
	fake.setHooks(nil, func(call engineCall) {
		if call == (engineCall{kind: callEnqueueBurst, count: 1}) {
			queue.Close()
		}
	})

	pushed, err := queue.TryPushBatch(sequence(0, 5))

	assert.Same(t, ErrClosedDuringPush, err)
	assert.Equal(t, 2, pushed, "only the first chunk")
	assert.Equal(t, 1, fake.callCount(callEnqueueBurst), "no chunk after the close")
	assert.Equal(t, sequence(0, 2), fake.takeRaw(2))
	requireIdleQueue(t, queue)
}

func TestTryPushBatchClosedDuringPartialChunk(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake, WithMaxPushBatch(2))
	fake.setMaxMove(1)
	fake.setHooks(nil, func(call engineCall) {
		if call == (engineCall{kind: callEnqueueBurst, count: 1}) {
			queue.Close()
		}
	})

	pushed, err := queue.TryPushBatch(sequence(0, 5))

	assert.Same(t, ErrClosedDuringPush, err, "the close wins over the partial chunk")
	assert.Equal(t, 1, pushed)
	assert.Equal(t, []int{0}, fake.takeRaw(1))
	requireIdleQueue(t, queue)
}
