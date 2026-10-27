package workq

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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
			assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
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
	assertIdleQueue(t, queue)
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
	assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
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
	assertIdleQueue(t, queue)
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
	assertIdleQueue(t, queue)
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
	assertIdleQueue(t, queue)
}

// startPush runs Push in a new goroutine, and returns the channel that gets its error. The test
// owns the goroutine: the cleanup cancels its ctx and waits for it to return.
func startPush(ctx context.Context, t *testing.T, queue *Queue[int], item int) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	go func() {
		defer close(finished)
		results <- queue.Push(ctx, item)
	}()
	return results
}

// startPushBatch runs PushBatch in a new goroutine, and returns the channel that gets its
// result. The test owns the goroutine: the cleanup cancels its ctx and waits for it to return.
func startPushBatch(
	ctx context.Context,
	t *testing.T,
	queue *Queue[int],
	items []int,
) <-chan pushWaitResult {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan pushWaitResult, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	go func() {
		defer close(finished)
		pushed, err := queue.PushBatch(ctx, items)
		results <- pushWaitResult{pushed: pushed, err: err}
	}()
	return results
}

func awaitPush(t *testing.T, results <-chan error) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(waitTestTimeout):
		require.FailNow(t, "Push did not return")
		return nil
	}
}

// drainClosed calls TryReceive on a closed queue until it returns ErrClosed, and returns every
// item it received.
func drainClosed(t *testing.T, queue *Queue[int]) []int {
	t.Helper()
	buffer := make([]int, queue.Capacity())
	var received []int
	for {
		count, err := queue.TryReceive(buffer)
		if err != nil {
			require.Same(t, ErrClosed, err)
			return received
		}
		received = append(received, buffer[:count]...)
	}
}

// newDoneContext returns a context that is already canceled.
func newDoneContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// newWaitingPushQueue returns a real queue for engineCase whose producers park at once.
func newWaitingPushQueue(t *testing.T, engineCase engineCase) *Queue[int] {
	t.Helper()
	options := append(
		append([]Option{}, engineCase.options...),
		WithProducerWait(WaitStrategy{}),
	)
	queue, err := New[int](tryTestCapacity, options...)
	require.NoError(t, err)
	return queue
}

func TestPushDoneContextWithRoom(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)

			require.NoError(t, queue.Push(newDoneContext(), 1))

			assert.Equal(t, []int{1}, receiveAll(t, queue))
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushFullDoneContext(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(sequence(0, tryTestCapacity))
			require.NoError(t, err)
			require.Equal(t, tryTestCapacity, pushed)

			assert.Same(t, context.Canceled, queue.Push(newDoneContext(), tryTestCapacity))

			assert.Equal(t, sequence(0, tryTestCapacity), receiveAll(t, queue))
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushBatchDoneContext(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			chunkLength := queue.maxPushBatch
			require.Less(t, chunkLength, tryTestCapacity)

			pushed, err := queue.PushBatch(newDoneContext(), sequence(0, chunkLength))
			assert.NoError(t, err, "one chunk")
			assert.Equal(t, chunkLength, pushed, "one chunk")
			assert.Equal(t, sequence(0, chunkLength), receiveAll(t, queue), "one chunk")

			pushed, err = queue.PushBatch(newDoneContext(), sequence(0, tryTestCapacity))
			assert.Same(t, context.Canceled, err, "two chunks")
			assert.Equal(t, chunkLength, pushed, "two chunks")
			assert.Equal(t, sequence(0, chunkLength), receiveAll(t, queue), "two chunks")
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushBatchZeroLength(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			ctx, _ := newWaitContext(t)
			pushed, err := queue.PushBatch(ctx, nil)
			assert.NoError(t, err, "open")
			assert.Zero(t, pushed, "open")

			queue.Close()
			pushed, err = queue.PushBatch(ctx, []int{})
			assert.Same(t, ErrClosed, err, "closed")
			assert.Zero(t, pushed, "closed")
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushAfterClose(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			ctx, _ := newWaitContext(t)
			queue.Close()

			assert.Same(t, ErrClosed, queue.Push(ctx, 1))
			pushed, err := queue.PushBatch(ctx, []int{1, 2})
			assert.Same(t, ErrClosed, err)
			assert.Zero(t, pushed)

			received, err := queue.TryReceive(make([]int, 1))
			assert.Same(t, ErrClosed, err, "nothing was pushed after Close")
			assert.Zero(t, received)
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushBatchWakesParkedConsumer(t *testing.T) {
	const (
		capacity  = 4
		itemCount = 10_000
	)
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			options := append(
				append([]Option{}, engineCase.options...),
				WithConsumerWait(WaitStrategy{}),
			)
			synctest.Test(t, func(t *testing.T) {
				queue, err := New[int](capacity, options...)
				require.NoError(t, err)
				ctx, _ := newWaitContext(t)
				var received atomic.Int64
				countInOrder := func(_ context.Context, batch []int) error {
					for _, item := range batch {
						assert.Equal(t, int(received.Load()), item, "items in order")
						received.Add(1)
					}
					return nil
				}
				consumeResults := startConsume(ctx, t, queue, countInOrder)
				synctest.Wait()
				requireParked(t, &queue.consumers, 1)

				result := awaitPushResult(
					t,
					startPushBatch(ctx, t, queue, sequence(0, itemCount)),
				)

				require.NoError(t, result.err)
				assert.Equal(t, itemCount, result.pushed)
				synctest.Wait()
				require.Equal(t, int64(itemCount), received.Load(), "every item received")
				queue.Close()
				assert.NoError(t, awaitConsume(t, consumeResults))
				assertIdleQueue(t, queue)
			})
		})
	}
}

func TestPushWakesParkedConsumer(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			ctx, _ := newWaitContext(t)
			consumer := parkConsumer(t, &queue.consumers)

			require.NoError(t, queue.Push(ctx, 1))

			requireToken(t, consumer)
			queue.consumers.leavePolling(mustNotPeek(t))
			queue.consumers.putWaiter(consumer)
			assert.Equal(t, []int{1}, receiveAll(t, queue))
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushParkedUntilClose(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newWaitingPushQueue(t, engineCase)
				pushed, err := queue.TryPushBatch(sequence(0, tryTestCapacity))
				require.NoError(t, err)
				require.Equal(t, tryTestCapacity, pushed)
				ctx, _ := newWaitContext(t)
				results := startPush(ctx, t, queue, tryTestCapacity)
				synctest.Wait()
				requireParked(t, &queue.producers, 1)

				queue.Close()

				assert.Same(t, ErrClosed, awaitPush(t, results))
				assert.Equal(t, sequence(0, tryTestCapacity), drainClosed(t, queue))
				assertIdleQueue(t, queue)
			})
		})
	}
}

func TestPushBatchParkedUntilClose(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newWaitingPushQueue(t, engineCase)
				chunkLength := queue.maxPushBatch
				alreadyQueued := tryTestCapacity - chunkLength
				require.Positive(t, alreadyQueued)
				pushed, err := queue.TryPushBatch(sequence(0, alreadyQueued))
				require.NoError(t, err)
				require.Equal(t, alreadyQueued, pushed)
				ctx, _ := newWaitContext(t)
				// The first chunk fills the queue, so the second parks.
				results := startPushBatch(ctx, t, queue, sequence(alreadyQueued, 2*chunkLength))
				synctest.Wait()
				requireParked(t, &queue.producers, 1)

				queue.Close()

				result := awaitPushResult(t, results)
				assert.Same(t, ErrClosedDuringPush, result.err)
				assert.Equal(t, chunkLength, result.pushed)
				assert.Equal(t, sequence(0, tryTestCapacity), drainClosed(t, queue))
				assertIdleQueue(t, queue)
			})
		})
	}
}

func TestPushAllocs(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			ctx, _ := newWaitContext(t)
			var pushErr error
			// AllocsPerRun makes one extra warm-up run, so the runs fill the queue exactly.
			allocs := testing.AllocsPerRun(tryTestCapacity-1, func() {
				if err := queue.Push(ctx, 1); err != nil {
					pushErr = err
				}
			})

			require.NoError(t, pushErr)
			assert.Zero(t, allocs)
			assert.Len(t, receiveAll(t, queue), tryTestCapacity)
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushClosedDuringPush(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake)
	ctx, _ := newWaitContext(t)
	fake.setHooks(func(call engineCall) {
		if call == (engineCall{kind: callEnqueue, count: 1}) {
			queue.Close()
		}
	}, nil)

	assert.Same(t, ErrClosedDuringPush, queue.Push(ctx, 1))

	assert.Equal(t, []int{1}, fake.takeRaw(1), "the item was pushed")
	assertIdleQueue(t, queue)
}

func TestPushBatchClosedAfterFirstChunk(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake, WithMaxPushBatch(2))
	ctx, _ := newWaitContext(t)
	fake.setHooks(nil, func(call engineCall) {
		if call == (engineCall{kind: callEnqueueBurst, count: 1}) {
			queue.Close()
		}
	})

	pushed, err := queue.PushBatch(ctx, sequence(0, 5))

	assert.Same(t, ErrClosedDuringPush, err)
	assert.Equal(t, 2, pushed, "only the first chunk")
	assert.Equal(t, 1, fake.callCount(callEnqueueBurst), "no chunk after the close")
	assert.Equal(t, sequence(0, 2), fake.takeRaw(2))
	assertIdleQueue(t, queue)
}

// TestPushBatchClosedBetweenChunks closes the queue and cancels ctx after the first chunk's wake
// check and before the second chunk, where no engine call runs. The closed error must win over
// the done ctx.
func TestPushBatchClosedBetweenChunks(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake, WithMaxPushBatch(2))
	ctx, cancel := newWaitContext(t)
	queue.hooks.beforeChunkCheck = func() {
		queue.Close()
		cancel()
	}

	pushed, err := queue.PushBatch(ctx, sequence(0, 4))

	assert.Same(t, ErrClosedDuringPush, err)
	assert.Equal(t, 2, pushed, "only the first chunk")
	assert.Equal(t, 1, fake.callCount(callEnqueueBurst), "no chunk after the close")
	assert.Equal(t, sequence(0, 2), fake.takeRaw(2))
	assertIdleQueue(t, queue)
}

// TestPushBatchWakesAfterFirstChunk checks that the wake comes right after the first chunk
// moves, not only once the whole batch is in.
func TestPushBatchWakesAfterFirstChunk(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake, WithMaxPushBatch(2))
	ctx, _ := newWaitContext(t)
	consumer := parkConsumer(t, &queue.consumers)
	fake.setHooks(func(call engineCall) {
		if call == (engineCall{kind: callEnqueueBurst, count: 2}) {
			assert.Len(t, consumer.token, 1, "consumer token before the second chunk")
		}
	}, nil)

	pushed, err := queue.PushBatch(ctx, sequence(0, 5))

	require.NoError(t, err)
	assert.Equal(t, 5, pushed)
	assert.Equal(t, 3, fake.callCount(callEnqueueBurst))
	requireToken(t, consumer)
	queue.consumers.leavePolling(mustNotPeek(t))
	queue.consumers.putWaiter(consumer)
	assert.Equal(t, sequence(0, 5), fake.takeRaw(5))
	assertIdleQueue(t, queue)
}

// TestPushBatchWakesAfterPartialMove checks that the wake comes right after a burst moves part
// of a chunk, before the burst that moves the rest.
func TestPushBatchWakesAfterPartialMove(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake)
	ctx, _ := newWaitContext(t)
	fake.setMaxMove(1)
	consumer := parkConsumer(t, &queue.consumers)
	fake.setHooks(func(call engineCall) {
		if call == (engineCall{kind: callEnqueueBurst, count: 2}) {
			assert.Len(t, consumer.token, 1, "consumer token before the second burst")
		}
	}, nil)

	pushed, err := queue.PushBatch(ctx, sequence(0, 2))

	require.NoError(t, err)
	assert.Equal(t, 2, pushed)
	assert.Equal(t, 2, fake.callCount(callEnqueueBurst))
	requireToken(t, consumer)
	queue.consumers.leavePolling(mustNotPeek(t))
	queue.consumers.putWaiter(consumer)
	assert.Equal(t, sequence(0, 2), fake.takeRaw(2))
	assertIdleQueue(t, queue)
}

// TestPushBatchRetryIgnoresDoneContext checks that the rest of a partly pushed chunk is not a
// new chunk, so a done ctx does not stop it.
func TestPushBatchRetryIgnoresDoneContext(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake, WithMaxPushBatch(4))
	fake.setMaxMove(1)

	pushed, err := queue.PushBatch(newDoneContext(), sequence(0, 4))

	assert.NoError(t, err)
	assert.Equal(t, 4, pushed)
	assert.Equal(t, sequence(0, 4), fake.takeRaw(4))
	assertIdleQueue(t, queue)
}

// TestPushBatchWaitsMidChunk checks that a chunk that moves part of its items and then finds no
// room waits, and that the closed error then counts the items pushed before the wait.
func TestPushBatchWaitsMidChunk(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](4)
		queue := newTestQueue[int](t, fake, WithProducerWait(WaitStrategy{}))
		fake.pushRaw(sequence(0, 2)...)
		ctx, _ := newWaitContext(t)
		results := startPushBatch(ctx, t, queue, sequence(2, 3))
		synctest.Wait()
		requireParked(t, &queue.producers, 1)

		queue.Close()

		result := awaitPushResult(t, results)
		assert.Same(t, ErrClosedDuringPush, result.err)
		assert.Equal(t, 2, result.pushed)
		assert.Equal(t, sequence(0, 4), fake.takeRaw(4))
		assertIdleQueue(t, queue)
	})
}

func TestPushWokenByWaitingConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](1)
		queue := newTestQueue[int](
			t,
			fake,
			WithConsumerWait(WaitStrategy{}),
			WithProducerWait(WaitStrategy{}),
		)
		ctx, _ := newWaitContext(t)
		receiveResults := startReceive(ctx, t, queue, 1)
		// With no spins or yields, the consumer makes its fast-path dequeue and, after it
		// enlists, its extra attempt. Only a wake makes it dequeue again.
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)
		require.Equal(t, 2, fake.callCount(callDequeue), "dequeue count")

		fake.holdVisibility()
		require.NoError(t, queue.Push(ctx, 1), "producer A")
		synctest.Wait()
		require.Greater(
			t,
			fake.callCount(callDequeue),
			2,
			"the consumer woken by producer A dequeues",
		)
		requireParked(t, &queue.consumers, 1)
		pushResults := startPush(ctx, t, queue, 2)
		synctest.Wait()
		requireParked(t, &queue.producers, 1)

		fake.releaseVisibility()
		assert.False(t, queue.consumers.wakeCheck())

		assert.NoError(t, awaitPush(t, pushResults), "producer B")
		result := awaitResult(t, receiveResults)
		require.NoError(t, result.err)
		assert.Equal(t, []int{1}, result.items)
		assert.Equal(t, []int{2}, fake.takeRaw(1))
		assertIdleQueue(t, queue)
	})
}
