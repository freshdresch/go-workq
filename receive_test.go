package workq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const tryTestCapacity = 64

func newTryQueue(t *testing.T, engineCase engineCase) *Queue[int] {
	t.Helper()
	queue, err := New[int](tryTestCapacity, engineCase.options...)
	require.NoError(t, err)
	return queue
}

// sequence returns count consecutive items starting at first.
func sequence(first int, count int) []int {
	items := make([]int, count)
	for i := range items {
		items[i] = first + i
	}
	return items
}

// receiveAll calls TryReceive until it returns no items, and returns every item it received.
func receiveAll(t *testing.T, queue *Queue[int]) []int {
	t.Helper()
	buffer := make([]int, queue.Capacity())
	var received []int
	for {
		count, err := queue.TryReceive(buffer)
		require.NoError(t, err)
		if count == 0 {
			return received
		}
		received = append(received, buffer[:count]...)
	}
}

// parkProducer registers a new producer on side and parks it, as a producer that found no room
// does.
func parkProducer(t *testing.T, side *waitSide) *waiter {
	t.Helper()
	caller := side.getWaiter()
	require.False(t, side.register(), "register")
	require.False(t, side.enlist(caller, atNewest), "enlist")
	return caller
}

// mustNotPeek returns a peek for leavePolling and leaveList that fails the test if called. The
// tests that use it leave no other waiter parked and pass movedItems false, so the protocol
// owes no wake and must not peek.
func mustNotPeek(t *testing.T) func() bool {
	return func() bool {
		assert.Fail(t, "peek ran with no wake owed")
		return false
	}
}

func TestTryReceiveZeroLength(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			require.NoError(t, queue.TryPush(1))

			received, err := queue.TryReceive(nil)
			assert.NoError(t, err, "open")
			assert.Zero(t, received, "open")

			queue.Close()
			received, err = queue.TryReceive([]int{})
			assert.NoError(t, err, "closed")
			assert.Zero(t, received, "closed")

			dst := make([]int, 1)
			received, err = queue.TryReceive(dst)
			assert.NoError(t, err)
			assert.Equal(t, []int{1}, dst[:received], "the item is still queued")
			assertIdleQueue(t, queue)
		})
	}
}

func TestCloseDrainsRemainingItems(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(sequence(0, 5))
			require.NoError(t, err)
			require.Equal(t, 5, pushed)

			queue.Close()
			assert.NotPanics(t, queue.Close, "second Close")

			single := make([]int, 1)
			received, err := queue.TryReceive(single)
			assert.NoError(t, err)
			assert.Equal(t, 1, received)
			assert.Equal(t, []int{0}, single)
			buffer := make([]int, tryTestCapacity)
			received, err = queue.TryReceive(buffer)
			assert.NoError(t, err)
			assert.Equal(t, sequence(1, 4), buffer[:received])

			for _, dst := range [][]int{single, buffer} {
				received, err = queue.TryReceive(dst)
				assert.Same(t, ErrClosed, err, "len(dst) %d", len(dst))
				assert.Zero(t, received, "len(dst) %d", len(dst))
			}
			assertIdleQueue(t, queue)
		})
	}
}

func TestCloseWakesParkedConsumer(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			consumer := parkConsumer(t, &queue.consumers)

			queue.Close()

			select {
			case <-queue.done:
			default:
				require.FailNow(t, "Close left done open")
			}
			assertNoToken(t, consumer)
			assert.True(t, queue.consumers.load().closed(), "consumer side closed")
			assert.True(t, queue.producers.load().closed(), "producer side closed")
			queue.consumers.leaveList(consumer, false, mustNotPeek(t))
			queue.consumers.putWaiter(consumer)
			assertIdleQueue(t, queue)
		})
	}
}

func TestTryReceiveWakesParkedProducer(t *testing.T) {
	for _, engineCase := range engineCases() {
		for _, dstLength := range []int{1, 4} {
			name := fmt.Sprintf("%s/dst%d", engineCase.name, dstLength)
			t.Run(name, func(t *testing.T) {
				queue := newTryQueue(t, engineCase)
				pushed, err := queue.TryPushBatch(sequence(0, dstLength))
				require.NoError(t, err)
				require.Equal(t, dstLength, pushed)
				producer := parkProducer(t, &queue.producers)

				dst := make([]int, dstLength)
				received, err := queue.TryReceive(dst)
				require.NoError(t, err)
				assert.Equal(t, sequence(0, dstLength), dst[:received])

				requireToken(t, producer)
				queue.producers.leavePolling(mustNotPeek(t))
				queue.producers.putWaiter(producer)
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestTryReceiveAllocs(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(sequence(0, tryTestCapacity))
			require.NoError(t, err)
			require.Equal(t, tryTestCapacity, pushed)
			dst := make([]int, 1)
			var receiveErr error
			receivedTotal := 0
			// AllocsPerRun makes one extra warm-up run, so the runs empty the queue exactly.
			allocs := testing.AllocsPerRun(tryTestCapacity-1, func() {
				received, err := queue.TryReceive(dst)
				if err != nil {
					receiveErr = err
				}
				receivedTotal += received
			})

			require.NoError(t, receiveErr)
			assert.Zero(t, allocs)
			assert.Equal(t, tryTestCapacity, receivedTotal)
			assertIdleQueue(t, queue)
		})
	}
}

func TestTryReceiveDrainsItemPushedDuringClose(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake)
	firstDequeue := engineCall{kind: callDequeue, count: 1}
	fake.setHooks(
		func(call engineCall) {
			if call == firstDequeue {
				fake.setForceEmpty()
			}
		},
		func(call engineCall) {
			if call == firstDequeue {
				fake.pushRaw(7)
				queue.Close()
			}
		},
	)
	dst := make([]int, 1)

	received, err := queue.TryReceive(dst)
	require.NoError(t, err)
	assert.Equal(t, 1, received)
	assert.Equal(t, []int{7}, dst)
	assert.Equal(t, 2, fake.callCount(callDequeue), "the drain check made the second attempt")

	received, err = queue.TryReceive(dst)
	assert.Same(t, ErrClosed, err)
	assert.Zero(t, received)
	assertIdleQueue(t, queue)
}

func TestTryReceiveDrainCheckWakesParkedProducer(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue[int](t, fake)
	producer := parkProducer(t, &queue.producers)
	firstDequeue := engineCall{kind: callDequeue, count: 1}
	fake.setHooks(
		func(call engineCall) {
			if call == firstDequeue {
				fake.setForceEmpty()
			}
		},
		func(call engineCall) {
			if call == firstDequeue {
				fake.pushRaw(7)
				queue.Close()
			}
		},
	)
	dst := make([]int, 1)

	received, err := queue.TryReceive(dst)

	require.NoError(t, err)
	assert.Equal(t, []int{7}, dst[:received])
	assert.Equal(t, 2, fake.callCount(callDequeue), "the drain check made the second attempt")
	requireToken(t, producer)
	queue.producers.leavePolling(mustNotPeek(t))
	queue.producers.putWaiter(producer)
	assertIdleQueue(t, queue)
}

var errHandler = errors.New("test handler failed")

// startReceive runs Receive with a dst of dstLength in a new goroutine, and returns the channel
// that gets its result. The test owns the goroutine: the cleanup cancels its ctx and waits for it
// to return.
func startReceive(
	ctx context.Context,
	t *testing.T,
	queue *Queue[int],
	dstLength int,
) <-chan waitResult {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan waitResult, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	go func() {
		defer close(finished)
		dst := make([]int, dstLength)
		received, err := queue.Receive(ctx, dst)
		results <- waitResult{received: received, err: err, items: dst[:received]}
	}()
	return results
}

// startConsume runs Consume in a new goroutine, and returns the channel that gets its error. The
// test owns the goroutine: the cleanup cancels its ctx and waits for it to return.
func startConsume[T any](
	ctx context.Context,
	t *testing.T,
	queue *Queue[T],
	handler func(ctx context.Context, batch []T) error,
) <-chan error {
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
		results <- queue.Consume(ctx, handler)
	}()
	return results
}

func awaitConsume(t *testing.T, results <-chan error) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(waitTestTimeout):
		require.FailNow(t, "Consume did not return")
		return nil
	}
}

// forwardBatches returns a handler that sends a copy of each batch to batches. It gives up when
// ctx is done, so a test that stops reading never blocks Consume's return.
func forwardBatches[T any](batches chan<- []T) func(ctx context.Context, batch []T) error {
	return func(ctx context.Context, batch []T) error {
		select {
		case batches <- append([]T(nil), batch...):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func awaitBatch[T any](t *testing.T, batches <-chan []T) []T {
	t.Helper()
	select {
	case batch := <-batches:
		return batch
	case <-time.After(waitTestTimeout):
		require.FailNow(t, "the handler got no batch")
		return nil
	}
}

func TestReceiveClosedDrainedBeatsDoneContext(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			queue.Close()
			ctx, cancel := newWaitContext(t)
			cancel()

			received, err := queue.Receive(ctx, make([]int, 4))

			assert.Same(t, ErrClosed, err)
			assert.Zero(t, received)
			assertIdleQueue(t, queue)
		})
	}
}

func TestConsumeDoneContextBeatsClosedDrained(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			queue.Close()
			ctx, cancel := newWaitContext(t)
			cancel()

			err := queue.Consume(ctx, func(context.Context, []int) error {
				assert.Fail(t, "handler ran with no items")
				return nil
			})

			assert.Same(t, context.Canceled, err)
			assertIdleQueue(t, queue)
		})
	}
}

func TestConsumeDrainsAfterClose(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(sequence(0, 40))
			require.NoError(t, err)
			require.Equal(t, 40, pushed)
			queue.Close()
			ctx, _ := newWaitContext(t)
			var received []int

			err = queue.Consume(ctx, func(handlerCtx context.Context, batch []int) error {
				assert.Same(t, ctx, handlerCtx, "the handler gets Consume's ctx")
				received = append(received, batch...)
				return nil
			})

			assert.NoError(t, err)
			assert.Equal(t, sequence(0, 40), received)
			assertIdleQueue(t, queue)
		})
	}
}

func TestConsumeWrapsHandlerError(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(sequence(0, 3))
			require.NoError(t, err)
			require.Equal(t, 3, pushed)
			ctx, _ := newWaitContext(t)
			handlerCalls := 0

			err = queue.Consume(ctx, func(handlerCtx context.Context, batch []int) error {
				handlerCalls++
				assert.Same(t, ctx, handlerCtx, "the handler gets Consume's ctx")
				assert.Equal(t, sequence(0, 3), batch)
				return errHandler
			})

			assert.ErrorIs(t, err, errHandler)
			assert.EqualError(t, err, "workq: handler: test handler failed")
			assert.NotSame(t, errHandler, err, "wrapped")
			assert.Equal(t, 1, handlerCalls, "the first error stops Consume")
			assertIdleQueue(t, queue)
		})
	}
}

func TestReceiveParkedUntilClose(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newTryQueue(t, engineCase)
				ctx, _ := newWaitContext(t)
				results := startReceive(ctx, t, queue, 4)
				synctest.Wait()
				requireParked(t, &queue.consumers, 1)

				queue.Close()

				result := awaitResult(t, results)
				assert.Same(t, ErrClosed, result.err)
				assert.Zero(t, result.received)
				assertIdleQueue(t, queue)
			})
		})
	}
}

func TestConsumeParkedUntilClose(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newTryQueue(t, engineCase)
				ctx, _ := newWaitContext(t)
				results := startConsume(ctx, t, queue, func(context.Context, []int) error {
					assert.Fail(t, "handler ran with no items")
					return nil
				})
				synctest.Wait()
				requireParked(t, &queue.consumers, 1)

				queue.Close()

				assert.NoError(t, awaitConsume(t, results))
				assertIdleQueue(t, queue)
			})
		})
	}
}

func TestConsumeClearsBufferWhileWaiting(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue, err := New[*int](tryTestCapacity, engineCase.options...)
				require.NoError(t, err)
				first, second := 1, 2
				pushed, err := queue.TryPushBatch([]*int{&first, &second})
				require.NoError(t, err)
				require.Equal(t, 2, pushed)
				ctx, cancel := newWaitContext(t)
				// The handler sends the batch itself, not a copy, so the test sees Consume's
				// buffer.
				batches := make(chan []*int, 1)
				sendBatch := func(_ context.Context, batch []*int) error {
					batches <- batch
					return nil
				}
				results := startConsume(ctx, t, queue, sendBatch)
				batch := awaitBatch(t, batches)
				require.Len(t, batch, 2)

				// Consume clears the buffer before it registers, and the parked count comes after
				// that, so the loads below see the cleared elements.
				synctest.Wait()
				requireParked(t, &queue.consumers, 1)

				assert.Equal(t, []*int{nil, nil}, batch, "the used part of the buffer")
				cancel()
				assert.Same(t, context.Canceled, awaitConsume(t, results))
				assertIdleSide(t, &queue.consumers)
				assertIdleSide(t, &queue.producers)
			})
		})
	}
}

func TestConsumeNeverPark(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			options := append(
				[]Option{WithConsumerWait(WaitStrategy{Yields: 0, NeverPark: true})},
				engineCase.options...,
			)
			queue, err := New[int](tryTestCapacity, options...)
			require.NoError(t, err)
			ctx, cancel := newWaitContext(t)
			batches := make(chan []int, 1)
			results := startConsume(ctx, t, queue, forwardBatches(batches))
			awaitCondition(
				t,
				func() bool { return queue.consumers.load().pollers() == 1 },
				"consumer registered",
			)

			require.NoError(t, queue.TryPush(7))

			assert.Equal(t, []int{7}, awaitBatch(t, batches))
			assert.Zero(t, queue.consumers.load().parked(), "parked")
			cancel()
			assert.Same(t, context.Canceled, awaitConsume(t, results))
			assertIdleQueue(t, queue)
		})
	}
}

func TestReceiveAllocs(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(sequence(0, tryTestCapacity))
			require.NoError(t, err)
			require.Equal(t, tryTestCapacity, pushed)
			ctx := context.Background()
			dst := make([]int, 1)
			var receiveErr error
			receivedTotal := 0
			// AllocsPerRun makes one extra warm-up run, so the runs empty the queue exactly.
			allocs := testing.AllocsPerRun(tryTestCapacity-1, func() {
				received, err := queue.Receive(ctx, dst)
				if err != nil {
					receiveErr = err
				}
				receivedTotal += received
			})

			require.NoError(t, receiveErr)
			assert.Zero(t, allocs)
			assert.Equal(t, tryTestCapacity, receivedTotal)
			assertIdleQueue(t, queue)
		})
	}
}

func TestReceiveReturnsItemsWithDoneContext(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(sequence(0, 3))
			require.NoError(t, err)
			require.Equal(t, 3, pushed)
			ctx, cancel := newWaitContext(t)
			cancel()
			dst := make([]int, 4)

			received, err := queue.Receive(ctx, dst)

			assert.NoError(t, err)
			assert.Equal(t, sequence(0, 3), dst[:received])
			assertIdleQueue(t, queue)
		})
	}
}

func TestReceiveZeroLength(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			pushed, err := queue.TryPushBatch(sequence(0, 3))
			require.NoError(t, err)
			require.Equal(t, 3, pushed)
			ctx, cancel := newWaitContext(t)
			cancel()

			received, err := queue.Receive(ctx, nil)
			assert.NoError(t, err, "open")
			assert.Zero(t, received, "open")

			queue.Close()
			received, err = queue.Receive(ctx, []int{})
			assert.NoError(t, err, "closed")
			assert.Zero(t, received, "closed")

			dst := make([]int, 4)
			received, err = queue.TryReceive(dst)
			assert.NoError(t, err)
			assert.Equal(t, sequence(0, 3), dst[:received], "the items are still queued")
			assertIdleQueue(t, queue)
		})
	}
}

func TestReceiveWakesParkedProducer(t *testing.T) {
	for _, engineCase := range engineCases() {
		for _, dstLength := range []int{1, 4} {
			name := fmt.Sprintf("%s/dst%d", engineCase.name, dstLength)
			t.Run(name, func(t *testing.T) {
				queue := newTryQueue(t, engineCase)
				pushed, err := queue.TryPushBatch(sequence(0, dstLength))
				require.NoError(t, err)
				require.Equal(t, dstLength, pushed)
				producer := parkProducer(t, &queue.producers)
				ctx, _ := newWaitContext(t)

				dst := make([]int, dstLength)
				received, err := queue.Receive(ctx, dst)
				require.NoError(t, err)
				assert.Equal(t, sequence(0, dstLength), dst[:received])

				requireToken(t, producer)
				queue.producers.leavePolling(mustNotPeek(t))
				queue.producers.putWaiter(producer)
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestConsumeWakesParkedProducer(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			queue := newTryQueue(t, engineCase)
			producer := parkProducer(t, &queue.producers)
			pushed, err := queue.TryPushBatch(sequence(0, 3))
			require.NoError(t, err)
			require.Equal(t, 3, pushed)
			ctx, _ := newWaitContext(t)

			// The handler error ends Consume after its first batch, which came from the fast
			// path, so only that path's wake check can wake the producer.
			err = queue.Consume(ctx, func(context.Context, []int) error {
				return errHandler
			})

			require.ErrorIs(t, err, errHandler)
			requireToken(t, producer)
			queue.producers.leavePolling(mustNotPeek(t))
			queue.producers.putWaiter(producer)
			assertIdleQueue(t, queue)
		})
	}
}
