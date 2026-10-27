package workq

import (
	"fmt"
	"testing"

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
	require.False(t, side.enlist(caller), "enlist")
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
			requireIdleQueue(t, queue)
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
			requireIdleQueue(t, queue)
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
			requireIdleQueue(t, queue)
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
				requireIdleQueue(t, queue)
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
			requireIdleQueue(t, queue)
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
	requireIdleQueue(t, queue)
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
	requireIdleQueue(t, queue)
}
