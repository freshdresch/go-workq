package workq

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitTestTimeout bounds how long a test waits for receiveWait to return or reach a state, so a
// lost wakeup fails the test instead of hanging it.
const waitTestTimeout = 10 * time.Second

// waitResult is what one receiveWait call returned, with the items it received.
type waitResult struct {
	received int
	err      error
	items    []int
}

// startReceiveWait runs receiveWait with a one-item dst in a new goroutine, so every attempt is
// a Dequeue, and returns the channel that gets its result.
func startReceiveWait(ctx context.Context, t *testing.T, queue *Queue[int]) <-chan waitResult {
	t.Helper()
	return startReceiveWaitWith(ctx, t, queue, 1, nil)
}

// startReceiveWaitWith runs receiveWait with a dst of dstLength and the waiter own in a new
// goroutine, and returns the channel that gets its result. The test owns the goroutine: the
// cleanup cancels its ctx and waits for it to return, so no test leaves one running.
func startReceiveWaitWith(
	ctx context.Context,
	t *testing.T,
	queue *Queue[int],
	dstLength int,
	own *waiter,
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
		received, err := queue.receiveWait(ctx, dst, own, nil)
		results <- waitResult{received: received, err: err, items: dst[:received]}
	}()
	return results
}

// newWaitContext returns a context that the test cancels by hand. The cleanup cancels it too,
// which releases it when the test never does.
func newWaitContext(t *testing.T) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx, cancel
}

func awaitResult(t *testing.T, results <-chan waitResult) waitResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(waitTestTimeout):
		require.FailNow(t, "receiveWait did not return")
		return waitResult{}
	}
}

// awaitCondition polls condition, yielding between polls, until it holds.
func awaitCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(waitTestTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			require.FailNow(t, "timed out waiting", description)
		}
		runtime.Gosched()
	}
}

func awaitParked(t *testing.T, side *waitSide, wantParked uint32) {
	t.Helper()
	awaitCondition(t, func() bool { return side.load().parked() == wantParked }, "parked count")
}

func awaitDequeues(t *testing.T, fake *scriptedEngine[int], wantDequeues int) {
	t.Helper()
	awaitCondition(
		t,
		func() bool { return fake.callCount(callDequeue) == wantDequeues },
		"dequeue count",
	)
}

func dequeueCall(count int) engineCall {
	return engineCall{kind: callDequeue, count: count}
}

// forceEmptyThenPush returns hooks that force dequeue number count to find nothing and, once it
// has decided, push item and run then, as a producer that pushes during that dequeue would.
func forceEmptyThenPush(
	fake *scriptedEngine[int],
	count int,
	item int,
	then func(),
) (before func(engineCall), after func(engineCall)) {
	target := dequeueCall(count)
	before = func(call engineCall) {
		if call == target {
			fake.setForceEmpty()
		}
	}
	after = func(call engineCall) {
		if call == target {
			fake.pushRaw(item)
			then()
		}
	}
	return before, after
}

// pushBefore returns a before hook that pushes item just before dequeue number count.
func pushBefore(fake *scriptedEngine[int], count int, item int) func(engineCall) {
	target := dequeueCall(count)
	return func(call engineCall) {
		if call == target {
			fake.pushRaw(item)
		}
	}
}

func TestReceiveWaitItemDuringLastPoll(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Yields: 1}))
	// The wake check sees the consumer still registered for its yield-phase poll and skips, so
	// only the extra attempt after enlist can find the item.
	fake.setHooks(forceEmptyThenPush(fake, 1, 7, func() { queue.consumers.wakeCheck() }))
	ctx, _ := newWaitContext(t)

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	require.NoError(t, result.err)
	assert.Equal(t, []int{7}, result.items)
	assert.Equal(t, 2, fake.callCount(callDequeue), "the yield poll and the extra attempt")
	requireIdleQueue(t, queue)
}

func TestReceiveWaitItemDuringExtraAttempt(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	// With no spins or yields, the first dequeue is the extra attempt. The consumer is already
	// parked by then, so the wake check wakes it.
	fake.setHooks(forceEmptyThenPush(fake, 1, 7, func() { queue.consumers.wakeCheck() }))
	ctx, _ := newWaitContext(t)

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	require.NoError(t, result.err)
	assert.Equal(t, []int{7}, result.items)
	assert.Equal(t, 2, fake.callCount(callDequeue), "the extra attempt and the woken poll")
	requireIdleQueue(t, queue)
}

func TestReceiveWaitWokenPollsFirst(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	// Dequeue 1 is the extra attempt of the first park. The word at dequeue 2 shows whether the
	// woken consumer polled before it enlisted again.
	var wordAtSecondDequeue sideWord
	fake.setHooks(func(call engineCall) {
		if call == dequeueCall(2) {
			wordAtSecondDequeue = queue.consumers.load()
		}
	}, nil)
	ctx, cancel := newWaitContext(t)
	results := startReceiveWait(ctx, t, queue)
	awaitParked(t, &queue.consumers, 1)

	queue.consumers.wakeOne()

	awaitParked(t, &queue.consumers, 1)
	awaitDequeues(t, fake, 3)
	cancel()
	result := awaitResult(t, results)
	assert.Same(t, context.Canceled, result.err)
	assert.Zero(t, result.received)
	assert.Equal(t, uint32(1), wordAtSecondDequeue.pollers(), "woken consumer polls registered")
	assert.Zero(t, wordAtSecondDequeue.parked(), "woken consumer polls before it enlists")
	requireIdleQueue(t, queue)
}

func TestReceiveWaitCancelParked(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	ctx, cancel := newWaitContext(t)
	results := startReceiveWait(ctx, t, queue)
	awaitParked(t, &queue.consumers, 1)

	cancel()

	result := awaitResult(t, results)
	assert.Same(t, context.Canceled, result.err)
	assert.Zero(t, result.received)
	requireIdleQueue(t, queue)
}

func TestReceiveWaitCancelYielding(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Yields: 1000}))
	ctx, cancel := newWaitContext(t)
	fake.setHooks(func(call engineCall) {
		if call == dequeueCall(3) {
			cancel()
		}
	}, nil)

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	assert.Same(t, context.Canceled, result.err)
	assert.Zero(t, result.received)
	assert.Equal(t, 3, fake.callCount(callDequeue), "returned from the third yield-phase poll")
	requireIdleQueue(t, queue)
}

func TestReceiveWaitClosedBeatsContext(t *testing.T) {
	setGOMAXPROCS(t, 4)
	strategies := map[string]WaitStrategy{
		"spin":  {Spins: 1},
		"yield": {Yields: 1},
	}
	for name, strategy := range strategies {
		t.Run(name, func(t *testing.T) {
			fake := newScriptedEngine[int](8)
			queue := newTestQueue(t, fake, WithConsumerWait(strategy))
			ctx, cancel := newWaitContext(t)
			fake.setHooks(func(call engineCall) {
				if call == dequeueCall(1) {
					queue.Close()
					cancel()
				}
			}, nil)

			result := awaitResult(t, startReceiveWait(ctx, t, queue))

			assert.Same(t, ErrClosed, result.err)
			assert.Zero(t, result.received)
			requireIdleQueue(t, queue)
		})
	}
}

func TestReceiveWaitClosedWhileParked(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	ctx, _ := newWaitContext(t)
	results := startReceiveWait(ctx, t, queue)
	awaitParked(t, &queue.consumers, 1)

	queue.Close()

	result := awaitResult(t, results)
	assert.Same(t, ErrClosed, result.err)
	assert.Zero(t, result.received)
	assert.Equal(t, 2, fake.callCount(callDequeue), "the extra attempt and the drain check")
	requireIdleQueue(t, queue)
}

func TestReceiveWaitDrainsItemPushedDuringClose(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Yields: 1}))
	fake.setHooks(forceEmptyThenPush(fake, 1, 7, queue.Close))
	ctx, _ := newWaitContext(t)

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	require.NoError(t, result.err)
	assert.Equal(t, []int{7}, result.items)
	assert.Equal(t, 2, fake.callCount(callDequeue), "the yield poll and the drain check")
	requireIdleQueue(t, queue)
}

func TestReceiveWaitWokenSeesClosed(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	fake.holdVisibility()
	fake.pushRaw(7)
	ctx, _ := newWaitContext(t)
	results := startReceiveWait(ctx, t, queue)
	awaitParked(t, &queue.consumers, 1)

	// No waiting between these three, so the consumer may see the token or done first.
	queue.consumers.wakeOne()
	fake.releaseVisibility()
	queue.Close()

	result := awaitResult(t, results)
	require.NoError(t, result.err)
	assert.Equal(t, []int{7}, result.items)
	received, err := queue.receiveWait(ctx, make([]int, 1), nil, nil)
	assert.Same(t, ErrClosed, err, "second call")
	assert.Zero(t, received, "second call")
	assert.Zero(t, queue.consumers.load().pollers(), "pollers")
	requireIdleQueue(t, queue)
}

// The producer wake check tests park a producer by hand and check that receiveWait wakes it
// when it takes an item at one site.

func TestReceiveWaitWakesProducerSpin(t *testing.T) {
	setGOMAXPROCS(t, 4)
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Spins: 1}))
	fake.setHooks(pushBefore(fake, 1, 7), nil)
	requireReceiveWaitWakesProducer(t, queue)
}

func TestReceiveWaitWakesProducerYield(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Yields: 1}))
	fake.setHooks(pushBefore(fake, 1, 7), nil)
	requireReceiveWaitWakesProducer(t, queue)
}

func TestReceiveWaitWakesProducerExtraAttempt(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	fake.setHooks(pushBefore(fake, 1, 7), nil)
	requireReceiveWaitWakesProducer(t, queue)
}

func TestReceiveWaitWakesProducerDrain(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	fake.setHooks(forceEmptyThenPush(fake, 1, 7, queue.Close))
	requireReceiveWaitWakesProducer(t, queue)
}

// requireReceiveWaitWakesProducer parks a producer, runs receiveWait, which the caller's hooks
// make take item 7 with its first dequeue that moves items, and checks that the producer got a
// token.
func requireReceiveWaitWakesProducer(t *testing.T, queue *Queue[int]) {
	t.Helper()
	producer := parkProducer(t, &queue.producers)
	ctx, _ := newWaitContext(t)

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	require.NoError(t, result.err)
	assert.Equal(t, []int{7}, result.items)
	requireToken(t, producer)
	queue.producers.leavePolling(mustNotPeek(t))
	queue.producers.putWaiter(producer)
	requireIdleQueue(t, queue)
}

// neverParkLastPoll is the dequeue at which the NeverPark tests act, after several empty polls.
const neverParkLastPoll = 5

// neverParkStrategies reach the never-park loop straight from registering and after the yield
// phase.
func neverParkStrategies() map[string]WaitStrategy {
	return map[string]WaitStrategy{
		"no yields": {NeverPark: true},
		"one yield": {Yields: 1, NeverPark: true},
	}
}

// runNeverParkWait runs receiveWait with strategy on an empty queue, runs act from the before
// hook of dequeue neverParkLastPoll, and checks that the consumer stayed registered and never
// parked through every dequeue up to that one.
func runNeverParkWait(
	t *testing.T,
	strategy WaitStrategy,
	act func(queue *Queue[int], fake *scriptedEngine[int], cancel context.CancelFunc),
) (waitResult, *Queue[int]) {
	t.Helper()
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(strategy))
	ctx, cancel := newWaitContext(t)
	var words []sideWord
	fake.setHooks(func(call engineCall) {
		if call.kind != callDequeue || call.count > neverParkLastPoll {
			return
		}
		words = append(words, queue.consumers.load())
		if call.count == neverParkLastPoll {
			act(queue, fake, cancel)
		}
	}, nil)

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	require.Len(t, words, neverParkLastPoll, "dequeues up to the last poll")
	for i, word := range words {
		assert.Equal(t, uint32(1), word.pollers(), "pollers at dequeue %d", i+1)
		assert.Zero(t, word.parked(), "parked at dequeue %d", i+1)
	}
	return result, queue
}

func TestReceiveWaitNeverParkItem(t *testing.T) {
	for name, strategy := range neverParkStrategies() {
		t.Run(name, func(t *testing.T) {
			result, queue := runNeverParkWait(
				t,
				strategy,
				func(_ *Queue[int], fake *scriptedEngine[int], _ context.CancelFunc) {
					fake.pushRaw(7)
				},
			)
			require.NoError(t, result.err)
			assert.Equal(t, []int{7}, result.items)
			requireIdleQueue(t, queue)
		})
	}
}

func TestReceiveWaitNeverParkCancel(t *testing.T) {
	for name, strategy := range neverParkStrategies() {
		t.Run(name, func(t *testing.T) {
			result, queue := runNeverParkWait(
				t,
				strategy,
				func(_ *Queue[int], _ *scriptedEngine[int], cancel context.CancelFunc) {
					cancel()
				},
			)
			assert.Same(t, context.Canceled, result.err)
			assert.Zero(t, result.received)
			requireIdleQueue(t, queue)
		})
	}
}

func TestReceiveWaitNeverParkClose(t *testing.T) {
	for name, strategy := range neverParkStrategies() {
		t.Run(name, func(t *testing.T) {
			result, queue := runNeverParkWait(
				t,
				strategy,
				func(queue *Queue[int], _ *scriptedEngine[int], _ context.CancelFunc) {
					queue.Close()
				},
			)
			assert.Same(t, ErrClosed, result.err)
			assert.Zero(t, result.received)
			requireIdleQueue(t, queue)
		})
	}
}

func TestReceiveWaitSpinClosedBeforeCall(t *testing.T) {
	setGOMAXPROCS(t, 4)
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Spins: 1}))
	ctx, cancel := newWaitContext(t)
	queue.Close()
	cancel()

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	assert.Same(t, ErrClosed, result.err)
	assert.Zero(t, result.received)
	assert.Equal(t, 1, fake.callCount(callDequeue), "only the drain attempt")
	requireIdleQueue(t, queue)
}

func TestReceiveWaitSpinCanceledBeforeCall(t *testing.T) {
	setGOMAXPROCS(t, 4)
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Spins: 100}))
	ctx, cancel := newWaitContext(t)
	cancel()

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	assert.Same(t, context.Canceled, result.err)
	assert.Zero(t, result.received)
	assert.Zero(t, fake.callCount(callDequeue), "ctx is checked before the first poll")
	requireIdleQueue(t, queue)
}

func TestReceiveWaitSpinDrainsItemClosedBeforeCall(t *testing.T) {
	setGOMAXPROCS(t, 4)
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Spins: 1}))
	ctx, _ := newWaitContext(t)
	fake.pushRaw(7)
	queue.Close()

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	require.NoError(t, result.err)
	assert.Equal(t, []int{7}, result.items)
	received, err := queue.receiveWait(ctx, make([]int, 1), nil, nil)
	assert.Same(t, ErrClosed, err, "second call")
	assert.Zero(t, received, "second call")
	requireIdleQueue(t, queue)
}

func TestReceiveWaitSpinChecksContextEvery64Polls(t *testing.T) {
	setGOMAXPROCS(t, 4)
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Spins: 1000}))
	ctx, cancel := newWaitContext(t)
	fake.setHooks(func(call engineCall) {
		if call == dequeueCall(64) {
			cancel()
		}
	}, nil)

	result := awaitResult(t, startReceiveWait(ctx, t, queue))

	assert.Same(t, context.Canceled, result.err)
	assert.Zero(t, result.received)
	assert.Equal(t, 64, fake.callCount(callDequeue), "the check before poll 65 saw the cancel")
	requireIdleQueue(t, queue)
}

// newClearOnYield returns a slice of non-nil pointers, as the part of Consume's buffer that its
// last batch used.
func newClearOnYield() []*int {
	first, second := 1, 2
	return []*int{&first, &second}
}

func TestReceiveWaitClearsOnYield(t *testing.T) {
	fake := newScriptedEngine[*int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Yields: 1}))
	clearOnYield := newClearOnYield()
	item := 7
	var atYieldPoll []*int
	var wordAtYieldPoll sideWord
	fake.setHooks(func(call engineCall) {
		if call.kind == callDequeue && call.count == 1 {
			atYieldPoll = append([]*int(nil), clearOnYield...)
			wordAtYieldPoll = queue.consumers.load()
			fake.pushRaw(&item)
		}
	}, nil)
	ctx, _ := newWaitContext(t)
	dst := make([]*int, 1)

	received, err := queue.receiveWait(ctx, dst, nil, clearOnYield)

	require.NoError(t, err)
	assert.Equal(t, 1, received)
	assert.Same(t, &item, dst[0])
	assert.Equal(t, uint32(1), wordAtYieldPoll.pollers(), "the poll ran registered")
	assert.Equal(t, []*int{nil, nil}, atYieldPoll, "cleared once the consumer registered")
	requireIdleSide(t, &queue.consumers)
	requireIdleSide(t, &queue.producers)
}

func TestReceiveWaitSpinKeepsClearOnYield(t *testing.T) {
	setGOMAXPROCS(t, 4)
	fake := newScriptedEngine[*int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Spins: 1}))
	clearOnYield := newClearOnYield()
	want := append([]*int(nil), clearOnYield...)
	item := 7
	fake.pushRaw(&item)
	ctx, _ := newWaitContext(t)
	dst := make([]*int, 1)

	received, err := queue.receiveWait(ctx, dst, nil, clearOnYield)

	require.NoError(t, err)
	assert.Equal(t, 1, received)
	assert.Same(t, &item, dst[0])
	assert.Equal(t, want, clearOnYield, "untouched when the spin phase finds an item")
	requireIdleSide(t, &queue.consumers)
	requireIdleSide(t, &queue.producers)
}

// TestReceiveWaitCloseAndCancelParked closes and cancels with no wait in between, so the parked
// consumer may wake on either. The ctx branch then sees closed, so every repetition returns
// ErrClosed.
func TestReceiveWaitCloseAndCancelParked(t *testing.T) {
	const repetitions = 50
	for repetition := range repetitions {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		ctx, cancel := newWaitContext(t)
		results := startReceiveWait(ctx, t, queue)
		awaitParked(t, &queue.consumers, 1)

		queue.Close()
		cancel()

		result := awaitResult(t, results)
		require.Same(t, ErrClosed, result.err, "repetition %d", repetition)
		require.Zero(t, result.received, "repetition %d", repetition)
		requireIdleQueue(t, queue)
	}
}

func TestReceiveWaitExtraAttemptLeavesItemsWakesWaiter(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	other := parkConsumer(t, &queue.consumers)
	// With no spins or yields, the first burst is the extra attempt. It takes a full dst of 2
	// and leaves item 3, which the parked waiter must be woken to find.
	fake.setHooks(func(call engineCall) {
		if call.kind == callDequeueBurst && call.count == 1 {
			fake.pushRaw(1, 2, 3)
		}
	}, nil)
	ctx, _ := newWaitContext(t)

	result := awaitResult(t, startReceiveWaitWith(ctx, t, queue, 2, nil))

	require.NoError(t, result.err)
	assert.Equal(t, []int{1, 2}, result.items)
	requireToken(t, other)
	queue.consumers.leavePolling(mustNotPeek(t))
	queue.consumers.putWaiter(other)
	assert.Equal(t, []int{3}, fake.takeRaw(1))
	requireIdleQueue(t, queue)
}

func TestReceiveWaitOwnWaiter(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	own := newWaiter()
	ctx, cancel := newWaitContext(t)
	results := startReceiveWaitWith(ctx, t, queue, 1, own)
	awaitParked(t, &queue.consumers, 1)
	requireOnlyListed(t, &queue.consumers, own)

	// A wake makes the consumer park again, so the same waiter goes through a token too.
	queue.consumers.wakeOne()
	awaitParked(t, &queue.consumers, 1)
	requireOnlyListed(t, &queue.consumers, own)
	cancel()

	result := awaitResult(t, results)
	assert.Same(t, context.Canceled, result.err)
	assert.False(t, own.listed, "listed")
	assert.Nil(t, own.prev, "prev")
	assert.Nil(t, own.next, "next")
	assert.Empty(t, own.token, "token")
	requireIdleQueue(t, queue)
}

// requireOnlyListed fails the test unless caller is the one waiter in side's list.
func requireOnlyListed(t *testing.T, side *waitSide, caller *waiter) {
	t.Helper()
	side.mu.Lock()
	defer side.mu.Unlock()
	require.Same(t, caller, side.oldest, "oldest")
	require.Same(t, caller, side.newest, "newest")
}
