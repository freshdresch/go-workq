package workq

import (
	"context"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitTestTimeout bounds how long a test waits for receiveWait to return or reach a state, so a
// lost wakeup fails the test instead of hanging it. In a synctest bubble, the fake clock jumps
// to it as soon as every goroutine is blocked, so a lost wakeup there fails at once.
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
		received, err := queue.receiveWait(ctx, nil, dst, own, nil)
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

// requireParked fails the test unless side's parked count is wantParked. A test in a synctest
// bubble calls it after synctest.Wait, which returns only once every waiter it started is
// blocked in its park select or has returned.
func requireParked(t *testing.T, side *waitSide, wantParked uint32) {
	t.Helper()
	require.Equal(t, wantParked, side.load().parked(), "parked count")
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
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Yields: 1}))
		// The wake check sees the consumer still registered for its yield-phase poll and skips, so
		// only the extra attempt after enlist can find the item.
		fake.setHooks(forceEmptyThenPush(fake, 1, 7, func() { queue.consumers.wakeCheck(nil) }))
		ctx, _ := newWaitContext(t)

		result := awaitResult(t, startReceiveWait(ctx, t, queue))

		require.NoError(t, result.err)
		assert.Equal(t, []int{7}, result.items)
		assert.Equal(t, 2, fake.callCount(callDequeue), "the yield poll and the extra attempt")
		assertIdleQueue(t, queue)
	})
}

func TestReceiveWaitItemDuringExtraAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		// With no spins or yields, the first dequeue is the extra attempt. The consumer is already
		// parked by then, so the wake check wakes it.
		fake.setHooks(forceEmptyThenPush(fake, 1, 7, func() { queue.consumers.wakeCheck(nil) }))
		ctx, _ := newWaitContext(t)

		result := awaitResult(t, startReceiveWait(ctx, t, queue))

		require.NoError(t, result.err)
		assert.Equal(t, []int{7}, result.items)
		assert.Equal(t, 2, fake.callCount(callDequeue), "the extra attempt and the woken poll")
		assertIdleQueue(t, queue)
	})
}

func TestReceiveWaitWokenPollsFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		// Dequeue 1 is the extra attempt of the first park. The word at dequeue 2 shows whether
		// the woken consumer polled before it enlisted again.
		var wordAtSecondDequeue sideWord
		fake.setHooks(func(call engineCall) {
			if call == dequeueCall(2) {
				wordAtSecondDequeue = queue.consumers.load()
			}
		}, nil)
		ctx, cancel := newWaitContext(t)
		results := startReceiveWait(ctx, t, queue)
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)

		queue.consumers.wakeOne(nil)

		synctest.Wait()
		requireParked(t, &queue.consumers, 1)
		require.Equal(t, 3, fake.callCount(callDequeue), "dequeue count")
		cancel()
		result := awaitResult(t, results)
		assert.Same(t, context.Canceled, result.err)
		assert.Zero(t, result.received)
		assert.Equal(
			t,
			uint32(1),
			wordAtSecondDequeue.pollers(),
			"woken consumer polls registered",
		)
		assert.Zero(t, wordAtSecondDequeue.parked(), "woken consumer polls before it enlists")
		assertIdleQueue(t, queue)
	})
}

func TestReceiveWaitCancelParked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		ctx, cancel := newWaitContext(t)
		results := startReceiveWait(ctx, t, queue)
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)

		cancel()

		result := awaitResult(t, results)
		assert.Same(t, context.Canceled, result.err)
		assert.Zero(t, result.received)
		assertIdleQueue(t, queue)
	})
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
	assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
		})
	}
}

func TestReceiveWaitClosedWhileParked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		ctx, _ := newWaitContext(t)
		results := startReceiveWait(ctx, t, queue)
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)

		queue.Close()

		result := awaitResult(t, results)
		assert.Same(t, ErrClosed, result.err)
		assert.Zero(t, result.received)
		assert.Equal(t, 2, fake.callCount(callDequeue), "the extra attempt and the drain check")
		assertIdleQueue(t, queue)
	})
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
	assertIdleQueue(t, queue)
}

func TestReceiveWaitWokenSeesClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		fake.holdVisibility()
		fake.pushRaw(7)
		ctx, _ := newWaitContext(t)
		results := startReceiveWait(ctx, t, queue)
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)

		// The consumer is parked, so the token from wakeOne decides its select before Close
		// runs, and the test covers the token path.
		queue.consumers.wakeOne(nil)
		fake.releaseVisibility()
		queue.Close()

		result := awaitResult(t, results)
		require.NoError(t, result.err)
		assert.Equal(t, []int{7}, result.items)
		received, err := queue.receiveWait(ctx, nil, make([]int, 1), nil, nil)
		assert.Same(t, ErrClosed, err, "second call")
		assert.Zero(t, received, "second call")
		assert.Zero(t, queue.consumers.load().pollers(), "pollers")
		assertIdleQueue(t, queue)
	})
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
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		fake.setHooks(pushBefore(fake, 1, 7), nil)
		requireReceiveWaitWakesProducer(t, queue)
	})
}

func TestReceiveWaitWakesProducerDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		fake.setHooks(forceEmptyThenPush(fake, 1, 7, queue.Close))
		requireReceiveWaitWakesProducer(t, queue)
	})
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
	queue.producers.leavePolling(nil, mustNotPeek(t))
	queue.producers.putWaiter(producer)
	assertIdleQueue(t, queue)
}

// neverParkLastPoll is the attempt at which the NeverPark tests act, after several empty polls.
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
			assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
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
			assertIdleQueue(t, queue)
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
	assertIdleQueue(t, queue)
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
	assertIdleQueue(t, queue)
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
	received, err := queue.receiveWait(ctx, nil, make([]int, 1), nil, nil)
	assert.Same(t, ErrClosed, err, "second call")
	assert.Zero(t, received, "second call")
	assertIdleQueue(t, queue)
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
	assertIdleQueue(t, queue)
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

	received, err := queue.receiveWait(ctx, nil, dst, nil, clearOnYield)

	require.NoError(t, err)
	assert.Equal(t, 1, received)
	assert.Same(t, &item, dst[0])
	assert.Equal(t, uint32(1), wordAtYieldPoll.pollers(), "the poll ran registered")
	assert.Equal(t, []*int{nil, nil}, atYieldPoll, "cleared once the consumer registered")
	assertIdleSide(t, &queue.consumers)
	assertIdleSide(t, &queue.producers)
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

	received, err := queue.receiveWait(ctx, nil, dst, nil, clearOnYield)

	require.NoError(t, err)
	assert.Equal(t, 1, received)
	assert.Same(t, &item, dst[0])
	assert.Equal(t, want, clearOnYield, "untouched when the spin phase finds an item")
	assertIdleSide(t, &queue.consumers)
	assertIdleSide(t, &queue.producers)
}

// TestReceiveWaitCancelThenCloseParked holds cancellation cleanup until Close finishes.
// The hook proves the parked select chose cancellation before closure was published.
func TestReceiveWaitCancelThenCloseParked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		selected := make(chan struct{})
		resume := make(chan struct{})
		queue.hooks.afterParkContext = func() {
			close(selected)
			<-resume
		}
		ctx, cancel := newWaitContext(t)
		results := startReceiveWait(ctx, t, queue)
		// Release the hook before the worker's cleanup joins it if an assertion fails.
		defer close(resume)
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)

		cancel()
		<-selected
		queue.Close()
		resume <- struct{}{}

		result := awaitResult(t, results)
		require.Same(t, ErrClosed, result.err)
		require.Zero(t, result.received)
		assertIdleQueue(t, queue)
	})
}

func TestReceiveWaitExtraAttemptLeavesItemsWakesWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		queue.consumers.leavePolling(nil, mustNotPeek(t))
		queue.consumers.putWaiter(other)
		assert.Equal(t, []int{3}, fake.takeRaw(1))
		assertIdleQueue(t, queue)
	})
}

// TestReceiveWaitRemovedCallerHandsOff covers the branch of leaveList in which a waker already
// removed the caller. The caller's extra attempt takes items, but a wake check during the attempt
// has already removed it and registered it as a poller, so it must take its token and leave
// polling, and that Leave polling must wake the other parked consumer for the item left over. The
// test relies on oldest-first wake order, which TestWaitSideWakeOrderOldestFirst pins, so the wake
// check picks the caller, which parks first. If the order changes, the wake goes to the other
// consumer and the test fails, and the fix is to park the other consumer before the caller.
func TestReceiveWaitRemovedCallerHandsOff(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
	var other *waiter
	// With no spins or yields, the first burst is the caller's extra attempt, which runs after
	// the caller is listed.
	fake.setHooks(func(call engineCall) {
		if call.kind == callDequeueBurst && call.count == 1 {
			other = parkConsumer(t, &queue.consumers)
			fake.pushRaw(1, 2, 3)
			queue.consumers.wakeCheck(nil)
			assertNoToken(t, other)
		}
	}, nil)

	dst := make([]int, 2)
	received, err := queue.receiveWait(context.Background(), nil, dst, nil, nil)

	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, dst[:received])
	require.NotNil(t, other, "the extra attempt's hook ran")
	requireToken(t, other)
	queue.consumers.leavePolling(nil, mustNotPeek(t))
	queue.consumers.putWaiter(other)
	assert.Equal(t, []int{3}, fake.takeRaw(1))
	assertIdleQueue(t, queue)
}

func TestReceiveWaitOwnWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newScriptedEngine[int](8)
		queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{}))
		own := newWaiter()
		ctx, cancel := newWaitContext(t)
		results := startReceiveWaitWith(ctx, t, queue, 1, own)
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)
		requireOnlyListed(t, &queue.consumers, own)

		// A wake makes the consumer park again, so the same waiter goes through a token too.
		queue.consumers.wakeOne(nil)
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)
		requireOnlyListed(t, &queue.consumers, own)
		cancel()

		result := awaitResult(t, results)
		assert.Same(t, context.Canceled, result.err)
		assert.False(t, own.listed, "listed")
		assert.Nil(t, own.prev, "prev")
		assert.Nil(t, own.next, "next")
		assert.Empty(t, own.token, "token")
		assertIdleQueue(t, queue)
	})
}

// requireOnlyListed fails the test unless caller is the one waiter in side's list.
func requireOnlyListed(t *testing.T, side *waitSide, caller *waiter) {
	t.Helper()
	side.mu.Lock()
	defer side.mu.Unlock()
	require.Same(t, caller, side.oldest, "oldest")
	require.Same(t, caller, side.newest, "newest")
}

// pushWaitCapacity is the capacity of the fake in the pushWait tests, which start with it full
// of items 1 to pushWaitCapacity.
const pushWaitCapacity = 4

// pushWaitItem is the item a pushWait test's attempt pushes.
const pushWaitItem = 7

// pushWaitResult is what one pushWait call returned.
type pushWaitResult struct {
	pushed int
	err    error
}

// newFullTestQueue returns a queue over a fake that is full, with options, and the fake.
func newFullTestQueue(t *testing.T, options ...Option) (*Queue[int], *scriptedEngine[int]) {
	t.Helper()
	fake := newScriptedEngine[int](pushWaitCapacity)
	queue := newTestQueue(t, fake, options...)
	fake.pushRaw(sequence(1, pushWaitCapacity)...)
	return queue, fake
}

// enqueueAttempt returns an attempt that pushes item with one Enqueue, as a one-item push does.
func enqueueAttempt(queue *Queue[int], item int) func(AttemptPhase) int {
	return func(AttemptPhase) int {
		if err := queue.engine.Enqueue(item); err != nil {
			return 0
		}
		return 1
	}
}

// startPushWait runs pushWait with an attempt that enqueues pushWaitItem in a new goroutine,
// and returns the channel that gets its result.
func startPushWait(
	ctx context.Context,
	t *testing.T,
	queue *Queue[int],
	pushedBefore int,
) <-chan pushWaitResult {
	t.Helper()
	return startPushWaitWith(ctx, t, queue, enqueueAttempt(queue, pushWaitItem), pushedBefore)
}

// startPushWaitWith runs pushWait with attempt in a new goroutine, and returns the channel that
// gets its result. The test owns the goroutine: the cleanup cancels its ctx and waits for it to
// return, so no test leaves one running.
func startPushWaitWith(
	ctx context.Context,
	t *testing.T,
	queue *Queue[int],
	attempt func(AttemptPhase) int,
	pushedBefore int,
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
		pushed, err := queue.pushWait(ctx, nil, attempt, pushedBefore)
		results <- pushWaitResult{pushed: pushed, err: err}
	}()
	return results
}

func awaitPushResult(t *testing.T, results <-chan pushWaitResult) pushWaitResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(waitTestTimeout):
		require.FailNow(t, "push did not return")
		return pushWaitResult{}
	}
}

func enqueueCall(count int) engineCall {
	return engineCall{kind: callEnqueue, count: count}
}

// freeRoomBefore returns a before hook that frees one slot just before enqueue number count,
// as a consumer that takes an item then would.
func freeRoomBefore(fake *scriptedEngine[int], count int) func(engineCall) {
	target := enqueueCall(count)
	return func(call engineCall) {
		if call == target {
			fake.takeRaw(1)
		}
	}
}

// requirePushedItem fails the test unless pushWaitItem is the newest item in the fake.
func requirePushedItem(t *testing.T, fake *scriptedEngine[int]) {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.NotEmpty(t, fake.visible, "visible items")
	require.Equal(t, pushWaitItem, fake.visible[len(fake.visible)-1], "newest item")
}

// closedWaitCases are the pushedBefore values that decide which error a producer returns when
// it sees the queue closed while it waits.
func closedWaitCases() map[string]struct {
	pushedBefore int
	wantErr      error
} {
	return map[string]struct {
		pushedBefore int
		wantErr      error
	}{
		"nothing pushed before": {pushedBefore: 0, wantErr: ErrClosed},
		"pushed before":         {pushedBefore: 3, wantErr: ErrClosedDuringPush},
	}
}

func TestPushWaitPartialExtraAttempt(t *testing.T) {
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
	fake.setMaxMove(2)
	// With no spins or yields, the first burst is the extra attempt.
	fake.setHooks(func(call engineCall) {
		if call.kind == callEnqueueBurst && call.count == 1 {
			fake.takeRaw(3)
		}
	}, nil)
	attempt := func(AttemptPhase) int {
		enqueued, _ := queue.engine.EnqueueBurst([]int{5, 6, 7})
		return enqueued
	}
	ctx, _ := newWaitContext(t)

	result := awaitPushResult(t, startPushWaitWith(ctx, t, queue, attempt, 0))

	require.NoError(t, result.err)
	assert.Equal(t, 2, result.pushed)
	assert.Equal(t, 1, fake.callCount(callEnqueueBurst), "only the extra attempt")
	assert.Equal(t, []int{4, 5, 6}, fake.takeRaw(3))
	assertIdleQueue(t, queue)
}

func TestPushWaitClosedWhileYielding(t *testing.T) {
	for name, closedCase := range closedWaitCases() {
		t.Run(name, func(t *testing.T) {
			queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Yields: 1000}))
			fake.setHooks(func(call engineCall) {
				if call == enqueueCall(3) {
					queue.Close()
				}
			}, nil)
			ctx, _ := newWaitContext(t)

			result := awaitPushResult(t, startPushWait(ctx, t, queue, closedCase.pushedBefore))

			assert.Same(t, closedCase.wantErr, result.err)
			assert.Zero(t, result.pushed)
			assert.Equal(t, 3, fake.callCount(callEnqueue), "returned from the third attempt")
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushWaitClosedWhileParked(t *testing.T) {
	for name, closedCase := range closedWaitCases() {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
				ctx, _ := newWaitContext(t)
				results := startPushWait(ctx, t, queue, closedCase.pushedBefore)
				synctest.Wait()
				requireParked(t, &queue.producers, 1)

				queue.Close()

				result := awaitPushResult(t, results)
				assert.Same(t, closedCase.wantErr, result.err)
				assert.Zero(t, result.pushed)
				assert.Equal(t, 1, fake.callCount(callEnqueue), "only the extra attempt")
				assertIdleQueue(t, queue)
			})
		})
	}
}

// TestPushWaitClosedDuringMovingAttempt closes the queue during the attempt that moves the
// item. The wake check after it sees closed, and the producer must still leave the wait.
func TestPushWaitClosedDuringMovingAttempt(t *testing.T) {
	setGOMAXPROCS(t, 4)
	strategies := map[string]WaitStrategy{
		"spinning":   {Spins: 1},
		"listed":     {},
		"registered": {Yields: 1},
	}
	for name, strategy := range strategies {
		t.Run(name, func(t *testing.T) {
			queue, fake := newFullTestQueue(t, WithProducerWait(strategy))
			fake.setHooks(func(call engineCall) {
				if call == enqueueCall(1) {
					fake.takeRaw(1)
					queue.Close()
				}
			}, nil)
			ctx, _ := newWaitContext(t)

			result := awaitPushResult(t, startPushWait(ctx, t, queue, 0))

			assert.Same(t, ErrClosedDuringPush, result.err)
			assert.Equal(t, 1, result.pushed)
			requirePushedItem(t, fake)
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushWaitClosedBeatsContext(t *testing.T) {
	setGOMAXPROCS(t, 4)
	strategies := map[string]WaitStrategy{
		"spin":  {Spins: 1},
		"yield": {Yields: 1},
	}
	for name, strategy := range strategies {
		t.Run(name, func(t *testing.T) {
			queue, fake := newFullTestQueue(t, WithProducerWait(strategy))
			ctx, cancel := newWaitContext(t)
			fake.setHooks(func(call engineCall) {
				if call == enqueueCall(1) {
					queue.Close()
					cancel()
				}
			}, nil)

			result := awaitPushResult(t, startPushWait(ctx, t, queue, 0))

			assert.Same(t, ErrClosed, result.err)
			assert.Zero(t, result.pushed)
			assertIdleQueue(t, queue)
		})
	}
}

// The consumer wake check tests park a consumer by hand and check that pushWait wakes it when
// it pushes an item at one site.

func TestPushWaitWakesConsumerSpin(t *testing.T) {
	setGOMAXPROCS(t, 4)
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Spins: 1}))
	fake.setHooks(freeRoomBefore(fake, 1), nil)
	requirePushWaitWakesConsumer(t, queue, fake)
}

func TestPushWaitWakesConsumerYield(t *testing.T) {
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Yields: 1}))
	fake.setHooks(freeRoomBefore(fake, 1), nil)
	requirePushWaitWakesConsumer(t, queue, fake)
}

func TestPushWaitWakesConsumerExtraAttempt(t *testing.T) {
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
	fake.setHooks(freeRoomBefore(fake, 1), nil)
	requirePushWaitWakesConsumer(t, queue, fake)
}

// requirePushWaitWakesConsumer parks a consumer, runs pushWait, which the caller's hooks make
// push its item with its first enqueue, and checks that the consumer got a token.
func requirePushWaitWakesConsumer(t *testing.T, queue *Queue[int], fake *scriptedEngine[int]) {
	t.Helper()
	consumer := parkConsumer(t, &queue.consumers)
	ctx, _ := newWaitContext(t)

	result := awaitPushResult(t, startPushWait(ctx, t, queue, 0))

	require.NoError(t, result.err)
	assert.Equal(t, 1, result.pushed)
	assert.Equal(t, 1, fake.callCount(callEnqueue), "the first attempt moved the item")
	requirePushedItem(t, fake)
	requireToken(t, consumer)
	queue.consumers.leavePolling(nil, mustNotPeek(t))
	queue.consumers.putWaiter(consumer)
	assertIdleQueue(t, queue)
}

func TestPushWaitCancelParked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, _ := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
		ctx, cancel := newWaitContext(t)
		results := startPushWait(ctx, t, queue, 0)
		synctest.Wait()
		requireParked(t, &queue.producers, 1)

		cancel()

		result := awaitPushResult(t, results)
		assert.Same(t, context.Canceled, result.err)
		assert.Zero(t, result.pushed)
		assertIdleQueue(t, queue)
	})
}

func TestPushWaitCancelYielding(t *testing.T) {
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Yields: 1000}))
	ctx, cancel := newWaitContext(t)
	fake.setHooks(func(call engineCall) {
		if call == enqueueCall(3) {
			cancel()
		}
	}, nil)

	result := awaitPushResult(t, startPushWait(ctx, t, queue, 0))

	assert.Same(t, context.Canceled, result.err)
	assert.Zero(t, result.pushed)
	assert.Equal(t, 3, fake.callCount(callEnqueue), "returned from the third yield-phase attempt")
	assertIdleQueue(t, queue)
}

// TestPushWaitCancelThenCloseParked holds cancellation cleanup until Close finishes.
// The hook proves the parked select chose cancellation before closure was published.
func TestPushWaitCancelThenCloseParked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, _ := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
		selected := make(chan struct{})
		resume := make(chan struct{})
		queue.hooks.afterParkContext = func() {
			close(selected)
			<-resume
		}
		ctx, cancel := newWaitContext(t)
		results := startPushWait(ctx, t, queue, 0)
		// Release the hook before the worker's cleanup joins it if an assertion fails.
		defer close(resume)
		synctest.Wait()
		requireParked(t, &queue.producers, 1)

		cancel()
		<-selected
		queue.Close()
		resume <- struct{}{}

		result := awaitPushResult(t, results)
		require.Same(t, ErrClosed, result.err)
		require.Zero(t, result.pushed)
		assertIdleQueue(t, queue)
	})
}

// TestPushWaitCancelThenCloseMidway cancels a parked producer and then closes, and lets the
// producer run to completion while Close is between closing done and setting closed on the
// producer-side word. The cancel sends the producer to the ctx branch, where only done shows the
// close, so the closed error must still win.
func TestPushWaitCancelThenCloseMidway(t *testing.T) {
	setGOMAXPROCS(t, 1)
	testCases := []struct {
		name         string
		pushedBefore int
		wantErr      error
	}{
		{name: "nothing pushed before", pushedBefore: 0, wantErr: ErrClosed},
		{name: "items pushed before", pushedBefore: 3, wantErr: ErrClosedDuringPush},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue, _ := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
				ctx, cancel := newWaitContext(t)
				results := startPushWait(ctx, t, queue, testCase.pushedBefore)
				synctest.Wait()
				requireParked(t, &queue.producers, 1)
				// results has a buffer of 1, and the producer sends before it exits, so a
				// result in it shows the producer finished inside the window.
				finishedMidway := false
				queue.hooks.afterCloseDone = func() {
					synctest.Wait()
					finishedMidway = len(results) == 1
				}

				cancel()
				queue.Close()

				result := awaitPushResult(t, results)
				assert.Same(t, testCase.wantErr, result.err)
				assert.Zero(t, result.pushed)
				assert.True(t, finishedMidway, "producer finished between done and closed")
				assertIdleQueue(t, queue)
			})
		})
	}
}

func TestPushWaitWokenPollsFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
		// Enqueue 1 is the extra attempt of the first park. The word at enqueue 2 shows whether
		// the woken producer attempted before it enlisted again.
		var wordAtSecondEnqueue sideWord
		fake.setHooks(func(call engineCall) {
			if call == enqueueCall(2) {
				wordAtSecondEnqueue = queue.producers.load()
			}
		}, nil)
		ctx, cancel := newWaitContext(t)
		results := startPushWait(ctx, t, queue, 0)
		synctest.Wait()
		requireParked(t, &queue.producers, 1)

		queue.producers.wakeOne(nil)

		synctest.Wait()
		requireParked(t, &queue.producers, 1)
		require.Equal(t, 3, fake.callCount(callEnqueue), "enqueue count")
		cancel()
		result := awaitPushResult(t, results)
		assert.Same(t, context.Canceled, result.err)
		assert.Zero(t, result.pushed)
		assert.Equal(
			t,
			uint32(1),
			wordAtSecondEnqueue.pollers(),
			"woken producer attempts registered",
		)
		assert.Zero(t, wordAtSecondEnqueue.parked(), "woken producer attempts before it enlists")
		assertIdleQueue(t, queue)
	})
}

// TestPushWaitWokenFindsRoom frees room and runs the producer wake check, as a consumer does,
// while the producer is parked.
func TestPushWaitWokenFindsRoom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
		ctx, _ := newWaitContext(t)
		results := startPushWait(ctx, t, queue, 0)
		synctest.Wait()
		requireParked(t, &queue.producers, 1)

		fake.takeRaw(1)
		queue.producers.wakeCheck(nil)

		result := awaitPushResult(t, results)
		require.NoError(t, result.err)
		assert.Equal(t, 1, result.pushed)
		requirePushedItem(t, fake)
		assertIdleQueue(t, queue)
	})
}

func TestPushWaitExtraAttemptLeavesRoomWakesWaiter(t *testing.T) {
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
	other := parkProducer(t, &queue.producers)
	// With no spins or yields, the first enqueue is the extra attempt. It takes one of two
	// freed slots, and the parked producer must be woken to use the other.
	fake.setHooks(func(call engineCall) {
		if call == enqueueCall(1) {
			fake.takeRaw(2)
		}
	}, nil)
	ctx, _ := newWaitContext(t)

	result := awaitPushResult(t, startPushWait(ctx, t, queue, 0))

	require.NoError(t, result.err)
	assert.Equal(t, 1, result.pushed)
	requireToken(t, other)
	queue.producers.leavePolling(nil, mustNotPeek(t))
	queue.producers.putWaiter(other)
	assertIdleQueue(t, queue)
}

// TestPushWaitPooledWaiterLeftClean records each waiter the producer parks with, through a park
// that ends in a token and one that ends in a cancel, and checks that each is ready for reuse
// once pushWait returns.
func TestPushWaitPooledWaiterLeftClean(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, _ := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
		ctx, cancel := newWaitContext(t)
		results := startPushWait(ctx, t, queue, 0)
		synctest.Wait()
		requireParked(t, &queue.producers, 1)
		parkedWaiters := []*waiter{listedWaiter(t, &queue.producers)}

		queue.producers.wakeOne(nil)
		synctest.Wait()
		requireParked(t, &queue.producers, 1)
		parkedWaiters = append(parkedWaiters, listedWaiter(t, &queue.producers))
		cancel()

		result := awaitPushResult(t, results)
		assert.Same(t, context.Canceled, result.err)
		for i, parked := range parkedWaiters {
			assert.False(t, parked.listed, "listed, park %d", i+1)
			assert.Nil(t, parked.prev, "prev, park %d", i+1)
			assert.Nil(t, parked.next, "next, park %d", i+1)
			assert.Empty(t, parked.token, "token, park %d", i+1)
		}
		assertIdleQueue(t, queue)
	})
}

// listedWaiter returns the one waiter in side's list.
func listedWaiter(t *testing.T, side *waitSide) *waiter {
	t.Helper()
	side.mu.Lock()
	defer side.mu.Unlock()
	require.NotNil(t, side.oldest, "oldest")
	require.Same(t, side.oldest, side.newest, "one waiter")
	return side.oldest
}

// runNeverParkPushWait runs pushWait with strategy on a full queue, runs act from the before
// hook of enqueue neverParkLastPoll, and checks that the producer stayed registered and never
// parked through every enqueue up to that one.
func runNeverParkPushWait(
	t *testing.T,
	strategy WaitStrategy,
	act func(queue *Queue[int], fake *scriptedEngine[int], cancel context.CancelFunc),
) (pushWaitResult, *Queue[int]) {
	t.Helper()
	queue, fake := newFullTestQueue(t, WithProducerWait(strategy))
	ctx, cancel := newWaitContext(t)
	var words []sideWord
	fake.setHooks(func(call engineCall) {
		if call.kind != callEnqueue || call.count > neverParkLastPoll {
			return
		}
		words = append(words, queue.producers.load())
		if call.count == neverParkLastPoll {
			act(queue, fake, cancel)
		}
	}, nil)

	result := awaitPushResult(t, startPushWait(ctx, t, queue, 0))

	require.Len(t, words, neverParkLastPoll, "enqueues up to the last attempt")
	for i, word := range words {
		assert.Equal(t, uint32(1), word.pollers(), "pollers at enqueue %d", i+1)
		assert.Zero(t, word.parked(), "parked at enqueue %d", i+1)
	}
	return result, queue
}

func TestPushWaitNeverParkRoom(t *testing.T) {
	for name, strategy := range neverParkStrategies() {
		t.Run(name, func(t *testing.T) {
			result, queue := runNeverParkPushWait(
				t,
				strategy,
				func(_ *Queue[int], fake *scriptedEngine[int], _ context.CancelFunc) {
					fake.takeRaw(1)
				},
			)
			require.NoError(t, result.err)
			assert.Equal(t, 1, result.pushed)
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushWaitNeverParkCancel(t *testing.T) {
	for name, strategy := range neverParkStrategies() {
		t.Run(name, func(t *testing.T) {
			result, queue := runNeverParkPushWait(
				t,
				strategy,
				func(_ *Queue[int], _ *scriptedEngine[int], cancel context.CancelFunc) {
					cancel()
				},
			)
			assert.Same(t, context.Canceled, result.err)
			assert.Zero(t, result.pushed)
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushWaitNeverParkClose(t *testing.T) {
	for name, strategy := range neverParkStrategies() {
		t.Run(name, func(t *testing.T) {
			result, queue := runNeverParkPushWait(
				t,
				strategy,
				func(queue *Queue[int], _ *scriptedEngine[int], _ context.CancelFunc) {
					queue.Close()
				},
			)
			assert.Same(t, ErrClosed, result.err)
			assert.Zero(t, result.pushed)
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushWaitSpinClosedBeforeCall(t *testing.T) {
	setGOMAXPROCS(t, 4)
	for name, closedCase := range closedWaitCases() {
		t.Run(name, func(t *testing.T) {
			queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Spins: 1}))
			ctx, cancel := newWaitContext(t)
			queue.Close()
			cancel()

			result := awaitPushResult(t, startPushWait(ctx, t, queue, closedCase.pushedBefore))

			assert.Same(t, closedCase.wantErr, result.err)
			assert.Zero(t, result.pushed)
			assert.Zero(t, fake.callCount(callEnqueue), "closed is seen before the first attempt")
			assertIdleQueue(t, queue)
		})
	}
}

func TestPushWaitSpinCanceledBeforeCall(t *testing.T) {
	setGOMAXPROCS(t, 4)
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Spins: 100}))
	ctx, cancel := newWaitContext(t)
	cancel()

	result := awaitPushResult(t, startPushWait(ctx, t, queue, 0))

	assert.Same(t, context.Canceled, result.err)
	assert.Zero(t, result.pushed)
	assert.Zero(t, fake.callCount(callEnqueue), "ctx is checked before the first attempt")
	assertIdleQueue(t, queue)
}

func TestPushWaitSpinChecksContextEvery64Polls(t *testing.T) {
	setGOMAXPROCS(t, 4)
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Spins: 1000}))
	ctx, cancel := newWaitContext(t)
	fake.setHooks(func(call engineCall) {
		if call == enqueueCall(64) {
			cancel()
		}
	}, nil)

	result := awaitPushResult(t, startPushWait(ctx, t, queue, 0))

	assert.Same(t, context.Canceled, result.err)
	assert.Zero(t, result.pushed)
	assert.Equal(t, 64, fake.callCount(callEnqueue), "the check before attempt 65 saw the cancel")
	assertIdleQueue(t, queue)
}

func TestPushWaitClosedBeforeRegister(t *testing.T) {
	for name, closedCase := range closedWaitCases() {
		t.Run(name, func(t *testing.T) {
			queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
			queue.Close()
			ctx, _ := newWaitContext(t)

			result := awaitPushResult(t, startPushWait(ctx, t, queue, closedCase.pushedBefore))

			assert.Same(t, closedCase.wantErr, result.err)
			assert.Zero(t, result.pushed)
			assert.Zero(t, fake.callCount(callEnqueue), "register saw closed")
			assertIdleQueue(t, queue)
		})
	}
}

// actingContext runs action from the first Done call made once condition holds. A wait
// checks ctx right after the closed load that follows an empty attempt, so the action lands
// between that load and the wait's next step. The wait must run on the test's goroutine,
// because the fields are not synchronized.
type actingContext struct {
	context.Context
	condition func() bool
	action    func()
	acted     bool
}

func (ctx *actingContext) Done() <-chan struct{} {
	if !ctx.acted && ctx.condition() {
		ctx.acted = true
		ctx.action()
	}
	return ctx.Context.Done()
}

// closeAfterFirstAttempt returns a context that closes queue at the ctx check after the first
// call of kind.
func closeAfterFirstAttempt(
	t *testing.T,
	queue *Queue[int],
	fake *scriptedEngine[int],
	kind engineCallKind,
) context.Context {
	t.Helper()
	ctx, _ := newWaitContext(t)
	return &actingContext{
		Context:   ctx,
		condition: func() bool { return fake.callCount(kind) >= 1 },
		action:    queue.Close,
	}
}

// TestPushWaitClosedAtEnlist closes the queue after the last yield-phase attempt, so enlist
// sees closed.
func TestPushWaitClosedAtEnlist(t *testing.T) {
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Yields: 1}))
	ctx := closeAfterFirstAttempt(t, queue, fake, callEnqueue)

	pushed, err := queue.pushWait(ctx, nil, enqueueAttempt(queue, pushWaitItem), 0)

	assert.Same(t, ErrClosed, err)
	assert.Zero(t, pushed)
	assert.Equal(t, 1, fake.callCount(callEnqueue), "no extra attempt")
	assertIdleQueue(t, queue)
}

// TestPushWaitClosedBeforeRegisteredAttempt closes the queue between two yield-phase attempts,
// so the closed check before the second one sees it.
func TestPushWaitClosedBeforeRegisteredAttempt(t *testing.T) {
	queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Yields: 2}))
	ctx := closeAfterFirstAttempt(t, queue, fake, callEnqueue)

	pushed, err := queue.pushWait(ctx, nil, enqueueAttempt(queue, pushWaitItem), 0)

	assert.Same(t, ErrClosed, err)
	assert.Zero(t, pushed)
	assert.Equal(t, 1, fake.callCount(callEnqueue), "no second attempt")
	assertIdleQueue(t, queue)
}

// TestReceiveWaitClosedAtEnlist closes the queue after the last yield-phase poll, so enlist
// sees closed and the consumer goes to the drain check.
func TestReceiveWaitClosedAtEnlist(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Yields: 1}))
	ctx := closeAfterFirstAttempt(t, queue, fake, callDequeue)

	received, err := queue.receiveWait(ctx, nil, make([]int, 1), nil, nil)

	assert.Same(t, ErrClosed, err)
	assert.Zero(t, received)
	assert.Equal(t, 2, fake.callCount(callDequeue), "the yield poll and the drain check")
	assertIdleQueue(t, queue)
}

// TestReceiveWaitClosedBeforeRegisteredPoll closes the queue between two yield-phase polls, so
// the closed check before the second one sends the consumer to the drain check.
func TestReceiveWaitClosedBeforeRegisteredPoll(t *testing.T) {
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Yields: 2}))
	ctx := closeAfterFirstAttempt(t, queue, fake, callDequeue)

	received, err := queue.receiveWait(ctx, nil, make([]int, 1), nil, nil)

	assert.Same(t, ErrClosed, err)
	assert.Zero(t, received)
	assert.Equal(t, 2, fake.callCount(callDequeue), "the first poll and the drain check")
	assertIdleQueue(t, queue)
}

// The Leave polling peek tests park another waiter by hand on the waiting side and check that
// a registered caller leaving polling wakes it when room or items are left behind. Each case
// acts on the first attempt of a wait with one yield.

// leavePollingCase is one exit from polling. LeftOver is how many slots or items the case
// frees or pushes at attempt 1, before the attempt if before is set and after it otherwise,
// and act ends the wait at the same point.
type leavePollingCase struct {
	name     string
	before   bool
	leftOver int
	act      func(queue *Queue[int], cancel context.CancelFunc)
}

func leavePollingCases() []leavePollingCase {
	return []leavePollingCase{
		{name: "moving", before: true, leftOver: 2, act: func(*Queue[int], context.CancelFunc) {}},
		{
			name:     "ctx",
			leftOver: 1,
			act:      func(_ *Queue[int], cancel context.CancelFunc) { cancel() },
		},
		{
			name:     "closed",
			leftOver: 1,
			act:      func(queue *Queue[int], _ context.CancelFunc) { queue.Close() },
		},
	}
}

// leavePollingHooks returns hooks that run leaveBehind and then act at attempt 1 of kind,
// before the attempt when before is set and after it otherwise.
func leavePollingHooks(
	kind engineCallKind,
	before bool,
	leaveBehind func(),
	act func(),
) (beforeHook func(engineCall), afterHook func(engineCall)) {
	target := engineCall{kind: kind, count: 1}
	hook := func(call engineCall) {
		if call == target {
			leaveBehind()
			act()
		}
	}
	if before {
		return hook, nil
	}
	return nil, hook
}

func TestPushWaitLeavePollingWakesWaiter(t *testing.T) {
	wantErrs := map[string]error{
		"ctx":    context.Canceled,
		"closed": ErrClosed,
	}
	for _, exit := range leavePollingCases() {
		t.Run(exit.name, func(t *testing.T) {
			queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{Yields: 1}))
			other := parkProducer(t, &queue.producers)
			ctx, cancel := newWaitContext(t)
			fake.setHooks(leavePollingHooks(
				callEnqueue,
				exit.before,
				func() { fake.takeRaw(exit.leftOver) },
				func() { exit.act(queue, cancel) },
			))

			result := awaitPushResult(t, startPushWait(ctx, t, queue, 0))

			if wantErr, ok := wantErrs[exit.name]; ok {
				assert.Same(t, wantErr, result.err)
				assert.Zero(t, result.pushed)
			} else {
				require.NoError(t, result.err)
				assert.Equal(t, 1, result.pushed)
			}
			requireToken(t, other)
			queue.producers.leavePolling(nil, mustNotPeek(t))
			queue.producers.putWaiter(other)
			assertIdleQueue(t, queue)
		})
	}
}

func TestReceiveWaitLeavePollingWakesWaiter(t *testing.T) {
	for _, exit := range leavePollingCases() {
		t.Run(exit.name, func(t *testing.T) {
			fake := newScriptedEngine[int](8)
			queue := newTestQueue(t, fake, WithConsumerWait(WaitStrategy{Yields: 1}))
			other := parkConsumer(t, &queue.consumers)
			ctx, cancel := newWaitContext(t)
			fake.setHooks(leavePollingHooks(
				callDequeue,
				exit.before,
				func() { fake.pushRaw(sequence(1, exit.leftOver)...) },
				func() { exit.act(queue, cancel) },
			))

			result := awaitResult(t, startReceiveWait(ctx, t, queue))

			// The ctx exit returns ctx.Err() and leaves its item queued. The moving exit and the
			// drain check after the closed exit each take item 1, and the moving exit leaves
			// item 2.
			if exit.name == "ctx" {
				assert.Same(t, context.Canceled, result.err)
			} else {
				require.NoError(t, result.err)
				assert.Equal(t, []int{1}, result.items)
			}
			wantLeft := map[string][]int{"moving": {2}, "ctx": {1}, "closed": nil}[exit.name]
			assert.Equal(t, wantLeft, fake.takeRaw(len(wantLeft)), "items left for the woken one")
			requireToken(t, other)
			queue.consumers.leavePolling(nil, mustNotPeek(t))
			queue.consumers.putWaiter(other)
			assertIdleQueue(t, queue)
		})
	}
}
