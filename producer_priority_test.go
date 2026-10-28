package workq

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitOrder checks the protocol invariants and returns the list from oldest to newest.
func waitOrder(t *testing.T, side *waitSide) []*waiter {
	t.Helper()
	checkWaitSide(t, side, 0)
	side.mu.Lock()
	defer side.mu.Unlock()
	var callers []*waiter
	for caller := side.oldest; caller != nil; caller = caller.next {
		callers = append(callers, caller)
	}
	return callers
}

func TestProducerInitialParksFIFO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, _ := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
		t.Cleanup(func() { assertIdleQueue(t, queue) })
		var callers []*waiter
		for range 3 {
			ctx, cancel := newWaitContext(t)
			results := startPushWait(ctx, t, queue, 0)
			synctest.Wait()
			order := waitOrder(t, &queue.producers)
			callers = append(callers, order[len(order)-1])
			require.Equal(t, callers, order)
			t.Cleanup(func() {
				cancel()
				assert.Same(t, context.Canceled, awaitPushResult(t, results).err)
			})
		}
	})
}

func TestProducerFailedWakeRestoresOldest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
		ctx, cancel := newWaitContext(t)
		defer cancel()
		results := startPushWait(ctx, t, queue, 0)
		synctest.Wait()
		first := listedWaiter(t, &queue.producers)
		middle := parkProducer(t, &queue.producers)
		last := parkProducer(t, &queue.producers)
		defer func() {
			queue.producers.leaveList(middle, false, queue.peekRoom)
			queue.producers.leaveList(last, false, queue.peekRoom)
			queue.producers.putWaiter(middle)
			queue.producers.putWaiter(last)
		}()
		require.Equal(t, []*waiter{first, middle, last}, waitOrder(t, &queue.producers))

		for cycle := range 3 {
			reached := make(chan struct{})
			release := make(chan struct{})
			target := enqueueCall(fake.callCount(callEnqueue) + 1)
			fake.setHooks(func(call engineCall) {
				if call != target {
					return
				}
				close(reached)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}, nil)
			fake.takeRaw(1)
			queue.producers.wakeCheck()
			select {
			case <-reached:
			case <-time.After(waitTestTimeout):
				require.FailNow(t, "woken producer did not reach its enqueue attempt")
			}
			// Hold the woken caller before enqueue so the competing producer takes its room.
			require.NoError(t, queue.TryPush(100+cycle))
			close(release)
			synctest.Wait()
			order := waitOrder(t, &queue.producers)
			require.Len(t, order, 3)
			require.Equal(t, []*waiter{middle, last}, order[1:], "cycle %d", cycle)
			first = order[0]
		}
		cancel()
		result := awaitPushResult(t, results)
		assert.Same(t, context.Canceled, result.err)
		assert.Zero(t, result.pushed)
		assertUnlisted(t, first)
		assertNoToken(t, first)
	})
}

func TestProducerPartialProgressResetsPriority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, fake := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
		ctx, cancel := newWaitContext(t)
		results := make(chan pushWaitResult, 1)
		finished := make(chan struct{})
		t.Cleanup(func() {
			cancel()
			<-finished
		})
		go func() {
			defer close(finished)
			pushed, err := queue.PushBatch(ctx, []int{7, 8})
			results <- pushWaitResult{pushed: pushed, err: err}
		}()
		synctest.Wait()
		requireParked(t, &queue.producers, 1)
		other := parkProducer(t, &queue.producers)
		defer func() {
			queue.producers.leaveList(other, false, queue.peekRoom)
			queue.producers.putWaiter(other)
		}()

		queue.producers.wakeOne()
		synctest.Wait()
		order := waitOrder(t, &queue.producers)
		require.Len(t, order, 2)
		require.Same(t, other, order[1], "failed retry precedes the existing waiter")

		// One slot permits partial progress in the first chunk. Its blocked remainder must
		// start a fresh wait episode behind other, even if the pool supplies the same waiter.
		fake.takeRaw(1)
		queue.producers.wakeCheck()
		synctest.Wait()
		order = waitOrder(t, &queue.producers)
		require.Len(t, order, 2)
		require.Same(t, other, order[0], "partial progress ends retry eligibility")
		cancel()
		result := awaitPushResult(t, results)
		assert.Equal(t, 1, result.pushed)
		assert.Same(t, context.Canceled, result.err)
		assertUnlisted(t, order[1])
		assertNoToken(t, order[1])
	})
}

func TestWaitSideMultipleOldestInsertions(t *testing.T) {
	var side waitSide
	previous := parkProducer(t, &side)
	firstRetry := newWaiter()
	secondRetry := newWaiter()
	for _, caller := range []*waiter{firstRetry, secondRetry} {
		require.False(t, side.register())
		require.False(t, side.enlist(caller, atOldest))
	}
	newArrival := parkProducer(t, &side)
	order := []*waiter{secondRetry, firstRetry, previous, newArrival}
	require.Equal(t, order, waitOrder(t, &side))
	for _, caller := range order {
		side.wakeOne()
		requireToken(t, caller)
		assertUnlisted(t, caller)
		side.leavePolling(func() bool { return false })
		checkWaitSide(t, &side, 0)
	}
	assertIdleSide(t, &side)

	// Reuse the same object for an initial park after its earlier oldest insertion.
	other := parkProducer(t, &side)
	require.False(t, side.register())
	require.False(t, side.enlist(firstRetry, atNewest))
	require.Equal(t, []*waiter{other, firstRetry}, waitOrder(t, &side))
	side.leaveList(other, false, mustNotPeek(t))
	side.leaveList(firstRetry, false, mustNotPeek(t))
	assertIdleSide(t, &side)
}

func TestWaitSideOldestInsertionCleanup(t *testing.T) {
	for _, outcome := range []string{
		"empty list", "cancel listed", "cancel transferred", "close before", "close after",
	} {
		t.Run(outcome, func(t *testing.T) {
			var side waitSide
			var other *waiter
			if outcome != "empty list" {
				other = parkProducer(t, &side)
			}
			caller := newWaiter()
			require.False(t, side.register())
			if outcome == "close before" {
				side.mu.Lock()
				side.setClosedLocked()
				side.mu.Unlock()
			}
			before := side.load()
			closed := side.enlist(caller, atOldest)
			if outcome == "close before" {
				require.True(t, closed)
				assert.Equal(t, before, side.load())
				side.leavePolling(func() bool { return false })
			} else {
				require.False(t, closed)
				order := waitOrder(t, &side)
				require.Same(t, caller, order[0])
				switch outcome {
				case "cancel transferred":
					side.wakeOne()
					assertCounts(t, &side, 1, 1)
					assertUnlisted(t, caller)
				case "close after":
					side.mu.Lock()
					side.setClosedLocked()
					side.mu.Unlock()
				}
				side.leaveList(caller, false, func() bool { return false })
			}
			assertUnlisted(t, caller)
			assertNoToken(t, caller)
			if other != nil {
				require.Equal(t, []*waiter{other}, waitOrder(t, &side))
				side.leaveList(other, false, mustNotPeek(t))
			}
			assertIdleSide(t, &side)
		})
	}
}

func TestProducerRestoredRetryClose(t *testing.T) {
	for name, testCase := range closedWaitCases() {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue, _ := newFullTestQueue(t, WithProducerWait(WaitStrategy{}))
				ctx, cancel := newWaitContext(t)
				results := startPushWait(ctx, t, queue, testCase.pushedBefore)
				synctest.Wait()
				other := parkProducer(t, &queue.producers)
				queue.producers.wakeOne()
				synctest.Wait()
				order := waitOrder(t, &queue.producers)
				require.Len(t, order, 2)
				require.Same(t, other, order[1])

				queue.Close()
				cancel()
				result := awaitPushResult(t, results)
				assert.Same(t, testCase.wantErr, result.err)
				assert.Zero(t, result.pushed)
				assertUnlisted(t, order[0])
				assertNoToken(t, order[0])
				queue.producers.leaveList(other, false, queue.peekRoom)
				queue.producers.putWaiter(other)
				assertIdleQueue(t, queue)
			})
		})
	}
}
