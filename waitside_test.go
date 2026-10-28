package workq

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkWaitSide fails the test unless the side holds invariants I1, I2, and I3 with
// wantPollers registered pollers, and the list links agree from both ends.
func checkWaitSide(t *testing.T, side *waitSide, wantPollers uint32) {
	t.Helper()
	side.mu.Lock()
	defer side.mu.Unlock()

	word := side.load()
	assert.Equal(t, wantPollers, word.pollers(), "pollers (I2)")

	parked := int(word.parked())
	var forward []*waiter
	var previous *waiter
	for current := side.oldest; current != nil && len(forward) <= parked; current = current.next {
		assert.Same(t, previous, current.prev, "prev link of listed waiter %d", len(forward))
		assert.True(t, current.listed, "listed flag of listed waiter %d (I3)", len(forward))
		assert.Empty(t, current.token, "token of listed waiter %d (I3)", len(forward))
		forward = append(forward, current)
		previous = current
	}
	require.Len(t, forward, parked, "list length against parked (I1)")
	assert.Same(t, previous, side.newest, "newest is the last waiter walking forward")

	var backward []*waiter
	for current := side.newest; current != nil && len(backward) <= parked; current = current.prev {
		backward = append(backward, current)
	}
	require.Len(t, backward, parked, "list length walking from newest")
	for i, current := range backward {
		assert.Same(t, forward[parked-1-i], current, "waiter %d from newest", i)
	}
	if parked > 0 {
		assert.Nil(t, side.oldest.prev, "prev of oldest")
		assert.Nil(t, side.newest.next, "next of newest")
	}
}

// assertIdleSide fails the test unless no caller is registered or parked on the side.
func assertIdleSide(t *testing.T, side *waitSide) {
	t.Helper()
	side.mu.Lock()
	defer side.mu.Unlock()
	assert.Zero(t, side.load()&^closedFlag, "word apart from closed")
	assert.Nil(t, side.oldest, "oldest")
	assert.Nil(t, side.newest, "newest")
}

// visibleItems stands in for the engine in the model tests: it counts the items a dequeue can
// see.
type visibleItems struct {
	count atomic.Int64
}

func (items *visibleItems) peek() bool {
	return items.count.Load() > 0
}

// parkConsumer registers a new consumer and parks it, as a consumer whose polls and extra
// attempt found nothing does.
func parkConsumer(t *testing.T, side *waitSide) *waiter {
	t.Helper()
	caller := side.getWaiter()
	require.False(t, side.register(), "register")
	require.False(t, side.enlist(caller, atNewest), "enlist")
	return caller
}

// requireToken receives the token a waker sent to caller, failing the test if there is none.
func requireToken(t *testing.T, caller *waiter) {
	t.Helper()
	select {
	case <-caller.token:
	default:
		require.FailNow(t, "waiter has no wake token")
	}
}

// splitWoken returns the one waiter of first and second that holds a wake token, and the other
// one, without taking the token. It fails the test unless exactly one holds a token, so tests
// do not depend on the wake order.
func splitWoken(t *testing.T, first *waiter, second *waiter) (woken *waiter, other *waiter) {
	t.Helper()
	firstWoken := len(first.token) == 1
	secondWoken := len(second.token) == 1
	require.NotEqual(t, firstWoken, secondWoken, "exactly one waiter holds a wake token")
	if firstWoken {
		return first, second
	}
	return second, first
}

func assertNoToken(t *testing.T, caller *waiter) {
	t.Helper()
	assert.Empty(t, caller.token, "waiter got a wake token")
}

func assertCounts(t *testing.T, side *waitSide, wantPollers uint32, wantParked uint32) {
	t.Helper()
	word := side.load()
	assert.Equal(t, wantPollers, word.pollers(), "pollers")
	assert.Equal(t, wantParked, word.parked(), "parked")
}

func assertUnlisted(t *testing.T, caller *waiter) {
	t.Helper()
	assert.False(t, caller.listed, "listed")
	assert.Nil(t, caller.prev, "prev")
	assert.Nil(t, caller.next, "next")
}

func TestWaitSideStalledProducer(t *testing.T) {
	var side waitSide
	var items visibleItems
	consumer := parkConsumer(t, &side)

	// Producer B completes, but its item stays invisible behind stalled producer A.
	assert.False(t, side.wakeCheck())
	requireToken(t, consumer)
	assertCounts(t, &side, 1, 0)
	require.False(t, items.peek(), "B's item is visible before A completes")
	require.False(t, side.enlist(consumer, atNewest))

	// A completes, so A's and B's items become visible together.
	items.count.Store(2)
	assert.False(t, side.wakeCheck())
	requireToken(t, consumer)
	assertCounts(t, &side, 1, 0)
	items.count.Add(-2)
	side.leavePolling(items.peek)

	assertUnlisted(t, consumer)
	side.putWaiter(consumer)
	checkWaitSide(t, &side, 0)
}

func TestWaitSideCancelWhileParked(t *testing.T) {
	var side waitSide
	var items visibleItems
	older := parkConsumer(t, &side)
	consumer := parkConsumer(t, &side)
	newer := parkConsumer(t, &side)
	items.count.Store(1)

	side.leaveList(consumer, false, items.peek)

	assertUnlisted(t, consumer)
	assertNoToken(t, consumer)
	assertNoToken(t, older)
	assertNoToken(t, newer)
	assertCounts(t, &side, 0, 2)
	checkWaitSide(t, &side, 0)

	side.leaveList(older, false, items.peek)
	side.leaveList(newer, false, items.peek)
	assertNoToken(t, older)
	assertNoToken(t, newer)
	assertCounts(t, &side, 0, 0)
	checkWaitSide(t, &side, 0)
}

func TestWaitSideCancelWhilePolling(t *testing.T) {
	var side waitSide
	var items visibleItems
	parked := parkConsumer(t, &side)
	require.False(t, side.register(), "register the polling consumer")
	items.count.Store(1)

	assert.False(t, side.wakeCheck())
	assertNoToken(t, parked)
	assertCounts(t, &side, 1, 1)

	// The polling consumer's ctx is done before it polls.
	side.leavePolling(items.peek)
	requireToken(t, parked)
	assertCounts(t, &side, 1, 0)

	items.count.Add(-1)
	side.leavePolling(items.peek)
	checkWaitSide(t, &side, 0)
}

func TestWaitSideCancelRacingWaker(t *testing.T) {
	for _, itemVisible := range []bool{true, false} {
		name := "nothing visible"
		if itemVisible {
			name = "item visible"
		}
		t.Run(name, func(t *testing.T) {
			var side waitSide
			var items visibleItems
			first := parkConsumer(t, &side)
			second := parkConsumer(t, &side)

			assert.False(t, side.wakeCheck())
			woken, other := splitWoken(t, first, second)
			assertUnlisted(t, woken)
			assertCounts(t, &side, 1, 1)
			if itemVisible {
				items.count.Store(1)
			}

			// The woken waiter's ctx is done after the waker removed it.
			side.leaveList(woken, false, items.peek)
			assertUnlisted(t, woken)
			assertNoToken(t, woken)
			if !itemVisible {
				assertNoToken(t, other)
				assertCounts(t, &side, 0, 1)
				side.leaveList(other, false, items.peek)
				checkWaitSide(t, &side, 0)
				return
			}
			requireToken(t, other)
			assertCounts(t, &side, 1, 0)
			items.count.Add(-1)
			side.leavePolling(items.peek)
			checkWaitSide(t, &side, 0)
		})
	}
}

func TestWaitSideParkWhileClosing(t *testing.T) {
	t.Run("closed before enlist", func(t *testing.T) {
		var side waitSide
		var items visibleItems
		caller := side.getWaiter()
		require.False(t, side.register())

		side.mu.Lock()
		assert.True(t, side.setClosedLocked())
		assert.False(t, side.setClosedLocked(), "second close")
		side.mu.Unlock()
		before := side.load()

		assert.True(t, side.enlist(caller, atNewest))
		assert.Equal(t, before, side.load(), "enlist on a closed word")
		assertUnlisted(t, caller)
		assert.True(t, side.register(), "register on a closed word")
		assert.Equal(t, before, side.load(), "register on a closed word")

		side.leavePolling(items.peek)
		assert.True(t, side.load().closed())
		assertIdleSide(t, &side)
		checkWaitSide(t, &side, 0)
	})

	t.Run("closed after enlist", func(t *testing.T) {
		var side waitSide
		var items visibleItems
		caller := parkConsumer(t, &side)

		side.mu.Lock()
		assert.True(t, side.setClosedLocked())
		side.mu.Unlock()
		assert.True(t, caller.listed)
		assertCounts(t, &side, 0, 1)

		// done is closed, so the waiter stops waiting without a token.
		side.leaveList(caller, false, items.peek)
		assertUnlisted(t, caller)
		assertNoToken(t, caller)
		assert.True(t, side.load().closed())
		assertIdleSide(t, &side)
		checkWaitSide(t, &side, 0)
	})
}

func TestWaitSidePeekRacesCompletion(t *testing.T) {
	var side waitSide
	var items visibleItems
	parked := parkConsumer(t, &side)
	require.False(t, side.register(), "register the last poller")

	side.leavePolling(items.peek)
	assertNoToken(t, parked)
	assertCounts(t, &side, 0, 1)

	items.count.Store(1)
	assert.False(t, side.wakeCheck())
	requireToken(t, parked)
	assertCounts(t, &side, 1, 0)

	items.count.Add(-1)
	side.leavePolling(items.peek)
	checkWaitSide(t, &side, 0)
}

func TestWaitSidePeekAfterDecrement(t *testing.T) {
	var side waitSide
	var items visibleItems
	parked := parkConsumer(t, &side)
	require.False(t, side.register(), "register the last poller")

	peekRacingProducer := func() bool {
		seen := items.count.Load()
		items.count.Add(1)
		side.wakeCheck()
		return seen > 0
	}
	side.leavePolling(peekRacingProducer)

	require.Len(t, parked.token, 1, "wake tokens sent to the parked waiter")
	requireToken(t, parked)
	assertCounts(t, &side, 1, 0)

	items.count.Add(-1)
	side.leavePolling(items.peek)
	checkWaitSide(t, &side, 0)
}

func TestWaitSideLeaveListAfterItems(t *testing.T) {
	t.Run("items left behind", func(t *testing.T) {
		var side waitSide
		var items visibleItems
		leaving := parkConsumer(t, &side)
		other := parkConsumer(t, &side)
		items.count.Store(3)

		// The extra attempt takes a full batch of 2 and leaves 1 behind.
		items.count.Add(-2)
		side.leaveList(leaving, true, items.peek)

		assertUnlisted(t, leaving)
		requireToken(t, other)
		assertCounts(t, &side, 1, 0)
		items.count.Add(-1)
		side.leavePolling(items.peek)
		checkWaitSide(t, &side, 0)
	})

	t.Run("last item taken", func(t *testing.T) {
		var side waitSide
		var items visibleItems
		leaving := parkConsumer(t, &side)
		other := parkConsumer(t, &side)
		items.count.Store(1)

		items.count.Add(-1)
		side.leaveList(leaving, true, items.peek)

		assertUnlisted(t, leaving)
		assertNoToken(t, other)
		assertCounts(t, &side, 0, 1)
		side.leaveList(other, false, items.peek)
		checkWaitSide(t, &side, 0)
	})
}

func TestWaitSideHandoff(t *testing.T) {
	for _, itemVisible := range []bool{true, false} {
		name := "nothing visible"
		if itemVisible {
			name = "item visible"
		}
		t.Run(name, func(t *testing.T) {
			var side waitSide
			var items visibleItems
			parked := parkConsumer(t, &side)
			require.False(t, side.register(), "register the first poller")
			require.False(t, side.register(), "register the second poller")
			if itemVisible {
				items.count.Store(1)
			}

			side.leavePolling(items.peek)
			assertNoToken(t, parked)
			assertCounts(t, &side, 1, 1)

			side.leavePolling(items.peek)
			if !itemVisible {
				assertNoToken(t, parked)
				assertCounts(t, &side, 0, 1)
				side.leaveList(parked, false, items.peek)
				checkWaitSide(t, &side, 0)
				return
			}
			requireToken(t, parked)
			assertCounts(t, &side, 1, 0)
			items.count.Add(-1)
			side.leavePolling(items.peek)
			checkWaitSide(t, &side, 0)
		})
	}
}

func TestWaitSideWakeOneRegistersWoken(t *testing.T) {
	var side waitSide
	var items visibleItems
	first := parkConsumer(t, &side)
	second := parkConsumer(t, &side)
	items.count.Store(1)

	assert.False(t, side.wakeCheck())
	woken, other := splitWoken(t, first, second)
	assertCounts(t, &side, 1, 1)
	assert.False(t, side.wakeCheck(), "second wake check before the woken consumer polls")
	assertCounts(t, &side, 1, 1)
	assertNoToken(t, other)

	requireToken(t, woken)
	items.count.Add(-1)
	side.leavePolling(items.peek)
	assertNoToken(t, other)
	side.leaveList(other, false, items.peek)
	checkWaitSide(t, &side, 0)
}

func TestWaitSideWaiterReuse(t *testing.T) {
	var side waitSide
	var items visibleItems
	first := parkConsumer(t, &side)
	second := parkConsumer(t, &side)

	assert.False(t, side.wakeCheck())
	woken, leaving := splitWoken(t, first, second)
	requireToken(t, woken)
	side.leavePolling(items.peek)
	side.leaveList(leaving, false, items.peek)

	for name, caller := range map[string]*waiter{"woken": woken, "left while listed": leaving} {
		assertUnlisted(t, caller)
		assert.Empty(t, caller.token, "%s waiter's token", name)
		side.putWaiter(caller)
	}
	checkWaitSide(t, &side, 0)
}

func TestWaitSideClosedWakeCheck(t *testing.T) {
	var side waitSide
	var items visibleItems
	parked := parkConsumer(t, &side)
	side.mu.Lock()
	require.True(t, side.setClosedLocked())
	side.mu.Unlock()

	assert.True(t, side.wakeCheck())
	requireToken(t, parked)
	assertCounts(t, &side, 1, 0)

	side.leavePolling(items.peek)
	assertIdleSide(t, &side)
	checkWaitSide(t, &side, 0)
}

// TestWaitSideWakeOrderOldestFirst pins the current wake order on purpose. It is the only test
// that changes if benchmark 4 picks newest first.
func TestWaitSideWakeOrderOldestFirst(t *testing.T) {
	var side waitSide
	var items visibleItems
	oldest := parkConsumer(t, &side)
	middle := parkConsumer(t, &side)
	newest := parkConsumer(t, &side)

	assert.False(t, side.wakeCheck())
	requireToken(t, oldest)
	assertNoToken(t, middle)
	assertNoToken(t, newest)
	assertUnlisted(t, oldest)
	side.leavePolling(items.peek)

	side.leaveList(middle, false, items.peek)
	side.leaveList(newest, false, items.peek)
	checkWaitSide(t, &side, 0)
}

func TestWaitSideWakeOneSkipsWithPoller(t *testing.T) {
	var side waitSide
	var items visibleItems
	parked := parkConsumer(t, &side)
	require.False(t, side.register(), "register the poller")
	before := side.load()

	side.wakeOne()

	assertNoToken(t, parked)
	assert.Equal(t, before, side.load(), "word after a wake with a poller registered")
	side.leavePolling(items.peek)
	side.leaveList(parked, false, items.peek)
	checkWaitSide(t, &side, 0)
}

func TestWaitSideUnderflowPanics(t *testing.T) {
	t.Run("leavePolling", func(t *testing.T) {
		var side waitSide
		var items visibleItems
		assert.PanicsWithValue(t, "workq: leavePolling without a registered poller", func() {
			side.leavePolling(items.peek)
		})
		checkWaitSide(t, &side, 0)
	})

	t.Run("enlist", func(t *testing.T) {
		var side waitSide
		caller := side.getWaiter()
		assert.PanicsWithValue(t, "workq: enlist without a registered poller", func() {
			side.enlist(caller, atNewest)
		})
		assertUnlisted(t, caller)
		checkWaitSide(t, &side, 0)
	})

	t.Run("leaveList", func(t *testing.T) {
		var side waitSide
		var items visibleItems
		caller := parkConsumer(t, &side)
		// Clear parked behind the protocol's back, so the listed caller has no count to remove.
		side.word.Store(0)
		assert.PanicsWithValue(t, "workq: leaveList with no parked waiter", func() {
			side.leaveList(caller, false, items.peek)
		})
		// The panic left mu held, as a real invariant failure would.
		side.mu.Unlock()
		assertUnlisted(t, caller)
		checkWaitSide(t, &side, 0)
	})
}
