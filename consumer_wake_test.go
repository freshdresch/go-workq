package workq

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerScalarPublicationReplenishes(t *testing.T) {
	setGOMAXPROCS(t, 4)
	fake := newScriptedEngine[int](128)
	q := newTestQueue(t, fake)
	callers := make([]*waiter, 6)
	for i := range callers {
		callers[i] = parkConsumer(t, &q.consumers)
	}
	for item := 1; item <= 128; item++ {
		require.NoError(t, q.TryPush(item))
		want := uint32(min(4, 1+(item-1)/32))
		assertCounts(t, &q.consumers, want, 6-want)
		for i, caller := range callers {
			assert.Equal(t, i < int(want), len(caller.token) == 1)
		}
	}
	for _, caller := range callers {
		q.consumers.leaveList(caller, false, func() bool { return false })
		assertNoToken(t, caller)
		assertUnlisted(t, caller)
	}
	assertIdleQueue(t, q)
}

func TestConsumerPublicationSnapshotRaces(t *testing.T) {
	for _, race := range []string{"empty", "drained", "register", "decrement"} {
		t.Run(race, func(t *testing.T) {
			setGOMAXPROCS(t, 4)
			fake := newScriptedEngine[int](8)
			q := newTestQueue(t, fake, WithMaxReceiveBatch(1))
			callers := []*waiter{
				parkConsumer(t, &q.consumers),
				parkConsumer(t, &q.consumers),
				parkConsumer(t, &q.consumers),
			}
			if race != "empty" {
				fake.pushRaw(1, 2, 3)
			}
			if race == "decrement" {
				require.False(t, q.consumers.register())
			}
			fake.setHooks(nil, func(call engineCall) {
				if call.kind != callPeek {
					return
				}
				assertSideUnlocked(t, &q.consumers)
				switch race {
				case "drained":
					fake.takeRaw(3)
				case "register":
					require.False(t, q.consumers.register())
				case "decrement":
					q.consumers.leavePolling(func() bool { return false })
				}
			})
			assert.False(t, q.wakeConsumersAfterPublication())
			want := uint32(3)
			if race == "empty" {
				want = 1
			}
			checkWaitSide(t, &q.consumers, want)
			fake.setHooks(nil, nil)
			for _, caller := range callers {
				q.consumers.leaveList(caller, false, func() bool { return false })
			}
			if race == "register" {
				q.consumers.leavePolling(func() bool { return false })
			}
			assertIdleQueue(t, q)
		})
	}
}

func TestConsumerWakeGroupAccounting(t *testing.T) {
	for _, target := range []uint32{1, 3, 4} {
		for _, pollers := range []uint32{0, 1, 4, 5} {
			var side waitSide
			callers := make([]*waiter, 5)
			for i := range callers {
				callers[i] = parkConsumer(t, &side)
			}
			for range pollers {
				require.False(t, side.register())
			}
			want := uint32(0)
			if target > pollers {
				want = target - pollers
			}
			assert.Equal(t, want > 0, side.wakeGroup(target))
			checkWaitSide(t, &side, pollers+want)
			for i, caller := range callers {
				assert.Equal(t, i < int(want), len(caller.token) == 1)
				side.leaveList(caller, false, func() bool { return false })
				assertNoToken(t, caller)
				assertUnlisted(t, caller)
			}
			for range pollers {
				side.leavePolling(func() bool { return false })
			}
			assertIdleSide(t, &side)
		}
	}
}

func TestConsumerPublicationSkipsPeek(t *testing.T) {
	for _, mode := range []string{"single", "one P", "capacity one", "no parked", "at cap"} {
		t.Run(mode, func(t *testing.T) {
			setGOMAXPROCS(t, 4)
			capacity := 8
			var options []Option
			if mode == "single" {
				options = append(options, WithSingleConsumer())
			}
			if mode == "one P" {
				setGOMAXPROCS(t, 1)
			}
			if mode == "capacity one" {
				capacity = 1
			}
			fake := newScriptedEngine[int](capacity)
			q := newTestQueue(t, fake, options...)
			var caller *waiter
			if mode != "no parked" {
				caller = parkConsumer(t, &q.consumers)
			}
			if mode == "at cap" {
				for range 4 {
					require.False(t, q.consumers.register())
				}
			}
			require.NoError(t, q.Push(context.Background(), 1))
			assert.Zero(t, fake.callCount(callPeek))
			if caller != nil {
				q.consumers.leaveList(caller, false, func() bool { return false })
			}
			if mode == "at cap" {
				for range 4 {
					q.consumers.leavePolling(func() bool { return false })
				}
			}
			assertIdleQueue(t, q)
		})
	}
}

func TestConsumerWakeGroupRetryAndEarlyExit(t *testing.T) {
	var side waitSide
	callers := make([]*waiter, 6)
	for i := range callers {
		callers[i] = parkConsumer(t, &side)
	}
	attempts := 0
	side.beforeGroupCAS = func() {
		attempts++
		if attempts == 1 {
			require.False(t, side.register())
		}
	}
	sends := 0
	side.afterGroupToken = func() {
		caller := callers[sends]
		sends++
		assertUnlisted(t, caller)
		requireToken(t, caller)
		// A recipient may release its obligation before the remaining tokens are sent.
		side.leavePolling(func() bool { return false })
		if sends == 1 {
			assertCounts(t, &side, 3, 3)
			assertNoToken(t, callers[1])
		}
	}
	require.True(t, side.wakeGroup(4))
	assert.Equal(t, 2, attempts, "registration invalidated the first CAS")
	assert.Equal(t, 3, sends, "the original group never refills after recipients leave")
	checkWaitSide(t, &side, 1)
	for _, caller := range callers[3:] {
		side.leaveList(caller, false, func() bool { return false })
	}
	side.leavePolling(func() bool { return false })
	assertIdleSide(t, &side)
}

func TestConsumerCleanupReplenishesWithPollers(t *testing.T) {
	for _, path := range []string{"poll", "listed", "removed"} {
		t.Run(path, func(t *testing.T) {
			setGOMAXPROCS(t, 4)
			fake := newScriptedEngine[int](8)
			q := newTestQueue(t, fake, WithMaxReceiveBatch(1))
			own := parkConsumer(t, &q.consumers)
			callers := []*waiter{
				parkConsumer(t, &q.consumers),
				parkConsumer(t, &q.consumers),
				parkConsumer(t, &q.consumers),
			}
			if path != "listed" {
				require.True(t, q.consumers.wakeOne())
			}
			require.False(t, q.consumers.register())
			fake.pushRaw(1, 2, 3)
			if path == "poll" {
				requireToken(t, own)
				q.leaveConsumerPolling()
			} else {
				q.leaveConsumerList(own, true)
			}
			checkWaitSide(t, &q.consumers, 3)
			for _, caller := range callers {
				q.consumers.leaveList(caller, false, func() bool { return false })
			}
			q.consumers.leavePolling(func() bool { return false })
			assertIdleQueue(t, q)
		})
	}
}

func TestConsumerGroupCloseAndCancellationCleanup(t *testing.T) {
	for _, closedFirst := range []bool{false, true} {
		setGOMAXPROCS(t, 4)
		fake := newScriptedEngine[int](8)
		q := newTestQueue(t, fake, WithMaxReceiveBatch(1))
		callers := make([]*waiter, 5)
		for i := range callers {
			callers[i] = parkConsumer(t, &q.consumers)
		}
		fake.pushRaw(1, 2, 3, 4)
		if closedFirst {
			q.Close()
		}
		assert.Equal(t, closedFirst, q.wakeConsumersAfterPublication())
		if !closedFirst {
			q.Close()
		}
		fake.takeRaw(4)
		for _, caller := range callers {
			q.leaveConsumerList(caller, false)
			assertNoToken(t, caller)
			assertUnlisted(t, caller)
			q.consumers.putWaiter(caller)
		}
		assertIdleQueue(t, q)
		assert.ErrorIs(t, func() error {
			_, err := q.Receive(context.Background(), make([]int, 1))
			return err
		}(), ErrClosed)
	}
}

func TestConsumerConcurrentTargetsAndCancellation(t *testing.T) {
	setGOMAXPROCS(t, 4)
	for range 64 {
		fake := newScriptedEngine[int](32)
		q := newTestQueue(t, fake, WithMaxReceiveBatch(1))
		callers := make([]*waiter, 16)
		for i := range callers {
			callers[i] = parkConsumer(t, &q.consumers)
		}
		fake.pushRaw(1, 2, 3, 4)
		start := make(chan struct{})
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() {
				<-start
				q.wakeConsumersAfterPublication()
			})
		}
		for _, caller := range callers {
			workers.Go(func() {
				<-start
				// Cancellation releases either a listed waiter or a selected registration.
				q.leaveConsumerList(caller, false)
			})
		}
		close(start)
		workers.Wait()
		for _, caller := range callers {
			assertUnlisted(t, caller)
			assertNoToken(t, caller)
		}
		assertIdleQueue(t, q)
	}
}

func TestConsumerPublicationPathsWakeGroups(t *testing.T) {
	for _, path := range []string{
		"Push", "TryPush", "PushBatch", "TryPushBatch", "spin", "poll", "park",
	} {
		t.Run(path, func(t *testing.T) {
			setGOMAXPROCS(t, 4)
			fake := newScriptedEngine[int](8)
			q := newTestQueue(t, fake, WithMaxReceiveBatch(1))
			callers := make([]*waiter, 4)
			for i := range callers {
				callers[i] = parkConsumer(t, &q.consumers)
			}
			fake.pushRaw(1, 2, 3)
			ctx := context.Background()
			attempt := func() int {
				require.NoError(t, fake.Enqueue(4))
				return 1
			}
			switch path {
			case "Push":
				require.NoError(t, q.Push(ctx, 4))
			case "TryPush":
				require.NoError(t, q.TryPush(4))
			case "PushBatch":
				_, err := q.PushBatch(ctx, []int{4})
				require.NoError(t, err)
			case "TryPushBatch":
				_, err := q.TryPushBatch([]int{4})
				require.NoError(t, err)
			case "spin":
				q.producerWait.Spins = 1
				_, finished, err := q.spinForRoom(ctx, attempt, 0)
				require.True(t, finished)
				require.NoError(t, err)
			case "poll":
				require.False(t, q.producers.register())
				_, _, err := q.attemptRegistered(ctx, attempt, 0)
				require.NoError(t, err)
			case "park":
				require.False(t, q.producers.register())
				_, _, err := q.parkForRoom(ctx, attempt, 0, false)
				require.NoError(t, err)
			}
			checkWaitSide(t, &q.consumers, 4)
			for _, caller := range callers {
				q.consumers.leaveList(caller, false, func() bool { return false })
			}
			assertIdleQueue(t, q)
		})
	}
}

func TestConsumerWakeCapStaysAtConstructionValue(t *testing.T) {
	setGOMAXPROCS(t, 2)
	fake := newScriptedEngine[int](8)
	q := newTestQueue(t, fake, WithMaxReceiveBatch(1))
	setGOMAXPROCS(t, 4)
	callers := make([]*waiter, 4)
	for i := range callers {
		callers[i] = parkConsumer(t, &q.consumers)
	}
	_, err := q.TryPushBatch([]int{1, 2, 3, 4})
	require.NoError(t, err)
	checkWaitSide(t, &q.consumers, 2)
	for i, caller := range callers {
		assert.Equal(t, i < 2, len(caller.token) == 1)
		q.consumers.leaveList(caller, false, func() bool { return false })
	}
	assertIdleQueue(t, q)
}
