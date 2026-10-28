package workq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerHandoffYieldSuccess(t *testing.T) {
	for _, path := range []string{"poll", "listed", "removed"} {
		for _, ending := range []string{"open", "cancel", "close"} {
			t.Run(path+"/"+ending, func(t *testing.T) {
				fake := newScriptedEngine[int](8)
				q := newTestQueue(t, fake)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				own := newWaiter()
				var target *waiter
				if path != "removed" {
					target = parkConsumer(t, &q.consumers)
				}
				producer := parkConsumer(t, &q.producers)
				require.False(t, q.consumers.register())
				fake.pushRaw(1, 2)
				fake.setHooks(func(call engineCall) {
					if call.kind == callDequeue && path == "removed" {
						q.consumers.wakeCheck()
						require.Len(t, own.token, 1, "own token is buffered before cleanup")
						target = parkConsumer(t, &q.consumers)
					}
				}, func(call engineCall) {
					if call.kind != callDequeue {
						return
					}
					switch ending {
					case "cancel":
						cancel()
					case "close":
						q.Close()
					}
				})
				yields := 0
				q.hooks.beforeConsumerHandoffYield = func() {
					yields++
					assertSideUnlocked(t, &q.consumers)
					assertSideUnlocked(t, &q.producers)
					assertUnlisted(t, own)
					assertNoToken(t, own)
					assertUnlisted(t, target)
					assert.Len(t, target.token, 1, "handoff does not require a waiting receiver")
					assert.Len(t, producer.token, 1, "producer wake precedes consumer yield")
					assertCounts(t, &q.consumers, 1, 0)
				}
				dst := make([]int, 1)
				var received int
				var finished bool
				var err error
				if path == "poll" {
					received, finished, err = q.pollRegistered(ctx, dst)
				} else {
					received, finished, err = q.parkForItems(ctx, dst, own)
				}
				assert.Equal(t, 1, received)
				assert.True(t, finished)
				assert.NoError(t, err, "completed dequeue wins over cancellation or Close")
				assert.Equal(t, []int{1}, dst)
				assert.Equal(t, 1, yields, "one yield boundary before successful return")
				q.consumers.leaveList(target, false, func() bool { return false })
				q.producers.leaveList(producer, false, func() bool { return false })
				assertIdleQueue(t, q)
			})
		}
	}
}

func assertSideUnlocked(t *testing.T, side *waitSide) {
	t.Helper()
	if !side.mu.TryLock() {
		t.Error("side mutex held at consumer yield")
		return
	}
	side.mu.Unlock()
}

func TestConsumerHandoffYieldOnlyWithOwnSend(t *testing.T) {
	for _, path := range []string{"poll", "listed", "removed"} {
		for _, cause := range []string{
			"no waiter", "last item", "other poller", "registration", "other wake",
		} {
			t.Run(path+"/"+cause, func(t *testing.T) {
				fake := newScriptedEngine[int](8)
				q := newTestQueue(t, fake)
				own := newWaiter()
				var target *waiter
				if cause != "no waiter" && path != "removed" {
					target = parkConsumer(t, &q.consumers)
				}
				require.False(t, q.consumers.register())
				fake.pushRaw(1)
				if cause != "last item" {
					fake.pushRaw(2)
				}
				otherPoller := false
				fake.setHooks(func(call engineCall) {
					if call.kind == callDequeue {
						if path == "removed" {
							q.consumers.wakeCheck()
							if cause != "no waiter" {
								target = parkConsumer(t, &q.consumers)
							}
						}
						if cause == "other poller" {
							require.False(t, q.consumers.register())
							otherPoller = true
						}
					}
				}, func(call engineCall) {
					if call.kind == callPeek && cause == "other wake" {
						require.True(t, q.consumers.wakeOne())
					}
					if call.kind == callPeek && cause == "registration" && !otherPoller {
						require.False(t, q.consumers.register())
						otherPoller = true
					}
				})
				yields := 0
				q.hooks.beforeConsumerHandoffYield = func() { yields++ }
				dst := make([]int, 1)
				if path == "poll" {
					_, _, err := q.pollRegistered(context.Background(), dst)
					assert.NoError(t, err)
				} else {
					_, _, err := q.parkForItems(context.Background(), dst, own)
					assert.NoError(t, err)
				}
				assert.Zero(t, yields)
				fake.setHooks(nil, nil)
				if target != nil {
					assert.Equal(t, cause == "other wake", len(target.token) == 1)
					q.consumers.leaveList(target, false, func() bool { return false })
				}
				if otherPoller {
					q.consumers.leavePolling(func() bool { return false })
				}
				assertIdleQueue(t, q)
			})
		}
	}
}

func TestWaitSideActualWakeResult(t *testing.T) {
	for _, operation := range []string{"wake", "locked", "poll", "listed", "removed"} {
		for _, outcome := range []string{"send", "no waiter", "poller", "empty", "registration"} {
			if (operation == "wake" || operation == "locked") &&
				(outcome == "empty" || outcome == "registration") {
				continue
			}
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				var side waitSide
				var own *waiter
				if operation == "listed" || operation == "removed" {
					own = parkConsumer(t, &side)
					if operation == "removed" {
						require.True(t, side.wakeOne())
					}
				}
				var target *waiter
				if outcome != "no waiter" {
					target = parkConsumer(t, &side)
				}
				if operation == "poll" {
					require.False(t, side.register())
				}
				otherPoller := false
				if outcome == "poller" {
					require.False(t, side.register())
					otherPoller = true
				}
				peek := func() bool {
					if outcome == "registration" {
						require.False(t, side.register())
						otherPoller = true
					}
					return outcome != "empty"
				}
				var woke bool
				switch operation {
				case "wake":
					woke = side.wakeOne()
				case "locked":
					side.mu.Lock()
					woke = side.wakeOneLocked()
					side.mu.Unlock()
				case "poll":
					woke = side.leavePolling(peek)
				default:
					woke = side.leaveList(own, true, peek)
					assertNoToken(t, own)
					assertUnlisted(t, own)
				}
				assert.Equal(t, outcome == "send", woke)
				assertSideUnlocked(t, &side)
				if target != nil {
					assert.Equal(t, outcome == "send", len(target.token) == 1)
					side.leaveList(target, false, func() bool { return false })
				}
				if otherPoller {
					side.leavePolling(func() bool { return false })
				}
				assertIdleSide(t, &side)
			})
		}
	}
}

func TestConsumerHandoffYieldExcludedPaths(t *testing.T) {
	for _, path := range []string{
		"fast", "spin", "try", "cancel", "drain", "producer poll", "producer park",
	} {
		t.Run(path, func(t *testing.T) {
			fake := newScriptedEngine[int](8)
			q := newTestQueue(t, fake)
			q.hooks.beforeConsumerHandoffYield = func() {
				t.Error("excluded path reached consumer handoff yield")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dst := make([]int, 1)
			var target *waiter
			if path == "producer poll" || path == "producer park" {
				target = parkConsumer(t, &q.producers)
				require.False(t, q.producers.register())
				attempt := func() int {
					require.NoError(t, fake.Enqueue(1))
					return 1
				}
				if path == "producer poll" {
					_, _, err := q.attemptRegistered(ctx, attempt, 0)
					assert.NoError(t, err)
				} else {
					_, _, err := q.parkForRoom(ctx, attempt, 0, false)
					assert.NoError(t, err)
				}
				assert.Len(t, target.token, 1, "producer cleanup really handed off")
				q.producers.leaveList(target, false, func() bool { return false })
				assertIdleQueue(t, q)
				return
			}
			fake.pushRaw(1, 2)
			if path == "cancel" || path == "drain" {
				target = parkConsumer(t, &q.consumers)
				require.False(t, q.consumers.register())
				if path == "cancel" {
					cancel()
					fake.setForceEmpty()
				} else {
					q.Close()
				}
				_, _, err := q.pollRegistered(ctx, dst)
				if path == "cancel" {
					assert.ErrorIs(t, err, context.Canceled)
				} else {
					assert.NoError(t, err)
				}
				assert.Len(t, target.token, 1, "excluded cleanup really handed off")
				q.consumers.leaveList(target, false, func() bool { return false })
			} else {
				producer := parkConsumer(t, &q.producers)
				switch path {
				case "fast":
					_, err := q.Receive(ctx, dst)
					assert.NoError(t, err)
				case "spin":
					q.consumerWait.Spins = 1
					_, finished, err := q.spinForItems(ctx, dst)
					assert.True(t, finished)
					assert.NoError(t, err)
				case "try":
					_, err := q.TryReceive(dst)
					assert.NoError(t, err)
				}
				assert.Len(t, producer.token, 1)
				q.producers.leaveList(producer, false, func() bool { return false })
			}
			assertIdleQueue(t, q)
		})
	}
}

func TestConsumerHandoffTargetExit(t *testing.T) {
	for _, ending := range []string{"cancel", "close"} {
		t.Run(ending, func(t *testing.T) {
			fake := newScriptedEngine[int](8)
			q := newTestQueue(t, fake)
			target := parkConsumer(t, &q.consumers)
			next := parkConsumer(t, &q.consumers)
			require.False(t, q.consumers.register())
			fake.pushRaw(1, 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			release := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				<-target.token
				<-release
				_, _, err := q.pollRegistered(ctx, make([]int, 1))
				result <- err
			}()
			// The main caller owns release. Even a failed assertion must let the target exit.
			released := false
			defer func() {
				cancel()
				if !released {
					close(release)
					q.consumers.wakeCheck()
					if err := <-result; err != nil {
						assert.ErrorIs(t, err, context.Canceled)
					}
				}
			}()
			yields := 0
			q.hooks.beforeConsumerHandoffYield = func() {
				yields++
				if ending == "close" {
					q.Close()
				} else {
					cancel()
					fake.setForceEmpty()
				}
				close(release)
				released = true
				err := <-result
				if ending == "cancel" {
					assert.ErrorIs(t, err, context.Canceled)
				} else {
					assert.NoError(t, err, "closed target drains the remaining item")
				}
				assertNoToken(t, target)
				assertUnlisted(t, target)
				assert.Len(t, next.token, 1, "target exit preserves the next wake")
			}
			received, finished, err := q.pollRegistered(context.Background(), make([]int, 1))
			assert.Equal(t, 1, received)
			assert.True(t, finished)
			assert.NoError(t, err)
			assert.Equal(t, 1, yields, "target cancellation/drain adds no success yield")
			q.consumers.leaveList(next, false, func() bool { return false })
			assertIdleQueue(t, q)
		})
	}
}

func TestConsumerHandoffYieldBeforeHandler(t *testing.T) {
	fake := newScriptedEngine[int](8)
	q := newTestQueue(
		t,
		fake,
		WithConsumerWait(WaitStrategy{Yields: 1}),
		WithMaxReceiveBatch(1),
	)
	target := parkConsumer(t, &q.consumers)
	fake.pushRaw(1, 2)
	fake.setHooks(func(call engineCall) {
		if call.kind == callDequeue && call.count == 1 {
			fake.setForceEmpty()
		}
	}, nil)
	yields := 0
	q.hooks.beforeConsumerHandoffYield = func() { yields++ }
	err := q.Consume(context.Background(), func(_ context.Context, batch []int) error {
		assert.Equal(t, []int{1}, batch)
		assert.Equal(t, 1, yields)
		return context.Canceled
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, yields)
	q.consumers.leaveList(target, false, func() bool { return false })
	assertIdleQueue(t, q)
}
