package workq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerSuccessWithoutHandoffYield(t *testing.T) {
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
				require.False(t, q.consumers.register(nil))
				fake.pushRaw(1, 2)
				fake.setHooks(func(call engineCall) {
					if call.kind == callDequeue && path == "removed" {
						q.consumers.wakeCheck(nil)
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
				q.hooks.beforeConsumerHandoffYield = func() { yields++ }
				dst := make([]int, 1)
				var received int
				var finished bool
				var err error
				if path == "poll" {
					received, finished, err = q.pollRegistered(ctx, nil, PhaseYield, dst)
				} else {
					received, finished, err = q.parkForItems(ctx, nil, dst, own)
				}
				assert.Equal(t, 1, received)
				assert.True(t, finished)
				assert.NoError(t, err, "completed dequeue wins over cancellation or Close")
				assert.Equal(t, []int{1}, dst)
				assert.Zero(t, yields, "successful acquisition adds no handoff yield")
				assertSideUnlocked(t, &q.consumers)
				assertSideUnlocked(t, &q.producers)
				assertUnlisted(t, own)
				assertNoToken(t, own)
				assertUnlisted(t, target)
				assert.Len(t, target.token, 1)
				assert.Len(t, producer.token, 1)
				assertCounts(t, &q.consumers, 1, 0)
				q.consumers.leaveList(nil, target, false, func() bool { return false })
				q.producers.leaveList(nil, producer, false, func() bool { return false })
				assertIdleQueue(t, q)
			})
		}
	}
}

func assertSideUnlocked(t *testing.T, side *waitSide) {
	t.Helper()
	if !side.mu.TryLock() {
		t.Error("side mutex held after cleanup")
		return
	}
	side.mu.Unlock()
}
