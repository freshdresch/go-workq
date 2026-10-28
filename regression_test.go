package workq

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// observedContext lets a test publish closure after the caller's earlier closed load.
// It preserves its parent's Done channel and error.
type observedContext struct {
	context.Context
	onDone func()
}

func (ctx observedContext) Done() <-chan struct{} {
	ctx.onDone()
	return ctx.Context.Done()
}

// publishConsumerClose exposes the first Close publication while done is still open.
// finish completes the remaining publications in their production order.
func publishConsumerClose(t *testing.T, queue *Queue[int]) (finish func()) {
	t.Helper()
	queue.consumers.mu.Lock()
	require.True(t, queue.consumers.setClosedLocked())
	queue.consumers.mu.Unlock()
	return func() {
		close(queue.done)
		queue.producers.mu.Lock()
		queue.producers.setClosedLocked()
		queue.producers.mu.Unlock()
	}
}

func TestProducerCancellationObservation(t *testing.T) {
	setGOMAXPROCS(t, 4)
	phases := []struct {
		name     string
		strategy WaitStrategy
		attempts int
	}{
		{name: "spin", strategy: WaitStrategy{Spins: 65}, attempts: 65},
		{name: "yield", strategy: WaitStrategy{Yields: 1}, attempts: 2},
		{
			name:     "never park",
			strategy: WaitStrategy{Yields: 1, NeverPark: true},
			attempts: 3,
		},
	}
	for _, phase := range phases {
		for _, partial := range []bool{false, true} {
			for _, outcome := range []string{"cancel", "deadline", "consumer closed", "done closed"} {
				name := fmt.Sprintf("%s/partial=%t/%s", phase.name, partial, outcome)
				t.Run(name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						fake := newScriptedEngine[int](4)
						queue := newTestQueue(t, fake, WithProducerWait(phase.strategy))
						prior := 0
						if !partial {
							fake.pushRaw(1, 2, 3, 4)
						}
						ctx, cancel := context.WithCancel(context.Background())
						if outcome == "deadline" {
							ctx, cancel = context.WithTimeout(context.Background(), time.Second)
						}
						defer cancel()
						var finish func()
						checks := 0
						observed := observedContext{Context: ctx, onDone: func() {
							checks++
							target := 1
							if phase.name == "spin" || phase.name == "never park" {
								target = 2
							}
							if checks != target {
								return
							}
							kind := callEnqueue
							if partial {
								kind = callEnqueueBurst
								prior = 4
							}
							wantAttempts := phase.attempts
							if partial {
								wantAttempts++
							}
							assert.Equal(t, wantAttempts, fake.callCount(kind))
							if phase.name == "spin" {
								assertCounts(t, &queue.producers, 0, 0)
							} else {
								assertCounts(t, &queue.producers, 1, 0)
							}
							if outcome == "consumer closed" {
								finish = publishConsumerClose(t, queue)
							}
							if outcome == "done closed" {
								// Close remains in the publication gap until the producer returns.
								finish = publishConsumerClose(t, queue)
								close(queue.done)
								finish = func() {
									queue.producers.mu.Lock()
									queue.producers.setClosedLocked()
									queue.producers.mu.Unlock()
								}
							}
							if outcome == "deadline" {
								<-ctx.Done()
							} else {
								cancel()
							}
						}}
						// PushBatch checks its context between chunks before it starts waiting.
						// Skip observing that check so only the actual wait publishes closure.
						if partial {
							queue.hooks.beforeChunkCheck = func() { checks = -1 }
						}
						var pushed int
						var err error
						if partial {
							pushed, err = queue.PushBatch(observed, sequence(1, 5))
						} else {
							err = queue.Push(observed, 9)
						}
						wantErr := error(context.Canceled)
						if outcome == "deadline" {
							wantErr = context.DeadlineExceeded
						}
						if finish != nil {
							wantErr = closedWhileWaiting(prior)
							finish()
						}
						assert.Equal(t, wantErr, err)
						assert.Equal(t, prior, pushed)
						assertIdleQueue(t, queue)
					})
				})
			}
		}
	}
}

func TestPushBatchCancellationBetweenChunks(t *testing.T) {
	for _, outcome := range []string{"cancel", "deadline", "consumer closed"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fake := newScriptedEngine[int](8)
				queue := newTestQueue(t, fake, WithMaxPushBatch(2))
				ctx, cancel := context.WithCancel(context.Background())
				if outcome == "deadline" {
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
				}
				defer cancel()
				var finish func()
				observed := observedContext{Context: ctx, onDone: func() {
					assert.Equal(t, 1, fake.callCount(callEnqueueBurst))
					if outcome == "consumer closed" {
						finish = publishConsumerClose(t, queue)
					}
					if outcome == "deadline" {
						<-ctx.Done()
					} else {
						cancel()
					}
				}}
				pushed, err := queue.PushBatch(observed, []int{1, 2, 3})
				wantErr := error(context.Canceled)
				if outcome == "deadline" {
					wantErr = context.DeadlineExceeded
				}
				if finish != nil {
					wantErr = ErrClosedDuringPush
					finish()
				}
				assert.Equal(t, wantErr, err)
				assert.Equal(t, 2, pushed)
				assert.Equal(t, []int{1, 2}, fake.takeRaw(2))
				assertIdleQueue(t, queue)
			})
		})
	}
}

// observedReceiveEngine holds a real engine's registered attempts at test-owned barriers.
type observedReceiveEngine[T any] struct {
	engine[T]
	before func()
	after  func(int)
}

func (observed observedReceiveEngine[T]) DequeueBurst(dst []T) (int, int) {
	if len(dst) > 0 && observed.before != nil {
		observed.before()
	}
	received, available := observed.engine.DequeueBurst(dst)
	if len(dst) > 0 && observed.after != nil {
		observed.after(received)
	}
	return received, available
}

func TestConsumeClearsOlderBatchTails(t *testing.T) {
	setGOMAXPROCS(t, 4)
	strategies := []struct {
		name     string
		strategy WaitStrategy
	}{
		{name: "park", strategy: WaitStrategy{Spins: 2, Yields: 1}},
		{name: "zero spins and yields", strategy: WaitStrategy{}},
		{name: "never park", strategy: WaitStrategy{Yields: 1, NeverPark: true}},
	}
	for _, engineCase := range engineCases() {
		for _, strategy := range strategies {
			t.Run(engineCase.name+"/"+strategy.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					options := append([]Option{}, engineCase.options...)
					options = append(options, WithConsumerWait(strategy.strategy))
					queue, err := New[*int](8, options...)
					require.NoError(t, err)
					values := []*int{new(int), new(int), new(int), new(int)}
					for index, value := range values {
						*value = index + 1
					}
					_, err = queue.TryPushBatch(values)
					require.NoError(t, err)
					ctx, cancel := newWaitContext(t)
					barriers := make(chan chan struct{}, 2)
					idle := 0
					armed := true
					calls := 0
					spinRefill := false
					queue.engine = observedReceiveEngine[*int]{
						engine: queue.engine,
						before: func() {
							word := queue.consumers.load()
							if !strategy.strategy.NeverPark {
								return
							}
							if !armed || word.pollers() == 0 {
								return
							}
							armed = false
							idle++
							resume := make(chan struct{})
							barriers <- resume
							select {
							case <-resume:
							case <-ctx.Done():
							}
						},
						after: func(received int) {
							if calls == 2 && received == 0 && !spinRefill && strategy.strategy.Spins > 0 {
								spinRefill = true
								require.NoError(t, queue.TryPush(values[0]))
							}
						},
					}
					batches := make(chan []*int, 8)
					results := startConsume(ctx, t, queue, func(_ context.Context, batch []*int) error {
						calls++
						batches <- batch
						switch calls {
						case 1:
							_, err := queue.TryPushBatch(values[:2])
							return err
						case 2:
							if strategy.strategy.Spins == 0 {
								return queue.TryPush(values[0])
							}
						case 3:
							assert.Equal(t, values[:1], batch)
							assert.Equal(t, values, batch[:4], "successful receives keep old tails")
							if strategy.strategy.Spins > 0 {
								assert.True(t, spinRefill, "the third batch arrived during spinning")
							}
						case 4:
							assert.Equal(t, values[:3], batch)
							return queue.TryPush(values[0])
						case 5:
							armed = true
						}
						return nil
					})
					// Release the engine barrier before startConsume joins its goroutine.
					t.Cleanup(cancel)
					first := awaitBatch(t, batches)
					require.Len(t, first, 4)
					require.Len(t, awaitBatch(t, batches), 2)
					require.Len(t, awaitBatch(t, batches), 1)
					var resume chan struct{}
					if strategy.strategy.NeverPark {
						resume = awaitBarrier(t, barriers)
					} else {
						synctest.Wait()
						requireParked(t, &queue.consumers, 1)
					}
					for slot, item := range first {
						assert.Nil(t, item, "old slot %d at first idle", slot)
					}
					_, err = queue.TryPushBatch(values[:3])
					require.NoError(t, err)
					if resume != nil {
						close(resume)
					}
					refill := awaitBatch(t, batches)
					// The handler has already returned, so inspect only at the next barrier.
					require.Len(t, refill, 3)
					require.Len(t, awaitBatch(t, batches), 1)
					if strategy.strategy.NeverPark {
						resume = awaitBarrier(t, barriers)
					} else {
						synctest.Wait()
						requireParked(t, &queue.consumers, 1)
					}
					for slot, item := range first {
						assert.Nil(t, item, "old slot %d at second idle", slot)
					}
					if strategy.strategy.NeverPark {
						assert.Equal(t, 2, idle)
					}
					cancel()
					if resume != nil {
						close(resume)
					}
					assert.ErrorIs(t, awaitConsume(t, results), context.Canceled)
					assertIdleSide(t, &queue.consumers)
					assertIdleSide(t, &queue.producers)
				})
			})
		}
	}
}

func awaitBarrier(t *testing.T, barriers <-chan chan struct{}) chan struct{} {
	t.Helper()
	select {
	case resume := <-barriers:
		return resume
	case <-time.After(waitTestTimeout):
		t.Fatal("consumer did not reach the waiting attempt")
		return nil
	}
}

func TestParkedProducerCancellationObservation(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, outcome := range []string{"cancel", "deadline", "consumer closed", "woken"} {
			t.Run(fmt.Sprintf("partial=%t/%s", partial, outcome), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					fake := newScriptedEngine[int](4)
					queue := newTestQueue(t, fake, WithProducerWait(WaitStrategy{}))
					prior := 0
					if partial {
						prior = 4
					} else {
						fake.pushRaw(1, 2, 3, 4)
					}
					ctx, cancel := context.WithCancel(context.Background())
					if outcome == "deadline" {
						ctx, cancel = context.WithTimeout(context.Background(), time.Second)
					}
					defer cancel()
					var finish func()
					observed := observedContext{Context: ctx, onDone: func() {
						if outcome != "woken" || queue.producers.load().pollers() == 0 {
							return
						}
						assertCounts(t, &queue.producers, 1, 0)
						// The woken registered attempt failed and checked its earlier closed bit.
						finish = publishConsumerClose(t, queue)
						cancel()
					}}
					results := make(chan pushWaitResult, 1)
					finished := make(chan struct{})
					t.Cleanup(func() {
						cancel()
						<-finished
					})
					go func() {
						defer close(finished)
						if partial {
							pushed, err := queue.PushBatch(observed, sequence(1, 5))
							results <- pushWaitResult{pushed: pushed, err: err}
							return
						}
						results <- pushWaitResult{err: queue.Push(observed, 9)}
					}()
					synctest.Wait()
					requireParked(t, &queue.producers, 1)
					caller := listedWaiter(t, &queue.producers)
					switch outcome {
					case "woken":
						queue.producers.wakeOne()
					case "consumer closed":
						finish = publishConsumerClose(t, queue)
						cancel()
					case "cancel":
						cancel()
					}
					result := awaitPushResult(t, results)
					wantErr := error(context.Canceled)
					if outcome == "deadline" {
						wantErr = context.DeadlineExceeded
					}
					if finish != nil {
						wantErr = closedWhileWaiting(prior)
						finish()
					}
					assert.Equal(t, wantErr, result.err)
					assert.Equal(t, prior, result.pushed)
					assertUnlisted(t, caller)
					assertNoToken(t, caller)
					assertIdleQueue(t, queue)
				})
			})
		}
	}
}

func TestParkedProducerCancellationAfterTokenTransfer(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%t", partial), func(t *testing.T) {
			fake := newScriptedEngine[int](4)
			queue := newTestQueue(t, fake, WithProducerWait(WaitStrategy{}))
			prior := 0
			if partial {
				prior = 4
			} else {
				fake.pushRaw(1, 2, 3, 4)
			}
			ctx, cancel := newWaitContext(t)
			results := make(chan pushWaitResult, 1)
			finished := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				<-finished
			})
			go func() {
				defer close(finished)
				if partial {
					pushed, err := queue.PushBatch(ctx, sequence(1, 5))
					results <- pushWaitResult{pushed: pushed, err: err}
					return
				}
				results <- pushWaitResult{err: queue.Push(ctx, 9)}
			}()
			awaitCondition(t, func() bool {
				return queue.producers.load().parked() == 1
			}, "producer to park")
			caller := listedWaiter(t, &queue.producers)
			finish := publishConsumerClose(t, queue)
			defer finish()
			func() {
				queue.producers.mu.Lock()
				defer queue.producers.mu.Unlock()
				cancel()
				// Mutex waits do not durably block in synctest. Observe this producer's
				// stack to prove it selected cancellation before transferring its token.
				stacks := make([]byte, 1<<20)
				awaitCondition(t, func() bool {
					length := runtime.Stack(stacks, true)
					for _, stack := range strings.Split(string(stacks[:length]), "\n\n") {
						if strings.Contains(stack, "TestParkedProducerCancellationAfterTokenTransfer.") &&
							strings.Contains(stack, "(*waitSide).leaveList(") {
							return true
						}
					}
					return false
				}, "parked producer to enter cancellation cleanup")
				queue.producers.wakeOneLocked()
				assertUnlisted(t, caller)
				assertCounts(t, &queue.producers, 1, 0)
				assert.Len(t, caller.token, 1, "cleanup still owes the transferred token")
			}()
			result := awaitPushResult(t, results)
			assert.Equal(t, closedWhileWaiting(prior), result.err)
			assert.Equal(t, prior, result.pushed)
			assertUnlisted(t, caller)
			assertNoToken(t, caller)
			assertIdleQueue(t, queue)
			assert.Equal(t, sequence(1, 4), fake.takeRaw(4))
			received, available := fake.DequeueBurst(make([]int, 1))
			assert.Zero(t, received)
			assert.Zero(t, available)
		})
	}
}

func TestReceivePreservesCallerStorageWhileWaiting(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				options := append([]Option{}, engineCase.options...)
				options = append(options, WithConsumerWait(WaitStrategy{}))
				queue, err := New[*int](8, options...)
				require.NoError(t, err)
				values := []*int{new(int), new(int), new(int), new(int)}
				dst := append([]*int{}, values...)
				ctx, cancel := newWaitContext(t)
				results := make(chan error, 1)
				finished := make(chan struct{})
				t.Cleanup(func() {
					cancel()
					<-finished
				})
				go func() {
					defer close(finished)
					_, err := queue.Receive(ctx, dst)
					results <- err
				}()
				synctest.Wait()
				requireParked(t, &queue.consumers, 1)
				assert.Equal(t, values, dst)
				cancel()
				assert.ErrorIs(t, awaitConsume(t, results), context.Canceled)
				assert.Equal(t, values, dst)
				assertIdleSide(t, &queue.consumers)
				assertIdleSide(t, &queue.producers)
			})
		})
	}
}

func TestProducerClosePublicationGap(t *testing.T) {
	setGOMAXPROCS(t, 4)
	for _, strategy := range []WaitStrategy{{Spins: 1}, {Yields: 1}} {
		t.Run(fmt.Sprintf("%+v", strategy), func(t *testing.T) {
			queue, fake := newFullTestQueue(t, WithProducerWait(strategy))
			ctx, cancel := newWaitContext(t)
			entered := make(chan struct{})
			resume := make(chan struct{})
			finished := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				select {
				case <-resume:
				default:
					close(resume)
				}
				<-finished
			})
			fake.setHooks(func(call engineCall) {
				if call == enqueueCall(1) {
					close(entered)
					<-resume
				}
			}, nil)
			results := make(chan pushWaitResult, 1)
			go func() {
				defer close(finished)
				results <- pushWaitResult{err: queue.Push(ctx, 9)}
			}()
			select {
			case <-entered:
			case <-time.After(waitTestTimeout):
				t.Fatal("producer did not enter its initial attempt")
			}
			queue.hooks.afterCloseDone = func() {
				assert.False(t, queue.producers.load().closed())
				cancel()
				close(resume)
				result := awaitPushResult(t, results)
				assert.Same(t, ErrClosed, result.err)
				assertIdleQueue(t, queue)
			}
			queue.Close()
		})
	}
}
