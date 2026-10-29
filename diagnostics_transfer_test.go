package workq

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newProducerDiagnosticsQueue(
	t *testing.T,
	sampleEvery uint32,
	options ...Option,
) (*Queue[int], *scriptedEngine[int]) {
	t.Helper()
	fake := newScriptedEngine[int](8)
	options = append(options, WithDiagnostics(DiagnosticsConfig{
		Mode: DiagnosticsCounters, Shards: 2, CapacitySampleEvery: sampleEvery,
	}))
	return newTestQueue(t, fake, options...), fake
}

func TestDiagnosticsProducerSingleton(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		t.Run(map[bool]string{false: "try", true: "blocking"}[blocking], func(t *testing.T) {
			queue, fake := newProducerDiagnosticsQueue(t, 0)
			push := queue.TryPush
			if blocking {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				push = func(item int) error { return queue.Push(ctx, item) }
			}
			require.NoError(t, push(1))
			fake.pushRaw(sequence(2, 7)...)
			if blocking {
				require.ErrorIs(t, push(9), context.Canceled)
			} else {
				require.ErrorIs(t, push(9), ErrFull)
			}
			snapshot := queue.Diagnostics().Producer
			assert.Equal(t, uint64(2), snapshot.Attempts[PhaseInitial])
			assert.Equal(t, uint64(1), snapshot.ZeroProgress[PhaseInitial])
			assert.Equal(t, uint64(1), snapshot.FullProgress[PhaseInitial])
			assert.Zero(t, snapshot.PartialProgress[PhaseInitial])
			assert.Equal(t, uint64(2), snapshot.RequestedItems[PhaseInitial])
			assert.Equal(t, uint64(1), snapshot.MovedItems[PhaseInitial])
			assert.Equal(t, uint64(2), snapshot.AttemptsWithoutRemainingCount)
			assert.Equal(t, uint64(2), snapshot.CapacityEligibleAttempts)
			assert.Zero(t, snapshot.CapacitySelectedProbes)
			assert.Zero(t, snapshot.ReusableSlots)
			assert.Equal(t, 2, fake.callCount(callEnqueue))
			assert.Zero(t, fake.callCount(callEnqueueBurst))
			assert.Zero(t, fake.callCount(callPeek))
		})
	}
}

func TestDiagnosticsProducerBatchClamping(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		t.Run(map[bool]string{false: "try", true: "blocking"}[blocking], func(t *testing.T) {
			queue, _ := newProducerDiagnosticsQueue(t, 0, WithMaxPushBatch(3))
			var moved int
			var err error
			if blocking {
				moved, err = queue.PushBatch(context.Background(), sequence(1, 8))
			} else {
				moved, err = queue.TryPushBatch(sequence(1, 8))
			}
			require.NoError(t, err)
			require.Equal(t, 8, moved)
			snapshot := queue.Diagnostics().Producer
			assert.Equal(t, uint64(3), snapshot.Attempts[PhaseInitial])
			assert.Equal(t, uint64(3), snapshot.FullProgress[PhaseInitial])
			assert.Equal(t, uint64(8), snapshot.RequestedItems[PhaseInitial])
			assert.Equal(t, uint64(8), snapshot.MovedItems[PhaseInitial])
			assert.Equal(t, uint64(2), snapshot.RequestedSize.Buckets[3])
			assert.Equal(t, uint64(1), snapshot.RequestedSize.Buckets[2])
			remaining := snapshot.ReusableSlots[TransferCachedAllowed]
			assert.Equal(t, uint64(3), remaining.Count)
			assert.Equal(t, uint64(7), remaining.Sum)
			assert.Equal(t, uint64(1), remaining.ZeroCount)
			assert.Zero(t, snapshot.ReusableSlots[TransferFresh].Count)
			assert.Zero(t, snapshot.AttemptsWithoutRemainingCount)
		})
	}
}

func TestDiagnosticsProducerStrictAndLogicalPartial(t *testing.T) {
	for _, scenario := range []string{"strict partial", "logical partial", "retry partial"} {
		t.Run(scenario, func(t *testing.T) {
			queue, fake := newProducerDiagnosticsQueue(t, 0, WithMaxPushBatch(3))
			switch scenario {
			case "strict partial":
				fake.pushRaw(sequence(1, 6)...)
				moved, err := queue.TryPushBatch([]int{7, 8, 9})
				require.Equal(t, 2, moved)
				require.ErrorIs(t, err, ErrFull)
				snapshot := queue.Diagnostics().Producer
				assert.Equal(t, uint64(1), snapshot.PartialProgress[PhaseInitial])
				assert.Equal(t, uint64(1), snapshot.ReusableSlots[TransferFresh].ZeroCount)
			case "logical partial":
				fake.pushRaw(sequence(1, 5)...)
				moved, err := queue.TryPushBatch([]int{6, 7, 8, 9})
				require.Equal(t, 3, moved)
				require.ErrorIs(t, err, ErrFull)
				snapshot := queue.Diagnostics().Producer
				assert.Zero(t, snapshot.PartialProgress)
				assert.Equal(t, uint64(1), snapshot.FullProgress[PhaseInitial])
				assert.Equal(t, uint64(1), snapshot.ZeroProgress[PhaseInitial])
			case "retry partial":
				fake.setMaxMove(1)
				moved, err := queue.PushBatch(context.Background(), []int{1, 2, 3})
				require.Equal(t, 3, moved)
				require.NoError(t, err)
				snapshot := queue.Diagnostics().Producer
				assert.Equal(t, uint64(3), snapshot.Attempts[PhaseInitial])
				assert.Equal(t, uint64(2), snapshot.PartialProgress[PhaseInitial])
				assert.Equal(t, uint64(1), snapshot.FullProgress[PhaseInitial])
				assert.Equal(t, uint64(6), snapshot.RequestedItems[PhaseInitial])
				assert.Equal(t, uint64(13), snapshot.ReusableSlots[TransferFresh].Sum)
			}
		})
	}
}

func TestDiagnosticsProducerWaitPhases(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		strategy WaitStrategy
		phase    AttemptPhase
	}{
		{"spin", WaitStrategy{Spins: 1}, PhaseSpin},
		{"yield", WaitStrategy{Yields: 1}, PhaseYield},
		{"never park", WaitStrategy{NeverPark: true}, PhaseNeverPark},
		{"extra", WaitStrategy{}, PhaseExtra},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			queue, fake := newProducerDiagnosticsQueue(t, 1, WithProducerWait(scenario.strategy))
			fake.pushRaw(sequence(1, 8)...)
			fake.setHooks(func(call engineCall) {
				if call.kind == callEnqueue && call.count == 2 {
					fake.takeRaw(1)
				}
			}, nil)
			require.NoError(t, queue.Push(context.Background(), 9))
			snapshot := queue.Diagnostics().Producer
			assert.Equal(t, uint64(1), snapshot.ZeroProgress[PhaseInitial])
			assert.Equal(t, uint64(1), snapshot.FullProgress[scenario.phase])
			assert.Equal(t, uint64(2), snapshot.AttemptsWithoutRemainingCount)
			assert.Equal(t, uint64(2), snapshot.CapacityCompletedProbes)
			assert.Equal(t, uint64(1), snapshot.WaitStarts)
			assert.Zero(t, snapshot.WakeRetryAttempts)
			assertIdleQueue(t, queue)
		})
	}
}

func TestDiagnosticsProducerMultipleEpisodes(t *testing.T) {
	queue, fake := newProducerDiagnosticsQueue(t, 1, WithProducerWait(WaitStrategy{Yields: 1}))
	fake.setMaxMove(1)
	fake.setHooks(func(call engineCall) {
		if call.kind == callEnqueueBurst && call.count%2 == 1 {
			fake.setForceEmpty()
		}
	}, nil)
	moved, err := queue.PushBatch(context.Background(), []int{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, 3, moved)
	snapshot := queue.Diagnostics().Producer
	assert.Equal(t, uint64(3), snapshot.ZeroProgress[PhaseInitial])
	assert.Equal(t, uint64(3), snapshot.Attempts[PhaseYield])
	assert.Equal(t, uint64(2), snapshot.PartialProgress[PhaseYield])
	assert.Equal(t, uint64(1), snapshot.FullProgress[PhaseYield])
	assert.Equal(t, uint64(6), snapshot.RequestedItems[PhaseInitial])
	assert.Equal(t, uint64(6), snapshot.RequestedItems[PhaseYield])
	assert.Equal(t, uint64(6), snapshot.CapacityCompletedProbes)
	assert.Equal(t, uint64(3), snapshot.Registrations)
	activeShards := 0
	for index := range queue.diagnostics.counters {
		counters := &queue.diagnostics.counters[index].producer
		if counters.attempts[PhaseInitial].Load() > 0 {
			activeShards++
		}
	}
	assert.Equal(t, 1, activeShards, "all episodes retain the invocation's shard")
	assertIdleQueue(t, queue)
}

func TestDiagnosticsProducerRejectedCalls(t *testing.T) {
	queue, fake := newProducerDiagnosticsQueue(t, 1)
	moved, err := queue.TryPushBatch(nil)
	require.NoError(t, err)
	require.Zero(t, moved)
	moved, err = queue.PushBatch(context.Background(), nil)
	require.NoError(t, err)
	require.Zero(t, moved)
	queue.Close()
	require.ErrorIs(t, queue.TryPush(1), ErrClosed)
	require.ErrorIs(t, queue.Push(context.Background(), 1), ErrClosed)
	moved, err = queue.TryPushBatch([]int{1})
	require.ErrorIs(t, err, ErrClosed)
	require.Zero(t, moved)
	moved, err = queue.PushBatch(context.Background(), []int{1})
	require.ErrorIs(t, err, ErrClosed)
	require.Zero(t, moved)
	assert.Zero(t, queue.Diagnostics().Producer)
	assert.Empty(t, fake.recordedCalls())
}

func newConsumerDiagnosticsQueue(
	t *testing.T,
	sampleEvery uint32,
	options ...Option,
) (*Queue[int], *scriptedEngine[int]) {
	t.Helper()
	fake := newScriptedEngine[int](8)
	options = append(options, WithDiagnostics(DiagnosticsConfig{
		Mode: DiagnosticsCounters, Shards: 16, CapacitySampleEvery: sampleEvery,
	}))
	return newTestQueue(t, fake, options...), fake
}

func TestDiagnosticsConsumerTransfers(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		for _, size := range []int{1, 5} {
			t.Run(fmt.Sprintf("blocking=%t/size=%d", blocking, size), func(t *testing.T) {
				queue, fake := newConsumerDiagnosticsQueue(t, 0, WithMaxReceiveBatch(4))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				receive := queue.TryReceive
				if blocking {
					receive = func(dst []int) (int, error) { return queue.Receive(ctx, dst) }
				}
				dst := make([]int, size)
				requested := min(size, 4)
				fake.pushRaw(sequence(1, requested)...)
				moved, err := receive(dst)
				require.NoError(t, err)
				require.Equal(t, requested, moved)
				if size > 1 {
					fake.pushRaw(9, 10)
					moved, err = receive(dst)
					require.NoError(t, err)
					require.Equal(t, 2, moved)
				}
				cancel()
				moved, err = receive(dst)
				if blocking {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
				require.Zero(t, moved)
				snapshot := queue.Diagnostics().Consumer
				assert.Equal(t, uint64(1), snapshot.FullProgress[PhaseInitial])
				assert.Equal(t, uint64(1), snapshot.ZeroProgress[PhaseInitial])
				if size == 1 {
					assert.Equal(t, uint64(2), snapshot.AttemptsWithoutRemainingCount)
					assert.Equal(t, uint64(2), snapshot.RequestedItems[PhaseInitial])
					assert.Equal(t, uint64(1), snapshot.MovedItems[PhaseInitial])
					assert.Zero(t, snapshot.AvailableItems)
					assert.Equal(t, 2, fake.callCount(callDequeue))
				} else {
					assert.Equal(t, uint64(1), snapshot.PartialProgress[PhaseInitial])
					assert.Equal(t, uint64(12), snapshot.RequestedItems[PhaseInitial])
					assert.Equal(t, uint64(6), snapshot.MovedItems[PhaseInitial])
					assert.Equal(t, uint64(1), snapshot.AvailableItems[TransferCachedAllowed].Count)
					assert.Equal(t, uint64(2), snapshot.AvailableItems[TransferFresh].ZeroCount)
					assert.Zero(t, snapshot.AttemptsWithoutRemainingCount)
				}
				assert.Zero(t, snapshot.CapacityCompletedProbes)
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestDiagnosticsConsumerWaitPhases(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		strategy WaitStrategy
		phase    AttemptPhase
	}{
		{"spin", WaitStrategy{Spins: 2}, PhaseSpin},
		{"yield", WaitStrategy{Yields: 2}, PhaseYield},
		{"never park", WaitStrategy{NeverPark: true}, PhaseNeverPark},
		{"extra", WaitStrategy{}, PhaseExtra},
	} {
		for _, size := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/size=%d", scenario.name, size), func(t *testing.T) {
				queue, fake := newConsumerDiagnosticsQueue(t, 1, WithConsumerWait(scenario.strategy))
				kind := callDequeue
				if size > 1 {
					kind = callDequeueBurst
				}
				fake.setHooks(func(call engineCall) {
					if call.kind == kind && call.count == 2 {
						fake.pushRaw(sequence(1, size)...)
					}
				}, nil)
				moved, err := queue.Receive(context.Background(), make([]int, size))
				require.NoError(t, err)
				require.Equal(t, size, moved)
				snapshot := queue.Diagnostics().Consumer
				assert.Equal(t, uint64(1), snapshot.ZeroProgress[PhaseInitial])
				assert.Equal(t, uint64(1), snapshot.FullProgress[scenario.phase])
				assert.Equal(t, uint64(2), snapshot.CapacityCompletedProbes)
				assert.Equal(t, uint64(2), snapshot.RequestedSize.Count)
				assert.Equal(t, uint64(size), snapshot.MovedSize.Sum)
				assert.Equal(t, uint64(1), snapshot.WaitStarts)
				assert.Zero(t, snapshot.WakeRetryAttempts)
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestDiagnosticsConsumerEmptyAndCanceledCalls(t *testing.T) {
	queue, fake := newConsumerDiagnosticsQueue(t, 1)
	for _, closed := range []bool{false, true} {
		if closed {
			queue.Close()
		}
		moved, err := queue.TryReceive(nil)
		require.NoError(t, err)
		require.Zero(t, moved)
		moved, err = queue.Receive(context.Background(), nil)
		require.NoError(t, err)
		require.Zero(t, moved)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = queue.Consume(ctx, func(context.Context, []int) error {
			t.Fatal("canceled Consume invoked handler")
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
	}
	assert.Zero(t, queue.Diagnostics().Consumer)
	assert.Empty(t, fake.recordedCalls())
}

func TestDiagnosticsConsumeAcquisition(t *testing.T) {
	for _, outcome := range []string{"closed", "handler error", "handler panic"} {
		t.Run(outcome, func(t *testing.T) {
			queue, fake := newConsumerDiagnosticsQueue(t, 1, WithMaxReceiveBatch(2))
			fake.pushRaw(1, 2, 3, 4)
			queue.Close()
			handled := 0
			handler := func(context.Context, []int) error {
				handled++
				snapshot := queue.Diagnostics().Consumer
				assert.Equal(t, uint64(2*handled), snapshot.MovedSize.Sum)
				assert.Equal(t, uint64(handled), snapshot.CapacityCompletedProbes)
				assertIdleQueue(t, queue)
				if outcome == "handler error" {
					return errHandler
				}
				if outcome == "handler panic" {
					panic("handler panic")
				}
				return nil
			}
			if outcome == "handler panic" {
				assert.PanicsWithValue(t, "handler panic", func() {
					_ = queue.Consume(context.Background(), handler)
				})
			} else {
				err := queue.Consume(context.Background(), handler)
				if outcome == "handler error" {
					require.ErrorIs(t, err, errHandler)
				} else {
					require.NoError(t, err)
				}
			}
			assert.Equal(t, uint64(2*handled), queue.Diagnostics().Consumer.MovedSize.Sum)
			activeShards := 0
			for index := range queue.diagnostics.counters {
				if queue.diagnostics.counters[index].consumer.requestedSize.count.Load() > 0 {
					activeShards++
				}
			}
			assert.Equal(t, 1, activeShards, "Consume retains one shard across batches and drain")
		})
	}
}

func TestDiagnosticsConsumeClearsBuffer(t *testing.T) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				options := append([]Option{}, engineCase.options...)
				options = append(options, WithDiagnostics(DiagnosticsConfig{
					Mode: DiagnosticsCounters, CapacitySampleEvery: 1,
				}))
				queue, err := New[*int](8, options...)
				require.NoError(t, err)
				payload := 7
				require.NoError(t, queue.TryPush(&payload))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				batches := make(chan []*int, 1)
				results := make(chan error, 1)
				go func() {
					results <- queue.Consume(ctx, func(_ context.Context, batch []*int) error {
						batches <- batch
						return nil
					})
				}()
				batch := <-batches
				synctest.Wait()
				requireParked(t, &queue.consumers, 1)
				assert.Equal(t, []*int{nil}, batch)
				assert.Equal(t, uint64(1), queue.Diagnostics().Consumer.MovedSize.Sum)
				cancel()
				require.ErrorIs(t, <-results, context.Canceled)
				assertIdleSide(t, &queue.consumers)
				assertIdleSide(t, &queue.producers)
			})
		})
	}
}
