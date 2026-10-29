package workq

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsProducerProbeSampling(t *testing.T) {
	for _, rate := range []uint32{0, 1} {
		t.Run(map[uint32]string{0: "disabled", 1: "every attempt"}[rate], func(t *testing.T) {
			queue, fake := newProducerDiagnosticsQueue(t, rate)
			require.NoError(t, queue.TryPush(1))
			moved, err := queue.TryPushBatch(sequence(2, 7))
			require.NoError(t, err)
			require.Equal(t, 7, moved)
			require.ErrorIs(t, queue.TryPush(9), ErrFull)
			moved, err = queue.TryPushBatch([]int{9})
			require.ErrorIs(t, err, ErrFull)
			require.Zero(t, moved)
			snapshot := queue.Diagnostics()
			assert.Equal(t, uint64(4), snapshot.Producer.CapacityEligibleAttempts)
			assert.Equal(t, uint64(4*rate), snapshot.Producer.CapacitySelectedProbes)
			assert.Equal(t, uint64(4*rate), snapshot.Producer.CapacityCompletedProbes)
			histogram := snapshot.Producer.ReusableSlots[DiagnosticProbe]
			assert.Equal(t, uint64(4*rate), histogram.Count)
			assert.Equal(t, uint64(3*rate), histogram.ZeroCount)
			assert.Equal(t, uint64(7*rate), histogram.Sum)
			assert.Zero(t, snapshot.Consumer.AvailableItems[DiagnosticProbe])
			for _, call := range fake.recordedCalls() {
				if call.kind == callPeek {
					assert.Equal(t, callEnqueueBurst, call.transferKind)
				}
			}
			before := fake.recordedCalls()
			queue.Diagnostics()
			assert.Equal(t, before, fake.recordedCalls(), "collecting must not call the engine")
		})
	}
}

func TestDiagnosticsProducerProbeAfterWakeAndCleanup(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		strategy WaitStrategy
		wait     bool
	}{
		{"initial", WaitStrategy{}, false},
		{"spin", WaitStrategy{Spins: 1}, true},
		{"yield", WaitStrategy{Yields: 1}, true},
		{"never park", WaitStrategy{NeverPark: true}, true},
		{"extra", WaitStrategy{}, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			queue, fake := newProducerDiagnosticsQueue(t, 1, WithProducerWait(scenario.strategy))
			caller := parkConsumer(t, &queue.consumers)
			if scenario.wait {
				fake.pushRaw(sequence(1, 8)...)
			}
			succeeded := false
			diagnosticAfterSuccess := false
			fake.setHooks(func(call engineCall) {
				if scenario.wait && call.kind == callEnqueue && call.count == 2 {
					fake.takeRaw(1)
				}
				if call.kind != callPeek || call.transferKind != callEnqueueBurst || !succeeded {
					return
				}
				diagnosticAfterSuccess = true
				assertIdleSide(t, &queue.producers)
				assert.Equal(t, uint64(1), queue.Diagnostics().Consumer.TokensSent)
				// Closure here must not replace the result already chosen after publication.
				queue.Close()
			}, func(call engineCall) {
				if call.kind == callEnqueue {
					succeeded = !scenario.wait || call.count == 2
				}
			})
			require.NoError(t, queue.Push(context.Background(), 9))
			require.True(t, diagnosticAfterSuccess)
			calls := fake.recordedCalls()
			require.GreaterOrEqual(t, len(calls), 3)
			assert.Equal(t, callDequeueBurst, calls[len(calls)-2].transferKind)
			assert.Equal(t, callPeek, calls[len(calls)-2].kind)
			assert.Equal(t, callEnqueueBurst, calls[len(calls)-1].transferKind)
			queue.consumers.leaveList(nil, caller, false, func() bool { return false })
			assertIdleQueue(t, queue)
		})
	}
}

func TestDiagnosticsProducerProbePreservesCanceledResult(t *testing.T) {
	for _, strategy := range []WaitStrategy{{Yields: 1}, {NeverPark: true}} {
		queue, fake := newProducerDiagnosticsQueue(t, 1, WithProducerWait(strategy))
		fake.pushRaw(sequence(1, 8)...)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		probes := 0
		fake.setHooks(func(call engineCall) {
			if call.kind != callPeek || call.transferKind != callEnqueueBurst {
				return
			}
			probes++
			if probes == 2 {
				assertIdleSide(t, &queue.producers)
				queue.Close()
			}
		}, func(call engineCall) {
			if call.kind == callEnqueue && call.count == 2 {
				cancel()
			}
		})
		require.ErrorIs(t, queue.Push(ctx, 9), context.Canceled)
		assert.Equal(t, 2, probes)
		assert.Equal(t, uint64(2), queue.Diagnostics().Producer.CapacityCompletedProbes)
		assertIdleQueue(t, queue)
	}
}

func TestDiagnosticsProducerProbeBeforeNextAttempt(t *testing.T) {
	queue, fake := newProducerDiagnosticsQueue(t, 1, WithProducerWait(WaitStrategy{Spins: 2}))
	fake.pushRaw(sequence(1, 8)...)
	fake.setHooks(func(call engineCall) {
		if call.kind == callEnqueue && call.count == 3 {
			fake.takeRaw(1)
		}
	}, nil)
	require.NoError(t, queue.Push(context.Background(), 9))
	calls := fake.recordedCalls()
	require.Len(t, calls, 6)
	for index, call := range calls {
		if index%2 == 0 {
			assert.Equal(t, callEnqueue, call.kind)
		} else {
			assert.Equal(t, callPeek, call.kind)
			assert.Equal(t, callEnqueueBurst, call.transferKind)
		}
	}
	snapshot := queue.Diagnostics().Producer
	assert.Equal(t, uint64(2), snapshot.Attempts[PhaseSpin])
	assert.Equal(t, uint64(1), snapshot.ZeroProgress[PhaseSpin])
	assert.Equal(t, uint64(1), snapshot.FullProgress[PhaseSpin])
}

func TestDiagnosticsProducerProbeBeforePark(t *testing.T) {
	queue, fake := newProducerDiagnosticsQueue(t, 1, WithProducerWait(WaitStrategy{}))
	fake.pushRaw(sequence(1, 8)...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake.setHooks(func(call engineCall) {
		if call.kind == callPeek && call.count == 2 {
			assert.Equal(t, uint32(1), queue.producers.load().parked())
			cancel()
		}
	}, nil)
	require.ErrorIs(t, queue.Push(ctx, 9), context.Canceled)
	snapshot := queue.Diagnostics().Producer
	assert.Equal(t, uint64(1), snapshot.ZeroProgress[PhaseExtra])
	assert.Equal(t, uint64(2), snapshot.CapacityCompletedProbes)
	assertIdleQueue(t, queue)
}

func TestDiagnosticsProducerMovedBeforeClose(t *testing.T) {
	for _, operation := range []string{"push", "try push", "batch", "try batch"} {
		t.Run(operation, func(t *testing.T) {
			queue, fake := newProducerDiagnosticsQueue(t, 1)
			fake.setHooks(nil, func(call engineCall) {
				if call.kind == callEnqueue || call.kind == callEnqueueBurst {
					queue.Close()
				}
			})
			var err error
			switch operation {
			case "push":
				err = queue.Push(context.Background(), 1)
			case "try push":
				err = queue.TryPush(1)
			case "batch":
				var moved int
				moved, err = queue.PushBatch(context.Background(), []int{1})
				require.Equal(t, 1, moved)
			case "try batch":
				var moved int
				moved, err = queue.TryPushBatch([]int{1})
				require.Equal(t, 1, moved)
			}
			require.ErrorIs(t, err, ErrClosedDuringPush)
			snapshot := queue.Diagnostics().Producer
			assert.Equal(t, uint64(1), snapshot.FullProgress[PhaseInitial])
			assert.Equal(t, uint64(1), snapshot.MovedItems[PhaseInitial])
			assert.Equal(t, uint64(1), snapshot.CapacityCompletedProbes)
		})
	}
}

func TestDiagnosticsProducerProbeCapacityAndTransferObservation(t *testing.T) {
	queue, fake := newProducerDiagnosticsQueue(t, 1)
	fake.setHooks(nil, func(call engineCall) {
		if call.kind == callEnqueueBurst {
			fake.takeRaw(1)
		}
	})
	moved, err := queue.TryPushBatch([]int{1})
	require.NoError(t, err)
	require.Equal(t, 1, moved)
	snapshot := queue.Diagnostics().Producer
	assert.Equal(t, uint64(7), snapshot.ReusableSlots[TransferCachedAllowed].Sum)
	assert.Equal(t, uint64(8), snapshot.ReusableSlots[DiagnosticProbe].Sum)
	assert.Equal(t, uint64(1), snapshot.ReusableSlots[DiagnosticProbe].AtCapacity)
}

func TestDiagnosticsConsumerDrainProbeOrder(t *testing.T) {
	for _, path := range []string{"try", "spin", "register", "yield", "never park", "park"} {
		for _, drainMoves := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/moves=%t", path, drainMoves), func(t *testing.T) {
				strategy := WaitStrategy{}
				closeAt := 1
				switch path {
				case "spin":
					strategy.Spins = 1
				case "yield":
					strategy.Yields = 1
					closeAt = 2
				case "never park":
					strategy.NeverPark = true
					closeAt = 2
				case "park":
					closeAt = 2
				}
				queue, fake := newConsumerDiagnosticsQueue(t, 1, WithConsumerWait(strategy))
				probes := 0
				fake.setHooks(func(call engineCall) {
					if call.kind == callPeek {
						assert.Equal(t, callDequeueBurst, call.transferKind)
						probes++
						if path != "park" {
							assertIdleSide(t, &queue.consumers)
						}
					}
					if call.kind == callDequeue {
						assert.Equal(t, call.count-1, probes, "probe precedes next transfer")
						if call.count == closeAt+1 {
							assertIdleSide(t, &queue.consumers)
						}
					}
				}, func(call engineCall) {
					if call.kind == callDequeue && call.count == closeAt {
						if drainMoves {
							fake.pushRaw(7)
						}
						queue.Close()
					}
				})
				dst := make([]int, 1)
				var moved int
				var err error
				if path == "try" {
					moved, err = queue.TryReceive(dst)
				} else {
					moved, err = queue.Receive(context.Background(), dst)
				}
				snapshot := queue.Diagnostics().Consumer
				assert.Equal(t, uint64(1), snapshot.Attempts[PhaseDrain])
				assert.Equal(t, uint64(closeAt+1), snapshot.CapacityCompletedProbes)
				if drainMoves {
					require.NoError(t, err)
					assert.Equal(t, 1, moved)
					assert.Equal(t, 7, dst[0])
					assert.Equal(t, uint64(1), snapshot.FullProgress[PhaseDrain])
				} else {
					require.ErrorIs(t, err, ErrClosed)
					assert.Zero(t, moved)
					assert.Equal(t, uint64(1), snapshot.ZeroProgress[PhaseDrain])
				}
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestDiagnosticsConsumerProbeAfterWakeAndCleanup(t *testing.T) {
	for _, strategy := range []WaitStrategy{
		{Spins: 1}, {Yields: 1}, {NeverPark: true}, {},
	} {
		queue, fake := newConsumerDiagnosticsQueue(t, 1, WithConsumerWait(strategy))
		caller := parkProducer(t, &queue.producers)
		checked := false
		fake.setHooks(func(call engineCall) {
			if call.kind == callDequeue && call.count == 2 {
				fake.pushRaw(7)
			}
			if call.kind == callPeek && call.count == 2 {
				checked = true
				assertIdleSide(t, &queue.consumers)
				assert.Equal(t, uint64(1), queue.Diagnostics().Producer.TokensSent)
				queue.Close()
			}
		}, nil)
		moved, err := queue.Receive(context.Background(), make([]int, 1))
		require.NoError(t, err)
		require.Equal(t, 1, moved)
		require.True(t, checked)
		queue.producers.leaveList(nil, caller, false, mustNotPeek(t))
		assertIdleQueue(t, queue)
	}
}

func TestDiagnosticsConsumerProbePreservesCanceledResult(t *testing.T) {
	for _, strategy := range []WaitStrategy{{Yields: 1}, {NeverPark: true}} {
		queue, fake := newConsumerDiagnosticsQueue(t, 1, WithConsumerWait(strategy))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fake.setHooks(func(call engineCall) {
			if call.kind == callPeek && call.count == 2 {
				assertIdleSide(t, &queue.consumers)
				queue.Close()
				fake.pushRaw(7)
			}
		}, func(call engineCall) {
			if call.kind == callDequeue && call.count == 2 {
				cancel()
			}
		})
		moved, err := queue.Receive(ctx, make([]int, 1))
		require.ErrorIs(t, err, context.Canceled)
		assert.Zero(t, moved)
		assert.Zero(t, queue.Diagnostics().Consumer.Attempts[PhaseDrain])
		assert.Equal(t, 2, fake.callCount(callDequeue))
		assertIdleQueue(t, queue)
	}
}

func TestDiagnosticsConsumerDrainResultBeforeProbe(t *testing.T) {
	queue, fake := newConsumerDiagnosticsQueue(t, 1)
	queue.Close()
	fake.setHooks(func(call engineCall) {
		if call.kind == callPeek && call.count == 2 {
			fake.pushRaw(7)
		}
	}, nil)
	moved, err := queue.TryReceive(make([]int, 1))
	require.ErrorIs(t, err, ErrClosed)
	assert.Zero(t, moved)
	assert.Equal(t, 2, fake.callCount(callDequeue))
	assert.Equal(t, uint64(1), queue.Diagnostics().Consumer.AvailableItems[DiagnosticProbe].Sum)
}

type diagnosticsProbeEngine struct {
	engine[int]
	producerProbes int
	consumerProbes int
}

func (e *diagnosticsProbeEngine) EnqueueBurst(items []int) (int, int) {
	if len(items) == 0 {
		e.producerProbes++
	}
	return e.engine.EnqueueBurst(items)
}

func (e *diagnosticsProbeEngine) DequeueBurst(dst []int) (int, int) {
	if len(dst) == 0 {
		e.consumerProbes++
	}
	return e.engine.DequeueBurst(dst)
}

func TestDiagnosticsConsumerProbeEngineOwnership(t *testing.T) {
	for _, engineCase := range engineCases() {
		for _, rate := range []uint32{0, 1} {
			t.Run(fmt.Sprintf("%s/rate=%d", engineCase.name, rate), func(t *testing.T) {
				options := append([]Option{}, engineCase.options...)
				options = append(options, WithDiagnostics(DiagnosticsConfig{
					Mode: DiagnosticsCounters, CapacitySampleEvery: rate,
				}))
				queue, err := New[int](8, options...)
				require.NoError(t, err)
				probeEngine := &diagnosticsProbeEngine{engine: queue.engine}
				queue.engine = probeEngine
				moved, _ := probeEngine.engine.EnqueueBurst(sequence(1, 8))
				require.Equal(t, 8, moved)
				for _, size := range []int{1, 4, 4, 1} {
					_, err = queue.TryReceive(make([]int, size))
					require.NoError(t, err)
				}
				assert.Equal(t, int(4*rate), probeEngine.consumerProbes)
				assert.Zero(t, probeEngine.producerProbes)
				snapshot := queue.Diagnostics().Consumer
				assert.Equal(t, uint64(4), snapshot.CapacityEligibleAttempts)
				assert.Equal(t, uint64(4*rate), snapshot.CapacitySelectedProbes)
				assert.Equal(t, uint64(4*rate), snapshot.CapacityCompletedProbes)
				assert.Equal(t, uint64(10*rate), snapshot.AvailableItems[DiagnosticProbe].Sum)
				assert.Equal(t, uint64(2*rate), snapshot.AvailableItems[DiagnosticProbe].ZeroCount)
				assert.Equal(t, uint64(3), snapshot.AvailableItems[TransferCachedAllowed].Sum)
				assert.Equal(t, uint64(1), snapshot.AvailableItems[TransferFresh].ZeroCount)
			})
		}
	}
}

func TestDiagnosticsConsumerProbeBeforeNextAttempt(t *testing.T) {
	for _, strategy := range []WaitStrategy{
		{Spins: 2}, {Yields: 2}, {NeverPark: true},
	} {
		queue, fake := newConsumerDiagnosticsQueue(t, 1, WithConsumerWait(strategy))
		fake.setHooks(func(call engineCall) {
			if call.kind == callDequeue && call.count == 3 {
				fake.pushRaw(7)
			}
		}, nil)
		moved, err := queue.Receive(context.Background(), make([]int, 1))
		require.NoError(t, err)
		require.Equal(t, 1, moved)
		calls := fake.recordedCalls()
		require.Len(t, calls, 6)
		for index, call := range calls {
			if index%2 == 0 {
				assert.Equal(t, callDequeue, call.kind)
			} else {
				assert.Equal(t, callPeek, call.kind)
				assert.Equal(t, callDequeueBurst, call.transferKind)
			}
		}
		assertIdleQueue(t, queue)
	}
}

func TestDiagnosticsConsumerInitialProbeAfterWake(t *testing.T) {
	queue, fake := newConsumerDiagnosticsQueue(t, 1)
	fake.pushRaw(7)
	caller := parkProducer(t, &queue.producers)
	fake.setHooks(func(call engineCall) {
		if call.kind == callPeek {
			assert.Equal(t, uint64(1), queue.Diagnostics().Producer.TokensSent)
			queue.Close()
		}
	}, nil)
	moved, err := queue.TryReceive(make([]int, 1))
	require.NoError(t, err)
	assert.Equal(t, 1, moved)
	queue.producers.leaveList(nil, caller, false, mustNotPeek(t))
	assertIdleQueue(t, queue)
}
