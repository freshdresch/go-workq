package workq

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertDiagnosticsWaitReconciled(t *testing.T, counters DiagnosticsSideSnapshot) {
	t.Helper()
	var waitEnds, selectEnds, retryEnds uint64
	for _, count := range counters.WaitEnds {
		waitEnds += count
	}
	for _, count := range counters.ParkSelectEnds {
		selectEnds += count
	}
	for _, count := range counters.WakeRetryEnds {
		retryEnds += count
	}
	assert.Equal(t, counters.WaitStarts, waitEnds)
	assert.Equal(t, counters.ParkSelectStarts, selectEnds)
	assert.Equal(t, counters.TokensReceivedForRetry, retryEnds)
}

func TestDiagnosticsWaitPhaseEntries(t *testing.T) {
	previous := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, producer := range []bool{false, true} {
		for _, scenario := range []struct {
			name     string
			strategy WaitStrategy
			phase    AttemptPhase
			attempts int
			yields   uint64
		}{
			{"zero polls", WaitStrategy{}, PhaseExtra, 1, 0},
			{"spin", WaitStrategy{Spins: 2}, PhaseSpin, 2, 0},
			{"one yield attempt", WaitStrategy{Yields: 1}, PhaseYield, 1, 0},
			{"two yield attempts", WaitStrategy{Yields: 2}, PhaseYield, 2, 1},
			{"never park", WaitStrategy{NeverPark: true}, PhaseNeverPark, 3, 2},
		} {
			side := "consumer"
			if producer {
				side = "producer"
			}
			t.Run(side+"/"+scenario.name, func(t *testing.T) {
				queue, fake := newProducerDiagnosticsQueue(
					t,
					0,
					WithProducerWait(scenario.strategy),
					WithConsumerWait(scenario.strategy),
				)
				kind := callDequeue
				if producer {
					kind = callEnqueue
					fake.pushRaw(sequence(1, 8)...)
				}
				fake.setHooks(func(call engineCall) {
					if call.kind != kind || call.count != scenario.attempts+1 {
						return
					}
					if producer {
						fake.takeRaw(1)
					} else {
						fake.pushRaw(1)
					}
				}, nil)
				if producer {
					require.NoError(t, queue.Push(context.Background(), 9))
				} else {
					moved, err := queue.Receive(context.Background(), make([]int, 1))
					require.NoError(t, err)
					require.Equal(t, 1, moved)
				}
				counters := queue.Diagnostics().Consumer.DiagnosticsSideSnapshot
				if producer {
					counters = queue.Diagnostics().Producer.DiagnosticsSideSnapshot
				}
				assert.Equal(t, uint64(1), counters.WaitStarts)
				assert.Equal(t, uint64(1), counters.WaitEnds[WaitProgress])
				assert.Equal(t, uint64(scenario.attempts), counters.Attempts[scenario.phase])
				assert.Equal(t, scenario.yields, counters.YieldCalls[scenario.phase])
				var spin, yield, neverPark uint64
				switch scenario.phase {
				case PhaseSpin:
					spin = 1
				case PhaseYield:
					yield = 1
				case PhaseNeverPark:
					neverPark = 1
				}
				assert.Equal(t, spin, counters.SpinEntries)
				assert.Equal(t, yield, counters.YieldEntries)
				assert.Equal(t, neverPark, counters.NeverParkEntries)
				assert.Zero(t, counters.ParkSelectStarts)
				assertDiagnosticsWaitReconciled(t, counters)
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestDiagnosticsWaitEffectiveZeroSpins(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, producer := range []bool{false, true} {
		queue, fake := newProducerDiagnosticsQueue(
			t,
			0,
			WithProducerWait(WaitStrategy{Spins: 10}),
			WithConsumerWait(WaitStrategy{Spins: 10}),
		)
		kind := callDequeue
		if producer {
			kind = callEnqueue
			fake.pushRaw(sequence(1, 8)...)
		}
		fake.setHooks(func(call engineCall) {
			if call.kind != kind || call.count != 2 {
				return
			}
			if producer {
				fake.takeRaw(1)
			} else {
				fake.pushRaw(1)
			}
		}, nil)
		if producer {
			require.NoError(t, queue.Push(context.Background(), 9))
		} else {
			_, err := queue.Receive(context.Background(), make([]int, 1))
			require.NoError(t, err)
		}
		counters := queue.Diagnostics().Consumer.DiagnosticsSideSnapshot
		if producer {
			counters = queue.Diagnostics().Producer.DiagnosticsSideSnapshot
		}
		assert.Zero(t, counters.SpinEntries)
		assert.Zero(t, counters.Attempts[PhaseSpin])
		assert.Equal(t, uint64(1), counters.Attempts[PhaseExtra])
		assertDiagnosticsWaitReconciled(t, counters)
	}
}

func TestDiagnosticsWaitNoAttemptExit(t *testing.T) {
	previous := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, producer := range []bool{false, true} {
		queue, fake := newProducerDiagnosticsQueue(
			t,
			0,
			WithProducerWait(WaitStrategy{Spins: 1}),
			WithConsumerWait(WaitStrategy{Spins: 1}),
		)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if producer {
			fake.pushRaw(sequence(1, 8)...)
			require.ErrorIs(t, queue.Push(ctx, 9), context.Canceled)
		} else {
			_, err := queue.Receive(ctx, make([]int, 1))
			require.ErrorIs(t, err, context.Canceled)
		}
		counters := queue.Diagnostics().Consumer.DiagnosticsSideSnapshot
		if producer {
			counters = queue.Diagnostics().Producer.DiagnosticsSideSnapshot
		}
		assert.Equal(t, uint64(1), counters.WaitStarts)
		assert.Equal(t, uint64(1), counters.WaitEnds[WaitCanceled])
		assert.Zero(t, counters.SpinEntries)
		assert.Zero(t, counters.YieldEntries)
		assert.Zero(t, counters.NeverParkEntries)
		assert.Zero(t, counters.ParkEnlistments)
		assertDiagnosticsWaitReconciled(t, counters)
	}
}

func TestDiagnosticsWaitForcedTokenRetry(t *testing.T) {
	for _, producer := range []bool{false, true} {
		t.Run(map[bool]string{false: "consumer", true: "producer"}[producer], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue, fake := newProducerDiagnosticsQueue(
					t,
					0,
					WithProducerWait(WaitStrategy{}),
					WithConsumerWait(WaitStrategy{}),
				)
				side := &queue.consumers
				if producer {
					side = &queue.producers
					fake.pushRaw(sequence(1, 8)...)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				results := make(chan error, 1)
				go func() {
					if producer {
						results <- queue.Push(ctx, 9)
						return
					}
					_, err := queue.Receive(ctx, make([]int, 1))
					results <- err
				}()
				synctest.Wait()
				requireParked(t, side, 1)
				for range 3 {
					side.wakeOne(nil)
					synctest.Wait()
					requireParked(t, side, 1)
				}
				if producer {
					fake.takeRaw(1)
				} else {
					fake.pushRaw(1)
				}
				side.wakeOne(nil)
				require.NoError(t, <-results)
				counters := queue.Diagnostics().Consumer.DiagnosticsSideSnapshot
				if producer {
					counters = queue.Diagnostics().Producer.DiagnosticsSideSnapshot
				}
				assert.Equal(t, uint64(4), counters.YieldEntries)
				assert.Equal(t, uint64(4), counters.Attempts[PhaseYield])
				assert.Zero(t, counters.YieldCalls)
				assert.Equal(t, uint64(4), counters.ParkSelectStarts)
				assert.Equal(t, uint64(4), counters.ParkSelectEnds[ParkToken])
				assert.Equal(t, uint64(4), counters.WakeRetryAttempts)
				assert.Equal(t, uint64(3), counters.WakeRetryZeroProgress)
				assert.Equal(t, uint64(3), counters.WakeRetryEnds[RetryReparked])
				assert.Equal(t, uint64(1), counters.WakeRetryEnds[RetryProgress])
				if producer {
					assert.Equal(t, uint64(3), counters.ProducerOldestReenlistments)
				}
				assertDiagnosticsWaitReconciled(t, counters)
				assertIdleQueue(t, queue)
			})
		})
	}
}

func TestDiagnosticsWaitZeroAttemptClosedRetry(t *testing.T) {
	queue, fake := newProducerDiagnosticsQueue(t, 0, WithProducerWait(WaitStrategy{}))
	fake.pushRaw(sequence(1, 8)...)
	fake.setHooks(nil, func(call engineCall) {
		if call.kind != callEnqueue || call.count != 2 {
			return
		}
		queue.producers.wakeOne(nil)
		queue.producers.mu.Lock()
		queue.producers.setClosedLocked()
		queue.producers.mu.Unlock()
	})
	require.ErrorIs(t, queue.Push(context.Background(), 9), ErrClosed)
	counters := queue.Diagnostics().Producer.DiagnosticsSideSnapshot
	assert.Equal(t, uint64(1), counters.TokensReceivedForRetry)
	assert.Zero(t, counters.WakeRetryAttempts)
	assert.Zero(t, counters.YieldEntries)
	assert.Equal(t, uint64(1), counters.WakeRetryEnds[RetryClosed])
	assertDiagnosticsWaitReconciled(t, counters)
	assertIdleQueue(t, queue)
}

func TestDiagnosticsWaitRetryTerminalClassification(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		err   error
		wait  WaitOutcome
		retry WakeRetryOutcome
	}{
		{"canceled", context.Canceled, WaitCanceled, RetryCanceled},
		{"deadline", context.DeadlineExceeded, WaitDeadline, RetryDeadline},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			queue, _ := newProducerDiagnosticsQueue(t, 0)
			recording := queue.diagnostics.chooseRecording()
			counters := recording.producerCounters()
			recording.startWait(counters)
			recording.startRetry(counters)
			recording.finishWait(counters, 0, scenario.err)
			snapshot := queue.Diagnostics().Producer.DiagnosticsSideSnapshot
			assert.Zero(t, snapshot.WakeRetryAttempts)
			assert.Equal(t, uint64(1), snapshot.WaitEnds[scenario.wait])
			assert.Equal(t, uint64(1), snapshot.WakeRetryEnds[scenario.retry])
			assertDiagnosticsWaitReconciled(t, snapshot)
		})
	}
}

func TestDiagnosticsWaitPostTokenCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			queue, fake := newProducerDiagnosticsQueue(t, 0, WithProducerWait(WaitStrategy{}))
			fake.pushRaw(sequence(1, 8)...)
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			}
			defer cancel()
			fake.setHooks(func(call engineCall) {
				if call.kind != callEnqueue || call.count != 3 {
					return
				}
				if deadline {
					<-ctx.Done()
				} else {
					cancel()
				}
			}, func(call engineCall) {
				if call.kind == callEnqueue && call.count == 2 {
					queue.producers.wakeOne(nil)
				}
			})
			err := queue.Push(ctx, 9)
			waitOutcome, retryOutcome := WaitCanceled, RetryCanceled
			if deadline {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				waitOutcome, retryOutcome = WaitDeadline, RetryDeadline
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			counters := queue.Diagnostics().Producer.DiagnosticsSideSnapshot
			assert.Equal(t, uint64(1), counters.WakeRetryAttempts)
			assert.Equal(t, uint64(1), counters.WakeRetryZeroProgress)
			assert.Equal(t, uint64(1), counters.WaitEnds[waitOutcome])
			assert.Equal(t, uint64(1), counters.WakeRetryEnds[retryOutcome])
			assertDiagnosticsWaitReconciled(t, counters)
		})
	}
}

func TestDiagnosticsWaitCloseRace(t *testing.T) {
	for _, progress := range []bool{false, true} {
		queue, fake := newProducerDiagnosticsQueue(
			t,
			1,
			WithProducerWait(WaitStrategy{Yields: 1}),
			WithMaxPushBatch(1),
		)
		fake.pushRaw(sequence(1, 7)...)
		fake.setHooks(func(call engineCall) {
			if call.kind != callEnqueueBurst || call.count != 3 {
				return
			}
			if progress {
				fake.takeRaw(1)
			} else {
				queue.Close()
			}
		}, func(call engineCall) {
			if progress && call.kind == callEnqueueBurst && call.count == 3 {
				queue.Close()
			}
		})
		moved, err := queue.PushBatch(context.Background(), []int{8, 9})
		require.ErrorIs(t, err, ErrClosedDuringPush)
		expectedMoved := 1
		outcome := WaitClosed
		if progress {
			expectedMoved = 2
			outcome = WaitProgress
		}
		require.Equal(t, expectedMoved, moved)
		counters := queue.Diagnostics().Producer.DiagnosticsSideSnapshot
		assert.Equal(t, uint64(1), counters.WaitEnds[outcome])
		assert.Equal(t, uint64(1), counters.WaitEndsWithCloseRace)
		assertDiagnosticsWaitReconciled(t, counters)
	}
}

func TestDiagnosticsWaitSelectTerminal(t *testing.T) {
	for _, producer := range []bool{false, true} {
		for _, closed := range []bool{false, true} {
			synctest.Test(t, func(t *testing.T) {
				queue, fake := newProducerDiagnosticsQueue(
					t,
					0,
					WithProducerWait(WaitStrategy{}),
					WithConsumerWait(WaitStrategy{}),
				)
				side := &queue.consumers
				if producer {
					side = &queue.producers
					fake.pushRaw(sequence(1, 8)...)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				results := make(chan error, 1)
				go func() {
					if producer {
						results <- queue.Push(ctx, 9)
						return
					}
					_, err := queue.Receive(ctx, make([]int, 1))
					results <- err
				}()
				synctest.Wait()
				requireParked(t, side, 1)
				outcome, selected := WaitCanceled, ParkContext
				expectedErr := context.Canceled
				if closed {
					queue.Close()
					outcome, selected = WaitClosed, ParkDone
					expectedErr = ErrClosed
				} else {
					cancel()
				}
				require.ErrorIs(t, <-results, expectedErr)
				counters := queue.Diagnostics().Consumer.DiagnosticsSideSnapshot
				if producer {
					counters = queue.Diagnostics().Producer.DiagnosticsSideSnapshot
				}
				assert.Equal(t, uint64(1), counters.WaitEnds[outcome])
				assert.Equal(t, uint64(1), counters.ParkSelectEnds[selected])
				assert.Zero(t, counters.TokensReceivedForRetry)
				assertDiagnosticsWaitReconciled(t, counters)
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestDiagnosticsWaitConsumerRetryDrain(t *testing.T) {
	for _, progress := range []bool{false, true} {
		queue, fake := newConsumerDiagnosticsQueue(t, 0, WithConsumerWait(WaitStrategy{}))
		fake.setHooks(nil, func(call engineCall) {
			if call.kind != callDequeue || call.count != 2 {
				return
			}
			queue.consumers.wakeOne(nil)
			queue.consumers.mu.Lock()
			queue.consumers.setClosedLocked()
			queue.consumers.mu.Unlock()
			if progress {
				fake.pushRaw(1)
			}
		})
		moved, err := queue.Receive(context.Background(), make([]int, 1))
		waitOutcome, retryOutcome := WaitClosed, RetryClosed
		if progress {
			require.NoError(t, err)
			require.Equal(t, 1, moved)
			waitOutcome, retryOutcome = WaitProgress, RetryProgress
		} else {
			require.ErrorIs(t, err, ErrClosed)
			require.Zero(t, moved)
		}
		counters := queue.Diagnostics().Consumer
		assert.Equal(t, uint64(1), counters.Attempts[PhaseDrain])
		assert.Equal(t, uint64(1), counters.WakeRetryAttempts)
		assert.Zero(t, counters.YieldEntries)
		assert.Equal(t, uint64(1), counters.WaitEnds[waitOutcome])
		assert.Equal(t, uint64(1), counters.WakeRetryEnds[retryOutcome])
		assertDiagnosticsWaitReconciled(t, counters.DiagnosticsSideSnapshot)
		assertIdleQueue(t, queue)
	}
}

func TestDiagnosticsWaitRefusedRetryEnlist(t *testing.T) {
	queue, fake := newProducerDiagnosticsQueue(t, 1, WithProducerWait(WaitStrategy{}))
	fake.pushRaw(sequence(1, 8)...)
	fake.setHooks(nil, func(call engineCall) {
		if call.kind == callEnqueue && call.count == 2 {
			queue.producers.wakeOne(nil)
		}
		if call.kind != callPeek || call.transferKind != callEnqueueBurst {
			return
		}
		if fake.callCount(callEnqueue) == 3 {
			queue.Close()
		}
	})
	require.ErrorIs(t, queue.Push(context.Background(), 9), ErrClosed)
	counters := queue.Diagnostics().Producer
	assert.Equal(t, uint64(1), counters.ParkEnlistments)
	assert.Equal(t, uint64(1), counters.ParkEnlistClosed)
	assert.Equal(t, uint64(1), counters.WakeRetryAttempts)
	assert.Equal(t, uint64(1), counters.WakeRetryEnds[RetryClosed])
	assert.Zero(t, counters.WakeRetryEnds[RetryReparked])
	assertDiagnosticsWaitReconciled(t, counters.DiagnosticsSideSnapshot)
	assertIdleQueue(t, queue)
}

func diagnosticsAdversarialConfigs() []DiagnosticsConfig {
	return []DiagnosticsConfig{
		{Mode: DiagnosticsDisabled},
		{Mode: DiagnosticsCounters, Shards: 1},
		{Mode: DiagnosticsCounters, Shards: 4},
		{Mode: DiagnosticsTiming, Shards: 1, TimingSampleEvery: 1},
		{Mode: DiagnosticsTiming, Shards: 4, TimingSampleEvery: 1},
	}
}

// Call only after every transfer, wake, and waiter cleanup has finished.
func assertDiagnosticsConserved(
	t *testing.T,
	snapshot DiagnosticsSnapshot,
	pushed, received uint64,
) {
	t.Helper()
	if snapshot.Config.Mode == DiagnosticsDisabled {
		assert.Zero(t, snapshot.Producer)
		assert.Zero(t, snapshot.Consumer)
		return
	}
	for _, side := range []struct {
		counters DiagnosticsSideSnapshot
		moved    uint64
	}{
		{snapshot.Producer.DiagnosticsSideSnapshot, pushed},
		{snapshot.Consumer.DiagnosticsSideSnapshot, received},
	} {
		counters := side.counters
		var attempts, requested, moved uint64
		for phase := range AttemptPhaseCount {
			classified := counters.ZeroProgress[phase] + counters.PartialProgress[phase] +
				counters.FullProgress[phase]
			assert.Equal(t, counters.Attempts[phase], classified)
			attempts += counters.Attempts[phase]
			requested += counters.RequestedItems[phase]
			moved += counters.MovedItems[phase]
		}
		assert.Equal(t, attempts, counters.RequestedSize.Count)
		assert.Equal(t, attempts, counters.MovedSize.Count)
		assert.Equal(t, requested, counters.RequestedSize.Sum)
		assert.Equal(t, moved, counters.MovedSize.Sum)
		assert.Equal(t, side.moved, moved)
		assertDiagnosticsWaitReconciled(t, counters)
		assert.Equal(t, counters.WakeRecipients, counters.TokensSent)
		assert.Equal(
			t,
			counters.TokensSent,
			counters.TokensReceivedForRetry+counters.TokensDrainedByCleanup,
		)
		assert.Equal(
			t,
			counters.WakeTransfers[WakeGroup],
			counters.GroupActualSize.Count-counters.GroupActualSize.Buckets[0],
		)
		assertTimingReconciled(t, counters.Timing)
		if snapshot.Config.Mode == DiagnosticsTiming && snapshot.Config.TimingSampleEvery == 1 {
			assert.Equal(t, counters.WaitStarts, counters.Timing.WaitStarts)
			assert.Equal(t, counters.ParkSelectStarts, counters.Timing.ParkSelectStarts)
		}
	}
}

func TestDiagnosticsExtraAttemptCleanup(t *testing.T) {
	for _, config := range diagnosticsAdversarialConfigs() {
		for _, producer := range []bool{false, true} {
			for _, selected := range []bool{false, true} {
				name := fmt.Sprintf("%+v/producer=%t/selected=%t", config, producer, selected)
				t.Run(name, func(t *testing.T) {
					fake := newScriptedEngine[int](8)
					queue := newTestQueue(
						t,
						fake,
						WithDiagnostics(config),
						WithProducerWait(WaitStrategy{}),
						WithConsumerWait(WaitStrategy{}),
					)
					kind, side := callDequeue, &queue.consumers
					if producer {
						kind, side = callEnqueue, &queue.producers
						fake.pushRaw(sequence(1, 8)...)
					}
					var wakeRecording *diagnosticsRecording
					if queue.diagnostics != nil {
						recording := queue.diagnostics.chooseRecording()
						wakeRecording = &recording
					}
					fake.setHooks(func(call engineCall) {
						if call.kind != kind || call.count != 2 {
							return
						}
						if producer {
							assert.Equal(t, []int{1}, fake.takeRaw(1))
						} else {
							fake.pushRaw(9)
						}
						if selected {
							counters := wakeRecording.consumerCounters()
							if producer {
								counters = wakeRecording.producerCounters()
							}
							require.True(t, side.wakeOne(counters))
						}
					}, nil)
					var pushed, received uint64
					if producer {
						require.NoError(t, queue.Push(context.Background(), 9))
						assert.Equal(t, sequence(2, 8), fake.takeRaw(8))
						pushed = 1
					} else {
						dst := make([]int, 1)
						moved, err := queue.Receive(context.Background(), dst)
						require.NoError(t, err)
						require.Equal(t, 1, moved)
						assert.Equal(t, []int{9}, dst)
						received = 1
					}
					snapshot := queue.Diagnostics()
					if config.Mode != DiagnosticsDisabled {
						counters := timingSideSnapshot(queue, producer)
						assert.Equal(t, uint64(1), counters.FullProgress[PhaseExtra])
						assert.Zero(t, counters.ParkSelectStarts)
						expectedDrains := uint64(0)
						if selected {
							expectedDrains = 1
						}
						assert.Equal(t, expectedDrains, counters.TokensDrainedByCleanup)
					}
					assertDiagnosticsConserved(t, snapshot, pushed, received)
					assertIdleQueue(t, queue)
				})
			}
		}
	}
}

func TestDiagnosticsCancellationRacingSelection(t *testing.T) {
	for _, config := range diagnosticsAdversarialConfigs() {
		for _, producer := range []bool{false, true} {
			for _, closed := range []bool{false, true} {
				name := fmt.Sprintf("%+v/producer=%t/closed=%t", config, producer, closed)
				t.Run(name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						fake := newScriptedEngine[int](8)
						queue := newTestQueue(
							t,
							fake,
							WithDiagnostics(config),
							WithProducerWait(WaitStrategy{}),
							WithConsumerWait(WaitStrategy{}),
						)
						side := &queue.consumers
						if producer {
							side = &queue.producers
							fake.pushRaw(sequence(1, 8)...)
						}
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						kind := callDequeue
						if producer {
							kind = callEnqueue
						}
						// Hold the extra attempt so the token and cancellation are both ready
						// when the caller enters select. Either branch must settle the token.
						fake.setHooks(nil, func(call engineCall) {
							if call.kind == kind && call.count == 2 {
								<-ctx.Done()
							}
						})
						results := make(chan error, 1)
						go func() {
							if producer {
								results <- queue.Push(ctx, 9)
								return
							}
							_, err := queue.Receive(ctx, make([]int, 1))
							results <- err
						}()
						synctest.Wait()
						requireParked(t, side, 1)
						var wakeRecording *diagnosticsRecording
						if queue.diagnostics != nil {
							recording := queue.diagnostics.chooseRecording()
							wakeRecording = &recording
						}
						counters := wakeRecording.consumerCounters()
						if producer {
							counters = wakeRecording.producerCounters()
						}
						require.True(t, side.wakeOne(counters))
						if closed {
							queue.Close()
						}
						cancel()
						expectedErr := context.Canceled
						if closed {
							expectedErr = ErrClosed
						}
						require.ErrorIs(t, <-results, expectedErr)
						synctest.Wait()
						assertDiagnosticsConserved(t, queue.Diagnostics(), 0, 0)
						assertIdleQueue(t, queue)
					})
				})
			}
		}
	}
}

func TestDiagnosticsWaiterReuse(t *testing.T) {
	for _, config := range diagnosticsAdversarialConfigs()[1:] {
		t.Run(fmt.Sprintf("%+v", config), func(t *testing.T) {
			fake := newScriptedEngine[int](8)
			queue := newTestQueue(
				t,
				fake,
				WithDiagnostics(config),
				WithConsumerWait(WaitStrategy{}),
			)
			for cycle := range 2 {
				caller := queue.consumers.getWaiter()
				for reuse := range 2 {
					episode := cycle*2 + reuse
					assertUnlisted(t, caller)
					assertNoToken(t, caller)
					recording := queue.diagnostics.chooseRecording()
					waker := queue.diagnostics.chooseRecording()
					fake.setHooks(func(call engineCall) {
						if call.kind != callDequeue {
							return
						}
						fake.pushRaw(episode)
						if episode%2 == 0 {
							require.True(t, queue.consumers.wakeOne(waker.consumerCounters()))
						}
					}, nil)
					dst := make([]int, 1)
					moved, err := queue.receiveWait(context.Background(), &recording, dst, caller, nil)
					require.NoError(t, err)
					require.Equal(t, 1, moved)
					assert.Equal(t, []int{episode}, dst)
					assertUnlisted(t, caller)
					assertNoToken(t, caller)
				}
				queue.consumers.putWaiter(caller)
			}
			snapshot := queue.Diagnostics()
			assert.Equal(t, uint64(4), snapshot.Consumer.WaitStarts)
			assert.Equal(t, uint64(2), snapshot.Consumer.TokensDrainedByCleanup)
			assertDiagnosticsConserved(t, snapshot, 0, 4)
			assertIdleQueue(t, queue)
		})
	}
}
