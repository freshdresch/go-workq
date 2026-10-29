package workq

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDiagnosticsWakeQueue(
	t *testing.T,
) (*Queue[int], *scriptedEngine[int], *diagnosticsRecording, *diagnosticsRecording) {
	t.Helper()
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(t, fake, WithMaxReceiveBatch(1), WithDiagnostics(DiagnosticsConfig{
		Mode: DiagnosticsCounters, Shards: 2,
	}))
	origin := &diagnosticsRecording{counters: &queue.diagnostics.counters[0]}
	recipient := &diagnosticsRecording{counters: &queue.diagnostics.counters[1]}
	return queue, fake, origin, recipient
}

func TestDiagnosticsWakeOneCallsAndTransfers(t *testing.T) {
	queue, _, recording, _ := newDiagnosticsWakeQueue(t)
	side := &queue.consumers
	counters := recording.consumerCounters()
	require.False(t, side.wakeOne(counters))
	caller := parkConsumer(t, side)
	require.True(t, side.wakeOne(counters))
	require.False(t, side.wakeOne(counters))
	side.mu.Lock()
	woke := side.wakeOneLocked(counters)
	side.mu.Unlock()
	require.False(t, woke)
	side.leaveList(counters, caller, false, func() bool { return false })
	snapshot := queue.Diagnostics().Consumer
	assert.Equal(t, uint64(4), snapshot.WakeCalls[WakeOne])
	assert.Equal(t, uint64(1), snapshot.WakeTransfers[WakeOne])
	assert.Equal(t, uint64(1), snapshot.WakeRecipients)
	assert.Equal(t, uint64(1), snapshot.TokensSent)
	assert.Equal(t, uint64(1), snapshot.TokensDrainedByCleanup)
	assert.Zero(t, snapshot.TokensReceivedForRetry)
	assert.Zero(t, snapshot.Registrations)
	assert.Zero(t, snapshot.GroupActualSize.Count)
	assertIdleSide(t, side)
}

func TestDiagnosticsWakeGroupNoTransfer(t *testing.T) {
	for _, scenario := range []string{"empty", "at target", "CAS retry"} {
		t.Run(scenario, func(t *testing.T) {
			queue, _, recording, _ := newDiagnosticsWakeQueue(t)
			side := &queue.consumers
			var caller *waiter
			if scenario != "empty" {
				caller = parkConsumer(t, side)
			}
			if scenario == "at target" {
				require.False(t, side.register(nil))
			}
			if scenario == "CAS retry" {
				side.beforeGroupCAS = func() { require.False(t, side.register(nil)) }
			}
			require.False(t, side.wakeGroup(recording.consumerGroupCounters(), 1))
			snapshot := queue.Diagnostics().Consumer
			assert.Equal(t, uint64(1), snapshot.WakeCalls[WakeGroup])
			assert.Zero(t, snapshot.WakeTransfers[WakeGroup])
			assert.Zero(t, snapshot.WakeRecipients)
			assert.Zero(t, snapshot.TokensSent)
			assert.Equal(t, uint64(1), snapshot.GroupTarget.Count)
			assert.Equal(t, uint64(1), snapshot.GroupTarget.Sum)
			assert.Equal(t, uint64(1), snapshot.GroupActualSize.Count)
			assert.Equal(t, uint64(1), snapshot.GroupActualSize.Buckets[0])
			if caller != nil {
				side.leaveList(nil, caller, false, func() bool { return false })
				side.leavePolling(nil, func() bool { return false })
			}
			assertIdleSide(t, side)
		})
	}
}

func TestDiagnosticsWakeGroupRetryAndEarlyExit(t *testing.T) {
	for _, config := range diagnosticsAdversarialConfigs()[1:] {
		t.Run(fmt.Sprintf("%+v", config), func(t *testing.T) {
			testDiagnosticsWakeGroupRetryAndEarlyExit(t, config)
		})
	}
}

func testDiagnosticsWakeGroupRetryAndEarlyExit(t *testing.T, config DiagnosticsConfig) {
	queue := newTestQueue(t, newScriptedEngine[int](8), WithDiagnostics(config))
	recording := &diagnosticsRecording{counters: &queue.diagnostics.counters[0]}
	recipient := &diagnosticsRecording{
		counters: &queue.diagnostics.counters[config.Shards-1],
	}
	side := &queue.consumers
	callers := make([]*waiter, 6)
	for i := range callers {
		callers[i] = parkConsumer(t, side)
	}
	attempts := 0
	side.beforeGroupCAS = func() {
		attempts++
		if attempts == 1 {
			require.False(t, side.register(recording.consumerCounters()))
		}
	}
	sends := 0
	side.afterGroupToken = func() {
		requireToken(t, callers[sends])
		recipient.startRetry(recipient.consumerCounters())
		recipient.finishRetry(recipient.consumerCounters(), RetryCanceled)
		sends++
		side.leavePolling(nil, func() bool { return false })
	}
	require.True(t, side.wakeGroup(recording.consumerGroupCounters(), 4))
	assert.Equal(t, 2, attempts)
	assert.Equal(t, 3, sends)
	snapshot := queue.Diagnostics().Consumer
	assert.Equal(t, uint64(1), snapshot.WakeCalls[WakeGroup])
	assert.Equal(t, uint64(1), snapshot.GroupTarget.Count)
	assert.Equal(t, uint64(4), snapshot.GroupTarget.Sum)
	assert.Equal(t, uint64(1), snapshot.GroupActualSize.Count)
	assert.Equal(t, uint64(3), snapshot.GroupActualSize.Sum)
	assert.Equal(t, uint64(1), snapshot.Registrations)
	for _, caller := range callers[3:] {
		side.leaveList(nil, caller, false, func() bool { return false })
	}
	side.leavePolling(nil, func() bool { return false })
	assertIdleSide(t, side)
	assertDiagnosticsConserved(t, queue.Diagnostics(), 0, 0)
}

func TestDiagnosticsWakeRegistrationAndEnlistment(t *testing.T) {
	queue, _, recording, _ := newDiagnosticsWakeQueue(t)
	side := &queue.producers
	counters := recording.producerCounters()
	caller := newWaiter()
	require.False(t, side.register(counters))
	require.False(t, side.enlist(counters, caller, atNewest))
	require.True(t, side.wakeOne(counters))
	requireToken(t, caller)
	require.False(t, side.enlist(counters, caller, atOldest))
	require.True(t, side.wakeOne(counters))
	requireToken(t, caller)
	side.mu.Lock()
	side.setClosedLocked()
	side.mu.Unlock()
	require.True(t, side.register(counters))
	require.True(t, side.enlist(counters, caller, atOldest))
	side.leavePolling(counters, func() bool { return false })
	snapshot := queue.Diagnostics().Producer
	assert.Equal(t, uint64(1), snapshot.Registrations)
	assert.Equal(t, uint64(2), snapshot.ParkEnlistments)
	assert.Equal(t, uint64(1), snapshot.ParkEnlistClosed)
	assert.Equal(t, uint64(1), snapshot.ProducerOldestReenlistments)
	assertIdleSide(t, side)
}

func TestDiagnosticsWakePublicationFloorAndSources(t *testing.T) {
	setGOMAXPROCS(t, 4)
	queue, fake, recording, _ := newDiagnosticsWakeQueue(t)
	caller := parkConsumer(t, &queue.consumers)
	fake.setHooks(func(engineCall) {
		assertSideUnlocked(t, &queue.consumers)
		assertSideUnlocked(t, &queue.producers)
	}, nil)
	require.False(t, queue.wakeConsumersAfterPublication(recording))
	snapshot := queue.Diagnostics().Consumer
	assert.Equal(t, uint64(1), snapshot.AvailableItems[PublicationProbe].ZeroCount)
	assert.Equal(t, uint64(1), snapshot.GroupTarget.Sum)
	assert.Equal(t, uint64(1), snapshot.GroupActualSize.Sum)
	queue.leaveConsumerList(recording, caller, false)
	fake.pushRaw(1, 2, 3)
	assert.Equal(t, uint32(3), queue.consumerTarget(recording, CleanupProbe))
	assert.True(t, queue.peekItems(recording))
	assert.True(t, queue.peekRoom(recording))
	snapshot = queue.Diagnostics().Consumer
	assert.Equal(t, uint64(2), snapshot.AvailableItems[CleanupProbe].Count)
	assert.Equal(t, uint64(6), snapshot.AvailableItems[CleanupProbe].Sum)
	assert.Equal(t, uint64(1), snapshot.AvailableItems[PublicationProbe].Count)
	producer := queue.Diagnostics().Producer
	assert.Equal(t, uint64(1), producer.ReusableSlots[CleanupProbe].Count)
	assert.Equal(t, uint64(5), producer.ReusableSlots[CleanupProbe].Sum)
	assert.Zero(t, producer.ReusableSlots[PublicationProbe].Count)
	assert.Zero(t, queue.diagnostics.counters[1].consumer.capacity[CleanupProbe].count.Load())
	assertIdleQueue(t, queue)
}

func TestDiagnosticsWakeEndpointTokenOwnership(t *testing.T) {
	for _, producer := range []bool{false, true} {
		for _, cleanup := range []bool{false, true} {
			t.Run(fmt.Sprintf("producer=%t/cleanup=%t", producer, cleanup), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					queue, fake := newTimingDiagnosticsQueue(t, 1)
					origin := queue.NewDiagnosticsEndpoint()
					recipient := queue.NewDiagnosticsEndpoint()
					kind, side := callDequeue, &queue.consumers
					if producer {
						kind, side = callEnqueue, &queue.producers
						fake.pushRaw(sequence(1, 8)...)
					}
					transfer := func() {
						if producer {
							count, err := origin.TryReceive(make([]int, 1))
							require.NoError(t, err)
							require.Equal(t, 1, count)
							return
						}
						require.NoError(t, origin.TryPush(9))
					}
					fake.setHooks(func(call engineCall) {
						if call.kind != kind {
							return
						}
						if call.count == 2 {
							transfer()
						}
						if !cleanup && (call.count >= 2 && call.count <= 4) {
							fake.setForceEmpty()
						}
					}, nil)
					result := make(chan error, 1)
					go func() {
						if producer {
							result <- recipient.Push(context.Background(), 10)
							return
						}
						_, err := recipient.Receive(context.Background(), make([]int, 1))
						result <- err
					}()
					transfers := uint64(1)
					if !cleanup {
						synctest.Wait()
						requireParked(t, side, 1)
						transfer()
						transfers++
					}
					require.NoError(t, <-result)
					originCounters := &queue.diagnostics.counters[origin.shard].consumer.diagnosticsSideCounters
					recipientShard := &queue.diagnostics.counters[recipient.shard]
					recipientCounters := &recipientShard.consumer.diagnosticsSideCounters
					pushed, received := transfers, uint64(1)
					if producer {
						originCounters = &queue.diagnostics.counters[origin.shard].producer.diagnosticsSideCounters
						recipientCounters = &recipientShard.producer.diagnosticsSideCounters
						pushed, received = 1, transfers
					}
					assert.Equal(t, transfers, originCounters.tokensSent.Load())
					assert.Zero(t, recipientCounters.tokensSent.Load())
					assert.Zero(t, originCounters.tokensReceivedForRetry.Load())
					assert.Zero(t, originCounters.tokensDrainedByCleanup.Load())
					if cleanup {
						assert.Equal(t, uint64(1), recipientCounters.tokensDrainedByCleanup.Load())
						assert.Zero(t, recipientCounters.tokensReceivedForRetry.Load())
					} else {
						assert.Equal(t, uint64(2), recipientCounters.tokensReceivedForRetry.Load())
						assert.Equal(t, uint64(1), recipientCounters.wakeRetryEnds[RetryReparked].Load())
						assert.Equal(t, uint64(1), recipientCounters.wakeRetryEnds[RetryProgress].Load())
						if producer {
							assert.Equal(t, uint64(1), recipientCounters.producerOldestReenlistments.Load())
						}
					}
					snapshot := queue.Diagnostics()
					assertDiagnosticsConserved(t, snapshot, pushed, received)
					assert.Equal(t, snapshot.Producer.CapacityEligibleAttempts,
						snapshot.Producer.CapacityCompletedProbes)
					assert.Equal(t, snapshot.Consumer.CapacityEligibleAttempts,
						snapshot.Consumer.CapacityCompletedProbes)
					assertIdleQueue(t, queue)
				})
			})
		}
	}
}

func TestDiagnosticsWakeEndpointConsumerGroup(t *testing.T) {
	setGOMAXPROCS(t, 4)
	synctest.Test(t, func(t *testing.T) {
		queue := newTestQueue(t, newScriptedEngine[int](8),
			WithConsumerWait(WaitStrategy{}), WithMaxReceiveBatch(1),
			WithDiagnostics(DiagnosticsConfig{Mode: DiagnosticsCounters, Shards: 4}))
		origin := queue.NewDiagnosticsEndpoint()
		results := make(chan error, 3)
		for range 3 {
			recipient := queue.NewDiagnosticsEndpoint()
			go func() {
				_, err := recipient.Receive(context.Background(), make([]int, 1))
				results <- err
			}()
		}
		synctest.Wait()
		requireParked(t, &queue.consumers, 3)
		count, err := origin.TryPushBatch([]int{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, 3, count)
		for range 3 {
			require.NoError(t, <-results)
		}
		counters := &queue.diagnostics.counters[origin.shard].consumer
		assert.Equal(t, uint64(3), counters.tokensSent.Load())
		assert.Equal(t, uint64(1), counters.wakeTransfers[WakeGroup].Load())
		assert.Zero(t, counters.tokensReceivedForRetry.Load())
		for index := 1; index < 4; index++ {
			recipient := &queue.diagnostics.counters[index].consumer
			assert.Equal(t, uint64(1), recipient.tokensReceivedForRetry.Load())
			assert.Equal(t, uint64(1), recipient.wakeRetryEnds[RetryProgress].Load())
			assert.Zero(t, recipient.tokensSent.Load())
		}
		assertDiagnosticsConserved(t, queue.Diagnostics(), 3, 3)
		assertIdleQueue(t, queue)
	})
}

func TestDiagnosticsWakeCloseDoesNotSendTokens(t *testing.T) {
	queue, _, recording, _ := newDiagnosticsWakeQueue(t)
	caller := parkConsumer(t, &queue.consumers)
	queue.Close()
	queue.leaveConsumerList(recording, caller, false)
	snapshot := queue.Diagnostics().Consumer
	assert.Zero(t, snapshot.WakeRecipients)
	assert.Zero(t, snapshot.TokensSent)
	assert.Zero(t, snapshot.TokensDrainedByCleanup)
	assert.Zero(t, snapshot.TokensReceivedForRetry)
	assertIdleQueue(t, queue)
}

func TestDiagnosticsWakeCleanupProbeAfterRelease(t *testing.T) {
	for _, procs := range []int{1, 4} {
		t.Run(map[int]string{1: "one", 4: "group"}[procs], func(t *testing.T) {
			setGOMAXPROCS(t, procs)
			queue, fake, recording, _ := newDiagnosticsWakeQueue(t)
			caller := parkConsumer(t, &queue.consumers)
			require.False(t, queue.consumers.register(nil))
			fake.pushRaw(1)
			fake.setHooks(func(call engineCall) {
				require.Equal(t, callPeek, call.kind)
				assertSideUnlocked(t, &queue.consumers)
				assertSideUnlocked(t, &queue.producers)
				assertCounts(t, &queue.consumers, 0, 1)
			}, nil)
			queue.leaveConsumerPolling(recording)
			snapshot := queue.Diagnostics().Consumer
			assert.Equal(t, uint64(1), snapshot.AvailableItems[CleanupProbe].Count)
			assert.Equal(t, uint64(1), snapshot.AvailableItems[CleanupProbe].Sum)
			assert.Zero(t, snapshot.AvailableItems[PublicationProbe].Count)
			assert.Equal(t, uint64(1), snapshot.WakeRecipients)
			assert.Equal(t, uint64(1), snapshot.TokensSent)
			if procs == 1 {
				assert.Equal(t, uint64(1), snapshot.WakeCalls[WakeOne])
				assert.Zero(t, snapshot.WakeCalls[WakeGroup])
			} else {
				assert.Zero(t, snapshot.WakeCalls[WakeOne])
				assert.Equal(t, uint64(1), snapshot.WakeCalls[WakeGroup])
			}
			fake.setHooks(nil, nil)
			queue.leaveConsumerList(recording, caller, false)
			assertIdleQueue(t, queue)
		})
	}
}

func TestDiagnosticsWakeProducerCleanupProbeAfterRelease(t *testing.T) {
	queue, fake, recording, _ := newDiagnosticsWakeQueue(t)
	caller := parkConsumer(t, &queue.producers)
	require.False(t, queue.producers.register(nil))
	queue.Close()
	fake.setHooks(func(call engineCall) {
		require.Equal(t, callPeek, call.kind)
		assertSideUnlocked(t, &queue.consumers)
		assertSideUnlocked(t, &queue.producers)
		assertCounts(t, &queue.producers, 0, 1)
	}, nil)
	require.ErrorIs(t, queue.leavePollingClosed(recording, 0), ErrClosed)
	snapshot := queue.Diagnostics().Producer
	assert.Equal(t, uint64(1), snapshot.ReusableSlots[CleanupProbe].Count)
	assert.Equal(t, uint64(8), snapshot.ReusableSlots[CleanupProbe].Sum)
	assert.Equal(t, uint64(1), snapshot.ReusableSlots[CleanupProbe].AtCapacity)
	assert.Equal(t, uint64(1), snapshot.WakeRecipients)
	assert.Equal(t, uint64(1), snapshot.TokensSent)
	fake.setHooks(nil, nil)
	queue.producers.leaveList(
		recording.producerCounters(),
		caller,
		false,
		func() bool { return queue.peekRoom(recording) },
	)
	assertIdleQueue(t, queue)
}
