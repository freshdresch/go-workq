package workq

import (
	"context"
	"fmt"
	"testing"

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

func TestDiagnosticsWakeConsumerTokenOwnership(t *testing.T) {
	for _, extraSuccess := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "extra success"}[extraSuccess], func(t *testing.T) {
			queue, fake, origin, recipient := newDiagnosticsWakeQueue(t)
			require.False(t, queue.consumers.register(recipient.consumerCounters()))
			fake.setHooks(func(call engineCall) {
				if call.kind != callDequeue {
					return
				}
				require.True(t, queue.consumers.wakeOne(origin.consumerCounters()))
				if extraSuccess {
					fake.pushRaw(1)
				}
			}, nil)
			received, finished, err := queue.parkForItems(
				context.Background(),
				recipient,
				make([]int, 1),
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, extraSuccess, finished)
			assert.Equal(t, uint64(1), origin.consumerCounters().tokensSent.Load())
			assert.Zero(t, recipient.consumerCounters().tokensSent.Load())
			if extraSuccess {
				assert.Equal(t, 1, received)
				assert.Equal(t, uint64(1), recipient.consumerCounters().tokensDrainedByCleanup.Load())
				assert.Zero(t, recipient.consumerCounters().tokensReceivedForRetry.Load())
			} else {
				assert.Zero(t, received)
				assert.Equal(t, uint64(1), recipient.consumerCounters().tokensReceivedForRetry.Load())
				assert.Zero(t, recipient.consumerCounters().tokensDrainedByCleanup.Load())
				queue.leaveConsumerPolling(recipient)
			}
			assertIdleQueue(t, queue)
		})
	}
}

func TestDiagnosticsWakeProducerTokenOwnership(t *testing.T) {
	for _, extraSuccess := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "extra success"}[extraSuccess], func(t *testing.T) {
			queue, _, origin, recipient := newDiagnosticsWakeQueue(t)
			require.False(t, queue.producers.register(recipient.producerCounters()))
			attempt := func(AttemptPhase) int {
				require.True(t, queue.producers.wakeOne(origin.producerCounters()))
				if extraSuccess {
					return 1
				}
				return 0
			}
			pushed, finished, err := queue.parkForRoom(
				context.Background(),
				recipient,
				attempt,
				0,
				false,
			)
			require.NoError(t, err)
			assert.Equal(t, extraSuccess, finished)
			assert.Equal(t, uint64(1), origin.producerCounters().tokensSent.Load())
			assert.Zero(t, recipient.producerCounters().tokensSent.Load())
			if extraSuccess {
				assert.Equal(t, 1, pushed)
				assert.Equal(t, uint64(1), recipient.producerCounters().tokensDrainedByCleanup.Load())
				assert.Zero(t, recipient.producerCounters().tokensReceivedForRetry.Load())
			} else {
				assert.Zero(t, pushed)
				assert.Equal(t, uint64(1), recipient.producerCounters().tokensReceivedForRetry.Load())
				assert.Zero(t, recipient.producerCounters().tokensDrainedByCleanup.Load())
				queue.producers.leavePolling(nil, func() bool { return false })
			}
			assertIdleQueue(t, queue)
		})
	}
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
