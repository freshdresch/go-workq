package workq

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsSampleSelection(t *testing.T) {
	assert.False(t, diagnosticsSampleSelected(0, 0))
	for _, every := range []uint32{1, 2, 3, 7, 64, 257} {
		var selected uint32
		for draw := uint32(0); draw < every; draw++ {
			if diagnosticsSampleSelected(every, draw) {
				selected++
			}
		}
		assert.Equal(t, uint32(1), selected, "rate %d", every)
	}
	assert.True(t, diagnosticsSampleSelected(^uint32(0), 0))
	assert.False(t, diagnosticsSampleSelected(^uint32(0), ^uint32(0)-1))
}

func newTimingDiagnosticsQueue(
	t *testing.T,
	capacityEvery uint32,
) (*Queue[int], *scriptedEngine[int]) {
	t.Helper()
	fake := newScriptedEngine[int](8)
	queue := newTestQueue(
		t,
		fake,
		WithProducerWait(WaitStrategy{}),
		WithConsumerWait(WaitStrategy{}),
		WithDiagnostics(DiagnosticsConfig{
			Mode: DiagnosticsTiming, Shards: 2,
			TimingSampleEvery: 1, CapacitySampleEvery: capacityEvery,
		}),
	)
	return queue, fake
}

func timingSideSnapshot(queue *Queue[int], producer bool) DiagnosticsSideSnapshot {
	if producer {
		return queue.Diagnostics().Producer.DiagnosticsSideSnapshot
	}
	return queue.Diagnostics().Consumer.DiagnosticsSideSnapshot
}

func assertTimingReconciled(t *testing.T, timing DiagnosticsTimingSnapshot) {
	t.Helper()
	var waits, parks uint64
	for _, histogram := range timing.WaitDuration {
		waits += histogram.Count
		var buckets uint64
		for _, count := range histogram.Buckets {
			buckets += count
		}
		assert.Equal(t, histogram.Count, buckets)
	}
	for _, histogram := range timing.ParkSelectDuration {
		parks += histogram.Count
		var buckets uint64
		for _, count := range histogram.Buckets {
			buckets += count
		}
		assert.Equal(t, histogram.Count, buckets)
	}
	assert.Equal(t, timing.WaitStarts, timing.WaitSamples)
	assert.Equal(t, timing.WaitSamples, waits)
	assert.Equal(t, timing.ParkSelectStarts, timing.ParkSelectSamples)
	assert.Equal(t, timing.ParkSelectSamples, parks)
}

func TestDiagnosticsTimingSelectedClusters(t *testing.T) {
	for _, producer := range []bool{false, true} {
		for _, selected := range []bool{false, true} {
			for _, capacityEvery := range []uint32{0, 1} {
				for _, outcome := range []WaitOutcome{
					WaitProgress, WaitCanceled, WaitDeadline, WaitClosed,
				} {
					name := fmt.Sprintf("%t/%t/%d/%d", producer, selected, capacityEvery, outcome)
					t.Run(name, func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							testTimingCluster(t, producer, selected, capacityEvery, outcome)
						})
					})
				}
			}
		}
	}
}

func testTimingCluster(
	t *testing.T,
	producer, selected bool,
	capacityEvery uint32,
	outcome WaitOutcome,
) {
	t.Helper()
	queue, fake := newTimingDiagnosticsQueue(t, capacityEvery)
	side := &queue.consumers
	if producer {
		side = &queue.producers
		fake.pushRaw(sequence(1, 8)...)
	}
	recording := queue.diagnostics.chooseRecording()
	// Override only invocation-local state before starting the worker. No global random hook
	// or production callback is needed to exercise the unselected wait path deterministically.
	if !selected {
		recording.timingSampleEvery = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	if outcome == WaitDeadline {
		ctx, cancel = context.WithTimeout(context.Background(), time.Hour)
	}
	defer cancel()
	results := make(chan error, 1)
	go func() {
		if producer {
			attempt := func(phase AttemptPhase) int {
				return queue.enqueueAttempt(&recording, phase, 9)
			}
			_, err := queue.pushWait(ctx, &recording, attempt, 0)
			results <- err
			return
		}
		_, err := queue.receiveWait(ctx, &recording, make([]int, 1), nil, nil)
		results <- err
	}()
	var samples uint64
	if selected {
		samples = 1
	}
	for completedParks := uint64(0); completedParks < 4; completedParks++ {
		synctest.Wait()
		requireParked(t, side, 1)
		timing := timingSideSnapshot(queue, producer).Timing
		assert.Equal(t, samples, timing.WaitStarts)
		assert.Zero(t, timing.WaitSamples)
		assert.Zero(t, timing.WaitDuration)
		assert.Equal(t, samples*(completedParks+1), timing.ParkSelectStarts)
		assert.Equal(t, samples*completedParks, timing.ParkSelectSamples)
		assert.Equal(t, samples*completedParks, timing.ParkSelectDuration[ParkToken].Count)
		if completedParks < 3 {
			side.wakeOne(nil)
		}
	}
	selectedCase := ParkContext
	switch outcome {
	case WaitProgress:
		selectedCase = ParkToken
		if producer {
			fake.takeRaw(1)
		} else {
			fake.pushRaw(1)
		}
		side.wakeOne(nil)
	case WaitClosed:
		selectedCase = ParkDone
		queue.Close()
	case WaitDeadline:
		<-ctx.Done()
	default:
		cancel()
	}
	err := <-results
	switch outcome {
	case WaitProgress:
		require.NoError(t, err)
	case WaitClosed:
		require.ErrorIs(t, err, ErrClosed)
	case WaitDeadline:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	default:
		require.ErrorIs(t, err, context.Canceled)
	}
	counters := timingSideSnapshot(queue, producer)
	assert.Equal(t, uint64(1), counters.WaitEnds[outcome])
	assert.Equal(t, samples, counters.Timing.WaitDuration[outcome].Count)
	assert.Equal(t, 4*samples, counters.Timing.ParkSelectSamples)
	caseSamples := samples
	if selectedCase == ParkToken {
		caseSamples *= 4
	}
	assert.Equal(t, caseSamples, counters.Timing.ParkSelectDuration[selectedCase].Count)
	assert.Equal(
		t,
		counters.CapacityEligibleAttempts*uint64(capacityEvery),
		counters.CapacitySelectedProbes,
	)
	assert.Equal(t, counters.CapacitySelectedProbes, counters.CapacityCompletedProbes)
	assertTimingReconciled(t, counters.Timing)
	assertDiagnosticsWaitReconciled(t, counters)
	assertIdleQueue(t, queue)
}

func TestDiagnosticsTimingEpisodeIncludesProbeCompletion(t *testing.T) {
	for _, producer := range []bool{false, true} {
		queue, fake := newTimingDiagnosticsQueue(t, 1)
		kind := callDequeue
		if producer {
			kind = callEnqueue
			fake.pushRaw(sequence(1, 8)...)
		}
		completedProbe := false
		fake.setHooks(func(call engineCall) {
			if call.kind == kind && call.count == 2 {
				if producer {
					fake.takeRaw(1)
				} else {
					fake.pushRaw(1)
				}
			}
		}, func(call engineCall) {
			if call.kind != callPeek || fake.callCount(kind) != 2 {
				return
			}
			counters := timingSideSnapshot(queue, producer)
			assert.Equal(t, uint64(1), counters.Timing.WaitStarts)
			assert.Zero(t, counters.Timing.WaitSamples)
			assert.Zero(t, counters.Timing.WaitDuration)
			assertIdleQueue(t, queue)
			completedProbe = true
		})
		if producer {
			require.NoError(t, queue.Push(context.Background(), 9))
		} else {
			moved, err := queue.Receive(context.Background(), make([]int, 1))
			require.NoError(t, err)
			require.Equal(t, 1, moved)
		}
		require.True(t, completedProbe)
		timing := timingSideSnapshot(queue, producer).Timing
		assert.Equal(t, uint64(1), timing.WaitDuration[WaitProgress].Count)
		assert.Zero(t, timing.ParkSelectStarts)
		assertTimingReconciled(t, timing)
	}
}

func TestDiagnosticsTimingReselectsEachEpisode(t *testing.T) {
	queue, _ := newTimingDiagnosticsQueue(t, 0)
	recording := queue.diagnostics.chooseRecording()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, every := range []uint32{1, 0, 1} {
		recording.timingSampleEvery = every
		_, err := queue.receiveWait(ctx, &recording, make([]int, 1), nil, nil)
		require.ErrorIs(t, err, context.Canceled)
	}
	counters := queue.Diagnostics().Consumer
	assert.Equal(t, uint64(3), counters.WaitStarts)
	assert.Equal(t, uint64(3), counters.WaitEnds[WaitCanceled])
	assert.Equal(t, uint64(2), counters.Timing.WaitDuration[WaitCanceled].Count)
	assert.Equal(t, uint64(2), counters.Timing.ParkSelectStarts)
	assert.Equal(t, uint64(2), counters.Timing.ParkSelectDuration[ParkContext].Count)
	assertTimingReconciled(t, counters.Timing)
	assertIdleQueue(t, queue)
}

func TestDiagnosticsTimingSelectFinishesBeforeDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, fake := newTimingDiagnosticsQueue(t, 0)
		observedDrain := false
		fake.setHooks(func(call engineCall) {
			if call.kind != callDequeue || call.count != 3 {
				return
			}
			timing := queue.Diagnostics().Consumer.Timing
			assert.Equal(t, uint64(1), timing.ParkSelectDuration[ParkDone].Count)
			assert.Equal(t, uint64(1), timing.ParkSelectSamples)
			assert.Zero(t, timing.WaitSamples)
			assert.Zero(t, timing.WaitDuration)
			observedDrain = true
		}, nil)
		results := make(chan error, 1)
		go func() {
			_, err := queue.Receive(context.Background(), make([]int, 1))
			results <- err
		}()
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)
		queue.Close()
		require.ErrorIs(t, <-results, ErrClosed)
		require.True(t, observedDrain)
		assertTimingReconciled(t, queue.Diagnostics().Consumer.Timing)
		assertIdleQueue(t, queue)
	})
}
