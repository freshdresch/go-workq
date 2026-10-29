package workq

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsSnapshotOverlappingSeries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, _ := newProducerDiagnosticsQueue(t, 0)
		counters := &queue.diagnostics.counters[0].producer.diagnosticsSideCounters
		counters.waitStarts.Store(10)
		loaded := make(chan struct{})
		resume := make(chan struct{})
		firstResults := make(chan DiagnosticsSnapshot, 1)
		go func() {
			firstResults <- queue.collectDiagnostics(&diagnosticsReductionControl{
				counters: counters,
				loaded:   loaded,
				resume:   resume,
			})
		}()
		<-loaded
		counters.waitStarts.Add(1)
		second := queue.Diagnostics()
		assert.Equal(t, uint64(11), second.Producer.WaitStarts)
		select {
		case <-firstResults:
			t.Fatal("first collector returned before its barrier was released")
		default:
		}
		close(resume)
		first := <-firstResults
		assert.Equal(t, uint64(10), first.Producer.WaitStarts)
		assert.Greater(t, second.Producer.WaitStarts, first.Producer.WaitStarts)

		// Each collector retains its own baseline despite the reversed completion order.
		counters.waitStarts.Add(2)
		firstNext := make(chan DiagnosticsSnapshot, 1)
		secondNext := make(chan DiagnosticsSnapshot, 1)
		go func() { firstNext <- queue.Diagnostics() }()
		go func() { secondNext <- queue.Diagnostics() }()
		firstDelta := (<-firstNext).Producer.WaitStarts - first.Producer.WaitStarts
		secondDelta := (<-secondNext).Producer.WaitStarts - second.Producer.WaitStarts
		assert.Equal(t, uint64(3), firstDelta)
		assert.Equal(t, uint64(2), secondDelta)
	})
}

func TestDiagnosticsSnapshotSequentialDelta(t *testing.T) {
	queue, _ := newProducerDiagnosticsQueue(t, 0)
	require.NoError(t, queue.TryPush(1))
	before := queue.Diagnostics()
	require.NoError(t, queue.TryPush(2))
	require.NoError(t, queue.TryPush(3))
	after := queue.Diagnostics()
	assert.Equal(t, uint64(1), before.Producer.Attempts[PhaseInitial])
	assert.Equal(t, uint64(3), after.Producer.Attempts[PhaseInitial])
	assert.Equal(
		t,
		uint64(2),
		after.Producer.Attempts[PhaseInitial]-before.Producer.Attempts[PhaseInitial],
	)
	assert.Equal(
		t,
		uint64(2),
		after.Producer.MovedItems[PhaseInitial]-before.Producer.MovedItems[PhaseInitial],
	)
	assert.Equal(t, uint64(2), after.Producer.RequestedSize.Count-before.Producer.RequestedSize.Count)
}

func assertDiagnosticsSnapshotDisabled(t *testing.T, snapshot DiagnosticsSnapshot) {
	t.Helper()
	assert.Equal(t, DiagnosticsProducerSnapshot{}, snapshot.Producer)
	assert.Equal(t, DiagnosticsConsumerSnapshot{}, snapshot.Consumer)
	assert.Equal(t, DiagnosticsWaitState{}, snapshot.WaitState)
}

func TestDiagnosticsSnapshotLiveClose(t *testing.T) {
	for _, mode := range []DiagnosticsMode{
		DiagnosticsDisabled, DiagnosticsCounters, DiagnosticsTiming,
	} {
		for _, producer := range []bool{false, true} {
			for _, polling := range []bool{false, true} {
				name := fmt.Sprintf("mode=%d/producer=%t/polling=%t", mode, producer, polling)
				t.Run(name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						testDiagnosticsSnapshotLiveClose(t, mode, producer, polling)
					})
				})
			}
		}
	}
}

func testDiagnosticsSnapshotLiveClose(
	t *testing.T,
	mode DiagnosticsMode,
	producer, polling bool,
) {
	t.Helper()
	fake := newScriptedEngine[int](8)
	strategy := WaitStrategy{}
	if polling {
		strategy.Yields = 1
	}
	config := DiagnosticsConfig{Mode: mode}
	if mode == DiagnosticsTiming {
		config.TimingSampleEvery = 1
	}
	queue := newTestQueue(
		t,
		fake,
		WithProducerWait(strategy),
		WithConsumerWait(strategy),
		WithDiagnostics(config),
	)
	kind := callDequeue
	if producer {
		kind = callEnqueue
		fake.pushRaw(sequence(1, 8)...)
	}
	attemptEntered := make(chan struct{})
	resumeAttempt := make(chan struct{})
	fake.setHooks(func(call engineCall) {
		if call.kind == kind && call.count == 2 {
			close(attemptEntered)
			<-resumeAttempt
		}
	}, nil)
	closePublished := make(chan struct{})
	resumeClose := make(chan struct{})
	queue.hooks.afterCloseDone = func() {
		close(closePublished)
		<-resumeClose
	}
	results := make(chan error, 1)
	go func() {
		if producer {
			results <- queue.Push(context.Background(), 9)
			return
		}
		_, err := queue.Receive(context.Background(), make([]int, 1))
		results <- err
	}()
	<-attemptEntered
	before := queue.Diagnostics()
	liveState := DiagnosticsSideWaitState{Parked: 1}
	if polling {
		liveState = DiagnosticsSideWaitState{Pollers: 1}
	}
	actualState := diagnosticsWaitState(queue.consumers.load())
	if producer {
		actualState = diagnosticsWaitState(queue.producers.load())
	}
	assert.Equal(t, liveState, actualState)
	if mode == DiagnosticsDisabled {
		assertDiagnosticsSnapshotDisabled(t, before)
	} else {
		state := before.WaitState.Consumer
		if producer {
			state = before.WaitState.Producer
		}
		assert.Equal(t, liveState, state)
	}
	closed := make(chan struct{})
	go func() {
		queue.Close()
		close(closed)
	}()
	<-closePublished
	during := queue.Diagnostics()
	if mode == DiagnosticsDisabled {
		assertDiagnosticsSnapshotDisabled(t, during)
	} else {
		assert.True(t, during.WaitState.Consumer.Closed)
		assert.False(t, during.WaitState.Producer.Closed)
		state := during.WaitState.Consumer
		if producer {
			state = during.WaitState.Producer
		}
		assert.Equal(t, liveState.Pollers, state.Pollers)
		assert.Equal(t, liveState.Parked, state.Parked)
	}
	close(resumeClose)
	<-closed
	afterClose := queue.Diagnostics()
	if mode == DiagnosticsDisabled {
		assertDiagnosticsSnapshotDisabled(t, afterClose)
	} else {
		assert.True(t, afterClose.WaitState.Consumer.Closed)
		assert.True(t, afterClose.WaitState.Producer.Closed)
	}
	close(resumeAttempt)
	require.ErrorIs(t, <-results, ErrClosed)
	afterJoin := queue.Diagnostics()
	assertIdleQueue(t, queue)
	assert.Equal(t, before.Config, afterJoin.Config)
	if mode == DiagnosticsDisabled {
		assertDiagnosticsSnapshotDisabled(t, afterJoin)
		return
	}
	assert.Equal(t, DiagnosticsWaitState{
		Producer: DiagnosticsSideWaitState{Closed: true},
		Consumer: DiagnosticsSideWaitState{Closed: true},
	}, afterJoin.WaitState)
	beforeCleanup := afterClose.Consumer.DiagnosticsSideSnapshot
	afterCleanup := afterJoin.Consumer.DiagnosticsSideSnapshot
	if producer {
		beforeCleanup = afterClose.Producer.DiagnosticsSideSnapshot
		afterCleanup = afterJoin.Producer.DiagnosticsSideSnapshot
	}
	assert.Zero(t, beforeCleanup.WaitEnds[WaitClosed])
	assert.Equal(t, uint64(1), afterCleanup.WaitEnds[WaitClosed])
	assert.Equal(t, uint64(1), afterCleanup.WaitStarts)
	if !polling {
		assert.Zero(t, beforeCleanup.ParkSelectStarts)
		assert.Equal(t, uint64(1), afterCleanup.ParkSelectEnds[ParkDone])
	}
	if mode == DiagnosticsTiming {
		assert.Zero(t, beforeCleanup.Timing.WaitSamples)
		assert.Equal(t, uint64(1), afterCleanup.Timing.WaitSamples)
	}
	assertDiagnosticsWaitReconciled(t, afterCleanup)
}

func TestDiagnosticsSnapshotEndpointCloseDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, fake := newTimingDiagnosticsQueue(t, 1)
		producer := queue.NewDiagnosticsEndpoint()
		consumer := queue.NewDiagnosticsEndpoint()
		require.NoError(t, producer.TryPush(1))
		before := queue.Diagnostics()
		detached := before
		loaded := make(chan struct{})
		resume := make(chan struct{})
		firstResults := make(chan DiagnosticsSnapshot, 1)
		go func() {
			firstResults <- queue.collectDiagnostics(&diagnosticsReductionControl{
				counters: &queue.diagnostics.counters[producer.shard].producer.diagnosticsSideCounters,
				loaded:   loaded,
				resume:   resume,
			})
		}()
		<-loaded
		require.NoError(t, queue.TryPush(2))
		require.NoError(t, producer.TryPush(3))
		queue.Close()
		count, err := consumer.TryReceive(make([]int, 2))
		require.NoError(t, err)
		require.Equal(t, 2, count)
		count, err = queue.TryReceive(make([]int, 1))
		require.NoError(t, err)
		require.Equal(t, 1, count)
		count, err = consumer.TryReceive(make([]int, 1))
		require.ErrorIs(t, err, ErrClosed)
		require.Zero(t, count)
		calls := fake.recordedCalls()
		second := queue.Diagnostics()
		close(resume)
		first := <-firstResults
		assert.Equal(t, calls, fake.recordedCalls(), "collectors must not call the engine")
		assert.Equal(t, detached, before)
		assert.Equal(t, uint64(1), before.Producer.MovedSize.Sum)
		assert.Zero(t, before.Consumer.MovedSize.Sum)
		assert.False(t, before.WaitState.Consumer.Closed)
		assert.Equal(t, uint64(2), second.Producer.MovedSize.Sum-before.Producer.MovedSize.Sum)
		assert.Equal(t, uint64(3), second.Consumer.MovedSize.Sum-before.Consumer.MovedSize.Sum)
		firstNext := queue.Diagnostics()
		secondNext := queue.Diagnostics()
		assert.Equal(t, uint64(0), secondNext.Producer.MovedSize.Sum-second.Producer.MovedSize.Sum)
		assert.Equal(t, uint64(0), secondNext.Consumer.MovedSize.Sum-second.Consumer.MovedSize.Sum)
		assert.GreaterOrEqual(t, firstNext.Producer.MovedSize.Sum, first.Producer.MovedSize.Sum)
		assert.Equal(t, calls, fake.recordedCalls())
		assertDiagnosticsConserved(t, secondNext, 3, 3)
		assertIdleQueue(t, queue)
	})
}
