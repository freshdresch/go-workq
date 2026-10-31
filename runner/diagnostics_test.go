package runner

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq"
)

func TestFinalDiagnosticsIncludeExclusiveAbortCleanup(t *testing.T) {
	for _, mode := range []workq.DiagnosticsMode{
		workq.DiagnosticsDisabled, workq.DiagnosticsCounters,
	} {
		for _, workers := range []int{1, 2} {
			name := string(rune('0'+mode)) + "/" + string(rune('0'+workers))
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					config := runtimeConfig(workers)
					config.Queue.MaxReceiveBatch = 1
					config.Queue.Diagnostics.Mode = mode
					entered := make(chan struct{}, workers)
					releaseHandlers := make(chan struct{})
					releaseCallbacks := make(chan struct{})
					defer releaseTestBarrier(releaseHandlers)
					defer releaseTestBarrier(releaseCallbacks)
					config.Handler = func(ctx context.Context, _ []int) []Result {
						entered <- struct{}{}
						<-releaseHandlers
						return []Result{PeerCanceled(ctx)}
					}
					callbackEntered := make(chan struct{}, workers)
					queuedCallbacks := make(chan int, 3)
					config.OnTerminal = func(item int, outcome ItemOutcome) error {
						assert.Equal(t, TerminalAbandoned, outcome.State)
						if item < workers {
							callbackEntered <- struct{}{}
							<-releaseCallbacks
							return nil
						}
						queuedCallbacks <- item
						return nil
					}
					runner, err := New(config)
					require.NoError(t, err)
					joinRunnerOnCleanup(t, runner)
					for item := range workers {
						require.NoError(t, runner.Submit(context.Background(), item))
					}
					for range workers {
						<-entered
					}
					for item := workers; item < workers+3; item++ {
						require.NoError(t, runner.Submit(context.Background(), item))
					}
					cause := errors.New("abort before cleanup")
					runner.Abort(cause)
					before := runner.Diagnostics()
					if mode != workq.DiagnosticsDisabled {
						assert.Equal(t, uint64(workers), diagnosticMoved(before.Consumer.MovedItems))
					}
					close(releaseHandlers)
					for range workers {
						<-callbackEntered
					}
					synctest.Wait()
					assertChannelOpen(t, runner.Done())
					select {
					case item := <-queuedCallbacks:
						t.Fatalf("queued cleanup ran while a worker callback was held: %d", item)
					default:
					}
					during := runner.Diagnostics()
					assert.Equal(t, before.Consumer.MovedItems, during.Consumer.MovedItems)
					close(releaseCallbacks)
					require.ErrorIs(t, runner.Wait(), cause)
					for range 3 {
						<-queuedCallbacks
					}
					final := runner.Diagnostics()
					if mode == workq.DiagnosticsDisabled {
						assert.Equal(t, workq.DiagnosticsProducerSnapshot{}, final.Producer)
						assert.Equal(t, workq.DiagnosticsConsumerSnapshot{}, final.Consumer)
						assert.Equal(t, workq.DiagnosticsWaitState{}, final.WaitState)
					} else {
						assert.Equal(t, uint64(workers+3), diagnosticMoved(final.Producer.MovedItems))
						assert.Equal(t, uint64(workers+3), diagnosticMoved(final.Consumer.MovedItems))
						assert.Zero(t, final.WaitState.Producer.Pollers)
						assert.Zero(t, final.WaitState.Producer.Parked)
						assert.Zero(t, final.WaitState.Consumer.Pollers)
						assert.Zero(t, final.WaitState.Consumer.Parked)
					}
					expected := final
					final.Consumer.MovedItems[workq.PhaseInitial]++
					final.Producer.MovedItems[workq.PhaseInitial]++
					final.Config.Capacity++
					assert.Equal(t, expected, runner.Diagnostics())
					assert.Equal(t, uint64(workers+3), runner.Summary().Abandoned)
					assert.Zero(t, runner.lifecycle.active)
				})
			})
		}
	}
}

func diagnosticMoved(phases [workq.AttemptPhaseCount]uint64) uint64 {
	var count uint64
	for _, moved := range phases {
		count += moved
	}
	return count
}

func TestFinalDiagnosticsAfterSuccessfulDrain(t *testing.T) {
	config := runtimeConfig(2)
	config.Queue.Diagnostics.Mode = workq.DiagnosticsCounters
	runner, err := New(config)
	require.NoError(t, err)
	joinRunnerOnCleanup(t, runner)
	count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 3, 4})
	require.NoError(t, err)
	require.Equal(t, 4, count)
	require.NoError(t, runner.Shutdown(context.Background()))
	snapshot := runner.Diagnostics()
	assert.Equal(t, uint64(4), diagnosticMoved(snapshot.Producer.MovedItems))
	assert.Equal(t, uint64(4), diagnosticMoved(snapshot.Consumer.MovedItems))
	assert.Equal(t, uint64(4), runner.Summary().Succeeded)
	assert.Equal(t, snapshot, runner.Diagnostics())
}
