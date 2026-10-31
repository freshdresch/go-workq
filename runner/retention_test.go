package runner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq"
)

type retainedPayload struct {
	value int
}

func TestProcessingClearsOwnedStorageAndPreservesHandlerResults(t *testing.T) {
	for _, scenario := range []string{"success", "failure", "aborted", "invalid result"} {
		t.Run(scenario, func(t *testing.T) {
			queue, err := workq.New[*retainedPayload](2)
			require.NoError(t, err)
			runner := newRunnerState(queue, 1, func() {})
			runner.config.FailureMode = Continue
			failure := errors.New("handler error")
			results := []Result{{}, {}}
			expectedResults := []Result{{}, {}}
			if scenario == "failure" {
				results[1] = Result{State: ResultFailed, Err: failure}
				expectedResults[1] = results[1]
			}
			if scenario == "invalid result" {
				results[1] = Result{State: ResultSucceeded, Err: failure}
				expectedResults[1] = results[1]
			}
			calls := 0
			runner.config.Handler = func(context.Context, []*retainedPayload) []Result {
				calls++
				return results
			}
			batch := []*retainedPayload{{value: 1}, {value: 2}}
			outcomes := make([]ItemOutcome, 2)
			callbacks := 0
			runner.config.OnTerminal = func(item *retainedPayload, outcome ItemOutcome) error {
				callbacks++
				assert.Equal(t, callbacks, item.value)
				if callbacks == 2 {
					assert.Nil(t, batch[0])
					assert.Equal(t, ItemOutcome{}, outcomes[0])
				}
				if scenario == "aborted" {
					assert.Equal(t, TerminalAbandoned, outcome.State)
					assert.Same(t, ErrAborted, outcome.Err)
				}
				return nil
			}
			// The fixture holds admission so its coordinator cannot race private processing.
			require.True(t, runner.registerSubmission())
			if scenario == "aborted" {
				runner.Abort(nil)
			}
			runner.processBatch(0, batch, outcomes)
			assert.Equal(t, []*retainedPayload{nil, nil}, batch)
			assert.Equal(t, make([]ItemOutcome, 2), outcomes)
			assert.Equal(t, expectedResults, results)
			assert.Equal(t, 2, callbacks)
			if scenario == "aborted" {
				assert.Zero(t, calls)
			} else {
				assert.Equal(t, 1, calls)
			}
			runner.unregisterSubmission()
			runner.StopAdmission()
			_ = runner.Wait()
		})
	}
}

func TestWorkerStorageIsClearBeforeCompletion(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "abort"}[abort], func(t *testing.T) {
			config := Config[*retainedPayload]{
				Capacity: 4, Workers: 1,
				Queue: QueueConfig{MaxReceiveBatch: 4},
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			defer releaseTestBarrier(release)
			var storage []*retainedPayload
			config.Handler = func(ctx context.Context, batch []*retainedPayload) []Result {
				// Retaining this borrowed view is only for inspection after the worker joins.
				storage = batch[:cap(batch)]
				close(entered)
				<-release
				if abort {
					return []Result{PeerCanceled(ctx), PeerCanceled(ctx)}
				}
				return nil
			}
			runner, err := New(config)
			require.NoError(t, err)
			joinRunnerOnCleanup(t, runner)
			count, err := runner.SubmitBatch(context.Background(), []*retainedPayload{
				{value: 1}, {value: 2},
			})
			require.NoError(t, err)
			require.Equal(t, 2, count)
			<-entered
			if abort {
				runner.Abort(nil)
			}
			close(release)
			finalErr := runner.Shutdown(context.Background())
			if abort {
				require.ErrorIs(t, finalErr, ErrAborted)
			} else {
				require.NoError(t, finalErr)
			}
			require.Len(t, storage, 4)
			assert.Equal(t, make([]*retainedPayload, 4), storage)
			runner.lifecycle.mu.Lock()
			assert.Zero(t, runner.lifecycle.active)
			assert.True(t, runner.lifecycle.completed)
			runner.lifecycle.mu.Unlock()
		})
	}
}

func TestQueuedCleanupClearsReusableStorageBeforeNextBatch(t *testing.T) {
	queue, err := workq.New[*retainedPayload](4, workq.WithSingleConsumer())
	require.NoError(t, err)
	runner := newRunnerState(queue, 1, func() {})
	runner.batchLimit = 2
	require.True(t, runner.registerSubmission())
	runner.Abort(nil)
	payloads := []*retainedPayload{{value: 1}, {value: 2}, {value: 3}}
	count, err := queue.PushBatch(context.Background(), payloads)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	queue.Close()
	callbacks := 0
	runner.config.OnTerminal = func(item *retainedPayload, outcome ItemOutcome) error {
		callbacks++
		assert.Equal(t, callbacks, item.value)
		assert.Equal(t, ItemOutcome{State: TerminalAbandoned, Err: ErrAborted}, outcome)
		return errors.New("cleanup error")
	}
	// Inspect the same private finalization boundary used for every queued cleanup batch.
	buffer := make([]*retainedPayload, 2)
	outcomes := make([]ItemOutcome, 2)
	for {
		count, err := queue.Receive(context.Background(), buffer)
		if errors.Is(err, workq.ErrClosed) {
			break
		}
		require.NoError(t, err)
		for position := range count {
			outcomes[position] = ItemOutcome{State: TerminalAbandoned, Err: ErrAborted}
		}
		runner.finalizeBatch(-1, buffer[:count], outcomes[:count])
		assert.Equal(t, make([]*retainedPayload, 2), buffer)
		assert.Equal(t, make([]ItemOutcome, 2), outcomes)
	}
	runner.unregisterSubmission()
	require.Error(t, runner.Wait())
	require.Equal(t, 3, callbacks)
	summary := runner.Summary()
	assert.Equal(t, uint64(2), summary.OmittedFailures)
	require.Len(t, summary.Failures, 1)
	assert.Equal(t, BoundaryTerminalCallback, summary.Failures[0].Boundary)
	assert.Equal(t, -1, summary.Failures[0].Worker)
	assert.Same(t, ErrAborted, summary.AbortCause)
}
