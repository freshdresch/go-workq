package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallbackFailureContinuesHeldAndQueuedCleanup(t *testing.T) {
	config := runtimeConfig(1)
	config.FailureMode = Continue
	config.ErrorLimit = 2
	config.Queue.MaxReceiveBatch = 3
	entered := make(chan struct{})
	release := make(chan struct{})
	defer releaseTestBarrier(release)
	handlerFailure := errors.New("handler failure")
	callbackFailures := []error{
		errors.New("callback one"), errors.New("callback two"), nil,
		errors.New("callback four"), errors.New("callback five"), errors.New("callback six"),
	}
	config.Handler = func(_ context.Context, batch []int) []Result {
		assert.Equal(t, []int{1, 2, 3}, batch)
		close(entered)
		<-release
		return []Result{{}, {State: ResultFailed, Err: handlerFailure}, {}}
	}
	outcomes := make(map[int]ItemOutcome)
	var runner *Runner[int]
	config.OnTerminal = func(item int, outcome ItemOutcome) error {
		if item > 1 {
			assert.Same(t, callbackFailures[0], runner.AbortCause())
			select {
			case <-runner.Aborting():
			default:
				t.Error("callback started before abort publication")
			}
		}
		outcomes[item] = outcome
		return callbackFailures[item-1]
	}
	var err error
	runner, err = New(config)
	require.NoError(t, err)
	joinRunnerOnCleanup(t, runner)
	count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, 3, count)
	<-entered
	count, err = runner.SubmitBatch(context.Background(), []int{4, 5, 6})
	require.NoError(t, err)
	require.Equal(t, 3, count)
	close(release)
	finalErr := runner.Wait()
	require.ErrorIs(t, finalErr, handlerFailure)
	require.ErrorIs(t, finalErr, callbackFailures[0])
	require.ErrorIs(t, finalErr, ErrOmittedFailures)
	for _, failure := range callbackFailures[1:] {
		if failure != nil {
			assert.NotErrorIs(t, finalErr, failure)
		}
	}
	expected := Summary{
		Accepted: 6, Succeeded: 2, Failed: 1, Abandoned: 3, Attempts: 3,
		Failures: []FailureRecord{
			{Boundary: BoundaryHandler, Worker: 0, Err: handlerFailure},
			{Boundary: BoundaryTerminalCallback, Worker: 0, Err: callbackFailures[0]},
		},
		OmittedFailures: 4, AbortCause: callbackFailures[0],
	}
	assert.Equal(t, expected, runner.Summary())
	assert.Equal(t, ItemOutcome{State: TerminalSucceeded, Attempts: 1}, outcomes[1])
	assert.Equal(t, ItemOutcome{
		State: TerminalFailed, Attempts: 1, Err: handlerFailure,
	}, outcomes[2])
	assert.Equal(t, ItemOutcome{State: TerminalSucceeded, Attempts: 1}, outcomes[3])
	for _, item := range []int{4, 5, 6} {
		assert.Equal(t, ItemOutcome{
			State: TerminalAbandoned, Err: callbackFailures[0],
		}, outcomes[item])
	}
	require.Len(t, outcomes, 6)
	snapshot := runner.Summary()
	snapshot.Failures[0] = FailureRecord{}
	snapshot.Failures = append(snapshot.Failures, FailureRecord{})
	runner.Abort(errors.New("late abort"))
	assert.Equal(t, expected, runner.Summary())
	assert.Same(t, finalErr, runner.Wait())
}

func TestCallbacksRunConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(2)
		config.Queue.MaxReceiveBatch = 1
		entered := make(chan int, 2)
		release := make(chan struct{})
		defer releaseTestBarrier(release)
		config.OnTerminal = func(item int, _ ItemOutcome) error {
			entered <- item
			<-release
			return nil
		}
		runner, err := New(config)
		require.NoError(t, err)
		joinRunnerOnCleanup(t, runner)
		count, err := runner.SubmitBatch(context.Background(), []int{1, 2})
		require.NoError(t, err)
		require.Equal(t, 2, count)
		first := <-entered
		second := <-entered
		assert.ElementsMatch(t, []int{1, 2}, []int{first, second})
		runner.StopAdmission()
		synctest.Wait()
		assertChannelOpen(t, runner.Done())
		close(release)
		require.NoError(t, runner.Wait())
		assert.Equal(t, uint64(2), runner.Summary().Succeeded)
	})
}

func TestAbandonmentCauseReachesCallbacksWithoutFailureRetention(t *testing.T) {
	config := runtimeConfig(1)
	config.ErrorLimit = 1
	config.FailureMode = Continue
	config.Queue.MaxReceiveBatch = 3
	entered := make(chan struct{})
	release := make(chan struct{})
	defer releaseTestBarrier(release)
	failure := errors.New("retained handler failure")
	cause := errors.New("explicit abandonment cause")
	config.Handler = func(ctx context.Context, _ []int) []Result {
		close(entered)
		<-release
		return []Result{{State: ResultFailed, Err: failure}, PeerCanceled(ctx), {
			State: ResultUnattempted,
		}}
	}
	var outcomes []ItemOutcome
	config.OnTerminal = func(_ int, outcome ItemOutcome) error {
		outcomes = append(outcomes, outcome)
		return nil
	}
	runner, err := New(config)
	require.NoError(t, err)
	joinRunnerOnCleanup(t, runner)
	count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, 3, count)
	<-entered
	runner.Abort(cause)
	close(release)
	require.ErrorIs(t, runner.Wait(), failure)
	assert.NotErrorIs(t, runner.Wait(), cause)
	assert.NotErrorIs(t, runner.Wait(), ErrOmittedFailures)
	require.Len(t, outcomes, 3)
	assert.Equal(t, ItemOutcome{
		State: TerminalAbandoned, Attempts: 1, Err: cause,
	}, outcomes[1])
	assert.Equal(t, ItemOutcome{State: TerminalAbandoned, Err: cause}, outcomes[2])
	assert.Equal(t, []FailureRecord{{
		Boundary: BoundaryHandler, Worker: 0, Err: failure,
	}}, runner.Summary().Failures)
}

func TestConcurrentCallbackFailuresRetainWorkerAttribution(t *testing.T) {
	config := runtimeConfig(2)
	config.FailureMode = Continue
	config.Queue.MaxReceiveBatch = 1
	failures := []error{errors.New("worker zero"), errors.New("worker one")}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	defer releaseTestBarrier(release)
	var mu sync.Mutex
	next := 0
	config.Handler = func(context.Context, []int) []Result { return nil }
	config.OnTerminal = func(_ int, _ ItemOutcome) error {
		mu.Lock()
		failure := failures[next]
		next++
		mu.Unlock()
		entered <- struct{}{}
		<-release
		return failure
	}
	runner, err := New(config)
	require.NoError(t, err)
	joinRunnerOnCleanup(t, runner)
	count, err := runner.SubmitBatch(context.Background(), []int{1, 2})
	require.NoError(t, err)
	require.Equal(t, 2, count)
	<-entered
	<-entered
	close(release)
	finalErr := runner.Wait()
	for _, failure := range failures {
		assert.ErrorIs(t, finalErr, failure)
	}
	summary := runner.Summary()
	require.Len(t, summary.Failures, 2)
	assert.ElementsMatch(t, []int{0, 1}, []int{
		summary.Failures[0].Worker, summary.Failures[1].Worker,
	})
	for _, failure := range summary.Failures {
		assert.Equal(t, BoundaryTerminalCallback, failure.Boundary)
	}
	assert.Contains(t, failures, summary.AbortCause)
	assert.Equal(t, uint64(2), summary.Succeeded)
	assert.Zero(t, summary.Failed)
	assert.Zero(t, summary.Abandoned)
}

func joinRunnerOnCleanup[T any](t *testing.T, runner *Runner[T]) {
	t.Helper()
	t.Cleanup(func() {
		runner.Abort(nil)
		_ = runner.Wait()
	})
}
