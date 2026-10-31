package runner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq"
)

func runtimeConfig(workers int) Config[int] {
	return Config[int]{
		Capacity: 8,
		Workers:  workers,
		Handler:  func(context.Context, []int) []Result { return nil },
		Queue: QueueConfig{
			ProducerWait: &workq.WaitStrategy{},
			ConsumerWait: &workq.WaitStrategy{},
		},
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	tests := map[string]func(*Config[int]){
		"workers": func(config *Config[int]) { config.Workers = 0 },
		"handler": func(config *Config[int]) { config.Handler = nil },
		"mode":    func(config *Config[int]) { config.FailureMode = FailureMode(200) },
		"limit":   func(config *Config[int]) { config.ErrorLimit = -1 },
		"capacity": func(config *Config[int]) {
			config.Capacity = 3
		},
		"queue wait": func(config *Config[int]) {
			config.Queue.ConsumerWait = &workq.WaitStrategy{Spins: -1}
		},
		"retry attempts": func(config *Config[int]) {
			config.Retry = &RetryPolicy{}
		},
		"retry delay": func(config *Config[int]) {
			config.Retry = &RetryPolicy{MaxAttempts: 2}
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			config := runtimeConfig(1)
			var called atomic.Bool
			config.Handler = func(context.Context, []int) []Result {
				called.Store(true)
				return nil
			}
			change(&config)
			runner, err := New(config)
			require.Error(t, err)
			assert.Nil(t, runner)
			assert.False(t, called.Load())
		})
	}
}

func TestRunnerDrainsEveryOccurrence(t *testing.T) {
	for _, workers := range []int{1, 3} {
		t.Run(string(rune('0'+workers)), func(t *testing.T) {
			config := runtimeConfig(workers)
			var mu sync.Mutex
			outcomes := make(map[int][]ItemOutcome)
			config.OnTerminal = func(item int, outcome ItemOutcome) error {
				mu.Lock()
				defer mu.Unlock()
				outcomes[item] = append(outcomes[item], outcome)
				return nil
			}
			runner, err := New(config)
			require.NoError(t, err)
			count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 1, 3, 4, 1})
			require.NoError(t, err)
			assert.Equal(t, 6, count)
			require.NoError(t, runner.Shutdown(context.Background()))
			assert.Equal(t, Summary{Accepted: 6, Succeeded: 6, Attempts: 6}, runner.Summary())
			assert.Len(t, outcomes[1], 3)
			for _, occurrences := range outcomes {
				for _, outcome := range occurrences {
					assert.Equal(t, ItemOutcome{State: TerminalSucceeded, Attempts: 1}, outcome)
				}
			}
			assert.Nil(t, runner.AbortCause())
			assertChannelOpen(t, runner.Aborting())
			assert.Equal(t, runner.Done(), runner.Done())
			assert.Same(t, ErrStopped, runner.Submit(context.Background(), 9))
			assert.Equal(t, config.Capacity, int(runner.Diagnostics().Config.Capacity))
		})
	}
}

func TestAcceptedItemsCompleteBeforeSubmitReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(1)
		config.Capacity = 1
		firstFinished := make(chan struct{})
		secondEntered := make(chan struct{})
		releaseSecond := make(chan struct{})
		defer releaseTestBarrier(releaseSecond)
		config.Handler = func(_ context.Context, batch []int) []Result {
			if batch[0] == 2 {
				close(secondEntered)
				<-releaseSecond
			}
			return nil
		}
		config.OnTerminal = func(item int, _ ItemOutcome) error {
			if item == 1 {
				close(firstFinished)
			}
			return nil
		}
		runner, err := New(config)
		require.NoError(t, err)
		submitted := make(chan submissionResult, 1)
		go func() {
			count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 3, 4})
			submitted <- submissionResult{count, err}
		}()
		<-firstFinished
		<-secondEntered
		synctest.Wait()
		select {
		case result := <-submitted:
			t.Fatalf("submission returned before queue space was available: %+v", result)
		default:
		}
		close(releaseSecond)
		result := <-submitted
		require.NoError(t, result.err)
		assert.Equal(t, 4, result.accepted)
		require.NoError(t, runner.Shutdown(context.Background()))
	})
}

func TestCallerCancellationDoesNotCancelAcceptedWork(t *testing.T) {
	config := runtimeConfig(1)
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	defer releaseTestBarrier(release)
	config.Handler = func(ctx context.Context, _ []int) []Result {
		entered <- ctx
		<-release
		return nil
	}
	runner, err := New(config)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, runner.Submit(ctx, 1))
	handlerCtx := <-entered
	cancel()
	assert.NoError(t, handlerCtx.Err())
	close(release)
	require.NoError(t, runner.Shutdown(context.Background()))
	assert.Equal(t, uint64(1), runner.Summary().Succeeded)
}

func TestMixedResultsAfterAbort(t *testing.T) {
	config := runtimeConfig(1)
	entered := make(chan struct{})
	release := make(chan struct{})
	defer releaseTestBarrier(release)
	failure := errors.New("ordinary failure")
	cause := errors.New("source failure")
	config.Handler = func(ctx context.Context, _ []int) []Result {
		close(entered)
		<-release
		return []Result{
			{State: ResultSucceeded},
			{State: ResultFailed, Err: failure},
			PeerCanceled(ctx),
			{State: ResultUnattempted},
		}
	}
	var outcomes []ItemOutcome
	config.OnTerminal = func(_ int, outcome ItemOutcome) error {
		outcomes = append(outcomes, outcome)
		return nil
	}
	runner, err := New(config)
	require.NoError(t, err)
	count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 3, 4})
	require.NoError(t, err)
	require.Equal(t, 4, count)
	<-entered
	runner.Abort(cause)
	close(release)
	require.ErrorIs(t, runner.Wait(), failure)
	assert.NotErrorIs(t, runner.Wait(), cause)
	assert.Equal(t, []ItemOutcome{
		{State: TerminalSucceeded, Attempts: 1},
		{State: TerminalFailed, Attempts: 1, Err: failure},
		{State: TerminalAbandoned, Attempts: 1, Err: cause},
		{State: TerminalAbandoned, Attempts: 0, Err: cause},
	}, outcomes)
	summary := runner.Summary()
	assert.Equal(t, uint64(4), summary.Accepted)
	assert.Equal(t, uint64(1), summary.Succeeded)
	assert.Equal(t, uint64(1), summary.Failed)
	assert.Equal(t, uint64(2), summary.Abandoned)
	assert.Equal(t, uint64(3), summary.Attempts)
	require.Len(t, summary.Failures, 1)
	assert.Equal(t, FailureRecord{Boundary: BoundaryHandler, Worker: 0, Err: failure},
		summary.Failures[0])
	assert.Same(t, cause, summary.AbortCause)
}

func TestInvalidResponsesAbortBeforeAnyCallback(t *testing.T) {
	failure := errors.New("invalid error")
	tests := map[string][]Result{
		"empty":                  {},
		"short":                  {{State: ResultSucceeded}},
		"long":                   {{}, {}, {}},
		"unknown":                {{}, {State: ResultState(200)}},
		"success with error":     {{}, {State: ResultSucceeded, Err: failure}},
		"failure without error":  {{}, {State: ResultFailed}},
		"unattempted with error": {{}, {State: ResultUnattempted, Err: failure}},
		"unattempted live":       {{}, {State: ResultUnattempted}},
		"interrupted without observation": {{}, {
			State: ResultInterrupted, Err: context.Canceled,
		}},
		"interrupted without error": {{}, {
			State: ResultInterrupted, peerCanceled: true,
		}},
	}
	for name, response := range tests {
		t.Run(name, func(t *testing.T) {
			config := runtimeConfig(1)
			config.FailureMode = Continue
			config.Handler = func(context.Context, []int) []Result { return response }
			var runner *Runner[int]
			var outcomes []ItemOutcome
			config.OnTerminal = func(_ int, outcome ItemOutcome) error {
				select {
				case <-runner.Aborting():
				default:
					t.Error("invalid response reached callback before abort publication")
				}
				assert.ErrorIs(t, runner.ctx.Err(), context.Canceled)
				outcomes = append(outcomes, outcome)
				return nil
			}
			var err error
			runner, err = New(config)
			require.NoError(t, err)
			count, err := runner.SubmitBatch(context.Background(), []int{1, 2})
			require.NoError(t, err)
			require.Equal(t, 2, count)
			require.Error(t, runner.Shutdown(context.Background()))
			require.Len(t, outcomes, 2)
			for _, outcome := range outcomes {
				assert.Equal(t, TerminalFailed, outcome.State)
				assert.Equal(t, 1, outcome.Attempts)
				assert.Error(t, outcome.Err)
			}
			summary := runner.Summary()
			assert.Equal(t, uint64(0), summary.Succeeded)
			assert.Equal(t, uint64(2), summary.Failed)
			assert.Equal(t, uint64(2), summary.Attempts)
			require.Len(t, summary.Failures, 1)
			assert.Equal(t, BoundaryResultValidation, summary.Failures[0].Boundary)
		})
	}
}

func TestFailureModesClassifyBeforeCallbacks(t *testing.T) {
	for _, mode := range []FailureMode{Abort, Continue} {
		t.Run(string(rune('0'+mode)), func(t *testing.T) {
			config := runtimeConfig(1)
			config.FailureMode = mode
			config.Queue.MaxReceiveBatch = 3
			failure := errors.New("permanent failure")
			config.Handler = func(_ context.Context, batch []int) []Result {
				if batch[0] != 1 {
					return nil
				}
				return []Result{
					{State: ResultSucceeded},
					{State: ResultFailed, Err: failure},
					{State: ResultFailed, Err: context.DeadlineExceeded},
				}
			}
			var runner *Runner[int]
			var outcomes []ItemOutcome
			config.OnTerminal = func(item int, outcome ItemOutcome) error {
				if item == 1 {
					assert.Len(t, runner.Summary().Failures, 2)
					if mode == Abort {
						<-runner.Aborting()
					} else {
						assertChannelOpen(t, runner.Aborting())
					}
				}
				outcomes = append(outcomes, outcome)
				return nil
			}
			var err error
			runner, err = New(config)
			require.NoError(t, err)
			count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 3, 4, 5})
			require.NoError(t, err)
			assert.Equal(t, 5, count)
			err = runner.Shutdown(context.Background())
			require.ErrorIs(t, err, failure)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			summary := runner.Summary()
			assert.Equal(t, uint64(5), summary.Accepted)
			assert.Equal(t, uint64(2), summary.Failed)
			assert.Len(t, outcomes, 5)
			if mode == Abort {
				assert.Equal(t, uint64(1), summary.Succeeded)
				assert.Equal(t, uint64(2), summary.Abandoned)
				assert.Equal(t, uint64(3), summary.Attempts)
			} else {
				assert.Equal(t, uint64(3), summary.Succeeded)
				assert.Equal(t, uint64(0), summary.Abandoned)
				assert.Equal(t, uint64(5), summary.Attempts)
				assert.Nil(t, summary.AbortCause)
				assertChannelOpen(t, runner.Aborting())
			}
		})
	}
}

func TestAbortJoinsBlockedHandlersAndCleansQueue(t *testing.T) {
	for _, workers := range []int{1, 3} {
		t.Run(string(rune('0'+workers)), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config := runtimeConfig(workers)
				config.Queue.MaxReceiveBatch = 1
				entered := make(chan struct{}, workers)
				release := make(chan struct{})
				defer releaseTestBarrier(release)
				config.Handler = func(ctx context.Context, _ []int) []Result {
					entered <- struct{}{}
					<-release
					return []Result{PeerCanceled(ctx)}
				}
				var mu sync.Mutex
				outcomes := make(map[int][]ItemOutcome)
				config.OnTerminal = func(item int, outcome ItemOutcome) error {
					mu.Lock()
					defer mu.Unlock()
					outcomes[item] = append(outcomes[item], outcome)
					return nil
				}
				runner, err := New(config)
				require.NoError(t, err)
				for item := range workers {
					require.NoError(t, runner.Submit(context.Background(), item))
				}
				for range workers {
					<-entered
				}
				for item := workers; item < workers+4; item++ {
					require.NoError(t, runner.Submit(context.Background(), item))
				}
				runner.Abort(nil)
				joined := make(chan error, 1)
				go func() { joined <- runner.Wait() }()
				synctest.Wait()
				assertChannelOpen(t, runner.Done())
				select {
				case err := <-joined:
					t.Fatalf("Wait returned before handler release: %v", err)
				default:
				}
				close(release)
				require.ErrorIs(t, <-joined, ErrAborted)
				summary := runner.Summary()
				assert.Equal(t, uint64(workers+4), summary.Accepted)
				assert.Equal(t, summary.Accepted, summary.Abandoned)
				assert.Equal(t, uint64(workers), summary.Attempts)
				assert.Empty(t, summary.Failures)
				for item := range workers + 4 {
					require.Len(t, outcomes[item], 1)
					outcome := outcomes[item][0]
					assert.Equal(t, TerminalAbandoned, outcome.State)
					assert.ErrorIs(t, outcome.Err, ErrAborted)
					if item < workers {
						assert.Equal(t, 1, outcome.Attempts)
					} else {
						assert.Zero(t, outcome.Attempts)
					}
				}
			})
		})
	}
}

func TestAcquiredValuesSkipHandlerAfterAbort(t *testing.T) {
	queue, err := workq.New[int](2, workq.WithSingleConsumer())
	require.NoError(t, err)
	runner := newRunnerState(queue, 16, 1, nil)
	runner.config = runtimeConfig(1)
	runner.batchLimit = 2
	runner.lifecycle.finish = runner.finishWorkers
	runner.config.Handler = func(context.Context, []int) []Result {
		t.Error("handler invoked after abort")
		return nil
	}
	var outcomes []ItemOutcome
	runner.config.OnTerminal = func(_ int, outcome ItemOutcome) error {
		outcomes = append(outcomes, outcome)
		return nil
	}
	require.NoError(t, runner.Submit(context.Background(), 1))
	require.True(t, runner.registerSubmission())
	runner.Abort(nil)
	buffer := make([]int, 2)
	count, err := queue.Receive(runner.ctx, buffer)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	runner.processBatch(0, buffer[:count], make([]ItemOutcome, count))
	runner.unregisterSubmission()
	require.ErrorIs(t, runner.Wait(), ErrAborted)
	assert.Equal(t, []ItemOutcome{{State: TerminalAbandoned, Err: ErrAborted}}, outcomes)
	assert.Equal(t, uint64(0), runner.Summary().Attempts)
	assert.Equal(t, []int{0, 0}, buffer)
}

func TestWorkerFinishWaitsForAbortPublicationBeforeCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, err := workq.New[int](2, workq.WithSingleConsumer())
		require.NoError(t, err)
		runner := newRunnerState(queue, 16, 1, func() {})
		runner.config = runtimeConfig(1)
		runner.batchLimit = 2
		callbackEntered := make(chan struct{}, 1)
		runner.config.OnTerminal = func(_ int, _ ItemOutcome) error {
			callbackEntered <- struct{}{}
			return nil
		}
		require.NoError(t, runner.Submit(context.Background(), 1))
		require.True(t, runner.registerSubmission())
		cancelEntered := make(chan struct{})
		cancelRelease := make(chan struct{})
		defer releaseTestBarrier(cancelRelease)
		cancel := runner.cancel
		var firstCancel atomic.Bool
		runner.cancel = func() {
			if firstCancel.CompareAndSwap(false, true) {
				close(cancelEntered)
				<-cancelRelease
			}
			cancel()
		}
		abortReturned := make(chan struct{})
		go func() {
			runner.Abort(nil)
			close(abortReturned)
		}()
		<-cancelEntered
		finished := make(chan struct{})
		go func() {
			runner.finishWorkers()
			close(finished)
		}()
		synctest.Wait()
		assertChannelOpen(t, finished)
		assertChannelOpen(t, runner.Aborting())
		select {
		case <-callbackEntered:
			t.Fatal("queued cleanup started before abort publication")
		default:
		}
		close(cancelRelease)
		<-abortReturned
		<-finished
		<-callbackEntered
		runner.unregisterSubmission()
		require.ErrorIs(t, runner.Wait(), ErrAborted)
		assert.Equal(t, uint64(1), runner.Summary().Abandoned)
	})
}

func TestIndependentContextErrorsRemainOperationalFailures(t *testing.T) {
	config := runtimeConfig(1)
	config.FailureMode = Continue
	config.Handler = Items(func(_ context.Context, item int) error {
		if item == 1 {
			return context.Canceled
		}
		return context.DeadlineExceeded
	})
	var outcomes []ItemOutcome
	config.OnTerminal = func(_ int, outcome ItemOutcome) error {
		outcomes = append(outcomes, outcome)
		return nil
	}
	runner, err := New(config)
	require.NoError(t, err)
	count, err := runner.SubmitBatch(context.Background(), []int{1, 2})
	require.NoError(t, err)
	require.Equal(t, 2, count)
	err = runner.Shutdown(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, outcomes, 2)
	for _, outcome := range outcomes {
		assert.Equal(t, TerminalFailed, outcome.State)
		assert.Equal(t, 1, outcome.Attempts)
	}
	assert.Nil(t, runner.AbortCause())
	assertChannelOpen(t, runner.Aborting())
	assert.Equal(t, uint64(2), runner.Summary().Failed)
}
