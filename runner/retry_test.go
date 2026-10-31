package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryAttemptsAndTerminalFailures(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		maxAttempts int
		decline     bool
		succeed     bool
		attempts    int
		policyCalls int
	}{
		{name: "recovered", maxAttempts: 3, succeed: true, attempts: 3, policyCalls: 2},
		{name: "declined", maxAttempts: 3, decline: true, attempts: 1, policyCalls: 1},
		{name: "exhausted", maxAttempts: 3, attempts: 3, policyCalls: 2},
		{name: "one attempt", maxAttempts: 1, attempts: 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			for _, mode := range []FailureMode{Abort, Continue} {
				config := runtimeConfig(1)
				config.FailureMode = mode
				failure := errors.New("ordinary error")
				attempts := 0
				config.Handler = func(context.Context, []int) []Result {
					attempts++
					if scenario.succeed && attempts == scenario.attempts {
						return nil
					}
					return []Result{{State: ResultFailed, Err: failure}}
				}
				var policyAttempts []int
				config.Retry = &RetryPolicy{
					MaxAttempts: scenario.maxAttempts,
					Delay: func(attempt int, err error) (time.Duration, bool) {
						assert.Same(t, failure, err)
						policyAttempts = append(policyAttempts, attempt)
						return 0, !scenario.decline
					},
				}
				var outcomes []ItemOutcome
				config.OnTerminal = func(_ int, outcome ItemOutcome) error {
					outcomes = append(outcomes, outcome)
					return nil
				}
				runner, err := New(config)
				require.NoError(t, err)
				require.NoError(t, runner.Submit(context.Background(), 1))
				err = runner.Shutdown(context.Background())
				assert.Equal(t, scenario.attempts, attempts)
				require.Len(t, outcomes, 1)
				assert.Equal(t, scenario.attempts, outcomes[0].Attempts)
				require.Len(t, policyAttempts, scenario.policyCalls)
				for index, attempt := range policyAttempts {
					assert.Equal(t, index+1, attempt)
				}
				summary := runner.Summary()
				assert.Equal(t, uint64(scenario.attempts), summary.Attempts)
				if scenario.succeed {
					require.NoError(t, err)
					assert.Equal(t, TerminalSucceeded, outcomes[0].State)
					assert.Nil(t, outcomes[0].Err)
					assert.Empty(t, summary.Failures)
					assert.Zero(t, summary.OmittedFailures)
					assert.Nil(t, summary.AbortCause)
					continue
				}
				require.ErrorIs(t, err, failure)
				assert.Equal(t, TerminalFailed, outcomes[0].State)
				assert.Same(t, failure, outcomes[0].Err)
				require.Len(t, summary.Failures, 1)
				assert.Equal(t, FailureRecord{
					Boundary: BoundaryHandler, Worker: 0, Err: failure,
				}, summary.Failures[0])
			}
		})
	}
}

func TestRetryNegativeDelayAbortsEvenWhenDeclined(t *testing.T) {
	for _, mode := range []FailureMode{Abort, Continue} {
		for _, retry := range []bool{false, true} {
			config := runtimeConfig(1)
			config.FailureMode = mode
			failure := errors.New("ordinary failure")
			config.Handler = WholeBatch(func(context.Context, []int) error { return failure })
			calls := 0
			config.Retry = &RetryPolicy{
				MaxAttempts: 2,
				Delay: func(int, error) (time.Duration, bool) {
					calls++
					return -time.Second, retry
				},
			}
			var runner *Runner[int]
			config.OnTerminal = func(_ int, outcome ItemOutcome) error {
				<-runner.Aborting()
				assert.Len(t, runner.Summary().Failures, 2)
				assert.Equal(t, ItemOutcome{
					State: TerminalFailed, Attempts: 1, Err: failure,
				}, outcome)
				return nil
			}
			var err error
			runner, err = New(config)
			require.NoError(t, err)
			require.NoError(t, runner.Submit(context.Background(), 1))
			require.ErrorIs(t, runner.Shutdown(context.Background()), failure)
			assert.Equal(t, 1, calls)
			failures := runner.Summary().Failures
			require.Len(t, failures, 2)
			assert.Equal(t, BoundaryHandler, failures[0].Boundary)
			assert.Same(t, failure, failures[0].Err)
			assert.Equal(t, BoundaryRetryPolicy, failures[1].Boundary)
			assert.Equal(t, 0, failures[1].Worker)
			assert.Same(t, failures[1].Err, runner.AbortCause())
		}
	}
}

func TestRetrySchedulesDueSubsetsAndFinalizesSuccessBeforeWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(1)
		config.Queue.MaxReceiveBatch = 4
		entered := make(chan struct{})
		release := make(chan struct{})
		defer releaseTestBarrier(release)
		failure := errors.New("retry")
		var mu sync.Mutex
		var batches [][]int
		config.Handler = func(_ context.Context, batch []int) []Result {
			if batch[0] == 0 {
				close(entered)
				<-release
				return nil
			}
			mu.Lock()
			batches = append(batches, append([]int(nil), batch...))
			invocation := len(batches)
			mu.Unlock()
			if invocation != 1 {
				return nil
			}
			return []Result{
				{}, {State: ResultFailed, Err: failure},
				{State: ResultFailed, Err: failure}, {State: ResultFailed, Err: failure},
			}
		}
		delays := []time.Duration{time.Second, 2 * time.Second, time.Second}
		config.Retry = &RetryPolicy{
			MaxAttempts: 2,
			Delay: func(int, error) (time.Duration, bool) {
				delay := delays[0]
				delays = delays[1:]
				return delay, true
			},
		}
		var terminal []int
		config.OnTerminal = func(item int, _ ItemOutcome) error {
			mu.Lock()
			terminal = append(terminal, item)
			mu.Unlock()
			return nil
		}
		runner, err := New(config)
		require.NoError(t, err)
		joinRunnerOnCleanup(t, runner)
		require.NoError(t, runner.Submit(context.Background(), 0))
		<-entered
		count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 3, 4})
		require.NoError(t, err)
		require.Equal(t, 4, count)
		close(release)
		synctest.Wait()
		mu.Lock()
		assert.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
		assert.Equal(t, []int{0, 1}, terminal)
		mu.Unlock()
		time.Sleep(time.Second)
		synctest.Wait()
		mu.Lock()
		assert.Equal(t, [][]int{{1, 2, 3, 4}, {2, 4}}, batches)
		assert.Equal(t, []int{0, 1, 2, 4}, terminal)
		mu.Unlock()
		time.Sleep(time.Second)
		synctest.Wait()
		require.NoError(t, runner.Shutdown(context.Background()))
		assert.Equal(t, [][]int{{1, 2, 3, 4}, {2, 4}, {3}}, batches)
		assert.Equal(t, Summary{Accepted: 5, Succeeded: 5, Attempts: 8}, runner.Summary())
	})
}

func TestEveryWorkerRetriesWhileQueueIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(3)
		config.Capacity = 4
		config.Queue.MaxReceiveBatch = 1
		entered := make(chan struct{}, config.Workers)
		release := make(chan struct{})
		defer releaseTestBarrier(release)
		var mu sync.Mutex
		attempts := make(map[int]int)
		failure := errors.New("first attempt")
		config.Handler = func(_ context.Context, batch []int) []Result {
			mu.Lock()
			attempts[batch[0]]++
			attempt := attempts[batch[0]]
			mu.Unlock()
			if batch[0] < config.Workers && attempt == 1 {
				entered <- struct{}{}
				<-release
				return []Result{{State: ResultFailed, Err: failure}}
			}
			return nil
		}
		config.Retry = &RetryPolicy{
			MaxAttempts: 2,
			Delay:       func(int, error) (time.Duration, bool) { return 0, true },
		}
		runner, err := New(config)
		require.NoError(t, err)
		joinRunnerOnCleanup(t, runner)
		for item := range config.Workers {
			require.NoError(t, runner.Submit(context.Background(), item))
		}
		for range config.Workers {
			<-entered
		}
		count, err := runner.SubmitBatch(context.Background(), []int{3, 4, 5, 6})
		require.NoError(t, err)
		require.Equal(t, 4, count)
		close(release)
		require.NoError(t, runner.Shutdown(context.Background()))
		assert.Equal(t, Summary{Accepted: 7, Succeeded: 7, Attempts: 10}, runner.Summary())
		for item := range config.Workers {
			assert.Equal(t, 2, attempts[item])
		}
	})
}

func TestAbortDuringRetryDelayReleasesSubmitters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(1)
		config.Capacity = 1
		failure := errors.New("latest ordinary error")
		config.Handler = WholeBatch(func(context.Context, []int) error { return failure })
		config.Retry = &RetryPolicy{
			MaxAttempts: 2,
			Delay:       func(int, error) (time.Duration, bool) { return time.Hour, true },
		}
		outcomes := make(map[int]ItemOutcome)
		config.OnTerminal = func(item int, outcome ItemOutcome) error {
			outcomes[item] = outcome
			return nil
		}
		runner, err := New(config)
		require.NoError(t, err)
		joinRunnerOnCleanup(t, runner)
		require.NoError(t, runner.Submit(context.Background(), 1))
		synctest.Wait()
		require.NoError(t, runner.Submit(context.Background(), 2))
		submitted := make(chan error, 1)
		go func() { submitted <- runner.Submit(context.Background(), 3) }()
		synctest.Wait()
		select {
		case err := <-submitted:
			t.Fatalf("submission returned while retry held queue acquisition: %v", err)
		default:
		}
		runner.Abort(nil)
		require.ErrorIs(t, <-submitted, context.Canceled)
		require.ErrorIs(t, runner.Wait(), ErrAborted)
		assert.Equal(t, ItemOutcome{
			State: TerminalAbandoned, Attempts: 1, Err: failure,
		}, outcomes[1])
		assert.Equal(t, ItemOutcome{State: TerminalAbandoned, Err: ErrAborted}, outcomes[2])
		assert.Equal(t, Summary{
			Accepted: 2, Abandoned: 2, Attempts: 1, AbortCause: ErrAborted,
		}, runner.Summary())
	})
}
