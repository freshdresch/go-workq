package runner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq"
)

func TestRetryClassificationTable(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		state         ResultState
		aborted       bool
		disabled      bool
		attempts      int
		decline       bool
		negative      bool
		terminal      TerminalState
		finalAttempts int
		policyCalls   int
		failures      int
	}{
		{name: "success", terminal: TerminalSucceeded, finalAttempts: 1},
		{name: "success after abort", aborted: true, terminal: TerminalSucceeded, finalAttempts: 1},
		{name: "disabled", state: ResultFailed, disabled: true,
			terminal: TerminalFailed, finalAttempts: 1, failures: 1},
		{name: "disabled after abort", state: ResultFailed, disabled: true, aborted: true,
			terminal: TerminalFailed, finalAttempts: 1, failures: 1},
		{name: "exhausted", state: ResultFailed, attempts: 1,
			terminal: TerminalFailed, finalAttempts: 2, failures: 1},
		{name: "exhausted after abort", state: ResultFailed, attempts: 1, aborted: true,
			terminal: TerminalFailed, finalAttempts: 2, failures: 1},
		{name: "declined", state: ResultFailed, decline: true,
			terminal: TerminalFailed, finalAttempts: 1, policyCalls: 1, failures: 1},
		{name: "declined after abort", state: ResultFailed, decline: true, aborted: true,
			terminal: TerminalFailed, finalAttempts: 1, policyCalls: 1, failures: 1},
		{name: "invalid delay", state: ResultFailed, negative: true,
			terminal: TerminalFailed, finalAttempts: 1, policyCalls: 1, failures: 2},
		{name: "invalid delay after abort", state: ResultFailed, negative: true, aborted: true,
			terminal: TerminalFailed, finalAttempts: 1, policyCalls: 1, failures: 2},
		{name: "candidate", state: ResultFailed,
			terminal: TerminalSucceeded, finalAttempts: 2, policyCalls: 1},
		{name: "candidate after abort", state: ResultFailed, aborted: true,
			terminal: TerminalAbandoned, finalAttempts: 1, policyCalls: 1},
		{name: "interrupted", state: ResultInterrupted, aborted: true,
			terminal: TerminalAbandoned, finalAttempts: 1},
		{name: "unattempted", state: ResultUnattempted, aborted: true,
			terminal: TerminalAbandoned},
		{name: "interrupted after failure", state: ResultInterrupted, aborted: true, attempts: 1,
			terminal: TerminalAbandoned, finalAttempts: 2},
		{name: "unattempted after failure", state: ResultUnattempted, aborted: true, attempts: 1,
			terminal: TerminalAbandoned, finalAttempts: 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			for _, limit := range []int{1, 4} {
				runner := retryRaceFixture(t, limit)
				runner.config.FailureMode = Continue
				failure := errors.New("ordinary failure")
				previous := errors.New("previous failure")
				cause := errors.New("external abort")
				calls := 0
				policyCalls := 0
				runner.config.Retry = &RetryPolicy{
					MaxAttempts: 2,
					Delay: func(attempt int, err error) (time.Duration, bool) {
						policyCalls++
						assert.Equal(t, scenario.attempts+1, attempt)
						assert.Same(t, failure, err)
						if scenario.negative {
							return -time.Second, false
						}
						return 0, !scenario.decline
					},
				}
				if scenario.disabled {
					runner.config.Retry = nil
				}
				runner.config.Handler = func(ctx context.Context, _ []int) []Result {
					calls++
					if calls > 1 {
						return nil
					}
					if scenario.aborted {
						runner.Abort(cause)
					}
					result := Result{State: scenario.state}
					if scenario.state == ResultFailed {
						result.Err = failure
					}
					if scenario.state == ResultInterrupted {
						result = PeerCanceled(ctx)
					}
					return []Result{result}
				}
				var observed ItemOutcome
				callbacks := 0
				runner.config.OnTerminal = func(_ int, outcome ItemOutcome) error {
					callbacks++
					observed = outcome
					return nil
				}
				metadata := []retryItem{{attempts: scenario.attempts}}
				if scenario.attempts != 0 {
					metadata[0].lastErr = previous
				}
				var timer retryTimer
				runner.processHeldBatch(
					0,
					&runner.accounting.workerOutcomes[0],
					[]int{1},
					make([]ItemOutcome, 1),
					metadata,
					&timer,
				)
				timer.stop()
				assert.Equal(t, 1, callbacks)
				assert.Equal(t, scenario.terminal, observed.State)
				assert.Equal(t, scenario.finalAttempts, observed.Attempts)
				switch scenario.terminal {
				case TerminalSucceeded:
					assert.Nil(t, observed.Err)
				case TerminalFailed:
					assert.Same(t, failure, observed.Err)
				case TerminalAbandoned:
					expected := cause
					if scenario.attempts != 0 {
						expected = previous
					}
					if scenario.state == ResultFailed {
						expected = failure
					}
					assert.Same(t, expected, observed.Err)
				}
				assert.Equal(t, scenario.policyCalls, policyCalls)
				summary := runner.Summary()
				retained := min(limit, scenario.failures)
				require.Len(t, summary.Failures, retained)
				assert.Equal(t, uint64(scenario.failures-retained), summary.OmittedFailures)
				if retained != 0 {
					assert.Equal(t, FailureRecord{
						Boundary: BoundaryHandler, Worker: 0, Err: failure,
					}, summary.Failures[0])
				}
				if retained == 2 {
					assert.Equal(t, BoundaryRetryPolicy, summary.Failures[1].Boundary)
					assert.Equal(t, 0, summary.Failures[1].Worker)
				}
				assert.Equal(t, []retryItem{{}}, metadata)
			}
		})
	}
}

func retryRaceFixture(t *testing.T, limit int) *Runner[int] {
	t.Helper()
	queue, err := workq.New[int](8)
	require.NoError(t, err)
	runner := newRunnerState(queue, limit, 1, func() {})
	// Hold coordinator ownership while the test drives the worker's private pipeline.
	require.True(t, runner.registerSubmission())
	t.Cleanup(func() {
		runner.unregisterSubmission()
		runner.StopAdmission()
		_ = runner.Wait()
	})
	return runner
}

func TestAbortBeforeMixedRetryResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner := retryRaceFixture(t, 1)
		runner.config.FailureMode = Continue
		cause := errors.New("external abort")
		declined := errors.New("declined")
		exhausted := errors.New("exhausted")
		candidate := errors.New("candidate")
		entered := make(chan struct{})
		release := make(chan struct{})
		defer releaseTestBarrier(release)
		runner.config.Handler = func(ctx context.Context, _ []int) []Result {
			close(entered)
			<-release
			return []Result{
				{State: ResultFailed, Err: declined}, {State: ResultFailed, Err: exhausted},
				{State: ResultFailed, Err: candidate}, PeerCanceled(ctx),
				{State: ResultUnattempted}, {},
			}
		}
		var policyErrors []error
		runner.config.Retry = &RetryPolicy{MaxAttempts: 2,
			Delay: func(_ int, err error) (time.Duration, bool) {
				assertChannelClosed(t, runner.Aborting())
				policyErrors = append(policyErrors, err)
				return time.Hour, !errors.Is(err, declined)
			},
		}
		outcomes := make(map[int]ItemOutcome)
		runner.config.OnTerminal = func(item int, outcome ItemOutcome) error {
			outcomes[item] = outcome
			return nil
		}
		metadata := make([]retryItem, 6)
		metadata[1].attempts = 1
		finished := make(chan struct{})
		go func() {
			var timer retryTimer
			runner.processHeldBatch(
				0,
				&runner.accounting.workerOutcomes[0],
				[]int{1, 2, 3, 4, 5, 6},
				make([]ItemOutcome, 6),
				metadata,
				&timer,
			)
			timer.stop()
			close(finished)
		}()
		<-entered
		runner.Abort(cause)
		close(release)
		<-finished
		assert.Equal(t, []error{declined, candidate}, policyErrors)
		assert.Equal(t, map[int]ItemOutcome{
			1: {State: TerminalFailed, Attempts: 1, Err: declined},
			2: {State: TerminalFailed, Attempts: 2, Err: exhausted},
			3: {State: TerminalAbandoned, Attempts: 1, Err: candidate},
			4: {State: TerminalAbandoned, Attempts: 1, Err: cause},
			5: {State: TerminalAbandoned, Err: cause},
			6: {State: TerminalSucceeded, Attempts: 1},
		}, outcomes)
		summary := runner.Summary()
		assert.Equal(t, uint64(1), summary.OmittedFailures)
		require.Len(t, summary.Failures, 1)
		assert.Same(t, declined, summary.Failures[0].Err)
		assert.Same(t, cause, summary.AbortCause)
	})
}

func TestAbortDuringClassificationPreservesEveryFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, trigger := range []string{"policy abort", "negative delay", "terminal failure"} {
			runner := retryRaceFixture(t, 2)
			runner.config.FailureMode = Continue
			first := errors.New("first")
			second := errors.New("second")
			third := errors.New("third")
			cause := errors.New("abort in policy")
			var policyErrors []error
			runner.config.Retry = &RetryPolicy{
				MaxAttempts: 3,
				Delay: func(_ int, err error) (time.Duration, bool) {
					policyErrors = append(policyErrors, err)
					if len(policyErrors) == 1 {
						switch trigger {
						case "policy abort":
							runner.Abort(cause)
						case "negative delay":
							return -time.Second, false
						case "terminal failure":
							return 0, false
						}
					} else {
						assertChannelClosed(t, runner.Aborting())
						assert.ErrorIs(t, runner.ctx.Err(), context.Canceled)
					}
					return time.Hour, true
				},
			}
			if trigger == "terminal failure" {
				runner.config.FailureMode = Abort
			}
			runner.config.Handler = func(context.Context, []int) []Result {
				return []Result{
					{State: ResultFailed, Err: first}, {State: ResultFailed, Err: second},
					{State: ResultFailed, Err: third}, {},
				}
			}
			outcomes := make(map[int]ItemOutcome)
			runner.config.OnTerminal = func(item int, outcome ItemOutcome) error {
				assert.Equal(t, []error{first, second, third}, policyErrors)
				outcomes[item] = outcome
				return nil
			}
			var timer retryTimer
			runner.processHeldBatch(
				0,
				&runner.accounting.workerOutcomes[0],
				[]int{1, 2, 3, 4},
				make([]ItemOutcome, 4),
				make([]retryItem, 4),
				&timer,
			)
			timer.stop()
			expectedState := TerminalAbandoned
			failures := 0
			if trigger != "policy abort" {
				expectedState = TerminalFailed
				failures = 1
			}
			if trigger == "negative delay" {
				failures = 2
			}
			assert.Equal(t, ItemOutcome{State: expectedState, Attempts: 1, Err: first}, outcomes[1])
			assert.Equal(t, ItemOutcome{State: TerminalAbandoned, Attempts: 1, Err: second}, outcomes[2])
			assert.Equal(t, ItemOutcome{State: TerminalAbandoned, Attempts: 1, Err: third}, outcomes[3])
			assert.Equal(t, ItemOutcome{State: TerminalSucceeded, Attempts: 1}, outcomes[4])
			assert.Len(t, runner.Summary().Failures, failures)
			assert.Zero(t, runner.Summary().OmittedFailures)
		}
	})
}

func TestRequiredAbortPublishesBeforeNextPolicy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner := retryRaceFixture(t, 4)
		failure := errors.New("declined")
		later := errors.New("later candidate")
		cancelEntered := make(chan struct{})
		cancelRelease := make(chan struct{})
		defer releaseTestBarrier(cancelRelease)
		cancel := runner.cancel
		runner.cancel = func() {
			close(cancelEntered)
			<-cancelRelease
			cancel()
		}
		calls := 0
		runner.config.Retry = &RetryPolicy{MaxAttempts: 2,
			Delay: func(int, error) (time.Duration, bool) {
				calls++
				if calls == 1 {
					return 0, false
				}
				assertChannelClosed(t, runner.Aborting())
				assert.ErrorIs(t, runner.ctx.Err(), context.Canceled)
				return 0, true
			},
		}
		runner.config.Handler = func(context.Context, []int) []Result {
			return []Result{{State: ResultFailed, Err: failure}, {State: ResultFailed, Err: later}}
		}
		finished := make(chan struct{})
		go func() {
			var timer retryTimer
			runner.processHeldBatch(
				0,
				&runner.accounting.workerOutcomes[0],
				[]int{1, 2},
				make([]ItemOutcome, 2),
				make([]retryItem, 2),
				&timer,
			)
			timer.stop()
			close(finished)
		}()
		<-cancelEntered
		synctest.Wait()
		assert.Equal(t, 1, calls)
		assertChannelOpen(t, runner.Aborting())
		assertChannelOpen(t, finished)
		require.Len(t, runner.Summary().Failures, 1)
		runner.cancel = cancel
		close(cancelRelease)
		<-finished
		assert.Equal(t, 2, calls)
	})
}

func TestCallbackAbortAbandonsOnlyRetryCandidates(t *testing.T) {
	for _, callbackFailure := range []bool{false, true} {
		runner := retryRaceFixture(t, 1)
		runner.config.FailureMode = Continue
		declined := errors.New("declined")
		candidate := errors.New("candidate")
		cleanup := errors.New("callback failure")
		calls := 0
		runner.config.Retry = &RetryPolicy{MaxAttempts: 2,
			Delay: func(_ int, err error) (time.Duration, bool) {
				calls++
				return time.Hour, !errors.Is(err, declined)
			},
		}
		runner.config.Handler = func(context.Context, []int) []Result {
			return []Result{{}, {State: ResultFailed, Err: declined},
				{State: ResultFailed, Err: candidate}, {}}
		}
		outcomes := make(map[int]ItemOutcome)
		runner.config.OnTerminal = func(item int, outcome ItemOutcome) error {
			assert.Equal(t, 2, calls)
			outcomes[item] = outcome
			if item != 1 {
				return nil
			}
			if callbackFailure {
				return cleanup
			}
			runner.Abort(cleanup)
			return nil
		}
		var timer retryTimer
		runner.processHeldBatch(
			0,
			&runner.accounting.workerOutcomes[0],
			[]int{1, 2, 3, 4},
			make([]ItemOutcome, 4),
			make([]retryItem, 4),
			&timer,
		)
		timer.stop()
		assert.Equal(t, map[int]ItemOutcome{
			1: {State: TerminalSucceeded, Attempts: 1},
			2: {State: TerminalFailed, Attempts: 1, Err: declined},
			3: {State: TerminalAbandoned, Attempts: 1, Err: candidate},
			4: {State: TerminalSucceeded, Attempts: 1},
		}, outcomes)
		summary := runner.Summary()
		require.Len(t, summary.Failures, 1)
		assert.Same(t, declined, summary.Failures[0].Err)
		expectedOmitted := uint64(0)
		if callbackFailure {
			expectedOmitted = 1
		}
		assert.Equal(t, expectedOmitted, summary.OmittedFailures)
		assert.Same(t, cleanup, summary.AbortCause)
	}
}

func TestMalformedRetryResponseNeverCallsPolicy(t *testing.T) {
	failure := errors.New("ordinary failure")
	for _, results := range [][]Result{
		{{State: ResultFailed, Err: failure}},
		{{State: ResultFailed, Err: failure}, {State: ResultState(255)}},
		{{State: ResultFailed, Err: failure}, {State: ResultSucceeded, Err: failure}},
		{{State: ResultFailed, Err: failure}, {State: ResultUnattempted}},
		{{State: ResultFailed, Err: failure}, {State: ResultInterrupted, Err: context.Canceled}},
	} {
		runner := retryRaceFixture(t, 1)
		runner.config.Handler = func(context.Context, []int) []Result { return results }
		runner.config.Retry = &RetryPolicy{MaxAttempts: 2,
			Delay: func(int, error) (time.Duration, bool) {
				t.Error("malformed response invoked policy")
				return 0, true
			},
		}
		callbacks := 0
		runner.config.OnTerminal = func(_ int, outcome ItemOutcome) error {
			callbacks++
			assertChannelClosed(t, runner.Aborting())
			assert.Equal(t, TerminalFailed, outcome.State)
			assert.Equal(t, 1, outcome.Attempts)
			assert.Same(t, runner.AbortCause(), outcome.Err)
			return nil
		}
		var timer retryTimer
		runner.processHeldBatch(
			0,
			&runner.accounting.workerOutcomes[0],
			[]int{1, 2},
			make([]ItemOutcome, 2),
			make([]retryItem, 2),
			&timer,
		)
		timer.stop()
		assert.Equal(t, 2, callbacks)
		summary := runner.Summary()
		require.Len(t, summary.Failures, 1)
		assert.Equal(t, BoundaryResultValidation, summary.Failures[0].Boundary)
		assert.Zero(t, summary.OmittedFailures)
	}
}

func assertChannelClosed(t *testing.T, channel <-chan struct{}) {
	t.Helper()
	select {
	case <-channel:
	default:
		t.Error("channel is still open")
	}
}

func TestRetryCompactionReleasesReferencesAndLatestErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, callbacks := range []bool{false, true} {
			for _, abort := range []bool{false, true} {
				queue, err := workq.New[*retainedPayload](4)
				require.NoError(t, err)
				runner := newRunnerState(queue, 4, 1, func() {})
				require.True(t, runner.registerSubmission())
				defer func() {
					runner.unregisterSubmission()
					runner.StopAdmission()
					_ = runner.Wait()
				}()
				runner.config.FailureMode = Continue
				first := errors.New("obsolete error")
				latest := errors.New("latest error")
				batch := []*retainedPayload{{value: 1}, {value: 2}, {value: 3}, {value: 4}}
				outcomes := make([]ItemOutcome, 4)
				metadata := make([]retryItem, 4)
				var timer retryTimer
				var invocations atomic.Int32
				runner.config.Handler = func(_ context.Context, due []*retainedPayload) []Result {
					switch invocations.Add(1) {
					case 1:
						return []Result{{}, {State: ResultFailed, Err: first}, {},
							{State: ResultFailed, Err: first}}
					case 2:
						assert.Equal(t, []*retainedPayload{{value: 2}}, due)
						assert.Equal(t, []*retainedPayload{nil, nil}, batch[2:])
						assert.Equal(t, []retryItem{{}, {}}, metadata[2:])
						assert.Equal(t, make([]ItemOutcome, 4), outcomes)
						return []Result{{State: ResultFailed, Err: latest}}
					default:
						for position, item := range due {
							expected := first
							if item.value == 2 {
								expected = latest
							}
							assert.Same(t, expected, metadata[position].lastErr)
						}
						return nil
					}
				}
				policyCalls := 0
				runner.config.Retry = &RetryPolicy{MaxAttempts: 3,
					Delay: func(_ int, _ error) (time.Duration, bool) {
						policyCalls++
						if policyCalls == 1 {
							return time.Second, true
						}
						return time.Hour, true
					},
				}
				observed := make(map[int]ItemOutcome)
				if callbacks {
					runner.config.OnTerminal = func(item *retainedPayload, outcome ItemOutcome) error {
						observed[item.value] = outcome
						return nil
					}
				}
				finished := make(chan struct{})
				defer func() {
					runner.Abort(nil)
					<-finished
					timer.stop()
				}()
				go func() {
					runner.processHeldBatch(
						0,
						&runner.accounting.workerOutcomes[0],
						batch,
						outcomes,
						metadata,
						&timer,
					)
					close(finished)
				}()
				synctest.Wait()
				require.Equal(t, int32(1), invocations.Load())
				require.NotNil(t, timer.timer)
				time.Sleep(time.Second)
				synctest.Wait()
				require.Equal(t, int32(2), invocations.Load())
				if abort {
					runner.Abort(nil)
				} else {
					time.Sleep(time.Hour)
				}
				<-finished
				if callbacks {
					assert.Equal(t, ItemOutcome{State: TerminalSucceeded, Attempts: 1}, observed[1])
					assert.Equal(t, ItemOutcome{State: TerminalSucceeded, Attempts: 1}, observed[3])
					if abort {
						assert.Equal(t, ItemOutcome{
							State: TerminalAbandoned, Attempts: 2, Err: latest,
						}, observed[2])
						assert.Equal(t, ItemOutcome{
							State: TerminalAbandoned, Attempts: 1, Err: first,
						}, observed[4])
					} else {
						assert.Equal(t, ItemOutcome{State: TerminalSucceeded, Attempts: 3}, observed[2])
						assert.Equal(t, ItemOutcome{State: TerminalSucceeded, Attempts: 2}, observed[4])
					}
				}
				assert.Equal(t, make([]*retainedPayload, 4), batch)
				assert.Equal(t, make([]ItemOutcome, 4), outcomes)
				assert.Equal(t, make([]retryItem, 4), metadata)
				assert.False(t, timer.timer.Stop(), "timer remained active after cancellation")
				assert.Empty(t, runner.Summary().Failures)
				assert.Zero(t, runner.Summary().OmittedFailures)
			}
		}
	})
}

func TestRetryExhaustionCancelsPeerBeforeSuccessCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(2)
		config.Capacity = 8
		config.Queue.MaxReceiveBatch = 4
		resourceHeld := make(chan struct{})
		resourceReleased := make(chan struct{})
		blockerEntered := make(chan struct{})
		blockerRelease := make(chan struct{})
		callbackEntered := make(chan struct{})
		callbackRelease := make(chan struct{})
		failure := errors.New("retry attempt")
		exhausted := errors.New("exhausted attempt")
		var mu sync.Mutex
		attempts := make(map[int]int)
		config.Handler = func(ctx context.Context, batch []int) []Result {
			if batch[0] == 0 {
				close(resourceHeld)
				<-ctx.Done()
				close(resourceReleased)
				return []Result{PeerCanceled(ctx)}
			}
			if batch[0] == -1 {
				close(blockerEntered)
				<-blockerRelease
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			results := make([]Result, len(batch))
			for position, item := range batch {
				attempts[item]++
				if item == 1 && attempts[item] == 2 {
					continue
				}
				err := failure
				if item == 3 && attempts[item] == 3 {
					err = exhausted
				}
				results[position] = Result{State: ResultFailed, Err: err}
			}
			return results
		}
		policyCalls := 0
		config.Retry = &RetryPolicy{MaxAttempts: 3,
			Delay: func(attempt int, _ error) (time.Duration, bool) {
				mu.Lock()
				defer mu.Unlock()
				policyCalls++
				if attempt == 2 {
					return time.Second, true
				}
				if policyCalls == 3 {
					return time.Second, true
				}
				return 2 * time.Second, true
			},
		}
		var runner *Runner[int]
		outcomes := make(map[int]ItemOutcome)
		config.OnTerminal = func(item int, outcome ItemOutcome) error {
			if item == 1 {
				assertChannelClosed(t, runner.Aborting())
				assert.Same(t, exhausted, runner.AbortCause())
				assert.ErrorIs(t, runner.ctx.Err(), context.Canceled)
				summary := runner.Summary()
				if assert.Len(t, summary.Failures, 1) {
					assert.Same(t, exhausted, summary.Failures[0].Err)
				}
				close(callbackEntered)
				<-resourceReleased
				<-callbackRelease
			}
			mu.Lock()
			outcomes[item] = outcome
			mu.Unlock()
			return nil
		}
		var err error
		runner, err = New(config)
		require.NoError(t, err)
		defer func() {
			runner.Abort(nil)
			releaseTestBarrier(blockerRelease)
			releaseTestBarrier(callbackRelease)
			_ = runner.Wait()
		}()
		require.NoError(t, runner.Submit(context.Background(), 0))
		<-resourceHeld
		require.NoError(t, runner.Submit(context.Background(), -1))
		<-blockerEntered
		count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, 3, count)
		close(blockerRelease)
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		time.Sleep(time.Second)
		<-callbackEntered
		<-resourceReleased
		assertChannelOpen(t, runner.Done())
		close(callbackRelease)
		require.ErrorIs(t, runner.Wait(), exhausted)
		assert.Equal(t, map[int]ItemOutcome{
			-1: {State: TerminalSucceeded, Attempts: 1},
			0:  {State: TerminalAbandoned, Attempts: 1, Err: exhausted},
			1:  {State: TerminalSucceeded, Attempts: 2},
			2:  {State: TerminalAbandoned, Attempts: 2, Err: failure},
			3:  {State: TerminalFailed, Attempts: 3, Err: exhausted},
		}, outcomes)
		assert.Equal(t, 5, policyCalls)
		summary := runner.Summary()
		assert.Equal(t, uint64(5), summary.Accepted)
		assert.Equal(t, uint64(2), summary.Succeeded)
		assert.Equal(t, uint64(1), summary.Failed)
		assert.Equal(t, uint64(2), summary.Abandoned)
		assert.Equal(t, uint64(9), summary.Attempts)
		assert.Zero(t, summary.OmittedFailures)
	})
}

func TestStartedRetrySuccessSurvivesAbort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(1)
		failure := errors.New("first attempt")
		cause := errors.New("abort after retry starts")
		entered := make(chan struct{})
		release := make(chan struct{})
		defer releaseTestBarrier(release)
		attempts := 0
		config.Handler = func(context.Context, []int) []Result {
			attempts++
			if attempts == 1 {
				return []Result{{State: ResultFailed, Err: failure}}
			}
			close(entered)
			<-release
			return nil
		}
		policyCalls := 0
		config.Retry = &RetryPolicy{MaxAttempts: 2,
			Delay: func(int, error) (time.Duration, bool) {
				policyCalls++
				return 0, true
			},
		}
		var outcomes []ItemOutcome
		config.OnTerminal = func(_ int, outcome ItemOutcome) error {
			outcomes = append(outcomes, outcome)
			return nil
		}
		runner, err := New(config)
		require.NoError(t, err)
		defer func() {
			runner.Abort(nil)
			releaseTestBarrier(release)
			_ = runner.Wait()
		}()
		require.NoError(t, runner.Submit(context.Background(), 1))
		<-entered
		runner.Abort(cause)
		assertChannelOpen(t, runner.Done())
		close(release)
		require.Same(t, cause, runner.Wait())
		assert.Equal(t, 1, policyCalls)
		assert.Equal(t, []ItemOutcome{{State: TerminalSucceeded, Attempts: 2}}, outcomes)
		assert.Equal(t, Summary{
			Accepted: 1, Succeeded: 1, Attempts: 2, AbortCause: cause,
		}, runner.Summary())
	})
}

func TestPeerAbortStillClassifiesOrdinaryFailure(t *testing.T) {
	for _, retryPeer := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			config := runtimeConfig(2)
			config.Queue.MaxReceiveBatch = 1
			config.ErrorLimit = 1
			peerEntered := make(chan struct{})
			primary := errors.New("primary failure")
			peer := errors.New("ordinary failure after cancellation")
			config.Handler = func(ctx context.Context, batch []int) []Result {
				if batch[0] == 0 {
					close(peerEntered)
					<-ctx.Done()
					return []Result{{State: ResultFailed, Err: peer}}
				}
				return []Result{{State: ResultFailed, Err: primary}}
			}
			var mu sync.Mutex
			policyErrors := make(map[error]int)
			config.Retry = &RetryPolicy{MaxAttempts: 2,
				Delay: func(_ int, err error) (time.Duration, bool) {
					mu.Lock()
					defer mu.Unlock()
					policyErrors[err]++
					return 0, errors.Is(err, peer) && retryPeer
				},
			}
			outcomes := make(map[int]ItemOutcome)
			config.OnTerminal = func(item int, outcome ItemOutcome) error {
				mu.Lock()
				outcomes[item] = outcome
				mu.Unlock()
				return nil
			}
			runner, err := New(config)
			require.NoError(t, err)
			joinRunnerOnCleanup(t, runner)
			require.NoError(t, runner.Submit(context.Background(), 0))
			<-peerEntered
			require.NoError(t, runner.Submit(context.Background(), 1))
			require.ErrorIs(t, runner.Wait(), primary)
			assert.Equal(t, map[error]int{primary: 1, peer: 1}, policyErrors)
			state := TerminalFailed
			omitted := uint64(1)
			if retryPeer {
				state = TerminalAbandoned
				omitted = 0
			}
			assert.Equal(t, ItemOutcome{State: state, Attempts: 1, Err: peer}, outcomes[0])
			assert.Equal(t, ItemOutcome{State: TerminalFailed, Attempts: 1, Err: primary}, outcomes[1])
			summary := runner.Summary()
			require.Len(t, summary.Failures, 1)
			assert.Same(t, primary, summary.Failures[0].Err)
			assert.Equal(t, omitted, summary.OmittedFailures)
			assert.Same(t, primary, summary.AbortCause)
		})
	}
}

func TestNextCallbackWaitsForCommittedAbort(t *testing.T) {
	for _, scenario := range []string{"peer abort", "callback error"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				runner := retryRaceFixture(t, 4)
				firstEntered := make(chan struct{})
				firstRelease := make(chan struct{})
				secondEntered := make(chan struct{})
				cancelEntered := make(chan struct{})
				cancelRelease := make(chan struct{})
				processed := make(chan struct{})
				var owners sync.WaitGroup
				defer func() {
					releaseTestBarrier(firstRelease)
					releaseTestBarrier(cancelRelease)
					owners.Wait()
				}()
				cancel := runner.cancel
				var firstCancel atomic.Bool
				runner.cancel = func() {
					if firstCancel.CompareAndSwap(false, true) {
						close(cancelEntered)
						<-cancelRelease
					}
					cancel()
				}
				cause := errors.New("abort before second callback")
				var handlerCtx context.Context
				runner.config.Handler = func(ctx context.Context, batch []int) []Result {
					handlerCtx = ctx
					assert.Equal(t, []int{1, 2}, batch)
					return []Result{{State: ResultSucceeded}, {State: ResultSucceeded}}
				}
				var callbacks []int
				runner.config.OnTerminal = func(item int, outcome ItemOutcome) error {
					callbacks = append(callbacks, item)
					assert.Equal(t, TerminalSucceeded, outcome.State)
					if item == 1 {
						close(firstEntered)
						<-firstRelease
						if scenario == "callback error" {
							return cause
						}
						return nil
					}
					close(secondEntered)
					assertChannelClosed(t, runner.Aborting())
					assert.ErrorIs(t, handlerCtx.Err(), context.Canceled)
					return nil
				}
				owners.Go(func() {
					runner.processBatch(0, []int{1, 2}, make([]ItemOutcome, 2))
					close(processed)
				})
				<-firstEntered
				if scenario == "peer abort" {
					owners.Go(func() { runner.Abort(cause) })
					<-cancelEntered
				}
				close(firstRelease)
				if scenario == "callback error" {
					<-cancelEntered
				}
				synctest.Wait()
				assert.Same(t, cause, runner.AbortCause())
				assert.NoError(t, handlerCtx.Err())
				assertChannelOpen(t, handlerCtx.Done())
				assertChannelOpen(t, runner.Aborting())
				assertChannelOpen(t, runner.Done())
				assertChannelOpen(t, secondEntered)
				assertChannelOpen(t, processed)
				close(cancelRelease)
				owners.Wait()
				assertChannelClosed(t, secondEntered)
				assert.Equal(t, []int{1, 2}, callbacks)
				assert.Equal(t, uint64(2), runner.Summary().Succeeded)
				if scenario == "callback error" {
					require.Len(t, runner.Summary().Failures, 1)
					assert.Equal(t, FailureRecord{
						Boundary: BoundaryTerminalCallback, Worker: 0, Err: cause,
					}, runner.Summary().Failures[0])
				} else {
					assert.Empty(t, runner.Summary().Failures)
				}
			})
		})
	}
}
