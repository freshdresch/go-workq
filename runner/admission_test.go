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

	"github.com/freshdresch/go-data-structures/workq"
)

type submissionResult struct {
	accepted int
	err      error
}

// The fixture owns the only consumer. It starts after the coordinator closes the queue.
func admissionFixture(t *testing.T, capacity int) (*Runner[int], *[]int) {
	t.Helper()
	queue, err := workq.New[int](
		capacity,
		workq.WithSingleConsumer(),
		workq.WithProducerWait(workq.WaitStrategy{}),
		workq.WithConsumerWait(workq.WaitStrategy{}),
	)
	require.NoError(t, err)
	var received []int
	runner := newRunnerState(queue, 16, 0, func() {
		queue.Close()
		buffer := make([]int, capacity)
		for {
			count, err := queue.Receive(context.Background(), buffer)
			if errors.Is(err, workq.ErrClosed) {
				return
			}
			if err != nil {
				t.Errorf("fixture receive: %v", err)
				return
			}
			received = append(received, buffer[:count]...)
			clear(buffer)
		}
	})
	return runner, &received
}

type admissionBarrierContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (ctx *admissionBarrierContext) Err() error {
	ctx.once.Do(func() { close(ctx.entered) })
	<-ctx.release
	return ctx.Context.Err()
}

func TestAdmissionCutoffJoinsAdmittedSubmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner, received := admissionFixture(t, 4)
		ctx := &admissionBarrierContext{
			Context: context.Background(),
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		defer releaseTestBarrier(ctx.release)
		result := make(chan error, 1)
		go func() { result <- runner.Submit(ctx, 7) }()
		<-ctx.entered
		runner.stopAdmission()
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		for _, submitCtx := range []context.Context{context.Background(), canceled} {
			for _, items := range [][]int{nil, {8, 9}} {
				count, err := runner.SubmitBatch(submitCtx, items)
				assert.Zero(t, count)
				assert.Same(t, ErrStopped, err)
			}
			assert.Same(t, ErrStopped, runner.Submit(submitCtx, 8))
		}
		synctest.Wait()
		assertChannelOpen(t, runner.lifecycle.done)
		require.NoError(t, runner.queue.TryPush(6))
		close(ctx.release)
		require.NoError(t, <-result)
		<-runner.lifecycle.done
		assert.Equal(t, []int{6, 7}, *received)
		assert.Equal(t, uint64(1), runner.accounting.snapshot(nil).Accepted)
	})
}

func TestAdmittedEmptyAndCanceledSubmissions(t *testing.T) {
	runner, received := admissionFixture(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, submitCtx := range []context.Context{context.Background(), ctx} {
		count, err := runner.SubmitBatch(submitCtx, nil)
		assert.Zero(t, count)
		require.NoError(t, err)
	}
	count, err := runner.SubmitBatch(ctx, []int{1, 2})
	assert.Zero(t, count)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, runner.Submit(ctx, 3), context.Canceled)
	runner.stopAdmission()
	<-runner.lifecycle.done
	assert.Empty(t, *received)
	assert.Empty(t, runner.accounting.snapshot(nil).Failures)
}

func TestPartialSubmissionCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner, received := admissionFixture(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		items := []int{10, 11, 12, 13}
		results := make(chan submissionResult, 1)
		go func() {
			count, err := runner.SubmitBatch(ctx, items)
			results <- submissionResult{count, err}
		}()
		synctest.Wait()
		cancel()
		result := <-results
		assert.Equal(t, 2, result.accepted)
		require.ErrorIs(t, result.err, context.Canceled)
		assert.Equal(t, []int{12, 13}, items[result.accepted:])
		runner.stopAdmission()
		<-runner.lifecycle.done
		assert.Equal(t, items[:result.accepted], *received)
		assert.Equal(t, uint64(2), runner.accounting.snapshot(nil).Accepted)
	})
}

type cancellationBarrierContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (ctx *cancellationBarrierContext) Err() error {
	err := ctx.Context.Err()
	if err == nil {
		return nil
	}
	ctx.once.Do(func() { close(ctx.entered) })
	<-ctx.release
	return err
}

func TestCancellationRegistrationJoinPrecedesUnregistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner, _ := admissionFixture(t, 1)
		require.NoError(t, runner.Submit(context.Background(), 1))
		callerCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx := &cancellationBarrierContext{
			Context: callerCtx,
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		defer releaseTestBarrier(ctx.release)
		returned := make(chan error, 1)
		go func() { returned <- runner.Submit(ctx, 2) }()
		synctest.Wait()
		cancel()
		<-ctx.entered
		// Abort releases the queue call while the caller's cancellation callback is held.
		runner.abort(nil)
		synctest.Wait()
		assertChannelOpen(t, runner.lifecycle.done)
		select {
		case err := <-returned:
			t.Fatalf("submission returned before its cancellation callback joined: %v", err)
		default:
		}
		assert.Equal(t, 1, runner.lifecycle.active)
		require.ErrorIs(t, runner.queue.TryPush(3), workq.ErrFull)
		close(ctx.release)
		require.ErrorIs(t, <-returned, context.Canceled)
		<-runner.lifecycle.done
	})
}

func TestUnexpectedPushClosureAttribution(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed before push", true: "closed during push"}[partial],
			func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					runner, _ := admissionFixture(t, 2)
					results := make(chan submissionResult, 1)
					if !partial {
						runner.queue.Close()
					}
					go func() {
						count, err := runner.SubmitBatch(context.Background(), []int{1, 2, 3})
						results <- submissionResult{count, err}
					}()
					if partial {
						synctest.Wait()
						// Only this fixture violates the queue's producer/Close rule.
						runner.queue.Close()
					}
					result := <-results
					if partial {
						assert.Equal(t, 2, result.accepted)
						require.ErrorIs(t, result.err, workq.ErrClosedDuringPush)
						assert.Contains(t, result.err.Error(), "closed during admitted push")
					} else {
						assert.Zero(t, result.accepted)
						require.ErrorIs(t, result.err, workq.ErrClosed)
						assert.NotErrorIs(t, result.err, workq.ErrClosedDuringPush)
					}
					<-runner.lifecycle.done
					summary := runner.accounting.snapshot(runner.AbortCause())
					require.Len(t, summary.Failures, 1)
					assert.Equal(t, BoundaryQueueOperation, summary.Failures[0].Boundary)
					assert.Equal(t, -1, summary.Failures[0].Worker)
					assert.ErrorIs(t, summary.AbortCause, result.err)
					assert.ErrorIs(t, runner.lifecycle.finalErr, result.err)
				})
			})
	}
}

func TestConcurrentSubmissionsAtPublicAdmissionCutoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(2)
		var mu sync.Mutex
		terminal := make(map[int]int)
		config.OnTerminal = func(item int, _ ItemOutcome) error {
			mu.Lock()
			defer mu.Unlock()
			terminal[item]++
			return nil
		}
		runner, err := New(config)
		require.NoError(t, err)
		var submitters sync.WaitGroup
		defer func() {
			runner.Abort(nil)
			submitters.Wait()
			_ = runner.Wait()
		}()
		start := make(chan struct{})
		type result struct {
			item int
			err  error
		}
		results := make(chan result, 32)
		for item := range 32 {
			submitters.Go(func() {
				<-start
				results <- result{item: item, err: runner.Submit(context.Background(), item)}
			})
		}
		submitters.Go(func() {
			<-start
			runner.StopAdmission()
		})
		close(start)
		submitters.Wait()
		require.NoError(t, runner.Wait())
		var accepted uint64
		for range 32 {
			result := <-results
			if result.err == nil {
				accepted++
				assert.Equal(t, 1, terminal[result.item])
				continue
			}
			assert.Same(t, ErrStopped, result.err)
			assert.Zero(t, terminal[result.item])
		}
		assert.Equal(t, accepted, runner.Summary().Accepted)
		assert.Equal(t, accepted, runner.Summary().Succeeded)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, items := range [][]int{nil, {100}} {
			count, err := runner.SubmitBatch(ctx, items)
			assert.Zero(t, count)
			assert.Same(t, ErrStopped, err)
		}
		assert.Nil(t, runner.AbortCause())
		assertChannelOpen(t, runner.Aborting())
	})
}

// Hiding cancellation internals makes context.AfterFunc use this observable registration path.
type registrationTrackingContext struct {
	context.Context
	mu      sync.Mutex
	active  int
	started int
}

func (*registrationTrackingContext) Value(any) any {
	return nil
}

func (ctx *registrationTrackingContext) AfterFunc(callback func()) func() bool {
	ctx.mu.Lock()
	ctx.active++
	ctx.started++
	ctx.mu.Unlock()
	var released sync.Once
	release := func() {
		released.Do(func() {
			ctx.mu.Lock()
			ctx.active--
			ctx.mu.Unlock()
		})
	}
	stop := context.AfterFunc(ctx.Context, func() {
		defer release()
		callback()
	})
	return func() bool {
		stopped := stop()
		if stopped {
			release()
		}
		return stopped
	}
}

func (ctx *registrationTrackingContext) registrations() int {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.active
}

func TestSubmissionReleasesCallerCancellationRegistrations(t *testing.T) {
	for _, scenario := range []string{"success", "caller cancellation", "abort"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				runner, _ := admissionFixture(t, 1)
				caller, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx := &registrationTrackingContext{Context: caller}
				if scenario == "success" {
					require.NoError(t, runner.Submit(ctx, 1))
					assert.Zero(t, ctx.registrations())
				} else {
					require.NoError(t, runner.Submit(context.Background(), 1))
					returned := make(chan error, 1)
					go func() { returned <- runner.Submit(ctx, 2) }()
					synctest.Wait()
					assert.Equal(t, 1, ctx.registrations())
					if scenario == "abort" {
						runner.Abort(nil)
					} else {
						cancel()
					}
					require.ErrorIs(t, <-returned, context.Canceled)
					synctest.Wait()
					assert.Zero(t, ctx.registrations())
				}
				runner.StopAdmission()
				_ = runner.Wait()
				assert.Zero(t, runner.lifecycle.active)
				assert.Zero(t, ctx.registrations())
				// A caller may outlive completion. Its later cancellation has no owned work to invoke.
				cancel()
				synctest.Wait()
				assert.Zero(t, ctx.registrations())
			})
		})
	}
}

func (ctx *registrationTrackingContext) registrationsStarted() int {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.started
}

type noncancelableContext struct{}

func (noncancelableContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (noncancelableContext) Done() <-chan struct{}       { return nil }
func (noncancelableContext) Err() error                  { return nil }
func (noncancelableContext) Value(any) any               { return nil }

func TestNoncancelableSubmissionSkipsRegistrations(t *testing.T) {
	type valueKey struct{}
	contexts := map[string]context.Context{
		"Background":      context.Background(),
		"TODO":            context.TODO(),
		"value wrapper":   context.WithValue(context.Background(), valueKey{}, 1),
		"custom nil Done": noncancelableContext{},
	}
	for name, caller := range contexts {
		t.Run(name, func(t *testing.T) {
			runner, received := admissionFixture(t, 4)
			defer func() {
				runner.StopAdmission()
				_ = runner.Wait()
			}()
			runnerCtx := &registrationTrackingContext{Context: runner.ctx}
			runner.ctx = runnerCtx
			callerCtx := &registrationTrackingContext{Context: caller}
			count, err := runner.SubmitBatch(callerCtx, []int{1, 2})
			require.NoError(t, err)
			assert.Equal(t, 2, count)
			assert.Zero(t, callerCtx.registrationsStarted())
			assert.Zero(t, runnerCtx.registrationsStarted())
			runner.StopAdmission()
			require.NoError(t, runner.Wait())
			assert.Equal(t, []int{1, 2}, *received)
		})
	}
}

type submissionInspectionContext struct {
	context.Context
	errCheck  func() error
	doneCheck func() <-chan struct{}
}

func (ctx submissionInspectionContext) Err() error            { return ctx.errCheck() }
func (ctx submissionInspectionContext) Done() <-chan struct{} { return ctx.doneCheck() }

func TestSubmissionContextInspectionOrder(t *testing.T) {
	runner, received := admissionFixture(t, 4)
	defer func() {
		runner.StopAdmission()
		_ = runner.Wait()
	}()
	unexpected := submissionInspectionContext{
		Context: context.Background(),
		errCheck: func() error {
			t.Error("unexpected caller Err inspection")
			return nil
		},
		doneCheck: func() <-chan struct{} {
			t.Error("unexpected caller Done inspection")
			return nil
		},
	}
	count, err := runner.SubmitBatch(unexpected, nil)
	assert.Zero(t, count)
	require.NoError(t, err)
	errChecked := false
	checked := submissionInspectionContext{
		Context: context.Background(),
		errCheck: func() error {
			assert.Equal(t, 1, runner.lifecycle.active)
			errChecked = true
			return nil
		},
		doneCheck: func() <-chan struct{} {
			assert.True(t, errChecked)
			assert.Equal(t, 1, runner.lifecycle.active)
			return nil
		},
	}
	require.NoError(t, runner.Submit(checked, 1))
	checked.errCheck = func() error {
		assert.Equal(t, 1, runner.lifecycle.active)
		return context.Canceled
	}
	checked.doneCheck = unexpected.doneCheck
	require.ErrorIs(t, runner.Submit(checked, 2), context.Canceled)
	runner.StopAdmission()
	for _, items := range [][]int{nil, {3}} {
		count, err = runner.SubmitBatch(unexpected, items)
		assert.Zero(t, count)
		assert.Same(t, ErrStopped, err)
	}
	require.NoError(t, runner.Wait())
	assert.Equal(t, []int{1}, *received)
}

func TestPartialSubmissionCancellationRunsTerminalCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(1)
		config.Capacity = 2
		config.Queue.MaxReceiveBatch = 1
		entered := make(chan struct{})
		release := make(chan struct{})
		defer releaseTestBarrier(release)
		config.Handler = func(ctx context.Context, batch []int) []Result {
			if batch[0] == 10 {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return []Result{PeerCanceled(ctx)}
				}
			}
			return nil
		}
		outcomes := make(map[int][]ItemOutcome)
		config.OnTerminal = func(item int, outcome ItemOutcome) error {
			outcomes[item] = append(outcomes[item], outcome)
			return nil
		}
		runner, err := New(config)
		require.NoError(t, err)
		joinRunnerOnCleanup(t, runner)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		items := []int{10, 11, 12, 13, 14}
		results := make(chan submissionResult, 1)
		go func() {
			count, err := runner.SubmitBatch(ctx, items)
			results <- submissionResult{count, err}
		}()
		<-entered
		synctest.Wait()
		cancel()
		result := <-results
		require.ErrorIs(t, result.err, context.Canceled)
		require.Equal(t, 3, result.accepted)
		assert.Equal(t, []int{13, 14}, items[result.accepted:])
		close(release)
		runner.StopAdmission()
		require.NoError(t, runner.Wait())
		assert.Equal(t, Summary{Accepted: 3, Succeeded: 3, Attempts: 3}, runner.Summary())
		assert.Nil(t, runner.AbortCause())
		require.Len(t, outcomes, result.accepted)
		for _, item := range items[:result.accepted] {
			assert.Equal(t, []ItemOutcome{{
				State: TerminalSucceeded, Attempts: 1,
			}}, outcomes[item])
		}
		for _, item := range items[result.accepted:] {
			assert.Empty(t, outcomes[item])
		}
	})
}
