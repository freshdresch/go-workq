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
)

func assertChannelOpen(t *testing.T, channel <-chan struct{}) {
	t.Helper()
	select {
	case <-channel:
		t.Fatal("channel closed before its completion boundary")
	default:
	}
}

func TestAbortPublishesBeforeJoin(t *testing.T) {
	runner, _ := admissionFixture(t, 2)
	require.True(t, runner.registerSubmission())
	cause := errors.New("source failed")
	runner.abort(cause)
	<-runner.lifecycle.aborting
	assert.ErrorIs(t, runner.ctx.Err(), context.Canceled)
	assert.Same(t, cause, runner.lifecycle.abortCause)
	assertChannelOpen(t, runner.lifecycle.done)
	count, err := runner.SubmitBatch(context.Background(), nil)
	assert.Zero(t, count)
	assert.Same(t, ErrStopped, err)
	runner.unregisterSubmission()
	<-runner.lifecycle.done
	assert.Same(t, cause, runner.lifecycle.finalErr)
}

func TestConcurrentAbortsPreserveFirstCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner, _ := admissionFixture(t, 2)
		require.True(t, runner.registerSubmission())
		start := make(chan struct{})
		causes := []error{nil, errors.New("first candidate"), errors.New("second candidate")}
		var callers sync.WaitGroup
		for _, cause := range causes {
			callers.Go(func() {
				<-start
				runner.abort(cause)
			})
		}
		close(start)
		callers.Wait()
		<-runner.lifecycle.aborting
		cause := runner.lifecycle.abortCause
		require.NotNil(t, cause)
		assert.Contains(t, []error{ErrAborted, causes[1], causes[2]}, cause)
		runner.abort(errors.New("late candidate"))
		assert.Same(t, cause, runner.lifecycle.abortCause)
		assertChannelOpen(t, runner.lifecycle.done)
		runner.unregisterSubmission()
		<-runner.lifecycle.done
		assert.Same(t, cause, runner.lifecycle.finalErr)
	})
}

func TestCoordinatorAndCompletionWaitForAbortPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner, _ := admissionFixture(t, 4)
		entered := make(chan struct{})
		release := make(chan struct{})
		defer releaseTestBarrier(release)
		cancel := runner.cancel
		runner.cancel = func() {
			close(entered)
			<-release
			cancel()
		}
		abortReturned := make(chan struct{})
		go func() {
			runner.abort(nil)
			close(abortReturned)
		}()
		<-entered
		synctest.Wait()
		assertChannelOpen(t, runner.lifecycle.aborting)
		assertChannelOpen(t, runner.lifecycle.done)
		require.NoError(t, runner.queue.TryPush(1))
		// Restore the ordinary cleanup cancel before the coordinator can finish.
		runner.cancel = cancel
		close(release)
		<-abortReturned
		<-runner.lifecycle.done
		assert.ErrorIs(t, runner.lifecycle.finalErr, ErrAborted)
	})
}

func TestCompletionWinsBeforeLateAbort(t *testing.T) {
	for _, operationalFailure := range []bool{false, true} {
		runner, _ := admissionFixture(t, 2)
		failure := errors.New("continue failure")
		if operationalFailure {
			runner.accounting.recordFailure(FailureRecord{
				Boundary: BoundaryHandler,
				Worker:   0,
				Err:      failure,
			})
		}
		runner.stopAdmission()
		<-runner.lifecycle.done
		runner.abort(errors.New("too late"))
		assert.Nil(t, runner.lifecycle.abortCause)
		assertChannelOpen(t, runner.lifecycle.aborting)
		if operationalFailure {
			assert.ErrorIs(t, runner.lifecycle.finalErr, failure)
		} else {
			assert.NoError(t, runner.lifecycle.finalErr)
		}
	}
}

func TestBlockedSubmissionReportsCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner, _ := admissionFixture(t, 1)
		require.NoError(t, runner.Submit(context.Background(), 1))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		results := make(chan error, 1)
		go func() { results <- runner.Submit(ctx, 2) }()
		err := <-results
		runner.stopAdmission()
		<-runner.lifecycle.done
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestAbortReleasesBlockedSubmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner, received := admissionFixture(t, 1)
		results := make(chan submissionResult, 1)
		go func() {
			count, err := runner.SubmitBatch(context.Background(), []int{1, 2})
			results <- submissionResult{count, err}
		}()
		synctest.Wait()
		runner.abort(nil)
		result := <-results
		assert.Equal(t, 1, result.accepted)
		require.ErrorIs(t, result.err, context.Canceled)
		<-runner.lifecycle.done
		assert.Equal(t, []int{1}, *received)
		assert.Empty(t, runner.accounting.snapshot(nil).Failures)
	})
}

func TestOverlappingCallerDeadlineAndAbort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner, _ := admissionFixture(t, 1)
		require.NoError(t, runner.Submit(context.Background(), 1))
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
		defer cancel()
		results := make(chan error, 1)
		go func() { results <- runner.Submit(ctx, 2) }()
		go func() {
			time.Sleep(time.Second)
			runner.abort(nil)
		}()
		err := <-results
		assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
		<-runner.lifecycle.done
		assert.Empty(t, runner.accounting.snapshot(nil).Failures)
	})
}

func TestFinalPublicationWaitsForCommittedAbort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner, _ := admissionFixture(t, 2)
		finishEntered := make(chan struct{})
		finishRelease := make(chan struct{})
		defer releaseTestBarrier(finishRelease)
		finish := runner.lifecycle.finish
		runner.lifecycle.finish = func() {
			close(finishEntered)
			<-finishRelease
			finish()
		}
		runner.stopAdmission()
		<-finishEntered
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
			runner.abort(nil)
			close(abortReturned)
		}()
		<-cancelEntered
		close(finishRelease)
		synctest.Wait()
		assertChannelOpen(t, runner.lifecycle.done)
		assertChannelOpen(t, runner.lifecycle.aborting)
		close(cancelRelease)
		<-abortReturned
		<-runner.lifecycle.done
		assert.ErrorIs(t, runner.lifecycle.finalErr, ErrAborted)
	})
}

func releaseTestBarrier(barrier chan struct{}) {
	select {
	case <-barrier:
	default:
		close(barrier)
	}
}

func TestShutdownGraceCancellationStillJoins(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := map[bool]string{false: "explicit cancellation", true: "deadline"}[deadline]
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config := runtimeConfig(1)
				entered := make(chan context.Context, 1)
				release := make(chan struct{})
				config.Handler = func(ctx context.Context, _ []int) []Result {
					entered <- ctx
					<-release
					return nil
				}
				runner, err := New(config)
				require.NoError(t, err)
				var callers sync.WaitGroup
				defer func() {
					runner.Abort(nil)
					releaseTestBarrier(release)
					_ = runner.Wait()
					callers.Wait()
				}()
				require.NoError(t, runner.Submit(context.Background(), 1))
				handlerCtx := <-entered
				var grace context.Context
				var cancel context.CancelFunc
				if deadline {
					grace, cancel = context.WithTimeout(context.Background(), time.Second)
				} else {
					grace, cancel = context.WithCancel(context.Background())
				}
				defer cancel()
				joined := make(chan error, 1)
				callers.Go(func() { joined <- runner.Shutdown(grace) })
				synctest.Wait()
				if !deadline {
					cancel()
				}
				<-runner.Aborting()
				synctest.Wait()
				assert.ErrorIs(t, runner.AbortCause(), grace.Err())
				assert.ErrorIs(t, handlerCtx.Err(), context.Canceled)
				assertChannelOpen(t, runner.Done())
				select {
				case err := <-joined:
					t.Fatalf("Shutdown returned before handler release: %v", err)
				default:
				}
				close(release)
				assert.ErrorIs(t, <-joined, grace.Err())
				assert.ErrorIs(t, runner.Wait(), grace.Err())
				assert.Equal(t, uint64(1), runner.Summary().Succeeded)
				assert.Equal(t, runner.Aborting(), runner.Aborting())
				select {
				case <-runner.Aborting():
				default:
					t.Error("completed abort lost its early signal")
				}
			})
		})
	}
}

func TestMultipleShutdownCallersShareGlobalCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(1)
		entered := make(chan struct{})
		release := make(chan struct{})
		config.Handler = func(ctx context.Context, _ []int) []Result {
			close(entered)
			<-ctx.Done()
			<-release
			return []Result{PeerCanceled(ctx)}
		}
		runner, err := New(config)
		require.NoError(t, err)
		var callers sync.WaitGroup
		defer func() {
			runner.Abort(nil)
			releaseTestBarrier(release)
			_ = runner.Wait()
			callers.Wait()
		}()
		require.NoError(t, runner.Submit(context.Background(), 1))
		<-entered
		first, cancelFirst := context.WithCancel(context.Background())
		defer cancelFirst()
		later, cancelLater := context.WithTimeout(context.Background(), time.Hour)
		defer cancelLater()
		results := make(chan error, 3)
		for _, grace := range []context.Context{context.Background(), later, first} {
			callers.Go(func() { results <- runner.Shutdown(grace) })
		}
		synctest.Wait()
		cancelFirst()
		<-runner.Aborting()
		synctest.Wait()
		assert.Empty(t, results)
		assertChannelOpen(t, runner.Done())
		assert.Same(t, context.Canceled, runner.AbortCause())
		close(release)
		callers.Wait()
		for range 3 {
			assert.Same(t, context.Canceled, <-results)
		}
		assert.NoError(t, later.Err())
		assert.Equal(t, Summary{
			Accepted: 1, Abandoned: 1, Attempts: 1, AbortCause: context.Canceled,
		}, runner.Summary())
	})
}

func TestCompletedGracefulOutcomesIgnoreLateCancellation(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := map[bool]string{false: "success", true: "continue failure"}[failed]
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				failure := errors.New("completed operational failure")
				config := runtimeConfig(1)
				config.FailureMode = Continue
				if failed {
					config.Handler = WholeBatch(func(context.Context, []int) error {
						return failure
					})
				}
				runner, err := New(config)
				require.NoError(t, err)
				defer func() {
					runner.Abort(nil)
					_ = runner.Wait()
				}()
				signal := runner.Aborting()
				assert.Nil(t, runner.AbortCause())
				assertChannelOpen(t, signal)
				grace, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.NoError(t, runner.Submit(context.Background(), 1))
				outcome := runner.Shutdown(grace)
				if failed {
					assert.ErrorIs(t, outcome, failure)
				} else {
					assert.NoError(t, outcome)
				}
				before := runner.Summary()
				<-grace.Done()
				runner.Abort(errors.New("late explicit abort"))
				assert.Equal(t, outcome, runner.Shutdown(grace))
				assert.Equal(t, outcome, runner.Wait())
				assert.Equal(t, before, runner.Summary())
				assert.Nil(t, runner.AbortCause())
				assert.Equal(t, signal, runner.Aborting())
				assertChannelOpen(t, signal)
			})
		})
	}
}

func TestShutdownCompletionOverlapsGraceCancellation(t *testing.T) {
	for range 20 {
		synctest.Test(t, func(t *testing.T) {
			config := runtimeConfig(1)
			entered := make(chan struct{})
			release := make(chan struct{})
			config.Handler = func(context.Context, []int) []Result {
				close(entered)
				<-release
				return nil
			}
			runner, err := New(config)
			require.NoError(t, err)
			var shutdown sync.WaitGroup
			defer func() {
				runner.Abort(nil)
				releaseTestBarrier(release)
				_ = runner.Wait()
				shutdown.Wait()
			}()
			require.NoError(t, runner.Submit(context.Background(), 1))
			<-entered
			grace, cancel := context.WithCancel(context.Background())
			defer cancel()
			joined := make(chan error, 1)
			shutdown.Go(func() { joined <- runner.Shutdown(grace) })
			synctest.Wait()
			start := make(chan struct{})
			var contenders sync.WaitGroup
			contenders.Go(func() {
				<-start
				cancel()
			})
			contenders.Go(func() {
				<-start
				close(release)
			})
			close(start)
			contenders.Wait()
			outcome := <-joined
			if outcome == nil {
				assert.Nil(t, runner.AbortCause())
				assertChannelOpen(t, runner.Aborting())
			} else {
				assert.Same(t, context.Canceled, outcome)
				assert.Same(t, context.Canceled, runner.AbortCause())
				<-runner.Aborting()
			}
			assert.Equal(t, uint64(1), runner.Summary().Succeeded)
			assert.Equal(t, outcome, runner.Wait())
		})
	}
}

func TestConcurrentLifecycleCallsAndObservers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(1)
		entered := make(chan struct{})
		release := make(chan struct{})
		config.Handler = func(ctx context.Context, _ []int) []Result {
			close(entered)
			<-release
			return []Result{PeerCanceled(ctx)}
		}
		runner, err := New(config)
		require.NoError(t, err)
		var callers sync.WaitGroup
		defer func() {
			runner.Abort(nil)
			releaseTestBarrier(release)
			_ = runner.Wait()
			callers.Wait()
		}()
		require.NoError(t, runner.Submit(context.Background(), 1))
		<-entered
		signal, done := runner.Aborting(), runner.Done()
		assert.Nil(t, runner.AbortCause())
		grace, cancel := context.WithCancel(context.Background())
		cancel()
		explicit := errors.New("concurrent explicit abort")
		start := make(chan struct{})
		shutdown := make(chan error, 1)
		waited := make(chan error, 1)
		operations := []func(){
			runner.StopAdmission,
			func() { runner.Abort(explicit) },
			func() { shutdown <- runner.Shutdown(grace) },
			func() { waited <- runner.Wait() },
		}
		for _, operation := range operations {
			callers.Go(func() {
				<-start
				operation()
			})
		}
		for range 8 {
			callers.Go(func() {
				<-start
				for range 32 {
					assert.Equal(t, signal, runner.Aborting())
					assert.Equal(t, done, runner.Done())
					cause := runner.AbortCause()
					if cause != nil {
						assert.Contains(t, []error{explicit, context.Canceled}, cause)
					}
					_ = runner.Summary()
					_ = runner.Diagnostics()
				}
				<-signal
				cause := runner.AbortCause()
				<-done
				assert.Same(t, cause, runner.AbortCause())
				assert.Same(t, cause, runner.Wait())
			})
		}
		close(start)
		<-signal
		synctest.Wait()
		cause := runner.AbortCause()
		assert.Contains(t, []error{explicit, context.Canceled}, cause)
		assertChannelOpen(t, done)
		assert.Same(t, ErrStopped, runner.Submit(context.Background(), 2))
		close(release)
		callers.Wait()
		assert.Same(t, cause, <-shutdown)
		assert.Same(t, cause, <-waited)
		assert.Same(t, cause, runner.Summary().AbortCause)
		before := runner.Summary()
		runner.Abort(errors.New("abort after completed cancellation"))
		assert.Equal(t, before, runner.Summary())
		assert.Same(t, cause, runner.AbortCause())
		assert.Equal(t, signal, runner.Aborting())
		<-signal
	})
}
