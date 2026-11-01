package runner_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq"
	"github.com/freshdresch/go-data-structures/workq/runner"
)

func Example_finiteJob() {
	type job struct{ id int }
	var released atomic.Int64
	release := func(*job) { released.Add(1) }
	pool, err := runner.New(runner.Config[*job]{
		Capacity: 4,
		Workers:  2,
		Handler: runner.Items(func(_ context.Context, item *job) error {
			if item.id < 0 {
				return errors.New("invalid job")
			}
			return nil
		}),
		OnTerminal: func(item *job, _ runner.ItemOutcome) error {
			release(item)
			return nil
		},
	})
	if err != nil {
		panic(err)
	}
	jobs := []*job{{id: 1}, {id: 2}, {id: 3}}
	accepted, submitErr := pool.SubmitBatch(context.Background(), jobs)
	// The callback owns accepted resources. Only the suffix remains ours, even on error.
	for _, item := range jobs[accepted:] {
		release(item)
	}
	if submitErr != nil {
		pool.Abort(submitErr)
	}
	if err := pool.Shutdown(context.Background()); err != nil {
		panic(err)
	}
	summary := pool.Summary()
	fmt.Println(summary.Accepted, summary.Succeeded, summary.Failed, summary.Abandoned)
	fmt.Println("released:", released.Load())
	// Output:
	// 3 3 0 0
	// released: 3
}

func ExampleRunner_Shutdown() {
	pool, err := runner.New(runner.Config[int]{
		Capacity: 4,
		Workers:  1,
		Handler:  runner.Items(func(context.Context, int) error { return nil }),
	})
	if err != nil {
		panic(err)
	}
	// This service source belongs to the application, which stops and joins it separately.
	sourceCtx, stopSource := context.WithCancel(context.Background())
	sourceDone := make(chan error, 1)
	go func() {
		err := pool.Submit(sourceCtx, 1)
		<-sourceCtx.Done()
		sourceDone <- err
	}()
	stopSource()
	// A request or source context may already be canceled. Give drain its own grace period.
	graceCtx, cancelGrace := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelGrace()
	shutdownErr := pool.Shutdown(graceCtx)
	sourceErr := <-sourceDone
	if sourceErr != nil &&
		!errors.Is(sourceErr, context.Canceled) && !errors.Is(sourceErr, runner.ErrStopped) {
		panic(sourceErr)
	}
	fmt.Println(shutdownErr)
	// Output: <nil>
}

func ExampleRunner_Abort() {
	pool, err := runner.New(runner.Config[int]{
		Capacity: 4,
		Workers:  1,
		Handler:  runner.Items(func(context.Context, int) error { return nil }),
	})
	if err != nil {
		panic(err)
	}
	sourceErr := errors.New("input disconnected")
	pool.Abort(fmt.Errorf("read input: %w", sourceErr))
	// Abort publishes cancellation. Wait also joins workers and terminal cleanup.
	fmt.Println(errors.Is(pool.Wait(), sourceErr))
	// Output: true
}

func Example_partialResults() {
	invalid := errors.New("invalid item")
	pool, err := runner.New(runner.Config[int]{
		Capacity:    4,
		Workers:     1,
		FailureMode: runner.Continue,
		Handler: func(_ context.Context, batch []int) []runner.Result {
			results := make([]runner.Result, len(batch))
			for index, item := range batch {
				if item < 0 {
					results[index] = runner.Result{State: runner.ResultFailed, Err: invalid}
				}
			}
			return results
		},
	})
	if err != nil {
		panic(err)
	}
	items := []int{1, -1, 3}
	accepted, submitErr := pool.SubmitBatch(context.Background(), items)
	if submitErr != nil {
		// Persist or release items[accepted:] in an application with resource payloads.
		pool.Abort(submitErr)
	}
	finalErr := pool.Shutdown(context.Background())
	summary := pool.Summary()
	fmt.Println(accepted, summary.Succeeded, summary.Failed, errors.Is(finalErr, invalid))
	// Output: 3 2 1 true
}

func ExampleRetryPolicy() {
	temporary := errors.New("safe to retry before any write")
	attempts := 0 // One worker owns this example's handler state.
	pool, err := runner.New(runner.Config[int]{
		Capacity: 4,
		Workers:  1,
		Handler: runner.Items(func(context.Context, int) error {
			attempts++
			if attempts == 1 {
				return temporary
			}
			return nil
		}),
		Retry: &runner.RetryPolicy{
			MaxAttempts: 3,
			Delay: func(_ int, err error) (time.Duration, bool) {
				return 0, errors.Is(err, temporary)
			},
		},
	})
	if err != nil {
		panic(err)
	}
	if err := pool.Submit(context.Background(), 1); err != nil {
		pool.Abort(err)
	}
	if err := pool.Shutdown(context.Background()); err != nil {
		panic(err)
	}
	summary := pool.Summary()
	fmt.Println(summary.Succeeded, summary.Attempts, len(summary.Failures))
	// Output: 1 2 0
}

// supervisePair belongs to the application. It observes both stages until both complete.
func supervisePair(first, second *runner.Runner[int]) {
	firstAbort, firstDone := first.Aborting(), first.Done()
	secondAbort, secondDone := second.Aborting(), second.Done()
	propagated := false
	propagate := func(stage string, cause error) {
		if cause == nil || propagated {
			return
		}
		propagated = true
		cause = fmt.Errorf("%s: %w", stage, cause)
		// Cancel every dependency before waiting for either stage's callbacks to finish.
		first.Abort(cause)
		second.Abort(cause)
	}
	for firstDone != nil || secondDone != nil {
		select {
		case <-firstAbort:
			firstAbort = nil
			propagate("stage 1", first.AbortCause())
		case <-secondAbort:
			secondAbort = nil
			propagate("stage 2", second.AbortCause())
		case <-firstDone:
			firstDone, firstAbort = nil, nil
			// Done can win the select even when Aborting is already closed.
			propagate("stage 1", first.AbortCause())
		case <-secondDone:
			secondDone, secondAbort = nil, nil
			propagate("stage 2", second.AbortCause())
		}
	}
}

// forwardBatch treats downstream acceptance as upstream ownership transfer.
func forwardBatch(ctx context.Context, second *runner.Runner[int], batch []int) []runner.Result {
	accepted, forwardErr := second.SubmitBatch(ctx, batch)
	results := make([]runner.Result, len(batch))
	// Accepted positions are successful ownership transfers in this application.
	// The downstream stage alone determines their processing outcomes.
	if forwardErr == nil {
		return results
	}
	if cause := second.AbortCause(); cause != nil {
		forwardErr = fmt.Errorf("stage 2: %w", cause)
	}
	for position := accepted; position < len(batch); position++ {
		if ctx.Err() != nil {
			results[position] = runner.PeerCanceled(ctx)
			continue
		}
		results[position] = runner.Result{State: runner.ResultFailed, Err: forwardErr}
	}
	return results
}

func Example_pipeline() {
	second, err := runner.New(runner.Config[int]{
		Capacity: 4,
		Workers:  1,
		Handler:  runner.Items(func(context.Context, int) error { return nil }),
	})
	if err != nil {
		panic(err)
	}
	first, err := runner.New(runner.Config[int]{
		Capacity: 4,
		Workers:  1,
		// No upstream retry: replaying an entire batch could duplicate its forwarded prefix.
		Handler: func(ctx context.Context, batch []int) []runner.Result {
			return forwardBatch(ctx, second, batch)
		},
	})
	if err != nil {
		second.Abort(err)
		panic(errors.Join(err, second.Wait()))
	}
	supervisorDone := make(chan struct{})
	go func() {
		defer close(supervisorDone)
		supervisePair(first, second)
	}()
	items := []int{1, 2, 3}
	accepted, submitErr := first.SubmitBatch(context.Background(), items)
	if submitErr != nil {
		// An application can retain items[accepted:] for a later run. Never replay the prefix.
		first.Abort(submitErr)
	}
	// Outside sources have stopped. Keep supervision active throughout ordered drain.
	firstErr := first.Shutdown(context.Background())
	secondErr := second.Shutdown(context.Background())
	<-supervisorDone
	if err := errors.Join(firstErr, secondErr); err != nil {
		panic(err)
	}
	fmt.Println(accepted, first.Summary().Succeeded, second.Summary().Succeeded)
	// Output: 3 3 3
}

func TestPipelinePartialForwardingCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errors.New("stop pipeline")
		secondEntered := make(chan struct{})
		secondOutcomes := make(map[int][]runner.ItemOutcome)
		second, err := runner.New(runner.Config[int]{
			Capacity: 1,
			Workers:  1,
			Queue: runner.QueueConfig{
				ProducerWait: &workq.WaitStrategy{},
				ConsumerWait: &workq.WaitStrategy{},
			},
			Handler: func(ctx context.Context, batch []int) []runner.Result {
				if batch[0] == 1 {
					return nil
				}
				close(secondEntered)
				<-ctx.Done()
				return []runner.Result{runner.PeerCanceled(ctx)}
			},
			OnTerminal: func(item int, outcome runner.ItemOutcome) error {
				secondOutcomes[item] = append(secondOutcomes[item], outcome)
				return nil
			},
		})
		require.NoError(t, err)
		defer func() {
			second.Abort(cause)
			_ = second.Wait()
		}()
		forward := make(chan struct{})
		firstOutcomes := make(map[int][]runner.ItemOutcome)
		first, err := runner.New(runner.Config[int]{
			Capacity: 4,
			Workers:  1,
			Queue: runner.QueueConfig{
				MaxReceiveBatch: 4,
				ProducerWait:    &workq.WaitStrategy{},
				ConsumerWait:    &workq.WaitStrategy{},
			},
			Handler: func(ctx context.Context, batch []int) []runner.Result {
				select {
				case <-forward:
				case <-ctx.Done():
				}
				return forwardBatch(ctx, second, batch)
			},
			OnTerminal: func(item int, outcome runner.ItemOutcome) error {
				firstOutcomes[item] = append(firstOutcomes[item], outcome)
				return nil
			},
		})
		require.NoError(t, err)
		supervisorDone := make(chan struct{})
		go func() {
			defer close(supervisorDone)
			supervisePair(first, second)
		}()
		defer func() {
			// Release both stages before joining, including on an assertion failure.
			first.Abort(cause)
			second.Abort(cause)
			_ = first.Wait()
			_ = second.Wait()
			<-supervisorDone
		}()
		items := []int{1, 2, 3, 4}
		accepted, err := first.SubmitBatch(context.Background(), items)
		require.NoError(t, err)
		require.Equal(t, len(items), accepted)
		close(forward)
		<-secondEntered
		synctest.Wait()
		assert.Equal(t, uint64(1), second.Summary().Succeeded)
		first.Abort(cause)
		require.ErrorIs(t, first.Wait(), cause)
		require.ErrorIs(t, second.Wait(), cause)
		<-supervisorDone
		assert.ErrorIs(t, second.AbortCause(), cause)
		assert.Equal(t, runner.Summary{
			Accepted: 4, Succeeded: 3, Abandoned: 1, Attempts: 4, AbortCause: cause,
		}, first.Summary())
		secondSummary := second.Summary()
		assert.Equal(t, uint64(3), secondSummary.Accepted)
		assert.Equal(t, uint64(1), secondSummary.Succeeded)
		assert.Zero(t, secondSummary.Failed)
		assert.Equal(t, uint64(2), secondSummary.Abandoned)
		assert.Equal(t, uint64(2), secondSummary.Attempts)
		assert.Empty(t, secondSummary.Failures)
		require.Len(t, firstOutcomes, 4)
		require.Len(t, secondOutcomes, 3)
		for _, item := range items[:3] {
			assert.Equal(t, []runner.ItemOutcome{{
				State: runner.TerminalSucceeded, Attempts: 1,
			}}, firstOutcomes[item])
		}
		require.Len(t, firstOutcomes[4], 1)
		assert.Equal(t, runner.TerminalAbandoned, firstOutcomes[4][0].State)
		assert.Equal(t, 1, firstOutcomes[4][0].Attempts)
		assert.ErrorIs(t, firstOutcomes[4][0].Err, cause)
		assert.Equal(t, []runner.ItemOutcome{{
			State: runner.TerminalSucceeded, Attempts: 1,
		}}, secondOutcomes[1])
		for _, item := range []int{2, 3} {
			require.Len(t, secondOutcomes[item], 1)
			assert.Equal(t, runner.TerminalAbandoned, secondOutcomes[item][0].State)
			assert.Equal(t, 3-item, secondOutcomes[item][0].Attempts)
			assert.ErrorIs(t, secondOutcomes[item][0].Err, cause)
		}
		assert.Empty(t, secondOutcomes[4])
	})
}
