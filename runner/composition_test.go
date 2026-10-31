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

func TestMixedFailureCancelsPeerBeforeSuccessCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeConfig(2)
		config.Queue.MaxReceiveBatch = 2
		resourceHeld := make(chan struct{})
		resourceReleased := make(chan struct{})
		callbackEntered := make(chan struct{})
		callbackRelease := make(chan struct{})
		failure := errors.New("mixed response permanent failure")
		config.Handler = func(ctx context.Context, batch []int) []Result {
			if batch[0] == 0 {
				close(resourceHeld)
				<-ctx.Done()
				close(resourceReleased)
				return []Result{PeerCanceled(ctx)}
			}
			assert.Equal(t, []int{1, 2}, batch)
			return []Result{{State: ResultSucceeded}, {State: ResultFailed, Err: failure}}
		}
		var runner *Runner[int]
		var mu sync.Mutex
		outcomes := make(map[int][]ItemOutcome)
		config.OnTerminal = func(item int, outcome ItemOutcome) error {
			if item == 1 {
				select {
				case <-runner.Aborting():
				default:
					t.Error("success callback entered before abort publication")
				}
				assert.Same(t, failure, runner.AbortCause())
				assert.ErrorIs(t, runner.ctx.Err(), context.Canceled)
				summary := runner.Summary()
				if assert.Len(t, summary.Failures, 1) {
					assert.Equal(t, BoundaryHandler, summary.Failures[0].Boundary)
					assert.Same(t, failure, summary.Failures[0].Err)
				}
				close(callbackEntered)
				<-resourceReleased
				<-callbackRelease
			}
			mu.Lock()
			defer mu.Unlock()
			outcomes[item] = append(outcomes[item], outcome)
			return nil
		}
		var err error
		runner, err = New(config)
		require.NoError(t, err)
		defer func() {
			runner.Abort(nil)
			releaseTestBarrier(callbackRelease)
			_ = runner.Wait()
		}()
		require.NoError(t, runner.Submit(context.Background(), 0))
		<-resourceHeld
		count, err := runner.SubmitBatch(context.Background(), []int{1, 2})
		require.NoError(t, err)
		require.Equal(t, 2, count)
		<-callbackEntered
		<-resourceReleased
		assertChannelOpen(t, runner.Done())
		close(callbackRelease)
		require.ErrorIs(t, runner.Wait(), failure)
		assert.Equal(t, []ItemOutcome{
			{State: TerminalAbandoned, Attempts: 1, Err: failure},
		}, outcomes[0])
		assert.Equal(t, []ItemOutcome{{State: TerminalSucceeded, Attempts: 1}}, outcomes[1])
		assert.Equal(t, []ItemOutcome{
			{State: TerminalFailed, Attempts: 1, Err: failure},
		}, outcomes[2])
		summary := runner.Summary()
		assert.Equal(t, uint64(3), summary.Accepted)
		assert.Equal(t, uint64(1), summary.Succeeded)
		assert.Equal(t, uint64(1), summary.Failed)
		assert.Equal(t, uint64(1), summary.Abandoned)
		assert.Equal(t, uint64(3), summary.Attempts)
	})
}

func TestSupervisorReleasesCrossRunnerCancellationDependency(t *testing.T) {
	for _, orderedShutdown := range []bool{false, true} {
		name := map[bool]string{false: "active run", true: "ordered shutdown"}[orderedShutdown]
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				resourceHeld := make(chan struct{})
				resourceReleased := make(chan struct{})
				downstreamConfig := runtimeConfig(1)
				downstreamConfig.Handler = func(ctx context.Context, _ []int) []Result {
					close(resourceHeld)
					<-ctx.Done()
					close(resourceReleased)
					return []Result{PeerCanceled(ctx)}
				}
				var downstreamOutcomes []ItemOutcome
				downstreamConfig.OnTerminal = func(_ int, outcome ItemOutcome) error {
					downstreamOutcomes = append(downstreamOutcomes, outcome)
					return nil
				}
				downstream, err := New(downstreamConfig)
				require.NoError(t, err)
				defer func() {
					downstream.Abort(nil)
					_ = downstream.Wait()
				}()

				failure := errors.New("upstream permanent failure")
				failHandler := make(chan struct{})
				callbackEntered := make(chan struct{})
				upstreamConfig := runtimeConfig(1)
				upstreamConfig.Handler = func(context.Context, []int) []Result {
					<-resourceHeld
					<-failHandler
					return []Result{{State: ResultFailed, Err: failure}}
				}
				var upstreamOutcomes []ItemOutcome
				upstreamConfig.OnTerminal = func(_ int, outcome ItemOutcome) error {
					close(callbackEntered)
					<-resourceReleased
					upstreamOutcomes = append(upstreamOutcomes, outcome)
					return nil
				}
				upstream, err := New(upstreamConfig)
				require.NoError(t, err)
				defer func() {
					upstream.Abort(nil)
					downstream.Abort(nil)
					releaseTestBarrier(failHandler)
					_ = upstream.Wait()
				}()

				supervisorRelease := make(chan struct{})
				var supervisor sync.WaitGroup
				var owner sync.WaitGroup
				supervisor.Go(func() {
					<-supervisorRelease
					select {
					case <-upstream.Aborting():
					case <-upstream.Done():
					}
					cause := upstream.AbortCause()
					assert.Same(t, failure, cause)
					select {
					case <-upstream.Done():
						t.Error("upstream joined before dependency cancellation")
					default:
					}
					upstream.Abort(cause)
					downstream.Abort(cause)
					assert.ErrorIs(t, upstream.Wait(), failure)
					assert.Same(t, cause, downstream.Wait())
				})
				defer func() {
					upstream.Abort(nil)
					downstream.Abort(nil)
					releaseTestBarrier(failHandler)
					releaseTestBarrier(supervisorRelease)
					supervisor.Wait()
					owner.Wait()
				}()

				require.NoError(t, downstream.Submit(context.Background(), 20))
				<-resourceHeld
				require.NoError(t, upstream.Submit(context.Background(), 10))
				orderedResults := make(chan [2]error, 1)
				if orderedShutdown {
					owner.Go(func() {
						first := upstream.Shutdown(context.Background())
						second := downstream.Shutdown(context.Background())
						orderedResults <- [2]error{first, second}
					})
					synctest.Wait()
					assert.Same(t, ErrStopped, upstream.Submit(context.Background(), 11))
					assertChannelOpen(t, upstream.Done())
				}
				close(failHandler)
				<-callbackEntered
				assertChannelOpen(t, upstream.Done())
				assertChannelOpen(t, downstream.Done())
				assertChannelOpen(t, downstream.Aborting())
				assertChannelOpen(t, resourceReleased)
				<-upstream.Aborting()
				assert.Same(t, failure, upstream.AbortCause())
				assert.Nil(t, downstream.AbortCause())
				summary := upstream.Summary()
				require.Len(t, summary.Failures, 1)
				assert.Same(t, failure, summary.Failures[0].Err)
				close(supervisorRelease)
				supervisor.Wait()
				owner.Wait()
				if orderedShutdown {
					results := <-orderedResults
					assert.ErrorIs(t, results[0], failure)
					assert.Same(t, failure, results[1])
				}
				assert.Equal(t, []ItemOutcome{
					{State: TerminalFailed, Attempts: 1, Err: failure},
				}, upstreamOutcomes)
				assert.Equal(t, []ItemOutcome{
					{State: TerminalAbandoned, Attempts: 1, Err: failure},
				}, downstreamOutcomes)
				assert.Equal(t, uint64(1), upstream.Summary().Accepted)
				assert.Equal(t, uint64(1), upstream.Summary().Failed)
				assert.Equal(t, Summary{
					Accepted: 1, Abandoned: 1, Attempts: 1, AbortCause: failure,
				}, downstream.Summary())
			})
		})
	}
}
