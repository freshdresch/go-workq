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

func TestAccountingRetentionAndDetachment(t *testing.T) {
	first := errors.New("first")
	second := errors.New("second")
	omitted := errors.New("omitted")
	abortCause := errors.New("explicit abort")
	counts := accounting{errorLimit: 2, workerOutcomes: make([]outcomeCounters, 1)}
	counts.recordAccepted(3)
	counts.workerOutcomes[0].publish(outcomeToDelta(
		ItemOutcome{State: TerminalSucceeded, Attempts: 2},
	))
	counts.workerOutcomes[0].publish(outcomeToDelta(
		ItemOutcome{State: TerminalFailed, Attempts: 1, Err: first},
	))
	counts.cleanupOutcomes.publish(outcomeToDelta(ItemOutcome{State: TerminalAbandoned}))
	firstRecord := FailureRecord{Boundary: BoundaryHandler, Worker: 0, Err: first}
	secondRecord := FailureRecord{Boundary: BoundaryTerminalCallback, Worker: -1, Err: second}
	counts.recordFailure(firstRecord)
	earlier := counts.snapshot(nil)
	counts.recordFailure(secondRecord)
	counts.recordFailure(FailureRecord{Boundary: BoundaryQueueOperation, Worker: -1, Err: omitted})
	summary := counts.snapshot(abortCause)
	assert.Equal(t, Summary{
		Accepted: 3, Succeeded: 1, Failed: 1, Abandoned: 1, Attempts: 3,
		Failures:        []FailureRecord{firstRecord, secondRecord},
		OmittedFailures: 1, AbortCause: abortCause,
	}, summary)
	assert.Equal(t, []FailureRecord{firstRecord}, earlier.Failures)
	aggregate := aggregateFailures(summary)
	assert.ErrorIs(t, aggregate, first)
	assert.ErrorIs(t, aggregate, second)
	assert.ErrorIs(t, aggregate, ErrOmittedFailures)
	assert.NotErrorIs(t, aggregate, omitted)
	assert.NotErrorIs(t, aggregate, abortCause)
	assert.Equal(t, "first\nsecond\nrunner: additional failures omitted", aggregate.Error())
	summary.Failures[0] = FailureRecord{Err: omitted}
	summary.Failures = append(summary.Failures, FailureRecord{Err: omitted})
	assert.Equal(t, []FailureRecord{firstRecord, secondRecord}, counts.snapshot(nil).Failures)
}

func TestAggregatePrecedence(t *testing.T) {
	cause := errors.New("abort")
	failure := errors.New("failure")
	require.NoError(t, aggregateFailures(Summary{}))
	assert.Same(t, cause, aggregateFailures(Summary{AbortCause: cause}))
	aggregate := aggregateFailures(Summary{
		Failures: []FailureRecord{{Err: failure}}, AbortCause: cause,
	})
	assert.ErrorIs(t, aggregate, failure)
	assert.NotErrorIs(t, aggregate, cause)
	assert.NotErrorIs(t, aggregate, ErrOmittedFailures)
	assert.ErrorIs(t, aggregateFailures(Summary{OmittedFailures: 1}), ErrOmittedFailures)
}

func TestAccountingConcurrentSnapshots(t *testing.T) {
	counts := accounting{errorLimit: 3, workerOutcomes: make([]outcomeCounters, 8)}
	stopReader := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stopReader:
				return
			default:
				snapshot := counts.snapshot(nil)
				if len(snapshot.Failures) != 0 {
					snapshot.Failures[0] = FailureRecord{}
				}
			}
		}
	}()
	failure := errors.New("shared immutable failure")
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			for range 50 {
				counts.recordAccepted(1)
				counts.workerOutcomes[worker].publish(outcomeToDelta(
					ItemOutcome{State: TerminalFailed, Attempts: 2},
				))
				counts.recordFailure(FailureRecord{
					Boundary: BoundaryHandler, Worker: worker, Err: failure,
				})
				snapshot := counts.snapshot(nil)
				if len(snapshot.Failures) != 0 {
					snapshot.Failures[0] = FailureRecord{}
				}
			}
		})
	}
	workers.Wait()
	counts.recordAccepted(1)
	counts.cleanupOutcomes.publish(outcomeToDelta(ItemOutcome{State: TerminalAbandoned}))
	close(stopReader)
	<-readerDone
	summary := counts.snapshot(nil)
	assert.Equal(t, uint64(401), summary.Accepted)
	assert.Equal(t, uint64(400), summary.Failed)
	assert.Equal(t, uint64(800), summary.Attempts)
	assert.Equal(t, uint64(1), summary.Abandoned)
	assert.Equal(t, uint64(397), summary.OmittedFailures)
	require.Len(t, summary.Failures, 3)
	for _, record := range summary.Failures {
		assert.Same(t, failure, record.Err)
		assert.Equal(t, BoundaryHandler, record.Boundary)
		assert.Contains(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, record.Worker)
	}
}

func TestCallbackSummaryIncludesOwnOutcome(t *testing.T) {
	t.Run("exact success prefixes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			runner := retryRaceFixture(t, 1)
			runner.config.Handler = func(context.Context, []int) []Result { return nil }
			entered := make(chan int, 2)
			releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
			finished := make(chan struct{})
			defer func() {
				for _, release := range releases {
					releaseTestBarrier(release)
				}
				<-finished
			}()
			runner.config.OnTerminal = func(item int, outcome ItemOutcome) error {
				assert.Equal(t, ItemOutcome{State: TerminalSucceeded, Attempts: 1}, outcome)
				assert.Equal(t, Summary{Succeeded: uint64(item), Attempts: uint64(item)}, runner.Summary())
				entered <- item
				<-releases[item-1]
				return nil
			}
			go func() {
				defer close(finished)
				runner.processBatch(0, []int{1, 2}, make([]ItemOutcome, 2))
			}()
			for item := 1; item <= 2; item++ {
				require.Equal(t, item, <-entered)
				synctest.Wait()
				assert.Equal(t, Summary{Succeeded: uint64(item), Attempts: uint64(item)}, runner.Summary())
				close(releases[item-1])
			}
		})
	})
	for _, scenario := range []string{"success", "failure", "abandonment", "queued cleanup"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config := runtimeConfig(1)
				config.FailureMode = Continue
				config.Queue.MaxReceiveBatch = 1
				handlerEntered := make(chan struct{})
				handlerRelease := make(chan struct{})
				callbackEntered := make(chan struct{})
				callbackRelease := make(chan struct{})
				defer releaseTestBarrier(handlerRelease)
				defer releaseTestBarrier(callbackRelease)
				failure := errors.New("terminal failure")
				cause := errors.New("explicit abort")
				config.Handler = func(ctx context.Context, _ []int) []Result {
					close(handlerEntered)
					<-handlerRelease
					switch scenario {
					case "failure":
						return []Result{{State: ResultFailed, Err: failure}}
					case "abandonment":
						return []Result{PeerCanceled(ctx)}
					default:
						return nil
					}
				}
				expected := Summary{Accepted: 1, Succeeded: 1, Attempts: 1}
				switch scenario {
				case "failure":
					expected.Succeeded = 0
					expected.Failed = 1
					expected.Failures = []FailureRecord{{Boundary: BoundaryHandler, Worker: 0, Err: failure}}
				case "abandonment":
					expected.Succeeded = 0
					expected.Abandoned = 1
					expected.AbortCause = cause
				case "queued cleanup":
					expected.Accepted = 2
					expected.Abandoned = 1
					expected.AbortCause = cause
				}
				var runner *Runner[int]
				config.OnTerminal = func(item int, outcome ItemOutcome) error {
					if scenario == "queued cleanup" && item == 1 {
						return nil
					}
					assert.Equal(t, expected, runner.Summary())
					if scenario == "queued cleanup" {
						assert.Equal(t, ItemOutcome{State: TerminalAbandoned, Err: cause}, outcome)
					}
					close(callbackEntered)
					<-callbackRelease
					return nil
				}
				var err error
				runner, err = New(config)
				require.NoError(t, err)
				joinRunnerOnCleanup(t, runner)
				require.NoError(t, runner.Submit(context.Background(), 1))
				<-handlerEntered
				if scenario == "queued cleanup" {
					require.NoError(t, runner.Submit(context.Background(), 2))
				}
				if scenario == "queued cleanup" || scenario == "abandonment" {
					runner.Abort(cause)
				}
				close(handlerRelease)
				<-callbackEntered
				synctest.Wait()
				assert.Equal(t, expected, runner.Summary())
				runner.StopAdmission()
				assertChannelOpen(t, runner.Done())
				close(callbackRelease)
				finalErr := runner.Wait()
				switch scenario {
				case "success":
					require.NoError(t, finalErr)
				case "failure":
					require.ErrorIs(t, finalErr, failure)
				default:
					require.ErrorIs(t, finalErr, cause)
				}
				for range 3 {
					if finalErr == nil {
						assert.NoError(t, runner.Wait())
					} else {
						assert.Same(t, finalErr, runner.Wait())
					}
					assert.Equal(t, expected, runner.Summary())
				}
			})
		})
	}
}
