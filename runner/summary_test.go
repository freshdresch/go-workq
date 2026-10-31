package runner

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountingRetentionAndDetachment(t *testing.T) {
	first := errors.New("first")
	second := errors.New("second")
	omitted := errors.New("omitted")
	abortCause := errors.New("explicit abort")
	counts := accounting{errorLimit: 2}
	counts.recordAccepted(3)
	counts.recordOutcome(ItemOutcome{State: TerminalSucceeded, Attempts: 2})
	counts.recordOutcome(ItemOutcome{State: TerminalFailed, Attempts: 1, Err: first})
	counts.recordOutcome(ItemOutcome{State: TerminalAbandoned})
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
	counts := accounting{errorLimit: 3}
	failure := errors.New("shared immutable failure")
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			for range 50 {
				counts.recordAccepted(1)
				counts.recordOutcome(ItemOutcome{State: TerminalFailed, Attempts: 2})
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
	summary := counts.snapshot(nil)
	assert.Equal(t, uint64(400), summary.Accepted)
	assert.Equal(t, uint64(400), summary.Failed)
	assert.Equal(t, uint64(800), summary.Attempts)
	assert.Equal(t, uint64(397), summary.OmittedFailures)
	require.Len(t, summary.Failures, 3)
	for _, record := range summary.Failures {
		assert.Same(t, failure, record.Err)
		assert.Equal(t, BoundaryHandler, record.Boundary)
		assert.Contains(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, record.Worker)
	}
}
