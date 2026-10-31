package runner

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/freshdresch/go-data-structures/workq"
)

// ErrOmittedFailures indicates that the bounded final aggregate excludes some failure causes.
var ErrOmittedFailures = errors.New("runner: additional failures omitted")

// FailureBoundary identifies the operation that produced a retained failure.
type FailureBoundary uint8

const (
	// BoundaryHandler identifies a terminal ordinary handler failure.
	BoundaryHandler FailureBoundary = iota
	// BoundaryResultValidation identifies an invalid handler response.
	BoundaryResultValidation
	// BoundaryQueueOperation identifies an unexpected queue operation failure.
	BoundaryQueueOperation
	// BoundaryRetryPolicy identifies a retry policy contract failure.
	BoundaryRetryPolicy
	// BoundaryTerminalCallback identifies a reconciliation or cleanup failure.
	BoundaryTerminalCallback
)

// FailureRecord retains an operational error without retaining the payload itself.
type FailureRecord struct {
	// Boundary identifies the failing operation.
	Boundary FailureBoundary
	// Worker is zero-based, or -1 when no worker applies, including queued cleanup.
	Worker int
	// Err is an application error shared as an immutable object.
	Err error
}

// Summary contains accounting with detached failure records. Error objects remain shared.
// Live counts need not reconcile. At completion Accepted equals Succeeded + Failed + Abandoned.
type Summary struct {
	// Accepted incorporates each returned accepted prefix when the queue submission
	// operation returns. Live counts may lag publication.
	Accepted uint64
	// Succeeded counts terminal successful occurrences.
	Succeeded uint64
	// Failed counts terminal failed occurrences.
	Failed uint64
	// Abandoned counts occurrences finalized without establishing success before abort.
	Abandoned uint64
	// Attempts counts handler attempts, excluding positions reported unattempted.
	Attempts uint64
	// Failures retains the earliest operational failures in insertion order.
	Failures []FailureRecord
	// OmittedFailures counts operational failures excluded by the retention limit.
	OmittedFailures uint64
	// AbortCause preserves the first committed abort cause, even when failures take precedence.
	AbortCause error
}

type outcomeDelta struct {
	succeeded uint64
	failed    uint64
	abandoned uint64
	attempts  uint64
}

type outcomeCounters struct {
	succeeded atomic.Uint64
	failed    atomic.Uint64
	abandoned atomic.Uint64
	attempts  atomic.Uint64
}

func outcomeToDelta(outcome ItemOutcome) outcomeDelta {
	delta := outcomeDelta{attempts: uint64(outcome.Attempts)}
	switch outcome.State {
	case TerminalSucceeded:
		delta.succeeded = 1
	case TerminalFailed:
		delta.failed = 1
	case TerminalAbandoned:
		delta.abandoned = 1
	}
	return delta
}

func (delta *outcomeDelta) add(other outcomeDelta) {
	delta.succeeded += other.succeeded
	delta.failed += other.failed
	delta.abandoned += other.abandoned
	delta.attempts += other.attempts
}

func (c *outcomeCounters) publish(delta outcomeDelta) {
	if delta.succeeded != 0 {
		c.succeeded.Add(delta.succeeded)
	}
	if delta.failed != 0 {
		c.failed.Add(delta.failed)
	}
	if delta.abandoned != 0 {
		c.abandoned.Add(delta.abandoned)
	}
	if delta.attempts != 0 {
		c.attempts.Add(delta.attempts)
	}
}

func (c *outcomeCounters) snapshot() outcomeDelta {
	return outcomeDelta{
		succeeded: c.succeeded.Load(),
		failed:    c.failed.Load(),
		abandoned: c.abandoned.Load(),
		attempts:  c.attempts.Load(),
	}
}

type accounting struct {
	accepted        atomic.Uint64
	workerOutcomes  []outcomeCounters
	cleanupOutcomes outcomeCounters
	mu              sync.Mutex
	errorLimit      int
	failures        []FailureRecord
	omittedFailures uint64
}

func (a *accounting) recordAccepted(count uint64) {
	a.accepted.Add(count)
}

func (a *accounting) recordFailure(record FailureRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.failures) >= a.errorLimit {
		a.omittedFailures++
		return
	}
	a.failures = append(a.failures, record)
}

func (a *accounting) snapshot(abortCause error) Summary {
	snapshot := Summary{Accepted: a.accepted.Load(), AbortCause: abortCause}
	for index := range a.workerOutcomes {
		delta := a.workerOutcomes[index].snapshot()
		snapshot.Succeeded += delta.succeeded
		snapshot.Failed += delta.failed
		snapshot.Abandoned += delta.abandoned
		snapshot.Attempts += delta.attempts
	}
	cleanup := a.cleanupOutcomes.snapshot()
	snapshot.Succeeded += cleanup.succeeded
	snapshot.Failed += cleanup.failed
	snapshot.Abandoned += cleanup.abandoned
	snapshot.Attempts += cleanup.attempts
	a.mu.Lock()
	snapshot.Failures = slices.Clone(a.failures)
	snapshot.OmittedFailures = a.omittedFailures
	a.mu.Unlock()
	return snapshot
}

func aggregateFailures(summary Summary) error {
	if len(summary.Failures) == 0 && summary.OmittedFailures == 0 {
		return summary.AbortCause
	}
	causes := make([]error, 0, len(summary.Failures)+1)
	for _, failure := range summary.Failures {
		causes = append(causes, failure.Err)
	}
	if summary.OmittedFailures != 0 {
		causes = append(causes, ErrOmittedFailures)
	}
	return errors.Join(causes...)
}

// Summary returns detached accounting. Counts reconcile and stay stable after Done closes.
func (r *Runner[T]) Summary() Summary {
	return r.accounting.snapshot(r.AbortCause())
}

// Diagnostics returns queue transfer counters, including abort cleanup receives.
// Read final snapshots after Wait. Transfer counts do not imply successful processing.
func (r *Runner[T]) Diagnostics() workq.DiagnosticsSnapshot {
	return r.queue.Diagnostics()
}
