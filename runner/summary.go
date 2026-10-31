package runner

import (
	"errors"
	"slices"
	"sync"

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
	// Accepted counts published occurrences, independently of submission call completion.
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

type accounting struct {
	mu         sync.Mutex
	errorLimit int
	summary    Summary
}

func (a *accounting) recordAccepted(count uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.summary.Accepted += count
}

func (a *accounting) recordOutcome(outcome ItemOutcome) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.summary.Attempts += uint64(outcome.Attempts)
	switch outcome.State {
	case TerminalSucceeded:
		a.summary.Succeeded++
	case TerminalFailed:
		a.summary.Failed++
	case TerminalAbandoned:
		a.summary.Abandoned++
	}
}

func (a *accounting) recordFailure(record FailureRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.summary.Failures) >= a.errorLimit {
		a.summary.OmittedFailures++
		return
	}
	a.summary.Failures = append(a.summary.Failures, record)
}

func (a *accounting) snapshot(abortCause error) Summary {
	a.mu.Lock()
	defer a.mu.Unlock()
	snapshot := a.summary
	snapshot.Failures = slices.Clone(snapshot.Failures)
	snapshot.AbortCause = abortCause
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
