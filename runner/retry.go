package runner

import (
	"fmt"
	"time"
)

type retryItem struct {
	attempts int
	lastErr  error
	due      time.Time
	pending  bool
}

type retryTimer struct {
	timer *time.Timer
}

func (timer *retryTimer) stop() {
	if timer.timer != nil {
		timer.timer.Stop()
	}
}

func (r *Runner[T]) waitForRetry(metadata []retryItem, timer *retryTimer) bool {
	if r.AbortCause() != nil {
		return false
	}
	earliest := metadata[0].due
	for _, item := range metadata[1:] {
		if item.due.Before(earliest) {
			earliest = item.due
		}
	}
	delay := time.Until(earliest)
	if delay <= 0 {
		return true
	}
	if timer.timer == nil {
		timer.timer = time.NewTimer(delay)
	} else {
		timer.timer.Reset(delay)
	}
	select {
	case <-r.ctx.Done():
		timer.stop()
		return false
	case <-timer.timer.C:
		return true
	}
}

func (r *Runner[T]) processHeldBatch(
	index int,
	group *outcomeCounters,
	batch []T,
	outcomes []ItemOutcome,
	metadata []retryItem,
	timer *retryTimer,
) {
	held := len(batch)
	due := held
	for held != 0 {
		if r.AbortCause() != nil {
			r.abandonHeld(index, group, batch[:held], outcomes[:held], metadata[:held])
			return
		}
		r.invokeHeld(index, batch[:due], outcomes[:due], metadata[:due])
		// Classification finishes before any callback can block or request another abort.
		var delta outcomeDelta
		if r.config.OnTerminal == nil {
			r.awaitAbortPublication()
		}
		remaining := 0
		for position := range held {
			if position < due && !metadata[position].pending {
				if r.config.OnTerminal == nil {
					delta.add(outcomeToDelta(outcomes[position]))
					var zero T
					batch[position] = zero
					outcomes[position] = ItemOutcome{}
				} else {
					r.finalizeBatch(index, group, batch[position:position+1], outcomes[position:position+1])
				}
				metadata[position] = retryItem{}
				continue
			}
			batch[remaining] = batch[position]
			metadata[remaining] = metadata[position]
			remaining++
		}
		clear(batch[remaining:held])
		clear(outcomes[:held])
		clear(metadata[remaining:held])
		group.publish(delta)
		held = remaining
		if held == 0 {
			return
		}
		if !r.waitForRetry(metadata[:held], timer) {
			r.abandonHeld(index, group, batch[:held], outcomes[:held], metadata[:held])
			return
		}
		due = compactDue(batch[:held], metadata[:held], time.Now())
	}
}

func (r *Runner[T]) invokeHeld(
	index int,
	batch []T,
	outcomes []ItemOutcome,
	metadata []retryItem,
) {
	results := r.config.Handler(r.ctx, batch)
	if err := validateResults(results, len(batch), r.AbortCause() != nil); err != nil {
		r.recordFailureAndAbort(BoundaryResultValidation, index, err)
		for position := range batch {
			metadata[position].attempts++
			metadata[position].pending = false
			outcomes[position] = ItemOutcome{
				State: TerminalFailed, Attempts: metadata[position].attempts, Err: err,
			}
		}
		return
	}
	for position := range batch {
		result := Result{State: ResultSucceeded}
		if results != nil {
			result = results[position]
		}
		item := &metadata[position]
		item.pending = false
		if result.State != ResultUnattempted {
			item.attempts++
		}
		outcome := ItemOutcome{State: TerminalSucceeded, Attempts: item.attempts}
		switch result.State {
		case ResultFailed:
			item.lastErr = result.Err
			item.pending, item.due = r.classifyFailure(index, item.attempts, result.Err)
			outcome.State = TerminalFailed
			outcome.Err = result.Err
		case ResultUnattempted, ResultInterrupted:
			outcome.State = TerminalAbandoned
			outcome.Err = r.abandonmentError(*item)
		}
		outcomes[position] = outcome
	}
}

func (r *Runner[T]) classifyFailure(index, attempts int, err error) (bool, time.Time) {
	policy := r.config.Retry
	if policy != nil && attempts < policy.MaxAttempts {
		delay, retry := policy.Delay(attempts, err)
		if delay < 0 {
			failure := fmt.Errorf("runner: retry policy returned negative delay: %s", delay)
			r.accounting.recordFailure(FailureRecord{
				Boundary: BoundaryHandler, Worker: index, Err: err,
			})
			r.recordFailureAndAbort(BoundaryRetryPolicy, index, failure)
			return false, time.Time{}
		}
		if retry {
			return true, time.Now().Add(delay)
		}
	}
	r.accounting.recordFailure(FailureRecord{Boundary: BoundaryHandler, Worker: index, Err: err})
	if r.config.FailureMode == Abort {
		r.abort(err)
	}
	return false, time.Time{}
}

func (r *Runner[T]) abandonmentError(item retryItem) error {
	if item.lastErr != nil {
		return item.lastErr
	}
	return r.AbortCause()
}

func (r *Runner[T]) abandonHeld(
	index int,
	group *outcomeCounters,
	batch []T,
	outcomes []ItemOutcome,
	metadata []retryItem,
) {
	r.awaitAbortPublication()
	for position, item := range metadata {
		outcomes[position] = ItemOutcome{
			State: TerminalAbandoned, Attempts: item.attempts, Err: r.abandonmentError(item),
		}
	}
	r.finalizeBatch(index, group, batch, outcomes)
	clear(metadata)
}

// compactDue moves due payloads and their metadata to the front of the held storage.
func compactDue[T any](batch []T, metadata []retryItem, now time.Time) int {
	due := 0
	for position, item := range metadata {
		if item.due.After(now) {
			continue
		}
		batch[due], batch[position] = batch[position], batch[due]
		metadata[due], metadata[position] = metadata[position], metadata[due]
		due++
	}
	return due
}
