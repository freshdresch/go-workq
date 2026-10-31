package runner

import "context"

func (r *Runner[T]) stopAdmission() {
	r.lifecycle.mu.Lock()
	r.closeAdmissionLocked()
	r.startCoordinatorLocked()
	r.lifecycle.mu.Unlock()
}

func (r *Runner[T]) closeAdmissionLocked() {
	if r.lifecycle.stopped {
		return
	}
	r.lifecycle.stopped = true
	if r.lifecycle.active == 0 {
		close(r.lifecycle.admissionsDone)
	}
}

func (r *Runner[T]) startCoordinatorLocked() {
	if r.lifecycle.coordinatorStarted {
		return
	}
	r.lifecycle.coordinatorStarted = true
	go r.coordinate()
}

func (r *Runner[T]) abort(cause error) {
	if cause == nil {
		cause = ErrAborted
	}
	r.lifecycle.mu.Lock()
	if r.lifecycle.completed {
		r.lifecycle.mu.Unlock()
		return
	}
	if r.lifecycle.abortRecord.Load() != nil {
		published := r.lifecycle.aborting
		r.lifecycle.mu.Unlock()
		<-published
		return
	}
	r.lifecycle.abortRecord.Store(&abortRecord{cause: cause})
	r.closeAdmissionLocked()
	r.startCoordinatorLocked()
	r.lifecycle.mu.Unlock()

	// Cancellation can run context registrations, so it stays outside the lifecycle lock.
	r.cancel()
	close(r.lifecycle.aborting)
}

func (r *Runner[T]) awaitAbortPublication() {
	if r.lifecycle.abortRecord.Load() != nil {
		<-r.lifecycle.aborting
	}
}

func (r *Runner[T]) coordinate() {
	r.awaitAbortPublication()
	<-r.lifecycle.admissionsDone
	r.awaitAbortPublication()
	if r.lifecycle.abortRecord.Load() != nil {
		<-r.lifecycle.aborting
	} else {
		r.queue.Close()
	}
	// The owner joins workers, closes the queue, then performs exclusive abort cleanup.
	r.lifecycle.finish()
	r.publishCompletion()
}

func (r *Runner[T]) publishCompletion() {
	r.cancel()
	summary := r.accounting.snapshot(nil)
	for {
		r.awaitAbortPublication()
		r.lifecycle.mu.Lock()
		if r.lifecycle.abortRecord.Load() != nil {
			select {
			case <-r.lifecycle.aborting:
			default:
				r.lifecycle.mu.Unlock()
				continue
			}
		}
		summary.AbortCause = r.AbortCause()
		r.lifecycle.finalErr = aggregateFailures(summary)
		r.lifecycle.completed = true
		close(r.lifecycle.done)
		r.lifecycle.mu.Unlock()
		return
	}
}

// StopAdmission closes the gate synchronously and starts draining, without joining work.
func (r *Runner[T]) StopAdmission() {
	r.stopAdmission()
}

// Shutdown stops admission and joins all work. Grace cancellation requests global abort,
// then continues joining even when application handlers or callbacks exceed the grace period.
// Each concurrent caller can request global abort. Completion racing cancellation can win,
// and later cancellation cannot change a completed outcome.
func (r *Runner[T]) Shutdown(graceCtx context.Context) error {
	r.StopAdmission()
	select {
	case <-r.Done():
	case <-graceCtx.Done():
		r.Abort(graceCtx.Err())
	}
	return r.Wait()
}

// Abort publishes cancellation and its first cause without joining. Nil means ErrAborted.
// Requests after completion have no effect.
func (r *Runner[T]) Abort(cause error) {
	r.abort(cause)
}

// Aborting returns the stable early abort signal. It stays open after graceful completion.
func (r *Runner[T]) Aborting() <-chan struct{} {
	return r.lifecycle.aborting
}

// AbortCause returns the first committed cause, possibly before abort publication finishes.
func (r *Runner[T]) AbortCause() error {
	if record := r.lifecycle.abortRecord.Load(); record != nil {
		return record.cause
	}
	return nil
}

// Wait joins all submissions, workers and cleanup. It does not stop an open runner.
// It returns retained operational failures, otherwise the abort cause, otherwise nil.
// The aggregate includes ErrOmittedFailures when the retention limit omitted failures.
func (r *Runner[T]) Wait() error {
	<-r.lifecycle.done
	return r.lifecycle.finalErr
}

// Done closes after all owned work and terminal callbacks finish and the final outcome is stable.
func (r *Runner[T]) Done() <-chan struct{} {
	return r.lifecycle.done
}
