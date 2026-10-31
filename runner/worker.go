package runner

import (
	"context"
	"errors"
	"fmt"

	"github.com/freshdresch/go-data-structures/workq"
)

func (r *Runner[T]) runWorker(index int, endpoint workq.DiagnosticsEndpoint[T]) {
	buffer := make([]T, r.batchLimit)
	outcomes := make([]ItemOutcome, r.batchLimit)
	metadata := make([]retryItem, r.batchLimit)
	var timer retryTimer
	defer timer.stop()
	defer clear(metadata)
	defer clear(buffer)
	defer clear(outcomes)
	for {
		if r.AbortCause() != nil {
			return
		}
		count, err := endpoint.Receive(r.ctx, buffer)
		if count != 0 {
			r.processHeldBatch(index, buffer[:count], outcomes[:count], metadata[:count], &timer)
		}
		if err != nil {
			r.handleReceiveError(index, err)
			return
		}
	}
}

func (r *Runner[T]) handleReceiveError(index int, err error) {
	if errors.Is(err, workq.ErrClosed) {
		return
	}
	if errors.Is(err, context.Canceled) && r.AbortCause() != nil {
		return
	}
	failure := fmt.Errorf("runner: receive: %w", err)
	r.recordFailureAndAbort(BoundaryQueueOperation, index, failure)
}

func (r *Runner[T]) processBatch(index int, batch []T, outcomes []ItemOutcome) {
	metadata := make([]retryItem, len(batch))
	var timer retryTimer
	defer timer.stop()
	defer clear(metadata)
	r.processHeldBatch(index, batch, outcomes, metadata, &timer)
}

func (r *Runner[T]) finalizeBatch(index int, batch []T, outcomes []ItemOutcome) {
	r.awaitAbortPublication()
	for position, item := range batch {
		outcome := outcomes[position]
		r.accounting.recordOutcome(outcome)
		if r.config.OnTerminal != nil {
			if err := r.config.OnTerminal(item, outcome); err != nil {
				r.recordFailureAndAbort(BoundaryTerminalCallback, index, err)
			}
		}
		var zero T
		batch[position] = zero
		outcomes[position] = ItemOutcome{}
	}
}

func (r *Runner[T]) recordFailureAndAbort(boundary FailureBoundary, index int, err error) {
	r.accounting.recordFailure(FailureRecord{Boundary: boundary, Worker: index, Err: err})
	r.abort(err)
}

func (r *Runner[T]) finishWorkers() {
	r.workers.Wait()
	// Abort can commit while a graceful coordinator is already joining workers.
	// Its publication must finish before exclusive cleanup can invoke callbacks.
	r.awaitAbortPublication()
	r.queue.Close()
	if r.AbortCause() == nil {
		return
	}
	r.awaitAbortPublication()
	r.cleanupQueued()
}

func (r *Runner[T]) cleanupQueued() {
	buffer := make([]T, r.batchLimit)
	outcomes := make([]ItemOutcome, r.batchLimit)
	defer clear(buffer)
	defer clear(outcomes)
	for {
		count, err := r.queue.Receive(context.Background(), buffer)
		for position := range count {
			outcomes[position] = ItemOutcome{State: TerminalAbandoned, Err: r.AbortCause()}
		}
		r.finalizeBatch(-1, buffer[:count], outcomes[:count])
		if errors.Is(err, workq.ErrClosed) {
			return
		}
		if err != nil {
			r.recordFailureAndAbort(BoundaryQueueOperation, -1, err)
			return
		}
	}
}
