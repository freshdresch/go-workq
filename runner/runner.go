package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/freshdresch/go-data-structures/workq"
)

// Runner owns admission and completion for one run. It must not be copied after use.
type Runner[T any] struct {
	queue      *workq.Queue[T]
	ctx        context.Context
	cancel     context.CancelFunc
	accounting accounting
	lifecycle  lifecycle
	config     Config[T]
	workers    sync.WaitGroup
	batchLimit int
}

// New validates config and starts a single-use runner. The caller must eventually join it
// through Shutdown, or Abort followed by Wait.
func New[T any](config Config[T]) (*Runner[T], error) {
	resolved, err := resolveConfig(config)
	if err != nil {
		return nil, err
	}
	queue, err := workq.New[T](resolved.Capacity, queueOptions(resolved)...)
	if err != nil {
		return nil, err
	}
	runner := newRunnerState(queue, resolved.ErrorLimit, nil)
	runner.config = resolved
	runner.batchLimit = queue.Diagnostics().Config.MaxReceiveBatch
	runner.lifecycle.finish = runner.finishWorkers
	for index := range resolved.Workers {
		endpoint := queue.NewDiagnosticsEndpoint()
		runner.workers.Go(func() { runner.runWorker(index, endpoint) })
	}
	return runner, nil
}

type lifecycle struct {
	mu                 sync.Mutex
	stopped            bool
	active             int
	admissionsDone     chan struct{}
	coordinatorStarted bool
	abortCause         error
	aborting           chan struct{}
	done               chan struct{}
	completed          bool
	finalErr           error
	finish             func()
}

func newRunnerState[T any](queue *workq.Queue[T], errorLimit int, finish func()) *Runner[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &Runner[T]{
		queue:      queue,
		ctx:        ctx,
		cancel:     cancel,
		accounting: accounting{errorLimit: errorLimit},
		lifecycle: lifecycle{
			admissionsDone: make(chan struct{}),
			aborting:       make(chan struct{}),
			done:           make(chan struct{}),
			finish:         finish,
		},
	}
}

// Submit transfers one occurrence to the runner. Acceptance does not establish processing success.
// After admission closes it returns ErrStopped directly, even with a canceled context.
func (r *Runner[T]) Submit(ctx context.Context, item T) error {
	_, err := r.SubmitBatch(ctx, []T{item})
	return err
}

// SubmitBatch transfers items[:n] and leaves items[n:] with the caller, including on error.
// Values are shallow copies and may finish before this call returns. The caller can reuse slice
// storage after return, but must not concurrently mutate accepted payloads. Caller cancellation
// affects submission only. Pre-canceled nonempty calls accept nothing after checking admission,
// while cancellation racing publication can still accept a prefix.
// An admitted empty batch succeeds. After the cutoff even empty batches return ErrStopped.
func (r *Runner[T]) SubmitBatch(ctx context.Context, items []T) (int, error) {
	if !r.registerSubmission() {
		return 0, ErrStopped
	}
	defer r.unregisterSubmission()
	if len(items) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	submitCtx, cancel := context.WithCancelCause(context.Background())
	callerRegistration := registerCancellation(ctx, func() { cancel(ctx.Err()) })
	defer callerRegistration.stopAndJoin()
	abortRegistration := registerCancellation(r.ctx, func() { cancel(context.Canceled) })
	defer abortRegistration.stopAndJoin()
	defer cancel(nil)
	if err := ctx.Err(); err != nil {
		cancel(err)
	}
	if r.ctx.Err() != nil {
		cancel(context.Canceled)
	}
	accepted, err := r.queue.PushBatch(submitCtx, items)
	r.accounting.recordAccepted(uint64(accepted))
	if errors.Is(err, context.Canceled) {
		err = context.Cause(submitCtx)
	}
	return accepted, r.classifyPushError(err)
}

func (r *Runner[T]) registerSubmission() bool {
	r.lifecycle.mu.Lock()
	defer r.lifecycle.mu.Unlock()
	if r.lifecycle.stopped {
		return false
	}
	r.lifecycle.active++
	return true
}

func (r *Runner[T]) unregisterSubmission() {
	r.lifecycle.mu.Lock()
	defer r.lifecycle.mu.Unlock()
	r.lifecycle.active--
	if r.lifecycle.stopped && r.lifecycle.active == 0 {
		close(r.lifecycle.admissionsDone)
	}
}

func (r *Runner[T]) classifyPushError(err error) error {
	if err == nil {
		return nil
	}
	var failure error
	switch {
	case errors.Is(err, workq.ErrClosedDuringPush):
		failure = fmt.Errorf("runner: queue closed during admitted push: %w", err)
	case errors.Is(err, workq.ErrClosed):
		failure = fmt.Errorf("runner: queue closed before admitted push: %w", err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	default:
		failure = fmt.Errorf("runner: admitted push: %w", err)
	}
	r.accounting.recordFailure(FailureRecord{
		Boundary: BoundaryQueueOperation,
		Worker:   -1,
		Err:      failure,
	})
	r.abort(failure)
	return failure
}

type cancellationRegistration struct {
	stop func() bool
	done chan struct{}
}

func registerCancellation(ctx context.Context, cancel context.CancelFunc) cancellationRegistration {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		cancel()
	})
	return cancellationRegistration{stop: stop, done: done}
}

func (registration cancellationRegistration) stopAndJoin() {
	if !registration.stop() {
		<-registration.done
	}
}
