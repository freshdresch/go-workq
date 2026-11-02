package durable

import (
	"context"
	"errors"

	"github.com/freshdresch/go-data-structures/workq"
)

// Open validates and owns directory until the returned Queue joins. Its context
// controls startup only. A failed publication returns nil and releases all handles.
// Directory input, absolute form, and resolved form must each fit 4095 bytes.
// Longer paths return [ErrInvalidInput]. See the package path-resolution memory boundary.
func Open(ctx context.Context, directory string, config Config) (*Queue, error) {
	opened, err := startStore(ctx, directory, config)
	if err != nil {
		return nil, err
	}
	capacity := 1
	for capacity < int(config.Runtime.Delivery.Items) {
		capacity *= 2
	}
	deliveryQueue, err := workq.New[int](capacity)
	if err != nil {
		return nil, errors.Join(err, opened.close())
	}
	queue := &Queue{
		deliveryQueue: deliveryQueue, claims: make([]claim, config.Runtime.Delivery.Items),
		deliveryDirty: true, pending: opened.metadata.counts.PendingItems,
		opened: opened, config: config,
		ordinary: admissionPool{slots: make([]command, config.Runtime.Ordinary.Commands),
			budget: config.Runtime.Ordinary},
		progress: admissionPool{slots: make([]command, config.Runtime.Progress.Commands),
			budget: config.Runtime.Progress},
		changed: make(chan struct{}), stopping: make(chan struct{}), done: make(chan struct{}),
	}
	if err := cancellation(ctx); err != nil {
		return nil, errors.Join(err, opened.close())
	}
	queue.walEnvelope = walEnvelopeBytes
	queue.snapshot.Store = StoreInfo{UUID: opened.metadata.uuid, Epoch: opened.metadata.epoch,
		Policy: config.Policy, Durability: config.Durability, Counts: opened.metadata.counts}
	queue.snapshot.Validation = opened.validation
	queue.snapshot.Disk.Cache = config.Runtime.SQLite.Cache / 1024 * 1024
	queue.snapshot.Disk.Mmap = config.Runtime.SQLite.Mmap
	go queue.coordinate()
	return queue, nil
}

// StopAdmission rejects later Submit handoffs, including empty batches. Earlier
// handoffs finish and Lookup remains available.
func (q *Queue) StopAdmission() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.admissionStopped = true
	q.signalLocked()
}

func (q *Queue) cutoffLocked() {
	if q.closing {
		return
	}
	q.stopDeliveryLocked()
	q.closing = true
	q.admissionStopped = true
	q.cacheCauseLocked()
	q.cacheStateLocked()
	q.publishStoppingLocked()
	q.signalLocked()
}

// Stopping closes after a terminal cutoff. Resource ownership lasts until Done.
func (q *Queue) Stopping() <-chan struct{} { return q.stopping }

// Done closes after the coordinator releases the connection and directory lock.
func (q *Queue) Done() <-chan struct{} { return q.done }

// Abort requests terminal shutdown without a new explicit checkpoint. Executing
// storage finishes at a safe boundary. Wait joins and returns the terminal cause.
func (q *Queue) Abort(cause error) {
	if cause == nil {
		cause = ErrStopped
	}
	detail := describeCause(cause)
	detail.Operation = AbortOperation
	q.mu.Lock()
	defer q.mu.Unlock()
	select {
	case <-q.done:
		return
	default:
	}
	if q.terminal == nil {
		q.terminal = cause
		q.snapshot.Cause = detail
		q.faulted = detail.Storage
		q.exhausted = detail.Exhausted
	}
	q.aborting = true
	q.cutoffLocked()
	q.signalLocked()
}

// Wait joins the coordinator and returns terminal and observable closure errors.
func (q *Queue) Wait() error {
	<-q.done
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.terminal
}

// Close cuts off handoffs, settles earlier commands, explicitly synchronizes the
// store, and joins. Context cancellation requests Abort and still waits for join.
func (q *Queue) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	q.mu.Lock()
	q.cutoffLocked()
	q.mu.Unlock()
	select {
	case <-q.done:
		return q.Wait()
	case <-ctx.Done():
		q.Abort(cancellation(ctx))
		return q.Wait()
	}
}

func (q *Queue) publishStoppingLocked() {
	select {
	case <-q.stopping:
	default:
		close(q.stopping)
	}
}
