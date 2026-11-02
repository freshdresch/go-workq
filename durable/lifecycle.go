package durable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Open validates and owns directory until the returned Queue joins. Its context
// controls startup only. A failed publication returns nil and releases all handles.
func Open(ctx context.Context, directory string, config Config) (*Queue, error) {
	opened, err := startStore(ctx, directory, config)
	if err != nil {
		return nil, err
	}
	queue := &Queue{
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
	q.closing = true
	q.admissionStopped = true
	close(q.stopping)
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
	q.mu.Lock()
	defer q.mu.Unlock()
	select {
	case <-q.done:
		return
	default:
	}
	if q.terminal == nil {
		q.terminal = cause
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

func (q *Queue) shutdownBarrier(ctx context.Context) error {
	if err := q.opened.storage.selectDurability(ctx, Synchronized); err != nil {
		return errors.Join(ErrStorage, err)
	}
	var busy, frames, checkpointed int64
	err := q.opened.storage.conn.QueryRowContext(ctx,
		"PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &frames, &checkpointed)
	if err != nil {
		return errors.Join(ErrStorage, err)
	}
	if busy != 0 || frames != checkpointed {
		return fmt.Errorf("shutdown checkpoint busy=%d frames=%d completed=%d: %w",
			busy, frames, checkpointed, ErrStorage)
	}
	info, err := os.Stat(filepath.Join(q.opened.owner.path, "queue.sqlite3-wal"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Join(ErrStorage, err)
	}
	if err == nil && info.Size() != 0 {
		return fmt.Errorf("shutdown WAL: %w", ErrStorage)
	}
	return nil
}
