package durable

import (
	"context"
	"errors"
)

func (q *Queue) nextLocked() (*admissionPool, *command) {
	var selected *command
	var selectedPool *admissionPool
	for _, pool := range []*admissionPool{&q.ordinary, &q.progress} {
		for index := range pool.slots {
			slot := &pool.slots[index]
			if !slot.used || !slot.handed || slot.finished {
				continue
			}
			if selected == nil || slot.sequence < selected.sequence {
				selected, selectedPool = slot, pool
			}
		}
	}
	return selectedPool, selected
}

func (q *Queue) coordinate() {
	// This is the session execution lifetime, independent of every caller context.
	ctx := context.Background()
	q.mu.Lock()
	for {
		pool, slot := q.nextLocked()
		if slot == nil {
			if q.closing {
				break
			}
			changed := q.changed
			q.mu.Unlock()
			<-changed
			q.mu.Lock()
			continue
		}
		reject := q.aborting
		cause := errors.Join(ErrStopped, q.terminal)
		q.mu.Unlock()
		result := CommitResult{Decision: NotApplied}
		var statuses []Status
		var err error
		if reject {
			err = cause
		} else {
			if slot.observer != nil {
				slot.observer.BeforeExecute()
			}
			if slot.submit {
				result, err = q.executeSubmit(ctx, slot.entries)
			} else {
				statuses, err = q.opened.storage.lookup(ctx, slot.ids)
			}
		}
		q.mu.Lock()
		// Rejections carry the established cause, not a new execution failure.
		if !reject && errors.Is(err, ErrStorage) {
			q.terminal = errors.Join(q.terminal, err)
			q.aborting = true
			q.cutoffLocked()
		}
		slot.entries = nil
		slot.ids = nil
		slot.observer = nil
		slot.result, slot.statuses, slot.err = result, statuses, err
		slot.finished = true
		if slot.abandoned {
			q.clearLocked(pool, slot)
		} else {
			q.signalLocked()
		}
	}
	graceful := !q.aborting
	q.mu.Unlock()
	var err error
	if graceful {
		err = q.shutdownBarrier(ctx)
	}
	err = errors.Join(err, q.opened.close())
	q.mu.Lock()
	if err != nil {
		q.terminal = errors.Join(q.terminal, ErrStorage, err)
	}
	close(q.done)
	q.signalLocked()
	q.mu.Unlock()
}

func (q *Queue) rollbackSubmit(ctx context.Context, cause error) (CommitResult, error) {
	_, rollbackErr := q.opened.storage.conn.ExecContext(ctx, "ROLLBACK")
	ended, stateErr := q.opened.storage.autoCommit()
	err := errors.Join(cause, rollbackErr, stateErr)
	if rollbackErr == nil && stateErr == nil && ended {
		return CommitResult{Decision: NotApplied}, err
	}
	return CommitResult{Decision: Unknown}, errors.Join(ErrStorage, err)
}
