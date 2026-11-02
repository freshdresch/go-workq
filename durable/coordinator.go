package durable

import (
	"context"
	"errors"
)

func (q *Queue) nextLocked() (*admissionPool, *command) {
	return q.nextAfterLocked(0)
}

func (q *Queue) nextAfterLocked(after uint64) (*admissionPool, *command) {
	var selected *command
	var selectedPool *admissionPool
	for _, pool := range []*admissionPool{&q.ordinary, &q.progress} {
		for index := range pool.slots {
			slot := &pool.slots[index]
			if !slot.used || !slot.handed || slot.finished || slot.sequence <= after {
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
	var commandTurns Count
	for {
		if q.deliveryDirty && !q.deliveryStopped {
			q.mu.Unlock()
			err := q.dispatch(ctx)
			q.mu.Lock()
			if err != nil {
				q.terminal = errors.Join(q.terminal, err)
				q.aborting = true
				q.cutoffLocked()
			}
		}
		if q.deliveryStopped && !q.deliveryClosed {
			q.closeDeliveryLocked()
		}
		pool, slot := q.nextLocked()
		cleanupDue := q.opened.metadata.counts.CleanupMarkers > 0 &&
			(slot == nil || commandTurns >= q.config.Runtime.Maintenance.MaxCommandTurns)
		if cleanupDue && !q.aborting && !q.closing {
			q.mu.Unlock()
			err := q.cleanupTurn(ctx)
			q.mu.Lock()
			commandTurns = 0
			if err != nil {
				q.faultLocked(err)
			}
			continue
		}
		if slot == nil {
			if q.closing {
				break
			}
			if q.deliveryDirty && !q.deliveryStopped {
				continue
			}
			changed := q.changed
			q.mu.Unlock()
			<-changed
			q.mu.Lock()
			continue
		}
		if slot.mutation() && !q.aborting {
			group := q.collectGroupLocked(pool, slot)
			if !q.aborting {
				q.mu.Unlock()
				err := q.executeGroup(ctx, &group)
				q.mu.Lock()
				if err != nil {
					q.faultLocked(err)
				}
				q.pending = q.opened.metadata.counts.PendingItems
				for index := range group.count {
					command := &group.commands[index]
					if command.err == nil {
						q.deliveryDirty = true
						q.scanWrapped = false
					}
					q.finishLocked(command.pool, command.slot, command.result, nil, command.err)
				}
				commandTurns = min(commandTurns+1, q.config.Runtime.Maintenance.MaxCommandTurns)
				continue
			}
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
			if slot.kind == releaseCommand {
				err = q.executeRelease(slot.token)
			} else {
				statuses, err = q.opened.storage.lookup(ctx, slot.ids)
			}
		}
		q.mu.Lock()
		if !reject && errors.Is(err, ErrStorage) {
			q.faultLocked(err)
		}
		if err == nil && slot.kind == releaseCommand {
			q.deliveryDirty = true
			q.scanWrapped = false
		}
		q.finishLocked(pool, slot, result, statuses, err)
		commandTurns = min(commandTurns+1, q.config.Runtime.Maintenance.MaxCommandTurns)
	}
	q.closeDeliveryLocked()
	for index := range q.claims {
		q.clearClaimLocked(index)
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

func (q *Queue) faultLocked(err error) {
	q.terminal = errors.Join(q.terminal, err)
	q.aborting = true
	q.cutoffLocked()
}

func (q *Queue) finishLocked(
	pool *admissionPool,
	slot *command,
	result CommitResult,
	statuses []Status,
	err error,
) {
	slot.tokens, slot.entries, slot.ids, slot.observer = nil, nil, nil, nil
	slot.result, slot.statuses, slot.err = result, statuses, err
	slot.finished = true
	if slot.abandoned {
		q.clearLocked(pool, slot)
	} else {
		q.signalLocked()
	}
}
