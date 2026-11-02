package durable

import (
	"context"
	"errors"
	"time"
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
	ticker := time.NewTicker(q.config.Runtime.SQLite.CheckpointInterval)
	defer ticker.Stop()
	if err := q.sampleStorage(ctx); err != nil {
		detail := describeCause(err)
		q.mu.Lock()
		if detail.Storage {
			q.faultLocked(err, detail)
		}
		q.mu.Unlock()
	}
	q.mu.Lock()
	var commandTurns Count
	for {
		if q.deliveryDirty && !q.deliveryStopped {
			q.mu.Unlock()
			err := q.dispatch(ctx)
			detail := describeCause(err)
			q.mu.Lock()
			if err != nil && !detail.Maintenance {
				q.faultLocked(err, detail)
			}
		}
		if q.deliveryStopped && !q.deliveryClosed {
			q.closeDeliveryLocked()
		}
		pool, slot := q.nextLocked()
		cleanupDue := q.opened.metadata.counts.CleanupMarkers > 0 &&
			(slot == nil || commandTurns >= q.config.Runtime.Maintenance.MaxCommandTurns)
		if cleanupDue && !q.aborting && !q.closing && !q.maintenance {
			q.mu.Unlock()
			err := q.cleanupTurn(ctx)
			detail := describeCause(err)
			q.mu.Lock()
			commandTurns = 0
			if err != nil && !detail.Maintenance {
				q.faultLocked(err, detail)
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
			select {
			case <-changed:
			case <-ticker.C:
				q.mu.Lock()
				healthy := !q.aborting && !q.maintenance && !q.closing
				q.mu.Unlock()
				if healthy {
					err := q.sampleStorage(ctx)
					if err == nil {
						err = q.idleCheckpoint(ctx)
						if errors.Is(err, ErrStopped) {
							err = nil
						}
					}
					detail := describeCause(err)
					if err != nil && !detail.Maintenance {
						q.mu.Lock()
						q.faultLocked(err, detail)
						q.mu.Unlock()
					}
				}
			}
			q.mu.Lock()
			continue
		}
		if slot.mutation() && !q.aborting && !q.maintenance {
			group := q.collectGroupLocked(pool, slot)
			if !q.aborting {
				q.mu.Unlock()
				err := q.executeGroup(ctx, &group)
				detail := describeCause(err)
				q.mu.Lock()
				if err != nil && !detail.Maintenance {
					q.faultLocked(err, detail)
				}
				q.snapshot.LastGroup = q.snapshot.CurrentGroup
				q.snapshot.CurrentGroup = GroupStats{}
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
		reject := q.aborting || (q.maintenance && slot.mutation())
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
		detail := describeCause(err)
		q.mu.Lock()
		if !reject && detail.Storage {
			q.faultLocked(err, detail)
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
	// This locked decision admits the barrier, including healthy maintenance shutdown.
	graceful := !q.aborting
	q.mu.Unlock()
	var err error
	if graceful {
		err = q.shutdownBarrier(ctx)
	}
	if closeErr := q.opened.close(); closeErr != nil {
		err = errors.Join(err, ErrStorage, closeErr)
	}
	detail := describeCause(err)
	q.mu.Lock()
	if err != nil {
		q.terminal = errors.Join(q.terminal, err)
		q.faulted = q.faulted || detail.Storage
		if q.snapshot.Cause.Detail == "" {
			q.snapshot.Cause = detail
			q.snapshot.Cause.Operation = CloseOperation
		}
	}
	q.cacheCauseLocked()
	q.cacheStateLocked()
	q.snapshot.CurrentGroup = GroupStats{}
	q.snapshot.State = Closed
	close(q.done)
	q.signalLocked()
	q.mu.Unlock()
}

func (q *Queue) rollbackSubmit(ctx context.Context, cause error) (CommitResult, error) {
	_, rollbackErr := q.opened.storage.conn.ExecContext(ctx, "ROLLBACK")
	ended, stateErr := q.opened.storage.autoCommit()
	err := errors.Join(cause, rollbackErr, stateErr)
	if rollbackErr == nil && stateErr == nil && ended {
		q.mu.Lock()
		q.addCounterLocked(&q.snapshot.Outcomes.Rollbacks, 1)
		q.mu.Unlock()
		return CommitResult{Decision: NotApplied}, err
	}
	return CommitResult{Decision: Unknown}, errors.Join(ErrStorage, err)
}

func (q *Queue) faultLocked(err error, detail CauseInfo) {
	if q.terminal == nil {
		q.terminal = err
		q.snapshot.Cause = detail
		if detail.Exhausted {
			q.snapshot.Cause.Operation = DeliveryOperation
		} else {
			q.snapshot.Cause.Operation = StorageOperation
		}
		q.exhausted = detail.Exhausted
	} else if detail.Storage {
		q.terminal = errors.Join(q.terminal, err)
	}
	q.faulted = q.faulted || detail.Storage
	q.cacheCauseLocked()
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
	if !slot.abandoned && slot.mutation() && result.Decision == Unknown {
		q.addCounterLocked(&q.snapshot.Outcomes.UnknownResponses, 1)
	}
	if slot.kind == ackCommand && !slot.submit {
		q.addLatencyLocked(&q.snapshot.AckLatency, time.Since(slot.handedAt))
		if err == nil {
			q.addCounterLocked(&q.snapshot.Outcomes.Acks, uint64(len(slot.tokens)))
		}
	}
	slot.tokens, slot.entries, slot.ids, slot.observer = nil, nil, nil, nil
	slot.result, slot.statuses, slot.err = result, statuses, err
	slot.finished = true
	if slot.abandoned {
		q.clearLocked(pool, slot)
	} else {
		q.signalLocked()
	}
}
