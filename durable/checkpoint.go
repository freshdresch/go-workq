package durable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mattn/go-sqlite3"
)

// The candidate remains unqualified until measured production installation in
// round 10b. Tests inject a finite envelope without equating it to the trigger.
const walEnvelopeBytes Bytes = 0

type walObserver interface{ WALBytes() (Bytes, error) }

func (q *Queue) observeWAL() (Bytes, error) {
	if q.walObserver != nil {
		return q.walObserver.WALBytes()
	}
	info, err := os.Stat(filepath.Join(q.opened.owner.path, "queue.sqlite3-wal"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, errors.Join(ErrStorage, err)
	}
	return Bytes(info.Size()), nil
}

func (q *Queue) sampleStorage(ctx context.Context) error {
	wal, err := q.observeWAL()
	if err != nil {
		return errors.Join(ErrStorage, err)
	}
	info, err := q.opened.main.Stat()
	if err != nil {
		return errors.Join(ErrStorage, err)
	}
	var pages, free uint64
	row := q.opened.storage.conn.QueryRowContext(ctx, "PRAGMA page_count")
	if err := row.Scan(&pages); err != nil {
		return errors.Join(ErrStorage, err)
	}
	row = q.opened.storage.conn.QueryRowContext(ctx, "PRAGMA freelist_count")
	if err := row.Scan(&free); err != nil {
		return errors.Join(ErrStorage, err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.snapshot.Store.DatabaseBytes = Bytes(info.Size())
	q.snapshot.Store.WALBytes = wal
	q.snapshot.Store.PageCount = pages
	q.snapshot.Store.FreePages = free
	q.snapshot.Disk.Valid = true
	q.snapshot.Disk.At = time.Now()
	q.addCounterLocked(&q.snapshot.Disk.Sequence, 1)
	return q.checkEnvelopeLocked(wal)
}

func (q *Queue) checkEnvelopeLocked(wal Bytes) error {
	if q.walEnvelope != 0 && wal > q.walEnvelope {
		return q.enterMaintenanceLocked(WALEnvelopeExceeded)
	}
	return nil
}

func (q *Queue) enterMaintenanceLocked(reason MaintenanceReason) error {
	if q.maintenance {
		return ErrMaintenanceRequired
	}
	q.maintenance = true
	if q.terminal == nil {
		q.terminal = ErrMaintenanceRequired
		q.snapshot.Cause.Detail = reason.String()
		q.snapshot.Cause.Operation = CheckpointOperation
	}
	q.snapshot.Maintenance = MaintenanceDiagnostic{
		Reason: reason, WALBytes: q.snapshot.Store.WALBytes, Checkpoint: q.snapshot.Checkpoint,
		DatabaseBytes: q.snapshot.Store.DatabaseBytes, PageCount: q.snapshot.Store.PageCount,
		FreePages: q.snapshot.Store.FreePages,
	}
	q.stopDeliveryLocked()
	q.cacheCauseLocked()
	q.cacheStateLocked()
	q.publishStoppingLocked()
	q.signalLocked()
	return ErrMaintenanceRequired
}

func (q *Queue) beforeWrite(ctx context.Context) error {
	q.mu.Lock()
	if err := q.writeRefusalLocked(); err != nil {
		q.mu.Unlock()
		return err
	}
	cached := q.snapshot.Store.WALBytes
	err := q.checkEnvelopeLocked(cached)
	q.mu.Unlock()
	if err != nil {
		return err
	}
	if err := q.sampleStorage(ctx); err != nil {
		return err
	}
	q.mu.Lock()
	current := q.snapshot.Store.WALBytes
	q.mu.Unlock()
	if max(cached, current) >= q.config.Runtime.SQLite.CheckpointTrigger {
		if err := q.checkpoint(ctx); err != nil {
			return err
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	// This decision admits the transaction, including its BeforeExecute hook.
	return q.writeRefusalLocked()
}

func (q *Queue) writeRefusalLocked() error {
	if q.aborting {
		return ErrStopped
	}
	if q.maintenance {
		return ErrMaintenanceRequired
	}
	return nil
}

func (q *Queue) checkpoint(ctx context.Context) error {
	q.mu.Lock()
	if err := q.writeRefusalLocked(); err != nil {
		q.mu.Unlock()
		return err
	}
	// Admission is ordered with Abort, but SQL never holds the state mutex.
	q.startCheckpointLocked()
	q.mu.Unlock()
	return q.runCheckpoint(ctx)
}

func (q *Queue) idleCheckpoint(ctx context.Context) error {
	q.mu.Lock()
	if q.closing {
		q.mu.Unlock()
		return nil
	}
	if err := q.writeRefusalLocked(); err != nil {
		q.mu.Unlock()
		return err
	}
	if q.snapshot.Store.WALBytes == 0 {
		q.mu.Unlock()
		return nil
	}
	// Idle admission shares the ordering of Close and Abort, with SQL outside the mutex.
	q.startCheckpointLocked()
	q.mu.Unlock()
	return q.runCheckpoint(ctx)
}

func (q *Queue) runCheckpoint(ctx context.Context) (err error) {
	defer func() { q.finishCheckpoint(err) }()
	return q.checkedCheckpoint(ctx, q.config.Durability)
}

func (q *Queue) startCheckpointLocked() {
	q.addCounterLocked(&q.snapshot.Outcomes.Checkpoints, 1)
	q.snapshot.Checkpoint = CheckpointStats{}
}

func (q *Queue) finishCheckpoint(err error) {
	if err == nil {
		return
	}
	q.mu.Lock()
	q.addCounterLocked(&q.snapshot.Outcomes.CheckpointFailures, 1)
	q.mu.Unlock()
}

func (q *Queue) checkedCheckpoint(ctx context.Context, durability Durability) error {
	if err := q.opened.storage.selectDurability(ctx, Synchronized); err != nil {
		return errors.Join(ErrStorage, err)
	}
	if err := q.truncateWAL(ctx); err != nil {
		return err
	}
	if durability == ProcessCrash {
		if err := q.opened.storage.selectDurability(ctx, durability); err != nil {
			return errors.Join(ErrStorage, err)
		}
	}
	return nil
}

func (q *Queue) truncateWAL(ctx context.Context) error {
	started := time.Now()
	var stats CheckpointStats
	err := q.opened.storage.conn.QueryRowContext(ctx,
		"PRAGMA wal_checkpoint(TRUNCATE)").Scan(&stats.Busy, &stats.Frames, &stats.Completed)
	stats.Duration = time.Since(started)
	stats.Valid = err == nil
	q.mu.Lock()
	q.snapshot.Checkpoint = stats
	q.mu.Unlock()
	if err != nil {
		var sqliteErr sqlite3.Error
		if errors.As(err, &sqliteErr) &&
			(sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked) {
			q.mu.Lock()
			result := q.enterMaintenanceLocked(CheckpointBlocked)
			q.mu.Unlock()
			return result
		}
		return errors.Join(ErrStorage, err)
	}
	wal, err := q.observeWAL()
	if err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.snapshot.Store.WALBytes = wal
	if stats.Busy != 0 || stats.Frames != stats.Completed || wal != 0 {
		return q.enterMaintenanceLocked(CheckpointBlocked)
	}
	return nil
}

func (q *Queue) shutdownBarrier(ctx context.Context) (err error) {
	// The coordinator admitted this barrier under the lifecycle mutex.
	q.mu.Lock()
	q.startCheckpointLocked()
	q.mu.Unlock()
	defer func() { q.finishCheckpoint(err) }()
	if err := q.checkedCheckpoint(ctx, Synchronized); err != nil {
		return fmt.Errorf("shutdown checkpoint: %w", err)
	}
	return q.sampleStorage(ctx)
}
