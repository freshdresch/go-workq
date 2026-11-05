package durable

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// cleanupTurn either removes eligible receipts or clears an empty marker. Its
// fixed key buffer is independent of the configured turn quota.
func (q *Queue) cleanupTurn(ctx context.Context) error {
	if err := q.beforeWrite(ctx); err != nil {
		return err
	}
	defer func() {
		if err := q.sampleStorage(ctx); err != nil {
			detail := describeCause(err)
			if !detail.Storage {
				return
			}
			q.mu.Lock()
			q.faultLocked(err, detail)
			q.mu.Unlock()
		}
	}()
	storage := q.opened.storage
	if _, err := storage.conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return errors.Join(ErrStorage, err)
	}
	metadata, err := q.deleteRetiredReceipts(ctx)
	if err != nil {
		_, failure := q.rollbackSubmit(ctx, errors.Join(ErrStorage, err))
		return failure
	}
	if err := storage.writeSubmitMetadata(ctx, metadata); err != nil {
		_, failure := q.rollbackSubmit(ctx, errors.Join(ErrStorage, err))
		return failure
	}
	commitStarted := time.Now()
	if _, err := storage.conn.ExecContext(ctx, "COMMIT"); err != nil {
		return errors.Join(ErrStorage, err)
	}
	previous := q.opened.metadata.counts
	q.opened.metadata = metadata
	q.mu.Lock()
	q.snapshot.Store.Counts = metadata.counts
	q.snapshot.Cleanup = CleanupStats{Deleted: previous.Receipts - metadata.counts.Receipts,
		MarkerCleared: previous.CleanupMarkers > metadata.counts.CleanupMarkers}
	q.addCounterLocked(&q.snapshot.Outcomes.Commits, 1)
	q.addLatencyLocked(&q.snapshot.CommitLatency, time.Since(commitStarted))
	q.addCounterLocked(&q.snapshot.Outcomes.CleanupTurns, 1)
	q.mu.Unlock()
	return nil
}

func (q *Queue) deleteRetiredReceipts(ctx context.Context) (storeMetadata, error) {
	metadata := q.opened.metadata
	conn := q.opened.storage.conn
	var producer, floor []byte
	err := conn.QueryRowContext(ctx,
		cleanupProducerQuery,
	).Scan(&producer, &floor)
	if err != nil {
		return storeMetadata{}, err
	}
	if len(producer) != 16 || len(floor) != 8 {
		return storeMetadata{}, ErrStorage
	}
	var lower, upper [24]byte
	copy(lower[:16], producer)
	copy(upper[:16], producer)
	copy(upper[16:], floor)
	limits := q.config.Runtime.Maintenance
	// An acknowledged receipt occupies 121 encoded bytes, plus 26 for its marker.
	remaining := min(Bytes(limits.CleanupRows), (limits.CleanupEncoded-26)/121)
	for remaining > 0 {
		limit := int(min(remaining, 256))
		count, err := q.deleteRetiredReceiptChunk(ctx, lower, upper, limit)
		if err != nil {
			return storeMetadata{}, err
		}
		if Count(count) > metadata.counts.Receipts {
			return storeMetadata{}, ErrStorage
		}
		metadata.counts.Receipts -= Count(count)
		remaining -= Bytes(count)
		if count < limit {
			break
		}
	}
	var exists int
	err = conn.QueryRowContext(ctx,
		"SELECT 1 FROM receipts WHERE id>=? AND id<=? AND state=1 LIMIT 1",
		lower[:], upper[:]).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := conn.ExecContext(ctx,
			"UPDATE producers SET cleanup_needed=0 WHERE producer=?", producer); err != nil {
			return storeMetadata{}, err
		}
		if metadata.counts.CleanupMarkers == 0 {
			return storeMetadata{}, ErrStorage
		}
		metadata.counts.CleanupMarkers--
	} else if err != nil {
		return storeMetadata{}, err
	}
	return metadata, nil
}

func (q *Queue) deleteRetiredReceiptChunk(
	ctx context.Context,
	lower, upper [24]byte,
	limit int,
) (int, error) {
	var keys [256][24]byte
	rows, err := q.opened.storage.conn.QueryContext(
		ctx,
		cleanupReceiptQuery,
		lower[:],
		upper[:],
		limit,
	)
	if err != nil {
		return 0, err
	}
	count := 0
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return 0, errors.Join(err, rows.Close())
		}
		if len(encoded) != 24 || count == len(keys) {
			return 0, errors.Join(ErrStorage, rows.Close())
		}
		copy(keys[count][:], encoded)
		count++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return 0, err
	}
	for index := range count {
		_, err := q.opened.storage.conn.ExecContext(
			ctx,
			"DELETE FROM receipts WHERE id=?",
			keys[index][:],
		)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

const cleanupProducerQuery = `SELECT producer,retired_through FROM producers
 WHERE cleanup_needed=1 ORDER BY producer LIMIT 1`

const cleanupReceiptQuery = `SELECT id FROM receipts
 WHERE id>=? AND id<=? AND state=1 ORDER BY id LIMIT ?`
