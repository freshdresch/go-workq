package durable

import (
	"context"
	"database/sql"
	"errors"
)

// RetireThrough establishes an inclusive replay barrier, including holes and zero.
// Pending work at or below sequence refuses the whole command with ErrConflict.
// Producer records consume lifetime capacity. Receipt capacity returns after cleanup.
// Cancellation after handoff can return Unknown.
func (q *Queue) RetireThrough(
	ctx context.Context,
	producer [16]byte,
	sequence uint64,
) (CommitResult, error) {
	return q.retireThroughObserved(ctx, producer, sequence, nil)
}

func (q *Queue) retireThroughObserved(
	ctx context.Context,
	producer [16]byte,
	sequence uint64,
	observer commandObserver,
) (CommitResult, error) {
	refused := CommitResult{Decision: NotApplied}
	slot, err := q.reserve(ctx, &q.progress, 384, false)
	if err != nil {
		return refused, err
	}
	slot.observer = observer
	slot.kind = retireCommand
	slot.retirement = ID{Producer: producer, Sequence: sequence}
	if err := q.handoff(ctx, slot); err != nil {
		q.mu.Lock()
		q.clearLocked(&q.progress, slot)
		q.mu.Unlock()
		return refused, err
	}
	result, _, err := q.await(ctx, &q.progress, slot)
	return result, err
}

func (q *Queue) applyRetirement(
	ctx context.Context,
	retirement ID,
	metadata storeMetadata,
) (storeMetadata, error) {
	conn := q.opened.storage.conn
	var has, cleanup int
	var floor []byte
	err := conn.QueryRowContext(ctx,
		"SELECT has_retirement,retired_through,cleanup_needed FROM producers WHERE producer=?",
		retirement.Producer[:]).Scan(&has, &floor, &cleanup)
	unknown := errors.Is(err, sql.ErrNoRows)
	if err != nil && !unknown {
		return storeMetadata{}, errors.Join(ErrStorage, err)
	}
	if has != 0 {
		previous, err := decodeUint64(floor)
		if err != nil {
			return storeMetadata{}, errors.Join(ErrStorage, err)
		}
		if retirement.Sequence <= previous {
			return metadata, nil
		}
	}
	if unknown && metadata.counts.Producers == metadata.policy.MaxProducers {
		return storeMetadata{}, ErrCapacity
	}
	lower := encodeID(ID{Producer: retirement.Producer})
	upper := encodeID(retirement)
	var pending int
	err = conn.QueryRowContext(ctx,
		"SELECT 1 FROM receipts WHERE state=0 AND id>=? AND id<=? LIMIT 1",
		lower[:], upper[:]).Scan(&pending)
	if err == nil {
		return storeMetadata{}, ErrConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return storeMetadata{}, errors.Join(ErrStorage, err)
	}
	encodedFloor := encodeUint64(retirement.Sequence)
	if unknown {
		_, err = conn.ExecContext(ctx, "INSERT INTO producers VALUES(?,1,?,1)",
			retirement.Producer[:], encodedFloor[:])
		metadata.counts.Producers++
	} else {
		_, err = conn.ExecContext(ctx,
			"UPDATE producers SET has_retirement=1,retired_through=?,cleanup_needed=1 WHERE producer=?",
			encodedFloor[:], retirement.Producer[:])
	}
	if err != nil {
		return storeMetadata{}, errors.Join(ErrStorage, err)
	}
	if has == 0 {
		metadata.counts.RetiredProducers++
	}
	if cleanup == 0 {
		metadata.counts.CleanupMarkers++
	}
	return metadata, nil
}
