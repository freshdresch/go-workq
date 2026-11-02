package durable

import (
	"context"
	"database/sql"
	"errors"
)

// Lookup returns owned statuses in input order, including duplicate IDs. Empty
// input is an ordered barrier. An error returns nil and never implies Absent.
func (q *Queue) Lookup(ctx context.Context, ids []ID) ([]Status, error) {
	if ctx == nil || len(ids) > int(q.config.Runtime.Batches.LookupItems) {
		return nil, ErrInvalidInput
	}
	charge, err := lookupCharge(Bytes(len(ids)))
	if err != nil {
		return nil, err
	}
	slot, err := q.reserve(ctx, &q.ordinary, charge, false)
	if err != nil {
		return nil, err
	}
	slot.ids = make([]ID, len(ids))
	copy(slot.ids, ids)
	if err := q.handoff(ctx, slot); err != nil {
		q.mu.Lock()
		q.clearLocked(&q.ordinary, slot)
		q.mu.Unlock()
		return nil, err
	}
	_, statuses, err := q.await(ctx, &q.ordinary, slot)
	return statuses, err
}

func (s *store) lookup(ctx context.Context, ids []ID) ([]Status, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	statuses := make([]Status, len(ids))
	for index, id := range ids {
		status, err := s.lookupID(ctx, id)
		if err != nil {
			return nil, errors.Join(ErrStorage, err)
		}
		statuses[index] = status
	}
	return statuses, nil
}

func (s *store) lookupID(ctx context.Context, id ID) (Status, error) {
	status := Status{ID: id, Kind: Absent}
	var hasRetirement int
	var encodedFloor []byte
	err := s.conn.QueryRowContext(ctx,
		"SELECT has_retirement,retired_through FROM producers WHERE producer=?",
		id.Producer[:]).Scan(&hasRetirement, &encodedFloor)
	if errors.Is(err, sql.ErrNoRows) {
		return status, nil
	}
	if err != nil {
		return Status{}, err
	}
	floor, err := decodeUint64(encodedFloor)
	if err != nil {
		return Status{}, err
	}
	status.HasRetirement = hasRetirement == 1
	if status.HasRetirement {
		status.RetiredThrough = floor
		if id.Sequence <= floor {
			status.Kind = Retired
			return status, nil
		}
	}
	encoded := encodeID(id)
	var state int
	err = s.conn.QueryRowContext(ctx,
		"SELECT state FROM receipts WHERE id=?", encoded[:]).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return status, nil
	}
	if err != nil {
		return Status{}, err
	}
	status.Kind = Pending
	if state == 1 {
		status.Kind = Acknowledged
	}
	return status, nil
}
