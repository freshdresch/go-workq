package durable

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
)

// SubmitBatch atomically accepts all entries or identical retries. Caller inputs
// must remain unchanged until return. After return callers may reuse storage even
// when cancellation returns Unknown. StopAdmission rejects empty batches too.
func (q *Queue) SubmitBatch(ctx context.Context, entries []Entry) (CommitResult, error) {
	return q.submitBatchObserved(ctx, entries, nil)
}

func (q *Queue) submitBatchObserved(
	ctx context.Context,
	entries []Entry,
	observer commandObserver,
) (CommitResult, error) {
	refused := CommitResult{Decision: NotApplied}
	allocation, err := q.validateSubmit(entries)
	if err != nil {
		return refused, err
	}
	slot, err := q.reserve(ctx, &q.ordinary, allocation.charge, true)
	if err != nil {
		return refused, err
	}
	slot.entries = copyEntries(entries, allocation.payload)
	slot.observer = observer
	if err := q.handoff(ctx, slot); err != nil {
		q.mu.Lock()
		q.clearLocked(&q.ordinary, slot)
		q.mu.Unlock()
		return refused, err
	}
	result, _, err := q.await(ctx, &q.ordinary, slot)
	return result, err
}

type submitAllocation struct {
	payload int
	charge  Bytes
}

func (q *Queue) validateSubmit(entries []Entry) (submitAllocation, error) {
	if len(entries) > int(q.config.Runtime.Batches.SubmitItems) {
		return submitAllocation{}, ErrInvalidInput
	}
	var encoded, payload, codecs, largestCodec Bytes
	for index, entry := range entries {
		codecBytes, payloadBytes := Bytes(len(entry.Codec)), Bytes(len(entry.Payload))
		if codecBytes > q.config.Policy.MaxCodec || payloadBytes > q.config.Policy.MaxPayload {
			return submitAllocation{}, ErrInvalidInput
		}
		work, err := checkedAdd(codecBytes, payloadBytes)
		if err != nil {
			return submitAllocation{}, err
		}
		work, err = checkedAdd(work, 88)
		if err != nil {
			return submitAllocation{}, err
		}
		encoded, err = checkedAdd(encoded, work)
		if err != nil || encoded > q.config.Runtime.Batches.SubmitEncoded {
			return submitAllocation{}, ErrInvalidInput
		}
		payload, err = checkedAdd(payload, payloadBytes)
		if err != nil {
			return submitAllocation{}, err
		}
		codec, err := allocationCharge(codecBytes)
		if err != nil {
			return submitAllocation{}, err
		}
		codecs, err = checkedAdd(codecs, codec)
		if err != nil {
			return submitAllocation{}, err
		}
		largestCodec = max(largestCodec, codec)
		for previous := range index {
			if entries[previous].ID == entry.ID {
				return submitAllocation{}, ErrInvalidInput
			}
		}
	}
	charge, err := submitCharge(Bytes(len(entries)), payload, codecs, largestCodec)
	if err != nil {
		return submitAllocation{}, err
	}
	return submitAllocation{payload: int(payload), charge: charge}, nil
}

func copyEntries(entries []Entry, payloadBytes int) []Entry {
	if len(entries) == 0 {
		return nil
	}
	arena := make([]byte, payloadBytes)
	copied := make([]Entry, len(entries))
	offset := 0
	for index, entry := range entries {
		end := offset + len(entry.Payload)
		copy(arena[offset:end], entry.Payload)
		copied[index] = Entry{ID: entry.ID, Codec: strings.Clone(entry.Codec),
			Payload: arena[offset:end:end]}
		offset = end
	}
	return copied
}

func (q *Queue) executeSubmit(ctx context.Context, entries []Entry) (CommitResult, error) {
	storage := q.opened.storage
	if _, err := storage.conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return CommitResult{Decision: NotApplied}, errors.Join(ErrStorage, err)
	}
	metadata, fresh, err := q.validateStoredSubmit(ctx, entries)
	if err != nil {
		return q.rollbackSubmit(ctx, err)
	}
	for index, entry := range entries {
		if !fresh[index] {
			continue
		}
		order := metadata.lastOrder - uint64(countFreshAfter(fresh, index))
		if err := storage.insertEntry(ctx, entry, order); err != nil {
			return q.rollbackSubmit(ctx, errors.Join(ErrStorage, err))
		}
	}
	if err := storage.writeSubmitMetadata(ctx, metadata); err != nil {
		return q.rollbackSubmit(ctx, errors.Join(ErrStorage, err))
	}
	if _, err := storage.conn.ExecContext(ctx, "COMMIT"); err != nil {
		// AutoCommit cannot prove a failed COMMIT did not persist.
		return CommitResult{Decision: Unknown}, errors.Join(ErrStorage, err)
	}
	q.opened.metadata = metadata
	return CommitResult{Decision: Applied}, nil
}

func countFreshAfter(fresh []bool, index int) int {
	count := 0
	for _, value := range fresh[index+1:] {
		if value {
			count++
		}
	}
	return count
}

func (q *Queue) validateStoredSubmit(
	ctx context.Context,
	entries []Entry,
) (storeMetadata, []bool, error) {
	metadata := q.opened.metadata
	fresh := make([]bool, len(entries))
	producers := make([][16]byte, 0, len(entries))
	for index, entry := range entries {
		status, err := q.opened.storage.lookupID(ctx, entry.ID)
		if err != nil {
			return storeMetadata{}, nil, errors.Join(ErrStorage, err)
		}
		if status.Kind == Retired {
			return storeMetadata{}, nil, ErrRetired
		}
		encoded := encodeID(entry.ID)
		var storedDigest []byte
		err = q.opened.storage.conn.QueryRowContext(ctx,
			"SELECT digest FROM receipts WHERE id=?", encoded[:]).Scan(&storedDigest)
		digest := entryDigest(entry.Codec, entry.Payload)
		if err == nil {
			if !bytes.Equal(storedDigest, digest[:]) {
				return storeMetadata{}, nil, ErrConflict
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return storeMetadata{}, nil, errors.Join(ErrStorage, err)
		}
		fresh[index] = true
		metadata.lastOrder, err = nextIdentity(metadata.lastOrder)
		if err != nil {
			return storeMetadata{}, nil, err
		}
		metadata.counts.PendingItems++
		metadata.counts.PendingPayload += Bytes(len(entry.Payload))
		metadata.counts.Receipts++
		var exists int
		err = q.opened.storage.conn.QueryRowContext(ctx,
			"SELECT 1 FROM producers WHERE producer=?", entry.ID.Producer[:]).Scan(&exists)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return storeMetadata{}, nil, errors.Join(ErrStorage, err)
		}
		if errors.Is(err, sql.ErrNoRows) {
			known := false
			for _, producer := range producers {
				if producer == entry.ID.Producer {
					known = true
					break
				}
			}
			if !known {
				producers = append(producers, entry.ID.Producer)
				metadata.counts.Producers++
			}
		}
	}
	counts, policy := metadata.counts, metadata.policy
	if counts.PendingItems > policy.MaxPendingItems ||
		counts.PendingPayload > policy.MaxPendingPayload || counts.Receipts > policy.MaxReceipts ||
		counts.Producers > policy.MaxProducers {
		return storeMetadata{}, nil, ErrCapacity
	}
	return metadata, fresh, nil
}

func (s *store) insertEntry(ctx context.Context, entry Entry, order uint64) error {
	zero := encodeUint64(0)
	if _, err := s.conn.ExecContext(ctx,
		"INSERT OR IGNORE INTO producers VALUES(?,0,?,0)", entry.ID.Producer[:], zero[:]); err != nil {
		return err
	}
	encoded := encodeID(entry.ID)
	digest := entryDigest(entry.Codec, entry.Payload)
	encodedOrder := encodeUint64(order)
	if _, err := s.conn.ExecContext(ctx,
		"INSERT INTO receipts VALUES(?,?,0,?,NULL)", encoded[:], digest[:], encodedOrder[:]); err != nil {
		return err
	}
	// A nonnil zero-length slice binds a BLOB. Nil would bind SQL NULL.
	payload := entry.Payload
	if payload == nil {
		payload = []byte{}
	}
	_, err := s.conn.ExecContext(ctx, "INSERT INTO payloads VALUES(?,?,?,?)",
		encoded[:], []byte(entry.Codec), payload, len(payload))
	return err
}

func (s *store) writeSubmitMetadata(ctx context.Context, metadata storeMetadata) error {
	values := [...]struct {
		key   int
		value uint64
	}{
		{metaLastOrder, metadata.lastOrder},
		{metaPendingItems, uint64(metadata.counts.PendingItems)},
		{metaPendingPayload, uint64(metadata.counts.PendingPayload)},
		{metaReceipts, uint64(metadata.counts.Receipts)},
		{metaProducers, uint64(metadata.counts.Producers)},
	}
	for _, value := range values {
		encoded := encodeUint64(value.value)
		if _, err := s.conn.ExecContext(ctx, "UPDATE meta SET value=? WHERE key=?",
			encoded[:], value.key); err != nil {
			return err
		}
	}
	return nil
}
