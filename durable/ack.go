package durable

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
)

// AckBatch atomically completes current attempts or exact retained duplicate Acks.
// Duplicate inputs are invalid. Cancellation after handoff can return Unknown.
// After reopening, every token from an earlier epoch is stale.
func (q *Queue) AckBatch(ctx context.Context, tokens []Token) (CommitResult, error) {
	refused := CommitResult{Decision: NotApplied}
	if len(tokens) > int(q.config.Runtime.Batches.AckItems) {
		return refused, ErrInvalidInput
	}
	for index, token := range tokens {
		for previous := range index {
			if tokens[previous] == token {
				return refused, ErrInvalidInput
			}
		}
	}
	slot, err := q.reserve(ctx, &q.progress, 256+128*Bytes(len(tokens)), false)
	if err != nil {
		return refused, err
	}
	slot.kind = ackCommand
	slot.tokens = make([]Token, len(tokens))
	copy(slot.tokens, tokens)
	if err := q.handoff(ctx, slot); err != nil {
		q.mu.Lock()
		q.clearLocked(&q.progress, slot)
		q.mu.Unlock()
		return refused, err
	}
	result, _, err := q.await(ctx, &q.progress, slot)
	return result, err
}

// Release invalidates a current acquired attempt in global handoff order. It has
// no cancellation ambiguity. Stop processing before calling it. Duplicate Release
// and Release after Ack are stale. A cutoff before handoff leaves the claim intact.
func (q *Queue) Release(token Token) error {
	ctx := context.Background()
	slot, err := q.reserve(ctx, &q.progress, 384, false)
	if err != nil {
		return err
	}
	slot.kind, slot.token = releaseCommand, token
	if err := q.handoff(ctx, slot); err != nil {
		q.mu.Lock()
		q.clearLocked(&q.progress, slot)
		q.mu.Unlock()
		return err
	}
	_, _, err = q.await(ctx, &q.progress, slot)
	return err
}

func (q *Queue) tokenID(token Token) (ID, error) {
	metadata := q.opened.metadata
	if !bytes.Equal(token.bytes[:16], metadata.uuid[:]) ||
		binary.BigEndian.Uint64(token.bytes[16:24]) != metadata.epoch ||
		binary.BigEndian.Uint64(token.bytes[48:]) == 0 {
		return ID{}, ErrStaleToken
	}
	return decodeID(token.bytes[24:48])
}

func (q *Queue) currentClaimLocked(id ID, token Token) int {
	index := q.claimLocked(id)
	if index < 0 {
		return -1
	}
	slot := &q.claims[index]
	if !slot.acquired || slot.delivery.Token != token {
		return -1
	}
	return index
}

func (q *Queue) executeRelease(token Token) error {
	id, err := q.tokenID(token)
	if err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	index := q.currentClaimLocked(id, token)
	if index < 0 {
		return ErrStaleToken
	}
	q.clearClaimLocked(index)
	return nil
}

func (q *Queue) validateAck(
	ctx context.Context,
	tokens []Token,
	counts StoreCounts,
) (StoreCounts, error) {
	for _, token := range tokens {
		id, err := q.tokenID(token)
		if err != nil {
			return StoreCounts{}, err
		}
		// SQL includes earlier acknowledgments in this group, while claims stay
		// unchanged until the outer commit establishes their completion.
		var state int
		var stored []byte
		err = q.opened.storage.conn.QueryRowContext(ctx,
			"SELECT state,ack_token FROM receipts WHERE id=?", token.bytes[24:48]).Scan(&state, &stored)
		if errors.Is(err, sql.ErrNoRows) {
			return StoreCounts{}, ErrStaleToken
		}
		if err != nil {
			return StoreCounts{}, errors.Join(ErrStorage, err)
		}
		if state == 1 {
			if !bytes.Equal(stored, token.bytes[:]) {
				return StoreCounts{}, ErrStaleToken
			}
			continue
		}
		q.mu.Lock()
		current := q.currentClaimLocked(id, token) >= 0
		q.mu.Unlock()
		if !current {
			return StoreCounts{}, ErrStaleToken
		}
		var length int64
		err = q.opened.storage.conn.QueryRowContext(ctx,
			"SELECT payload_length FROM payloads WHERE id=?", token.bytes[24:48]).Scan(&length)
		if err != nil {
			return StoreCounts{}, errors.Join(ErrStorage, err)
		}
		if length < 0 || counts.PendingItems == 0 || Bytes(length) > counts.PendingPayload {
			return StoreCounts{}, ErrStorage
		}
		counts.PendingItems--
		counts.PendingPayload -= Bytes(length)
	}
	return counts, nil
}

func (q *Queue) applyAck(ctx context.Context, tokens []Token) error {
	for _, token := range tokens {
		if _, err := q.opened.storage.conn.ExecContext(ctx,
			"UPDATE receipts SET state=1,ack_token=? WHERE id=? AND state=0",
			token.bytes[:], token.bytes[24:48]); err != nil {
			return err
		}
		if _, err := q.opened.storage.conn.ExecContext(ctx,
			"DELETE FROM payloads WHERE id=?", token.bytes[24:48]); err != nil {
			return err
		}
	}
	return nil
}
