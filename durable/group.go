package durable

import (
	"context"
	"errors"
	"time"
)

type groupedCommand struct {
	pool   *admissionPool
	slot   *command
	result CommitResult
	err    error
}

type commandGroup struct {
	commands   []groupedCommand
	count      int
	operations Count
	encoded    Bytes
}

func (c *command) mutation() bool {
	return c.submit || c.kind == ackCommand || c.kind == retireCommand
}

func (c *command) work() (Count, Bytes) {
	if c.submit {
		var encoded Bytes
		for _, entry := range c.entries {
			encoded += 88 + Bytes(len(entry.Codec)) + Bytes(len(entry.Payload))
		}
		return Count(len(c.entries)), encoded
	}
	if c.kind == ackCommand {
		return Count(len(c.tokens)), 56 * Bytes(len(c.tokens))
	}
	return 1, 24
}

// collectGroupLocked holds references to admitted storage. It neither copies
// public batches nor releases reservations while callers can abandon results.
func (q *Queue) collectGroupLocked(pool *admissionPool, first *command) commandGroup {
	limits := q.config.Runtime.Group
	group := commandGroup{commands: make([]groupedCommand, limits.Commands)}
	operations, encoded := first.work()
	group.commands[0] = groupedCommand{pool: pool, slot: first}
	group.count, group.operations, group.encoded = 1, operations, encoded
	var timer *time.Timer
	var expired <-chan time.Time
	if limits.MaxDelay > 0 {
		timer = time.NewTimer(limits.MaxDelay)
		defer timer.Stop()
		expired = timer.C
	}
	last := first.sequence
	for group.count < int(limits.Commands) {
		select {
		case <-expired:
			return group
		default:
		}
		if q.aborting {
			break
		}
		pool, next := q.nextAfterLocked(last)
		if next != nil {
			if !next.mutation() {
				break
			}
			operations, encoded := next.work()
			if operations > limits.Operations-group.operations || encoded > limits.Encoded-group.encoded {
				break
			}
			group.commands[group.count] = groupedCommand{pool: pool, slot: next}
			group.count++
			group.operations += operations
			group.encoded += encoded
			last = next.sequence
			continue
		}
		if limits.MaxDelay == 0 || q.closing {
			break
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-expired:
			q.mu.Lock()
			return group
		case <-changed:
			q.mu.Lock()
		}
		// A continuous ready stream must not extend deliberate collection time.
		select {
		case <-expired:
			return group
		default:
		}
	}
	return group
}

func (q *Queue) applyCommand(
	ctx context.Context,
	slot *command,
	metadata storeMetadata,
) (storeMetadata, error) {
	storage := q.opened.storage
	if slot.submit {
		next, fresh, err := q.validateStoredSubmit(ctx, slot.entries, metadata)
		if err != nil {
			return storeMetadata{}, err
		}
		for index, entry := range slot.entries {
			if !fresh[index] {
				continue
			}
			order := next.lastOrder - uint64(countFreshAfter(fresh, index))
			if err := storage.insertEntry(ctx, entry, order); err != nil {
				return storeMetadata{}, errors.Join(ErrStorage, err)
			}
		}
		return next, nil
	}
	if slot.kind == retireCommand {
		return q.applyRetirement(ctx, slot.retirement, metadata)
	}
	counts, err := q.validateAck(ctx, slot.tokens, metadata.counts)
	if err != nil {
		return storeMetadata{}, err
	}
	if err := q.applyAck(ctx, slot.tokens); err != nil {
		return storeMetadata{}, errors.Join(ErrStorage, err)
	}
	metadata.counts = counts
	return metadata, nil
}

func (g *commandGroup) fail(attempted int, result CommitResult, err error) {
	for index := range g.count {
		command := &g.commands[index]
		// An expected validation rejection remains authoritative even if the
		// transaction containing other commands later fails.
		if command.err == nil || errors.Is(command.err, ErrStorage) {
			command.result, command.err = result, err
			if index >= attempted {
				command.result = CommitResult{Decision: NotApplied}
			}
		}
	}
}

func (q *Queue) executeGroup(ctx context.Context, group *commandGroup) error {
	for index := range group.count {
		if observer := group.commands[index].slot.observer; observer != nil {
			observer.BeforeExecute()
		}
	}
	storage := q.opened.storage
	if _, err := storage.conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		err = errors.Join(ErrStorage, err)
		group.fail(0, CommitResult{Decision: NotApplied}, err)
		return err
	}
	metadata := q.opened.metadata
	for index := range group.count {
		command := &group.commands[index]
		next, err := q.applyCommand(ctx, command.slot, metadata)
		if errors.Is(err, ErrStorage) {
			result, failure := q.rollbackSubmit(ctx, err)
			group.fail(index+1, result, failure)
			return failure
		}
		if err != nil {
			command.result, command.err = CommitResult{Decision: NotApplied}, err
			continue
		}
		metadata = next
		command.result = CommitResult{Decision: Applied}
	}
	if err := storage.writeSubmitMetadata(ctx, metadata); err != nil {
		result, failure := q.rollbackSubmit(ctx, errors.Join(ErrStorage, err))
		group.fail(group.count, result, failure)
		return failure
	}
	if _, err := storage.conn.ExecContext(ctx, "COMMIT"); err != nil {
		err = errors.Join(ErrStorage, err)
		group.fail(group.count, CommitResult{Decision: Unknown}, err)
		return err
	}
	q.opened.metadata = metadata
	q.mu.Lock()
	for index := range group.count {
		command := &group.commands[index]
		if command.err != nil || command.slot.kind != ackCommand {
			continue
		}
		for _, token := range command.slot.tokens {
			id, _ := decodeID(token.bytes[24:48])
			if claim := q.currentClaimLocked(id, token); claim >= 0 {
				q.clearClaimLocked(claim)
			}
		}
	}
	q.mu.Unlock()
	return nil
}
