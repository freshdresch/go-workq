package durable

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/freshdresch/go-data-structures/workq"
)

type admissionPool struct {
	slots    []command
	budget   CommandBudget
	retained Bytes
	waiters  uint64
}

type commandObserver interface {
	BeforeExecute()
}

type commandKind uint8

const (
	lookupCommand commandKind = iota
	ackCommand
	releaseCommand
)

type command struct {
	used      bool
	handed    bool
	finished  bool
	abandoned bool
	sequence  uint64
	charge    Bytes
	submit    bool
	kind      commandKind
	tokens    []Token
	token     Token
	entries   []Entry
	ids       []ID
	result    CommitResult
	statuses  []Status
	err       error
	// A private per-call gate lets tests stop execution after ownership transferred.
	observer commandObserver
}

// Queue owns a validated store and one coordinator. It is safe for concurrent use.
// Its zero value is invalid. Call Close or Abort and Wait to release ownership.
type Queue struct {
	opened           *validatedStore
	config           Config
	mu               sync.Mutex
	ordinary         admissionPool
	progress         admissionPool
	changed          chan struct{}
	stopping         chan struct{}
	done             chan struct{}
	lastSequence     uint64
	admissionStopped bool
	closing          bool
	aborting         bool
	terminal         error
	deliveryStopped  bool
	deliveryDirty    bool
	deliveryClosed   bool
	claims           []claim
	deliveryQueue    *workq.Queue[int]
	deliveryRetained Bytes
	lastAttempt      uint64
	scanWrapped      bool
	scanOrder        [8]byte
	scanID           [24]byte
	pending          Count
	receiveSlot      [1]int
	deliveryObserver deliveryReadObserver
}

func (q *Queue) signalLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func cancellation(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	return errors.Join(ctx.Err(), context.Cause(ctx))
}

func (q *Queue) gateLocked(submit bool) error {
	if q.closing {
		return errors.Join(ErrStopped, q.terminal)
	}
	if submit && q.admissionStopped {
		return ErrStopped
	}
	return nil
}

func (q *Queue) reserve(
	ctx context.Context,
	pool *admissionPool,
	charge Bytes,
	submit bool,
) (*command, error) {
	if ctx == nil {
		return nil, ErrInvalidInput
	}
	q.mu.Lock()
	waiting := false
	defer func() {
		if waiting {
			pool.waiters--
		}
		q.mu.Unlock()
	}()
	for {
		if err := cancellation(ctx); err != nil {
			return nil, err
		}
		if err := q.gateLocked(submit); err != nil {
			return nil, err
		}
		if charge > pool.budget.Retained {
			return nil, ErrInvalidInput
		}
		if pool.retained <= pool.budget.Retained-charge {
			for index := range pool.slots {
				slot := &pool.slots[index]
				if slot.used {
					continue
				}
				*slot = command{used: true, charge: charge, submit: submit}
				pool.retained += charge
				return slot, nil
			}
		}
		if !waiting {
			waiting = true
			pool.waiters++
			q.signalLocked()
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		q.mu.Lock()
	}
}

func (q *Queue) clearLocked(pool *admissionPool, slot *command) {
	pool.retained -= slot.charge
	*slot = command{}
	q.signalLocked()
}

func (q *Queue) handoff(ctx context.Context, slot *command) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := cancellation(ctx); err != nil {
		return err
	}
	if err := q.gateLocked(slot.submit); err != nil {
		return err
	}
	if q.lastSequence == math.MaxUint64 {
		return ErrExhausted
	}
	q.lastSequence++
	slot.sequence = q.lastSequence
	slot.handed = true
	q.signalLocked()
	return nil
}

func (q *Queue) await(
	ctx context.Context,
	pool *admissionPool,
	slot *command,
) (CommitResult, []Status, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if slot.finished {
			result, statuses, err := slot.result, slot.statuses, slot.err
			q.clearLocked(pool, slot)
			return result, statuses, err
		}
		if err := cancellation(ctx); err != nil {
			slot.abandoned = true
			return CommitResult{Decision: Unknown}, nil, err
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		q.mu.Lock()
	}
}
