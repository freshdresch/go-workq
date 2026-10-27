package workq

import (
	"github.com/freshdresch/go-data-structures/ring/dpdk"
	"github.com/freshdresch/go-data-structures/ring/frontier"
)

// engine is the method set all four ring queues share.
type engine[T any] interface {
	Enqueue(item T) error
	EnqueueBurst(items []T) (enqueued, free int)
	Dequeue() (T, error)
	DequeueBurst(dst []T) (dequeued, available int)
	Capacity() int
}

// newEngine builds the engine the declared sides select. A declared single side is a promise the
// engine relies on, so the default is MPMC, the only engine safe for any caller. The engine
// checks the capacity, then the batch limits.
func newEngine[T any](capacity int, cfg config) (engine[T], error) {
	switch {
	case cfg.singleProducer && cfg.singleConsumer:
		return newSPSCEngine[T](capacity, cfg)
	case cfg.singleConsumer:
		return newFrontierEngine[T](frontier.NewMPSCQueue[T], capacity, cfg)
	case cfg.singleProducer:
		return newFrontierEngine[T](frontier.NewSPMCQueue[T], capacity, cfg)
	default:
		return newFrontierEngine[T](frontier.NewMPMCQueue[T], capacity, cfg)
	}
}

func newSPSCEngine[T any](capacity int, cfg config) (engine[T], error) {
	queue, err := dpdk.NewSPSCQueue[T](
		capacity,
		dpdk.WithMaxEnqueueBatch(cfg.maxPushBatch),
		dpdk.WithMaxDequeueBatch(cfg.maxReceiveBatch),
	)
	// A nil *SPSCQueue in an engine interface would not compare equal to nil.
	if err != nil {
		return nil, err
	}
	return queue, nil
}

func newFrontierEngine[T any, Q engine[T]](
	construct func(count int, options ...frontier.Option) (Q, error),
	capacity int,
	cfg config,
) (engine[T], error) {
	queue, err := construct(
		capacity,
		frontier.WithMaxEnqueueBatch(cfg.maxPushBatch),
		frontier.WithMaxDequeueBatch(cfg.maxReceiveBatch),
	)
	// A nil queue pointer in an engine interface would not compare equal to nil.
	if err != nil {
		return nil, err
	}
	return queue, nil
}
