package workq

import (
	"fmt"
	"runtime"
)

type config struct {
	singleProducer  bool
	singleConsumer  bool
	maxPushBatch    int
	maxReceiveBatch int
	consumerWait    WaitStrategy
	producerWait    WaitStrategy
}

// Queue is a fixed-capacity FIFO work queue on one of the ring queues. Construct it with New.
// Its zero value is unusable, and it must not be copied after use.
type Queue[T any] struct {
	engine          engine[T]
	maxPushBatch    int
	maxReceiveBatch int
	consumerWait    WaitStrategy
	producerWait    WaitStrategy
	done            chan struct{}
	_               [128]byte
	consumers       waitSide
	_               [128]byte
	producers       waitSide
}

// resolveConfig checks the wait strategies before the engine is built, so a bad strategy never
// allocates a large engine. The engine checks the capacity and the batch limits.
func resolveConfig(capacity int, options []Option) (config, error) {
	defaultBatch := min(defaultMaxBatch, capacity)
	cfg := config{
		maxPushBatch:    defaultBatch,
		maxReceiveBatch: defaultBatch,
		consumerWait:    defaultConsumerWait(),
		producerWait:    defaultProducerWait(),
	}
	for _, option := range options {
		option(&cfg)
	}
	if err := checkWaitStrategy("consumer", cfg.consumerWait); err != nil {
		return config{}, err
	}
	if err := checkWaitStrategy("producer", cfg.producerWait); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func checkWaitStrategy(side string, strategy WaitStrategy) error {
	if strategy.Spins < 0 {
		return fmt.Errorf(
			"new work queue: %s wait strategy has %d spins: %w",
			side,
			strategy.Spins,
			ErrInvalidWaitStrategy,
		)
	}
	if strategy.Yields < 0 {
		return fmt.Errorf(
			"new work queue: %s wait strategy has %d yields: %w",
			side,
			strategy.Yields,
			ErrInvalidWaitStrategy,
		)
	}
	return nil
}

// newQueue reads GOMAXPROCS once, because runtime.GOMAXPROCS takes the global scheduler lock
// even when it only reads. With one P, spinning cannot help, so Spins is treated as 0, as
// sync.Mutex does.
func newQueue[T any](engine engine[T], cfg config) *Queue[T] {
	consumerWait := cfg.consumerWait
	producerWait := cfg.producerWait
	if runtime.GOMAXPROCS(0) == 1 {
		consumerWait.Spins = 0
		producerWait.Spins = 0
	}
	return &Queue[T]{
		engine:          engine,
		maxPushBatch:    cfg.maxPushBatch,
		maxReceiveBatch: cfg.maxReceiveBatch,
		consumerWait:    consumerWait,
		producerWait:    producerWait,
		done:            make(chan struct{}),
	}
}

// Capacity returns the number of items the queue can hold.
func (q *Queue[T]) Capacity() int {
	return q.engine.Capacity()
}
