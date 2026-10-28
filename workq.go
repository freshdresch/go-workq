package workq

import (
	"errors"
	"fmt"

	"github.com/freshdresch/go-data-structures/ring"
)

var (
	// ErrClosed is returned once the queue is closed. A producer call that returns ErrClosed
	// unwrapped pushed nothing. A consumer call returns it once the queue is closed and drained.
	ErrClosed = errors.New("workq: queue is closed")
	// ErrClosedDuringPush is returned when a push call raced Close after it had started pushing.
	// It wraps ErrClosed, so errors.Is(err, ErrClosed) detects shutdown either way. Items the
	// call pushed may not be delivered, and the pushed count says how many went in.
	ErrClosedDuringPush = fmt.Errorf("workq: push raced Close, items may not be delivered: %w",
		ErrClosed)
	// ErrInvalidWaitStrategy is returned by New, wrapped, when a WaitStrategy has a negative
	// Spins or Yields.
	ErrInvalidWaitStrategy = errors.New("workq: wait strategy has a negative count")
	// ErrFull is returned by the Try producer calls when the queue has no room for every item.
	// It is ring.ErrFull, so errors.Is matches it across the ring packages.
	ErrFull = ring.ErrFull
)

// defaultMaxBatch is the default for both batch limits, capped at the capacity.
const defaultMaxBatch = 32

func defaultConsumerWait() WaitStrategy {
	return WaitStrategy{Spins: 64, Yields: 16}
}

func defaultProducerWait() WaitStrategy {
	return WaitStrategy{Spins: 16, Yields: 4}
}

// WaitStrategy sets how a caller waits when it cannot make progress. A waiting caller first
// polls Spins times without registering anywhere. It then registers as a poller and polls Yields
// more times, with runtime.Gosched between polls. Last, it parks until the other side wakes it.
// A caller woken from a park always polls at least once more, even when Yields is 0.
//
// The zero value parks after the first empty poll. It never busy-waits, so a caller who passes it
// by mistake gets slow wakes, not a spinning CPU. New rejects a negative Spins or Yields with an
// error that wraps ErrInvalidWaitStrategy.
//
// New reads runtime.GOMAXPROCS once, and with GOMAXPROCS at 1 the queue treats Spins as 0. That
// value and the consumer wake cap can go stale if the program changes GOMAXPROCS later, or if
// Go 1.25 or later adjusts it
// when a container's CPU limit changes. Staleness affects spinning and consumer fanout, never
// correctness.
type WaitStrategy struct {
	// Spins is the number of empty polls before the first yield.
	Spins int
	// Yields is the number of polls with runtime.Gosched between them, after the spins.
	Yields int
	// NeverPark makes the caller keep yielding after the yields instead of parking. It stays
	// registered as a poller for the whole wait, so the other side never takes a mutex to wake
	// it, at the cost of the CPU time it spends yielding.
	NeverPark bool
}

// Option configures a queue at construction. Options apply in argument order, and New validates
// only the final values.
type Option func(*config)

// WithSingleProducer declares that the queue has one producer. A single producer must serialize
// all producer calls against each other. The library does not check the promise, and breaking
// it corrupts items. A handler that pushes into its own queue makes the consumers producers, so
// such a queue must not use this option.
func WithSingleProducer() Option {
	return func(cfg *config) {
		cfg.singleProducer = true
	}
}

// WithSingleConsumer declares that the queue has one consumer. A single consumer must serialize
// Receive, TryReceive, and Consume against each other. The library does not check the promise,
// and breaking it corrupts items. A handler that pushes into its own queue makes the consumers
// producers, which does not affect this option but does rule out WithSingleProducer.
func WithSingleConsumer() Option {
	return func(cfg *config) {
		cfg.singleConsumer = true
	}
}

// WithMaxPushBatch limits each chunk a push call hands to the engine to at most n items. A
// frontier batch becomes visible only when its whole copy finishes, so the limit bounds how long
// one producer can hold back items reserved after it. The default is the smaller of 32 and the
// capacity. When the capacity is valid, New returns an error wrapping ring.ErrInvalidMaxBatch
// unless n is in [1, capacity].
func WithMaxPushBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxPushBatch = n
	}
}

// WithMaxReceiveBatch limits how many items one Receive or one handler call in Consume gets to
// at most n. The default is the smaller of 32 and the capacity. When the capacity is valid, New
// returns an error wrapping ring.ErrInvalidMaxBatch unless n is in [1, capacity].
func WithMaxReceiveBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxReceiveBatch = n
	}
}

// WithConsumerWait sets how consumers wait for items when the queue is empty. The default is
// WaitStrategy{Spins: 64, Yields: 16}.
func WithConsumerWait(strategy WaitStrategy) Option {
	return func(cfg *config) {
		cfg.consumerWait = strategy
	}
}

// WithProducerWait sets how producers wait for room when the queue is full. The default is
// WaitStrategy{Spins: 16, Yields: 4}.
func WithProducerWait(strategy WaitStrategy) Option {
	return func(cfg *config) {
		cfg.producerWait = strategy
	}
}

// New returns an empty queue with room for capacity items. Capacity must be a power of two in
// [1, 2^30], otherwise New returns an error wrapping ring.ErrInvalidCount. New checks the wait
// strategies first, then the capacity, then the batch limits, so a bad wait strategy is reported
// even when the capacity is also invalid.
//
// The declared sides pick the engine. Both single sides give dpdk.SPSCQueue, a single consumer
// alone gives frontier.MPSCQueue, a single producer alone gives frontier.SPMCQueue, and neither
// gives frontier.MPMCQueue, which is safe for any caller.
func New[T any](capacity int, options ...Option) (*Queue[T], error) {
	cfg, err := resolveConfig(capacity, options)
	if err != nil {
		return nil, err
	}
	engine, err := newEngine[T](capacity, cfg)
	if err != nil {
		return nil, fmt.Errorf("new work queue: %w", err)
	}
	return newQueue(engine, cfg), nil
}
