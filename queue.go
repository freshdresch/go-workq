package workq

import (
	"fmt"
	"runtime"
)

type config struct {
	diagnostics     DiagnosticsConfig
	singleProducer  bool
	singleConsumer  bool
	maxPushBatch    int
	maxReceiveBatch int
	consumerWait    WaitStrategy
	producerWait    WaitStrategy
}

// Queue is a fixed-capacity FIFO work queue on one of the ring queues. FIFO describes acquisition
// order, not concurrent API return order or handler start or completion order. Construct it with
// New. Its zero value is unusable, and it must not be copied after use.
type Queue[T any] struct {
	engine          engine[T]
	maxPushBatch    int
	maxReceiveBatch int
	consumerWakeCap uint32
	capacity        uint32
	consumerWait    WaitStrategy
	producerWait    WaitStrategy
	diagnostics     *diagnostics
	done            chan struct{}
	hooks           testHooks
	_               [128]byte
	consumers       waitSide
	_               [128]byte
	producers       waitSide
}

// testHooks lets tests act at points where no engine call runs. Tests set them, and production
// code never does, so each is nil outside tests.
type testHooks struct {
	// beforeConsumerHandoffYield detects regressions that restore a held-batch yield.
	// Successful acquisitions must never call it.
	beforeConsumerHandoffYield func()
	// afterCloseDone runs in Close after done is closed and before the producer side is marked
	// closed, with no mutex held.
	afterCloseDone func()
	// beforeChunkCheck runs in PushBatch before each chunk after the first, before the closed
	// load.
	beforeChunkCheck func()
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
	resolvedDiagnostics, err := resolveDiagnostics(cfg.diagnostics)
	if err != nil {
		return config{}, fmt.Errorf("new work queue: %w", err)
	}
	cfg.diagnostics = resolvedDiagnostics
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
	procs := runtime.GOMAXPROCS(0)
	capacity := engine.Capacity()
	wakeCap := max(1, min(procs, capacity))
	if cfg.singleConsumer {
		wakeCap = 1
	}
	if procs == 1 {
		consumerWait.Spins = 0
		producerWait.Spins = 0
	}
	return &Queue[T]{
		engine:          engine,
		maxPushBatch:    cfg.maxPushBatch,
		maxReceiveBatch: cfg.maxReceiveBatch,
		consumerWakeCap: uint32(wakeCap),
		capacity:        uint32(capacity),
		diagnostics:     newDiagnostics(cfg.diagnostics),
		consumerWait:    consumerWait,
		producerWait:    producerWait,
		done:            make(chan struct{}),
	}
}

// Capacity returns the number of items the queue can hold.
func (q *Queue[T]) Capacity() int {
	return q.engine.Capacity()
}

// Close closes the queue. Like closing a Go channel, Close must be called only after every
// producer call has returned. Consumers then receive every pushed item before they get
// ErrClosed. Unlike a channel, nothing panics. A second Close does nothing, and every producer
// call made after Close returns ErrClosed and pushes nothing.
//
// A producer call that races Close breaks that rule. If it pushed items and then saw the close,
// it returns ErrClosedDuringPush, and those items may not be delivered. A producer that is
// waiting for room when Close runs returns ErrClosed if it pushed nothing in that call, and
// ErrClosedDuringPush with its count if it pushed something.
//
// A nil error from a producer call means its items are delivered only when two conditions hold.
// No producer call overlaps Close, and consumers keep receiving until the queue is closed and
// drained. If any producer races Close, a nil error promises nothing, even to producers that
// kept the rule, because a racing producer stalled mid-copy can hold back items pushed after it.
//
// Close does not join handlers or establish successful processing. A consumer that sees the
// queue closed and drained can still have a peer holding acquired work. Join all owned consumers
// and reconcile their outcomes and accepted work. Joining alone does not prove success.
//
// Without a Close, a producer waiting for room after every consumer has returned waits until
// its ctx is done, so the owner of that ctx must cancel it when the consumers stop.
func (q *Queue[T]) Close() {
	// The consumer side is closed under its mutex, so a consumer parking at the same time either
	// sees closed and does not park, or is already listed and done wakes it. The two mutexes are
	// never held together.
	q.consumers.mu.Lock()
	if q.consumers.setClosedLocked() {
		close(q.done)
	}
	q.consumers.mu.Unlock()

	if q.hooks.afterCloseDone != nil {
		q.hooks.afterCloseDone()
	}

	q.producers.mu.Lock()
	q.producers.setClosedLocked()
	q.producers.mu.Unlock()
}

// peekItems reports whether a dequeue would find items, without moving any. A zero-length
// DequeueBurst returns a fresh available count. See A4 in "Engine assumptions" in wakeup.md.
func (q *Queue[T]) peekItems(recording *diagnosticsRecording) bool {
	_, available := q.engine.DequeueBurst(nil)
	recording.consumerCounters().recordCapacity(CleanupProbe, available, q.capacity)
	return available > 0
}

// peekRoom reports whether an enqueue would find room, without moving any items. A zero-length
// EnqueueBurst returns a fresh free count. See A4 in "Engine assumptions" in wakeup.md.
func (q *Queue[T]) peekRoom(recording *diagnosticsRecording) bool {
	_, free := q.engine.EnqueueBurst(nil)
	recording.producerCounters().recordCapacity(CleanupProbe, free, q.capacity)
	return free > 0
}
