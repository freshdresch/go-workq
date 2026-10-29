package workq

import (
	"context"
	"errors"
	"fmt"
)

// Receive fills a prefix of dst with at least one item, and at most the receive batch limit,
// and returns how many it received. If the queue is empty, Receive waits until an item arrives,
// ctx is done, or the queue is closed and drained. It returns 0 and ErrClosed once the queue is
// closed and drained, and 0 and ctx.Err() if ctx is done first. A zero-length dst returns 0 and
// nil at once, as io.Reader does.
//
// Received values were removed from the queue before processing. Processing failure does not
// return them automatically. Values are shallow copies, as described in the package's Ownership
// and completion section.
//
// Receive checks ctx only when it would wait. If items are available, it returns them even when
// ctx is already done, so a canceled caller can still empty its share of the queue. If the
// queue is closed and drained and ctx is also done, Receive returns ErrClosed.
//
// Dst belongs to the caller, so it keeps the received items reachable until the caller
// overwrites them. Receive hands dst to the engine through an interface, so dst escapes to the
// heap. A slice the caller reuses across calls stays allocation-free, but a fresh stack array
// per call moves to the heap and allocates on every call.
//
// Consumers may call Receive concurrently. On a queue built with WithSingleConsumer, the one
// consumer must serialize Receive with its other consumer calls.
func (q *Queue[T]) Receive(ctx context.Context, dst []T) (received int, err error) {
	if len(dst) == 0 {
		return 0, nil
	}
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if q.diagnostics != nil {
		state = q.diagnostics.chooseRecording()
		recording = &state
	}
	return q.receiveBatch(ctx, recording, dst)
}

func (q *Queue[T]) receiveBatch(
	ctx context.Context,
	recording *diagnosticsRecording,
	dst []T,
) (int, error) {
	dst = dst[:min(len(dst), q.maxReceiveBatch)]
	return q.receive(ctx, recording, dst, nil, nil)
}

// Consume receives items in batches and calls handler with each batch, on the caller's
// goroutine, until the queue is closed and drained, ctx is done, or handler returns an error.
// Consume removes each batch from the queue before calling handler.
// It returns nil once the queue is closed and drained, and ctx.Err() once ctx is done. If
// handler returns an error, Consume returns it wrapped, so errors.Is finds it, and the items in
// that batch are not redelivered. Consume does not recover a panic in handler.
//
// Consume checks ctx before each receive, so it stops on cancellation even when the queue never
// runs empty. If ctx is already done when Consume is about to receive, it returns ctx.Err(),
// even if the queue is also closed and drained. If both happen while it waits, the closed queue
// wins and Consume returns nil.
//
// Handler gets Consume's ctx, so the code that owns ctx decides when a pipeline stops. Each
// batch holds at least one item and at most the receive batch limit. Handler must not keep the
// batch slice after it returns, because the next batch reuses its memory. Copying the batch
// preserves its values but does not copy referenced payload objects. See the package's Ownership
// and completion section for payload ownership and synchronization.
//
// Concurrent consumers have no guaranteed handler start or completion order. One nil Consume
// result does not certify global completion. A peer may still hold acquired work. Join all owned
// consumers and reconcile their outcomes and accepted work. Joining alone does not prove success.
// Work started asynchronously by handlers needs its own owner and completion boundary.
//
// Consume allocates one buffer of the receive batch limit and reuses it for every batch. When a
// wait reaches its yield phase, Consume clears the whole buffer, so a quiet queue does not keep
// items from any earlier batch reachable.
//
// Consume does not start goroutines. To run several consumers, start a goroutine for each, for
// example with errgroup. On a queue built with WithSingleConsumer, the one consumer must
// serialize Consume with its other consumer calls.
func (q *Queue[T]) Consume(
	ctx context.Context,
	handler func(ctx context.Context, batch []T) error,
) error {
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if q.diagnostics != nil {
		state = q.diagnostics.chooseRecording()
		recording = &state
	}
	return q.consume(ctx, recording, handler)
}

func (q *Queue[T]) consume(
	ctx context.Context,
	recording *diagnosticsRecording,
	handler func(ctx context.Context, batch []T) error,
) error {
	buffer := make([]T, q.maxReceiveBatch)
	own := newWaiter()
	for {
		if isDone(ctx) {
			return ctx.Err()
		}
		received, err := q.receive(ctx, recording, buffer, own, buffer)
		if errors.Is(err, ErrClosed) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := handler(ctx, buffer[:received]); err != nil {
			return fmt.Errorf("workq: handler: %w", err)
		}
	}
}

// receive makes one attempt into dst and waits if it found nothing. Dst must be non-empty and
// within the receive batch limit. Own and clearOnYield are as receiveWait takes them.
func (q *Queue[T]) receive(
	ctx context.Context,
	recording *diagnosticsRecording,
	dst []T,
	own *waiter,
	clearOnYield []T,
) (int, error) {
	if received := q.dequeueAttempt(recording, PhaseInitial, dst); received > 0 {
		// Closed on the producer side tells a consumer nothing, so only the wake matters here.
		q.producers.wakeCheck(recording.producerCounters())
		q.flushConsumerProbe(recording)
		return received, nil
	}
	q.flushConsumerProbe(recording)
	return q.receiveWait(ctx, recording, dst, own, clearOnYield)
}

// TryReceive fills a prefix of dst with up to the receive batch limit of items, without
// waiting, and returns how many it received. It returns 0 and nil when the queue is empty, and
// 0 and ErrClosed once the queue is closed and drained. A zero-length dst returns 0 and nil,
// open or closed. Dst belongs to the caller, so it keeps the received items reachable until the
// caller overwrites them.
//
// Received values were removed from the queue before processing. Processing failure does not
// return them automatically. Values are shallow copies, as described in the package's Ownership
// and completion section.
//
// TryReceive hands dst to the engine through an interface, so dst escapes to the heap. A slice
// the caller reuses across calls stays allocation-free, but a fresh stack array per call moves
// to the heap and allocates on every call.
//
// Consumers may call TryReceive concurrently. On a queue built with WithSingleConsumer, the one
// consumer must serialize TryReceive with its other consumer calls.
func (q *Queue[T]) TryReceive(dst []T) (received int, err error) {
	if len(dst) == 0 {
		return 0, nil
	}
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if q.diagnostics != nil {
		state = q.diagnostics.chooseRecording()
		recording = &state
	}
	return q.tryReceive(recording, dst)
}

func (q *Queue[T]) tryReceive(
	recording *diagnosticsRecording,
	dst []T,
) (received int, err error) {
	dst = dst[:min(len(dst), q.maxReceiveBatch)]
	if received = q.dequeueAttempt(recording, PhaseInitial, dst); received > 0 {
		// Closed on the producer side tells a consumer nothing, so only the wake matters here.
		q.producers.wakeCheck(recording.producerCounters())
		q.flushConsumerProbe(recording)
		return received, nil
	}
	q.flushConsumerProbe(recording)
	if !q.consumers.load().closed() {
		return 0, nil
	}
	// The drain check. This attempt comes after the load that saw closed, so it sees every item
	// pushed before Close. An attempt made before that load could miss the last push.
	return q.drainCheck(recording, dst)
}

// dequeueAttempt makes one dequeue attempt into dst, which must be non-empty and within the
// receive batch limit, and returns how many items it moved.
func (q *Queue[T]) dequeueAttempt(
	recording *diagnosticsRecording,
	phase AttemptPhase,
	dst []T,
) int {
	if len(dst) == 1 {
		item, err := q.engine.Dequeue()
		if err != nil {
			recording.recordConsumerAttempt(phase, 1, 0, -1, q.capacity)
			return 0
		}
		recording.recordConsumerAttempt(phase, 1, 1, -1, q.capacity)
		dst[0] = item
		return 1
	}
	dequeued, remaining := q.engine.DequeueBurst(dst)
	recording.recordConsumerAttempt(phase, len(dst), dequeued, remaining, q.capacity)
	return dequeued
}
