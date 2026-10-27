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
	dst = dst[:min(len(dst), q.maxReceiveBatch)]
	return q.receive(ctx, dst, nil, nil)
}

// Consume receives items in batches and calls handler with each batch, on the caller's
// goroutine, until the queue is closed and drained, ctx is done, or handler returns an error.
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
// batch slice after it returns, because the next batch reuses its memory.
//
// Consume allocates one buffer of the receive batch limit and reuses it for every batch. When a
// wait reaches its yield phase, Consume clears the part of the buffer the last batch used, so a
// quiet queue does not keep old items reachable.
//
// Consume does not start goroutines. To run several consumers, start a goroutine for each, for
// example with errgroup. On a queue built with WithSingleConsumer, the one consumer must
// serialize Consume with its other consumer calls.
func (q *Queue[T]) Consume(
	ctx context.Context,
	handler func(ctx context.Context, batch []T) error,
) error {
	buffer := make([]T, q.maxReceiveBatch)
	own := newWaiter()
	lastLength := 0
	for {
		if isDone(ctx) {
			return ctx.Err()
		}
		received, err := q.receive(ctx, buffer, own, buffer[:lastLength])
		if errors.Is(err, ErrClosed) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := handler(ctx, buffer[:received]); err != nil {
			return fmt.Errorf("workq: handler: %w", err)
		}
		lastLength = received
	}
}

// receive makes one attempt into dst and waits if it found nothing. Dst must be non-empty and
// within the receive batch limit. Own and clearOnYield are as receiveWait takes them.
func (q *Queue[T]) receive(
	ctx context.Context,
	dst []T,
	own *waiter,
	clearOnYield []T,
) (int, error) {
	if received := q.dequeueAttempt(dst); received > 0 {
		// Closed on the producer side tells a consumer nothing, so only the wake matters here.
		q.producers.wakeCheck()
		return received, nil
	}
	return q.receiveWait(ctx, dst, own, clearOnYield)
}

// TryReceive fills a prefix of dst with up to the receive batch limit of items, without
// waiting, and returns how many it received. It returns 0 and nil when the queue is empty, and
// 0 and ErrClosed once the queue is closed and drained. A zero-length dst returns 0 and nil,
// open or closed. Dst belongs to the caller, so it keeps the received items reachable until the
// caller overwrites them.
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
	dst = dst[:min(len(dst), q.maxReceiveBatch)]
	if received = q.dequeueAttempt(dst); received > 0 {
		// Closed on the producer side tells a consumer nothing, so only the wake matters here.
		q.producers.wakeCheck()
		return received, nil
	}
	if !q.consumers.load().closed() {
		return 0, nil
	}
	// The drain check. This attempt comes after the load that saw closed, so it sees every item
	// pushed before Close. An attempt made before that load could miss the last push.
	if received = q.dequeueAttempt(dst); received > 0 {
		q.producers.wakeCheck()
		return received, nil
	}
	return 0, ErrClosed
}

// dequeueAttempt makes one dequeue attempt into dst, which must be non-empty and within the
// receive batch limit, and returns how many items it moved.
func (q *Queue[T]) dequeueAttempt(dst []T) int {
	if len(dst) == 1 {
		item, err := q.engine.Dequeue()
		if err != nil {
			return 0
		}
		dst[0] = item
		return 1
	}
	dequeued, _ := q.engine.DequeueBurst(dst)
	return dequeued
}
