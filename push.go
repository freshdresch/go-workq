package workq

import "context"

// Push adds item to the queue. If the queue is full, Push waits by the producer wait strategy
// until there is room, ctx is done, or the queue is closed. It returns nil once item is pushed,
// and ctx.Err() if ctx is done first.
//
// Success reports acceptance, not completion. Item is a shallow value copy, as described in the
// package's Ownership and completion section.
//
// Push checks ctx only when it would wait. While there is room, it pushes even when ctx is
// already done. PushBatch also checks ctx before each chunk after the first, so with ctx done
// and room in the queue, Push and a one-chunk PushBatch still push, while a longer PushBatch
// stops after its first chunk.
//
// Once Close has been called, Push returns ErrClosed and pushes nothing. If it is waiting when
// Close runs, it returns ErrClosed. If it pushed item but raced Close, it returns
// ErrClosedDuringPush, and item may not be delivered. When the queue is closed and ctx is also
// done, the closed error wins if closure is visible at the final observation after wait cleanup.
//
// A producer that can wait should get a ctx that its owner cancels when the consumers stop. A
// producer that waits after every consumer has returned, without a Close, waits until its ctx
// is done. Waiting has no fairness guarantee. A woken producer that makes no progress parks
// again at the oldest end until an attempt pushes items. A later wait starts at the newest
// end. Producers that have not started waiting can take the room first, so under sustained
// load a Push can wait without bound.
//
// A handler must not block in Push on its own queue, because if every consumer's handler does
// so, nothing drains it. TryPush describes the pattern that works.
//
// Producers may call Push concurrently. On a queue built with WithSingleProducer, the one
// producer must serialize all its producer calls.
func (q *Queue[T]) Push(ctx context.Context, item T) error {
	if q.consumers.load().closed() {
		return ErrClosed
	}
	if err := q.engine.Enqueue(item); err != nil {
		attempt := func() int {
			if err := q.engine.Enqueue(item); err != nil {
				return 0
			}
			return 1
		}
		_, err := q.pushWait(ctx, attempt, 0)
		return err
	}
	if q.wakeConsumersAfterPublication() {
		return ErrClosedDuringPush
	}
	return nil
}

// PushBatch adds every item in items, in order, and returns how many it pushed, which is less
// than len(items) only with an error. It hands items to the engine in chunks of at most the
// push batch limit, and whenever a chunk moves nothing, it waits for room as Push does. A
// single call's items stay in order, but with several producers another producer's items can
// land between chunks.
//
// PushBatch checks ctx before each chunk after the first. If ctx is done there, it returns the
// count pushed so far with ctx.Err(), so a large batch stops soon after cancellation even when
// the queue drains fast. Push checks ctx only when it would wait, so with ctx done and room in
// the queue, Push and a one-chunk PushBatch still push, while a longer PushBatch stops after its
// first chunk. While it waits, PushBatch returns the count with ctx.Err() once ctx is done.
//
// Once Close has been called, PushBatch returns 0 and ErrClosed, even for an empty items.
// Otherwise an empty items returns 0 and nil. If it is waiting when Close runs, it returns 0
// and ErrClosed if it pushed nothing, and its count with ErrClosedDuringPush if it pushed
// earlier chunks. If it pushed items but raced Close, it returns their count with
// ErrClosedDuringPush, and those items may not be delivered. When the queue is closed and ctx
// is also done, the closed error wins if closure is visible at the final observation after cleanup.
//
// A producer that can wait should get a ctx that its owner cancels when the consumers stop. A
// producer that waits after every consumer has returned, without a Close, waits until its ctx
// is done. Waiting has no fairness guarantee, as Push describes.
//
// For a returned count n, items[:n] is accepted and items[n:] is unaccepted, even on error.
// Acceptance is not completion and remains subject to the close-race delivery exception in
// [Queue.Close]. Never replay the accepted prefix solely because the call returned an error.
// After return, the caller can reuse the input slice's storage. Its values are shallow copies,
// so referenced payloads remain shared under the package's Ownership and completion contract.
//
// PushBatch hands items to the engine through an interface, so items escapes to the heap. A
// slice the caller reuses across calls stays allocation-free, but a fresh stack array per call
// moves to the heap and allocates on every call.
//
// A handler must not block in PushBatch on its own queue, as Push describes. Producers may call
// PushBatch concurrently. On a queue built with WithSingleProducer, the one producer must
// serialize all its producer calls.
func (q *Queue[T]) PushBatch(ctx context.Context, items []T) (pushed int, err error) {
	if q.consumers.load().closed() {
		return 0, ErrClosed
	}
	if len(items) == 0 {
		return 0, nil
	}
	for pushed < len(items) {
		if pushed > 0 {
			if err := q.checkBetweenChunks(ctx, pushed); err != nil {
				return pushed, err
			}
		}
		chunk := items[pushed:min(pushed+q.maxPushBatch, len(items))]
		chunkPushed, err := q.pushChunk(ctx, chunk, pushed)
		pushed += chunkPushed
		if err != nil {
			return pushed, err
		}
	}
	return pushed, nil
}

// checkBetweenChunks is PushBatch's check before each chunk after the first. It loads the
// consumer-side word before it checks ctx, and observes closure again after ctx is done.
func (q *Queue[T]) checkBetweenChunks(ctx context.Context, pushedBefore int) error {
	if q.hooks.beforeChunkCheck != nil {
		q.hooks.beforeChunkCheck()
	}
	// The wake check after the previous chunk normally catches a Close. This load catches one
	// that lands between that check and the next chunk.
	if q.consumers.load().closed() {
		return ErrClosedDuringPush
	}
	if isDone(ctx) {
		return q.producerContextError(ctx, pushedBefore)
	}
	return nil
}

// pushChunk pushes all of chunk, waiting for room whenever an attempt moves nothing, and
// returns how many items of chunk it pushed. PushedBefore is how many items the call pushed in
// earlier chunks. A retry of the rest of a partly pushed chunk is not a new chunk, so it checks
// neither closed nor ctx before it tries.
func (q *Queue[T]) pushChunk(
	ctx context.Context,
	chunk []T,
	pushedBefore int,
) (pushed int, err error) {
	for pushed < len(chunk) {
		enqueued, _ := q.engine.EnqueueBurst(chunk[pushed:])
		if enqueued > 0 {
			pushed += enqueued
			if q.wakeConsumersAfterPublication() {
				return pushed, ErrClosedDuringPush
			}
			continue
		}
		rest := chunk[pushed:]
		attempt := func() int {
			moved, _ := q.engine.EnqueueBurst(rest)
			return moved
		}
		moved, err := q.pushWait(ctx, attempt, pushedBefore+pushed)
		pushed += moved
		if err != nil {
			return pushed, err
		}
	}
	return pushed, nil
}

// TryPush adds item without waiting. It returns ErrFull when the queue has no room. Once Close
// has been called, it returns ErrClosed and pushes nothing. If it pushed item but raced Close,
// it returns ErrClosedDuringPush, and item may not be delivered.
//
// Success reports acceptance, not completion. Item is a shallow value copy, as described in the
// package's Ownership and completion section.
//
// Producers may call TryPush concurrently. On a queue built with WithSingleProducer, the one
// producer must serialize all its producer calls.
//
// A handler that pushes into its own queue must not drop an item that TryPush rejects with
// ErrFull, and retrying TryPush in a loop deadlocks like a blocking push. The handler keeps such
// items on a stack local to its call and processes that stack before it returns.
func (q *Queue[T]) TryPush(item T) error {
	if q.consumers.load().closed() {
		return ErrClosed
	}
	if err := q.engine.Enqueue(item); err != nil {
		return ErrFull
	}
	if q.wakeConsumersAfterPublication() {
		return ErrClosedDuringPush
	}
	return nil
}

// TryPushBatch adds as many of items as fit, in order, without waiting, and returns how many it
// pushed. It hands items to the engine in chunks of at most the push batch limit and stops after
// the first chunk that does not fit whole. If it pushed fewer than len(items), it returns the
// count with ErrFull, so a caller that checks only the error never drops items unknowingly.
// With several producers, another producer's items can land between chunks.
//
// Once Close has been called, TryPushBatch returns 0 and ErrClosed, even for an empty items.
// Otherwise an empty items returns 0 and nil. If it pushed items but raced Close, it returns
// their count with ErrClosedDuringPush, and those items may not be delivered.
//
// For a returned count n, items[:n] is accepted and items[n:] is unaccepted, even on error.
// Acceptance is not completion and remains subject to the close-race delivery exception in
// [Queue.Close]. Never replay the accepted prefix solely because the call returned an error.
// After return, the caller can reuse the input slice's storage. Its values are shallow copies,
// so referenced payloads remain shared under the package's Ownership and completion contract.
//
// TryPushBatch hands items to the engine through an interface, so items escapes to the heap. A
// slice the caller reuses across calls stays allocation-free, but a fresh stack array per call
// moves to the heap and allocates on every call.
//
// Producers may call TryPushBatch concurrently. On a queue built with WithSingleProducer, the
// one producer must serialize all its producer calls. A handler that pushes into its own queue
// must not drop the items TryPushBatch did not push, as TryPush describes.
func (q *Queue[T]) TryPushBatch(items []T) (pushed int, err error) {
	if q.consumers.load().closed() {
		return 0, ErrClosed
	}
	if len(items) == 0 {
		return 0, nil
	}
	for pushed < len(items) {
		chunk := items[pushed:min(pushed+q.maxPushBatch, len(items))]
		enqueued, _ := q.engine.EnqueueBurst(chunk)
		if enqueued > 0 {
			pushed += enqueued
			if q.wakeConsumersAfterPublication() {
				return pushed, ErrClosedDuringPush
			}
		}
		if enqueued < len(chunk) {
			break
		}
	}
	if pushed < len(items) {
		return pushed, ErrFull
	}
	return pushed, nil
}
