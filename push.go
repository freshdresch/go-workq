package workq

// TryPush adds item without waiting. It returns ErrFull when the queue has no room. Once Close
// has been called, it returns ErrClosed and pushes nothing. If it pushed item but raced Close,
// it returns ErrClosedDuringPush, and item may not be delivered.
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
	if q.consumers.wakeCheck() {
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
			if q.consumers.wakeCheck() {
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
