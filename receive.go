package workq

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
