package workq

// consumerTarget counts receive batches without overflowing available+batch-1.
// See "Consumer wake groups" in wakeup.md for the target and its limits.
func (q *Queue[T]) consumerTarget(recording *diagnosticsRecording, source CapacitySource) uint32 {
	_, available := q.engine.DequeueBurst(nil)
	recording.consumerCounters().recordCapacity(source, available, q.capacity)
	if available == 0 {
		return 0
	}
	return uint32(min(int(q.consumerWakeCap), 1+(available-1)/q.maxReceiveBatch))
}

// wakeConsumersAfterPublication preserves the first post-publication closed observation.
// The floor keeps the publication obligation even when a competing dequeue empties the queue.
func (q *Queue[T]) wakeConsumersAfterPublication(recording *diagnosticsRecording) (closed bool) {
	word := q.consumers.load()
	if word.parked() == 0 {
		return word.closed()
	}
	if q.consumerWakeCap == 1 {
		if word.needsWake() {
			q.consumers.wakeOne(recording.consumerCounters())
		}
		return word.closed()
	}
	if word.pollers() >= q.consumerWakeCap {
		return word.closed()
	}
	target := max(uint32(1), q.consumerTarget(recording, PublicationProbe))
	if word.pollers() < target {
		q.consumers.wakeGroup(recording.consumerGroupCounters(), target)
	}
	return word.closed()
}

// replenishConsumers samples visibility after accounting has released this caller.
// Engine discovery stays outside the side mutex because frontier discovery may yield.
func (q *Queue[T]) replenishConsumers(recording *diagnosticsRecording, word sideWord) bool {
	if word.parked() == 0 || word.pollers() >= q.consumerWakeCap {
		return false
	}
	if q.consumerWakeCap == 1 {
		if q.peekItems(recording) {
			return q.consumers.wakeOne(recording.consumerCounters())
		}
		return false
	}
	target := q.consumerTarget(recording, CleanupProbe)
	if word.pollers() < target {
		return q.consumers.wakeGroup(recording.consumerGroupCounters(), target)
	}
	return false
}

func (q *Queue[T]) leaveConsumerPolling(recording *diagnosticsRecording) {
	q.consumers.leavePollingWith(func(word sideWord) bool {
		return q.replenishConsumers(recording, word)
	})
}

func (q *Queue[T]) leaveConsumerList(
	recording *diagnosticsRecording,
	caller *waiter,
	movedItems bool,
) {
	q.consumers.leaveListWith(
		recording.consumerCounters(),
		caller,
		movedItems,
		func(word sideWord) bool {
			return q.replenishConsumers(recording, word)
		},
	)
}
