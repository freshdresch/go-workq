package workq

import "context"

// DiagnosticsEndpoint binds queue operations to one diagnostic shard. Copies keep the same
// assignment and may be used concurrently, subject to the queue-wide single-producer and
// single-consumer restrictions. Shards may be shared with other endpoints and ordinary calls.
// Retaining an endpoint retains its queue and queue-owned storage. There is nothing to release
// or flush. Construct endpoints with Queue.NewDiagnosticsEndpoint. The zero value is unusable.
type DiagnosticsEndpoint[T any] struct {
	queue *Queue[T]
	shard int
}

// NewDiagnosticsEndpoint returns an immutable binding to q. It is valid for any queue made
// with New, including disabled and closed queues, and records no metrics. Enabled assignments
// cycle through all shards, beginning at zero. Copies consume no assignment. Retain the result
// across operations to reuse its assignment. Construction does not serialize queue operations.
func (q *Queue[T]) NewDiagnosticsEndpoint() DiagnosticsEndpoint[T] {
	endpoint := DiagnosticsEndpoint[T]{queue: q}
	if q.diagnostics == nil {
		return endpoint
	}
	ticket := q.diagnostics.creationTicket.Add(1) - 1
	endpoint.shard = diagnosticsShardIndex(ticket, len(q.diagnostics.counters))
	return endpoint
}

func diagnosticsShardIndex(ticket uint64, shards int) int {
	return int(ticket & uint64(shards-1))
}

// Push follows [Queue.Push] and records diagnostics in the endpoint's fixed shard.
func (e DiagnosticsEndpoint[T]) Push(ctx context.Context, item T) error {
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if e.queue.diagnostics != nil {
		state = e.queue.diagnostics.recordingAt(e.shard)
		recording = &state
	}
	return e.queue.push(ctx, recording, item)
}

// PushBatch follows [Queue.PushBatch] and records diagnostics in the endpoint's fixed shard.
func (e DiagnosticsEndpoint[T]) PushBatch(ctx context.Context, items []T) (int, error) {
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if e.queue.diagnostics != nil {
		state = e.queue.diagnostics.recordingAt(e.shard)
		recording = &state
	}
	return e.queue.pushBatch(ctx, recording, items)
}

// TryPush follows [Queue.TryPush] and records diagnostics in the endpoint's fixed shard.
func (e DiagnosticsEndpoint[T]) TryPush(item T) error {
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if e.queue.diagnostics != nil {
		state = e.queue.diagnostics.recordingAt(e.shard)
		recording = &state
	}
	return e.queue.tryPush(recording, item)
}

// TryPushBatch follows [Queue.TryPushBatch] and records diagnostics in the endpoint's fixed shard.
func (e DiagnosticsEndpoint[T]) TryPushBatch(items []T) (int, error) {
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if e.queue.diagnostics != nil {
		state = e.queue.diagnostics.recordingAt(e.shard)
		recording = &state
	}
	return e.queue.tryPushBatch(recording, items)
}

// Receive follows [Queue.Receive] and records diagnostics in the endpoint's fixed shard.
func (e DiagnosticsEndpoint[T]) Receive(ctx context.Context, dst []T) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if e.queue.diagnostics != nil {
		state = e.queue.diagnostics.recordingAt(e.shard)
		recording = &state
	}
	return e.queue.receiveBatch(ctx, recording, dst)
}

// TryReceive follows [Queue.TryReceive] and records diagnostics in the endpoint's fixed shard.
func (e DiagnosticsEndpoint[T]) TryReceive(dst []T) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if e.queue.diagnostics != nil {
		state = e.queue.diagnostics.recordingAt(e.shard)
		recording = &state
	}
	return e.queue.tryReceive(recording, dst)
}

// Consume follows [Queue.Consume] and records diagnostics in the endpoint's fixed shard.
func (e DiagnosticsEndpoint[T]) Consume(
	ctx context.Context,
	handler func(ctx context.Context, batch []T) error,
) error {
	var state diagnosticsRecording
	var recording *diagnosticsRecording
	if e.queue.diagnostics != nil {
		state = e.queue.diagnostics.recordingAt(e.shard)
		recording = &state
	}
	return e.queue.consume(ctx, recording, handler)
}
