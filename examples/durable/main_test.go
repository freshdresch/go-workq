package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable"
	"github.com/freshdresch/go-data-structures/workq/runner"
)

func openQueue(t *testing.T) (*durable.Queue, string) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "queue")
	config, err := exampleConfig()
	require.NoError(t, err)
	queue, err := durable.Open(t.Context(), directory, config)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
	return queue, directory
}

func TestRunGracefulAndIdenticalSourceRetry(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "queue")
	getenv := func(string) string { return directory }
	var output, diagnostics bytes.Buffer
	require.NoError(t, run(t.Context(), []string{"durable"}, getenv, nil, &output, &diagnostics))
	assert.Contains(t, output.String(), "1: one\n")
	assert.Contains(t, output.String(), "3: three\n")
	output.Reset()
	require.NoError(t, run(t.Context(), []string{"durable"}, getenv, nil, &output, &diagnostics))
	assert.Equal(t, "all source records acknowledged\n", output.String())
	inspection, err := durable.Inspect(t.Context(), directory, []durable.ID{sourceEntries()[0].ID})
	require.NoError(t, err)
	assert.Equal(t, durable.Acknowledged, inspection.Statuses[0].Kind)
	assert.Zero(t, inspection.Store.Counts.PendingItems)
}

func TestSupervisorAbortJoinsWorkerBeforeClose(t *testing.T) {
	t.Parallel()
	queue, directory := openQueue(t)
	_, err := queue.SubmitBatch(t.Context(), sourceEntries())
	require.NoError(t, err)
	queue.StopAdmission()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	exited := make(chan struct{})
	handler := func(ctx context.Context, batch []durable.Delivery) []runner.Result {
		close(started)
		<-ctx.Done()
		defer close(exited)
		results := make([]runner.Result, len(batch))
		for index := range results {
			results[index] = runner.PeerCanceled(ctx)
		}
		return results
	}
	finished := make(chan error, 1)
	go func() { finished <- supervise(ctx, queue, handler) }()
	<-started
	cancel()
	assert.ErrorIs(t, <-finished, context.Canceled)
	select {
	case <-exited:
	default:
		t.Fatal("supervisor returned before handler exited")
	}
	require.NoError(t, queue.Close(t.Context()))
	inspection, err := durable.Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	assert.Equal(t, durable.Count(3), inspection.Store.Counts.PendingItems)
}

func TestAckFailureSignalsSupervisor(t *testing.T) {
	t.Parallel()
	queue, _ := openQueue(t)
	_, err := queue.SubmitBatch(t.Context(), sourceEntries()[:1])
	require.NoError(t, err)
	queue.StopAdmission()
	handler := func(_ context.Context, batch []durable.Delivery) []runner.Result {
		// Deliberately invalidate the attempt to exercise a real stale-token Ack failure.
		for _, delivery := range batch {
			if err := queue.Release(delivery.Token); err != nil {
				return []runner.Result{{State: runner.ResultFailed, Err: err}}
			}
		}
		return nil
	}
	assert.ErrorIs(t, supervise(t.Context(), queue, handler), durable.ErrStaleToken)
}

func TestTransferReleasesOnlyUnacceptedSuffix(t *testing.T) {
	t.Parallel()
	queue, _ := openQueue(t)
	entries := sourceEntries()
	entries = append(entries, durable.Entry{ID: durable.ID{Sequence: 4}, Payload: []byte("four")})
	_, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	var deliveries []durable.Delivery
	for len(deliveries) < len(entries) {
		batch, err := queue.Receive(t.Context(), len(entries)-len(deliveries))
		require.NoError(t, err)
		deliveries = append(deliveries, batch...)
	}
	queue.StopDelivery()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	handlerExited := make(chan struct{})
	workers, err := runner.New(runner.Config[durable.Delivery]{
		Capacity: 1, Workers: 1,
		Queue: runner.QueueConfig{MaxPushBatch: 1, MaxReceiveBatch: 1},
		Handler: func(ctx context.Context, _ []durable.Delivery) []runner.Result {
			cancel()
			<-ctx.Done()
			close(handlerExited)
			return []runner.Result{runner.PeerCanceled(ctx)}
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		workers.Abort(context.Canceled)
		// This fallback joins after an earlier assertion fails. The normal path checks Wait.
		_ = workers.Wait()
	})
	accepted, err := transfer(ctx, queue, workers, deliveries)
	require.ErrorIs(t, err, context.Canceled)
	require.Greater(t, accepted, 0)
	require.Less(t, accepted, len(deliveries))
	workers.Abort(context.Canceled)
	require.ErrorIs(t, workers.Wait(), context.Canceled)
	<-handlerExited
	// A current prefix token can still Ack. Released suffix tokens must be stale.
	for _, delivery := range deliveries[:accepted] {
		result, err := queue.AckBatch(t.Context(), []durable.Token{delivery.Token})
		require.NoError(t, err)
		assert.Equal(t, durable.Applied, result.Decision)
	}
	for _, delivery := range deliveries[accepted:] {
		_, err := queue.AckBatch(t.Context(), []durable.Token{delivery.Token})
		assert.ErrorIs(t, err, durable.ErrStaleToken)
	}
}

func TestHandlerFailureDoesNotAcknowledge(t *testing.T) {
	t.Parallel()
	queue, _ := openQueue(t)
	entries := sourceEntries()[:1]
	_, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	queue.StopAdmission()
	failure := errors.New("destination refused")
	handler := func(context.Context, []durable.Delivery) []runner.Result {
		return []runner.Result{{State: runner.ResultFailed, Err: failure}}
	}
	assert.ErrorIs(t, supervise(t.Context(), queue, handler), failure)
	statuses, err := queue.Lookup(t.Context(), []durable.ID{entries[0].ID})
	require.NoError(t, err)
	assert.Equal(t, durable.Pending, statuses[0].Kind)
}

func TestRunRequiresDirectory(t *testing.T) {
	t.Parallel()
	err := run(
		t.Context(),
		[]string{"durable"},
		func(string) string { return "" },
		nil,
		io.Discard,
		io.Discard,
	)
	assert.Error(t, err)
}
