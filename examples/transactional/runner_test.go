//go:build linux && amd64 && cgo

package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/durable"
	"github.com/freshdresch/go-workq/examples/internal/inventory"
	"github.com/freshdresch/go-workq/runner"
)

var errFixtureUnknown = errors.New("destination observation lost, resolve original key")

// This adapter is a test fixture, not another dispatcher. The production supervision reference
// is ../durable/main.go. Losing one successful Apply observation models destination uncertainty.
func effectRunnerHandler(
	store *inventory.Store,
	loseObservation durable.ID,
) func(context.Context, []durable.Delivery) []runner.Result {
	return func(ctx context.Context, deliveries []durable.Delivery) []runner.Result {
		results := make([]runner.Result, len(deliveries))
		for index := range results {
			results[index] = runner.Result{State: runner.ResultFailed, Err: errFixtureUnknown}
		}
		for index, delivery := range deliveries {
			_, err := store.Apply(ctx, effectRequest(delivery))
			if err != nil {
				results[index] = runner.Result{State: runner.ResultFailed, Err: err}
				continue
			}
			if delivery.ID == loseObservation {
				continue
			}
			results[index] = runner.Result{State: runner.ResultSucceeded}
		}
		return results
	}
}

func transferEffectDeliveries(
	ctx context.Context,
	queue *durable.Queue,
	workers *runner.Runner[durable.Delivery],
	deliveries []durable.Delivery,
) (int, error) {
	accepted, err := workers.SubmitBatch(ctx, deliveries)
	// Even on error the accepted prefix belongs to workers until their terminal callbacks finish.
	for _, delivery := range deliveries[accepted:] {
		err = errors.Join(err, queue.Release(delivery.Token))
	}
	return accepted, err
}

func TestRunnerSignalsTerminalFailureAndJoinsBeforeClose(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openRecoveryQueue(t, directory)
	store := openRecoveryStore(t, destination)
	entries := mixedEffectEntries()
	submitted, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, durable.Applied, submitted.Decision)
	queue.StopAdmission()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	feedCtx, stopFeeder := context.WithCancel(ctx)
	defer stopFeeder()
	failures := make(chan error, 1)
	admitted := make(chan struct{})
	var outcomesMu sync.Mutex
	outcomes := make(map[durable.ID]runner.ItemOutcome)
	handler := effectRunnerHandler(store, entries[1].ID)
	workers, err := runner.New(runner.Config[durable.Delivery]{
		Capacity: 4, Workers: 1, FailureMode: runner.Continue,
		Handler: func(ctx context.Context, deliveries []durable.Delivery) []runner.Result {
			select {
			case <-admitted:
			case <-ctx.Done():
			}
			return handler(ctx, deliveries)
		},
		OnTerminal: func(delivery durable.Delivery, outcome runner.ItemOutcome) error {
			outcomesMu.Lock()
			outcomes[delivery.ID] = outcome
			outcomesMu.Unlock()
			failure := outcome.Err
			var callbackErr error
			if outcome.State == runner.TerminalSucceeded {
				ackCtx, cancelAck := context.WithTimeout(context.Background(), operationTimeout)
				defer cancelAck()
				callbackErr = acknowledge(ackCtx, queue, delivery)
				failure = callbackErr
			}
			if failure != nil {
				// Notify the owner and return. Joining here would wait on this very callback.
				select {
				case failures <- failure:
				default:
				}
			}
			return callbackErr
		},
	})
	require.NoError(t, err)
	feederDone := make(chan error, 1)
	go func() {
		deliveries, err := receiveEffectBatch(feedCtx, queue, len(entries))
		if err == nil {
			_, err = transferEffectDeliveries(feedCtx, queue, workers, deliveries)
		}
		close(admitted)
		if err == nil {
			// Failed durable entries remain pending, so ErrDrained cannot end this feeder.
			_, err = queue.Receive(feedCtx, 1)
		}
		feederDone <- err
	}()
	feederJoined := false
	t.Cleanup(func() {
		stopFeeder()
		workers.Abort(context.Canceled)
		if !feederJoined {
			assert.ErrorIs(t, <-feederDone, context.Canceled)
		}
		assert.Error(t, workers.Wait())
	})
	select {
	case failure := <-failures:
		assert.ErrorIs(t, failure, errFixtureUnknown)
	case <-ctx.Done():
		t.Fatal("owner did not receive terminal failure")
	}
	stopFeeder()
	select {
	case err := <-feederDone:
		feederJoined = true
		assert.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("feeder did not join")
	}
	// Continue finishes the already accepted items. No worker or callback outlives Wait.
	workers.StopAdmission()
	require.Error(t, workers.Wait())
	outcomesMu.Lock()
	observed := outcomes
	outcomesMu.Unlock()
	require.Len(t, observed, 3)
	assert.Equal(t, runner.TerminalSucceeded, observed[entries[0].ID].State)
	assert.Equal(t, runner.TerminalFailed, observed[entries[1].ID].State)
	assert.ErrorIs(t, observed[entries[1].ID].Err, errFixtureUnknown)
	assert.Equal(t, runner.TerminalFailed, observed[entries[2].ID].State)
	assert.ErrorIs(t, observed[entries[2].ID].Err, inventory.ErrRejected)
	for index, entry := range entries {
		expected := durable.Pending
		if index == 0 {
			expected = durable.Acknowledged
		}
		assertQueueStatus(t, queue, entry.ID, expected)
	}
	first := effectRequest(durable.Delivery{
		ID: entries[0].ID, Codec: entries[0].Codec, Payload: entries[0].Payload,
	})
	second := effectRequest(durable.Delivery{
		ID: entries[1].ID, Codec: entries[1].Codec, Payload: entries[1].Payload,
	})
	assertRecoveryDestination(t, destination, 12,
		expectedReceipt{first, 13}, expectedReceipt{second, 12})
	require.NoError(t, queue.Close(t.Context()))
	require.NoError(t, store.Close())
	inspection, err := durable.Inspect(t.Context(), directory, sourceIDs())
	require.NoError(t, err)
	assert.Equal(t, durable.Count(2), inspection.Store.Counts.PendingItems)
}

func TestRunnerTransferReleasesOnlyUnacceptedSuffix(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openRecoveryQueue(t, filepath.Join(root, "queue"))
	store := openRecoveryStore(t, destination)
	entries := sourceEntries()
	submitted, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, durable.Applied, submitted.Decision)
	deliveries, err := receiveEffectBatch(t.Context(), queue, len(entries))
	require.NoError(t, err)
	require.Len(t, deliveries, len(entries))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	submitCtx, cancelSubmit := context.WithCancel(ctx)
	defer cancelSubmit()
	started := make(chan struct{})
	finish := make(chan struct{})
	var startOnce sync.Once
	var finishOnce sync.Once
	handler := effectRunnerHandler(store, durable.ID{})
	workers, err := runner.New(runner.Config[durable.Delivery]{
		Capacity: 1, Workers: 1,
		Handler: func(ctx context.Context, batch []durable.Delivery) []runner.Result {
			startOnce.Do(func() { close(started) })
			select {
			case <-finish:
			case <-ctx.Done():
			}
			return handler(ctx, batch)
		},
		OnTerminal: func(delivery durable.Delivery, outcome runner.ItemOutcome) error {
			if outcome.State != runner.TerminalSucceeded {
				return outcome.Err
			}
			return acknowledge(ctx, queue, delivery)
		},
	})
	require.NoError(t, err)
	type transferResult struct {
		accepted int
		err      error
	}
	transferred := make(chan transferResult, 1)
	go func() {
		accepted, err := transferEffectDeliveries(submitCtx, queue, workers, deliveries)
		transferred <- transferResult{accepted: accepted, err: err}
	}()
	feederJoined := false
	workersJoined := false
	t.Cleanup(func() {
		cancelSubmit()
		finishOnce.Do(func() { close(finish) })
		workers.Abort(context.Canceled)
		if !feederJoined {
			<-transferred
		}
		if !workersJoined {
			assert.Error(t, workers.Wait())
		}
	})
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("worker did not acquire accepted prefix")
	}
	cancelSubmit()
	var transfer transferResult
	select {
	case transfer = <-transferred:
		feederJoined = true
	case <-ctx.Done():
		t.Fatal("canceled feeder did not return")
	}
	require.ErrorIs(t, transfer.err, context.Canceled)
	require.GreaterOrEqual(t, transfer.accepted, 1)
	require.Less(t, transfer.accepted, len(deliveries))
	// The blocked worker can own one item and the ring one more. Only the suffix is redeliverable.
	for _, original := range deliveries[transfer.accepted:] {
		current := receiveRecoveryEntry(t, queue)
		assert.Equal(t, original.ID, current.ID)
		assert.NotEqual(t, original.Token, current.Token)
	}
	assertRecoveryDestination(t, destination, 10)
	finishOnce.Do(func() { close(finish) })
	workers.StopAdmission()
	require.NoError(t, workers.Wait())
	workersJoined = true
	var expected []expectedReceipt
	quantities := []int64{13, 12}
	for index, delivery := range deliveries[:transfer.accepted] {
		assertQueueStatus(t, queue, delivery.ID, durable.Acknowledged)
		expected = append(expected, expectedReceipt{effectRequest(delivery), quantities[index]})
	}
	for _, delivery := range deliveries[transfer.accepted:] {
		assertQueueStatus(t, queue, delivery.ID, durable.Pending)
	}
	assertRecoveryDestination(t, destination, quantities[transfer.accepted-1], expected...)
	require.NoError(t, queue.Close(t.Context()))
	require.NoError(t, store.Close())
}
