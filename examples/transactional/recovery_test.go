//go:build linux && amd64 && cgo

package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"io"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable"
	"github.com/freshdresch/go-data-structures/workq/examples/internal/inventory"
)

func openRecoveryQueue(t *testing.T, directory string) *durable.Queue {
	t.Helper()
	config, err := exampleConfig()
	require.NoError(t, err)
	queue, err := durable.Open(t.Context(), directory, config)
	if errors.Is(err, durable.ErrUnsupported) {
		t.Skipf("queue storage profile unsupported: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
	return queue
}

func openRecoveryStore(t *testing.T, path string) *inventory.Store {
	t.Helper()
	store, err := inventory.Open(t.Context(), path, 64)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	return store
}

func submitRecoveryEntry(t *testing.T, queue *durable.Queue, entry durable.Entry) {
	t.Helper()
	result, err := queue.SubmitBatch(t.Context(), []durable.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, durable.Applied, result.Decision)
}

func receiveRecoveryEntry(t *testing.T, queue *durable.Queue) durable.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	deliveries, err := queue.Receive(ctx, 1)
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	return deliveries[0]
}

func assertQueueStatus(
	t *testing.T,
	queue *durable.Queue,
	id durable.ID,
	kind durable.StatusKind,
) {
	t.Helper()
	statuses, err := queue.Lookup(t.Context(), []durable.ID{id})
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.Equal(t, kind, statuses[0].Kind)
}

type expectedReceipt struct {
	request  inventory.Request
	quantity int64
}

// Read every persisted receipt field independently of Apply and worker observations.
func assertRecoveryDestination(
	t *testing.T,
	path string,
	quantity int64,
	expected ...expectedReceipt,
) {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	database, err := sql.Open("sqlite3", uri.String())
	require.NoError(t, err)
	defer func() { assert.NoError(t, database.Close()) }()
	require.NoError(t, database.PingContext(t.Context()))
	var actualQuantity, adjustments int64
	err = database.QueryRowContext(t.Context(),
		"SELECT quantity, adjustments FROM inventory WHERE item = 'widget'",
	).Scan(&actualQuantity, &adjustments)
	require.NoError(t, err)
	assert.Equal(t, quantity, actualQuantity)
	assert.Equal(t, int64(len(expected)), adjustments)
	rows, err := database.QueryContext(t.Context(),
		`SELECT version, namespace, producer, sequence, effect, codec, request, result_quantity
   FROM receipts ORDER BY namespace, sequence`)
	require.NoError(t, err)
	defer func() { assert.NoError(t, rows.Close()) }()
	var actual []expectedReceipt
	for rows.Next() {
		var version int
		var producer, sequence []byte
		var receipt expectedReceipt
		var saved sql.NullInt64
		err := rows.Scan(&version, &receipt.request.Key.Namespace, &producer, &sequence,
			&receipt.request.Key.Effect, &receipt.request.Codec, &receipt.request.Payload, &saved)
		require.NoError(t, err)
		assert.Equal(t, 1, version)
		require.Len(t, producer, 16)
		require.Len(t, sequence, 8)
		copy(receipt.request.Key.Producer[:], producer)
		receipt.request.Key.Sequence = binary.BigEndian.Uint64(sequence)
		assert.True(t, saved.Valid, "no provisional receipt may survive commit")
		receipt.quantity = saved.Int64
		actual = append(actual, receipt)
	}
	require.NoError(t, rows.Err())
	assert.ElementsMatch(t, expected, actual)
}

func TestAckObservationRecovery(t *testing.T) {
	for _, history := range []durable.StatusKind{durable.Pending, durable.Acknowledged} {
		t.Run(history.String(), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			directory := filepath.Join(root, "queue")
			destination := filepath.Join(root, "inventory.sqlite")
			queue := openRecoveryQueue(t, directory)
			store := openRecoveryStore(t, destination)
			entry := sourceEntries()[0]
			submitRecoveryEntry(t, queue, entry)
			delivery := receiveRecoveryEntry(t, queue)
			request := effectRequest(delivery)
			_, err := store.Apply(t.Context(), request)
			require.NoError(t, err)
			// These are lost-observation histories, not injected native queue failures.
			// Pending stops before Ack. Acknowledged performs real Ack but loses its reply.
			if history == durable.Acknowledged {
				_, err := queue.AckBatch(t.Context(), []durable.Token{delivery.Token})
				require.NoError(t, err)
			}
			assertQueueStatus(t, queue, entry.ID, history)
			assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
			require.NoError(t, store.Close())
			require.NoError(t, queue.Close(t.Context()))
			inspection, err := durable.Inspect(t.Context(), directory, []durable.ID{entry.ID})
			require.NoError(t, err)
			assert.Equal(t, history, inspection.Statuses[0].Kind)
			queue = openRecoveryQueue(t, directory)
			store = openRecoveryStore(t, destination)
			queue.StopAdmission()
			if history == durable.Pending {
				fresh := receiveRecoveryEntry(t, queue)
				assert.NotEqual(t, delivery.Token, fresh.Token)
				result, err := store.Apply(t.Context(), effectRequest(fresh))
				require.NoError(t, err)
				assert.True(t, result.Duplicate)
				assert.Equal(t, int64(13), result.Quantity)
				require.NoError(t, acknowledge(t.Context(), queue, fresh))
			}
			_, err = queue.Receive(t.Context(), 1)
			assert.ErrorIs(t, err, durable.ErrDrained)
			assertQueueStatus(t, queue, entry.ID, durable.Acknowledged)
			assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
		})
	}
}

func TestStaleTokensPreserveDestinationSuccess(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openRecoveryQueue(t, directory)
	store := openRecoveryStore(t, destination)
	entry := sourceEntries()[0]
	submitRecoveryEntry(t, queue, entry)
	first := receiveRecoveryEntry(t, queue)
	request := effectRequest(first)
	_, err := store.Apply(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, queue.Release(first.Token))
	second := receiveRecoveryEntry(t, queue)
	assert.NotEqual(t, first.Token, second.Token)
	assertStaleRecoveryToken(t, queue, first.Token)
	require.NoError(t, queue.Close(t.Context()))
	queue = openRecoveryQueue(t, directory)
	assertStaleRecoveryToken(t, queue, second.Token)
	fresh := receiveRecoveryEntry(t, queue)
	assert.NotEqual(t, second.Token, fresh.Token)
	result, err := store.Apply(t.Context(), effectRequest(fresh))
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	assert.Equal(t, int64(13), result.Quantity)
	require.NoError(t, acknowledge(t.Context(), queue, fresh))
	assertQueueStatus(t, queue, entry.ID, durable.Acknowledged)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
}

func assertStaleRecoveryToken(t *testing.T, queue *durable.Queue, token durable.Token) {
	t.Helper()
	assert.ErrorIs(t, queue.Release(token), durable.ErrStaleToken)
	result, err := queue.AckBatch(t.Context(), []durable.Token{token})
	assert.ErrorIs(t, err, durable.ErrStaleToken)
	assert.Equal(t, durable.NotApplied, result.Decision)
}

func TestQueueRecreationPreservesEffectIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	destination := filepath.Join(root, "inventory.sqlite")
	store := openRecoveryStore(t, destination)
	entry := sourceEntries()[0]
	queue := openRecoveryQueue(t, filepath.Join(root, "original"))
	submitRecoveryEntry(t, queue, entry)
	original := receiveRecoveryEntry(t, queue)
	request := effectRequest(original)
	result, err := store.Apply(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, acknowledge(t.Context(), queue, original))
	require.NoError(t, queue.Close(t.Context()))

	recreated := openRecoveryQueue(t, filepath.Join(root, "recreated"))
	submitRecoveryEntry(t, recreated, entry)
	replay := receiveRecoveryEntry(t, recreated)
	duplicate, err := store.Apply(t.Context(), effectRequest(replay))
	require.NoError(t, err)
	assert.True(t, duplicate.Duplicate)
	assert.Equal(t, result.AdjustmentID, duplicate.AdjustmentID)
	assert.Equal(t, result.Quantity, duplicate.Quantity)
	require.NoError(t, acknowledge(t.Context(), recreated, replay))
	assertQueueStatus(t, recreated, entry.ID, durable.Acknowledged)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})

	alteredQueue := openRecoveryQueue(t, filepath.Join(root, "altered"))
	altered := entry
	altered.Payload = []byte(`{"item":"widget","delta":4}`)
	submitRecoveryEntry(t, alteredQueue, altered)
	alteredDelivery := receiveRecoveryEntry(t, alteredQueue)
	_, err = store.Apply(t.Context(), effectRequest(alteredDelivery))
	assert.ErrorIs(t, err, inventory.ErrConflict)
	assertQueueStatus(t, alteredQueue, entry.ID, durable.Pending)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	// Conflict requires source correction. The altered entry remains pending.

	incarnation := openRecoveryQueue(t, filepath.Join(root, "incarnation"))
	submitRecoveryEntry(t, incarnation, entry)
	newDelivery := receiveRecoveryEntry(t, incarnation)
	newRequest := effectRequest(newDelivery)
	newRequest.Key.Namespace = "workq-transactional-demo-v2"
	newResult, err := store.Apply(t.Context(), newRequest)
	require.NoError(t, err)
	assert.False(t, newResult.Duplicate)
	assert.NotEqual(t, result.AdjustmentID, newResult.AdjustmentID)
	require.NoError(t, acknowledge(t.Context(), incarnation, newDelivery))
	assertQueueStatus(t, incarnation, entry.ID, durable.Acknowledged)
	assertRecoveryDestination(t, destination, 16,
		expectedReceipt{request, 13}, expectedReceipt{newRequest, 16})
}

func TestQueueCleanupRetainsDestinationReceipt(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	destination := filepath.Join(root, "inventory.sqlite")
	store := openRecoveryStore(t, destination)
	queue := openRecoveryQueue(t, filepath.Join(root, "queue"))
	entry := sourceEntries()[0]
	submitRecoveryEntry(t, queue, entry)
	delivery := receiveRecoveryEntry(t, queue)
	request := effectRequest(delivery)
	_, err := store.Apply(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, acknowledge(t.Context(), queue, delivery))
	retired, err := queue.RetireThrough(t.Context(), entry.ID.Producer, entry.ID.Sequence)
	require.NoError(t, err)
	require.Equal(t, durable.Applied, retired.Decision)
	// Ordered barriers allow the queue's scheduled cleanup to report committed progress.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for queue.Snapshot().Store.Counts.Receipts != 0 {
		_, err := queue.Lookup(ctx, []durable.ID{entry.ID})
		require.NoError(t, err)
	}
	assertQueueStatus(t, queue, entry.ID, durable.Retired)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	require.NoError(t, queue.Close(t.Context()))
	replayQueue := openRecoveryQueue(t, filepath.Join(root, "recreated"))
	submitRecoveryEntry(t, replayQueue, entry)
	replay := receiveRecoveryEntry(t, replayQueue)
	result, err := store.Apply(t.Context(), effectRequest(replay))
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	require.NoError(t, acknowledge(t.Context(), replayQueue, replay))
	assertQueueStatus(t, replayQueue, entry.ID, durable.Acknowledged)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
}

func TestCanceledWorkerAcknowledgesKnownSuccessAndJoins(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openRecoveryQueue(t, directory)
	store := openRecoveryStore(t, destination)
	entries := sourceEntries()[:2]
	for _, entry := range entries {
		submitRecoveryEntry(t, queue, entry)
	}
	queue.StopAdmission()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	committed := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	afterApply := func() {
		close(committed)
		select {
		case <-release:
		case <-t.Context().Done():
		}
	}
	go func() {
		_, err := work(ctx, context.WithoutCancel(ctx), queue, store, io.Discard, false, afterApply)
		done <- err
	}()
	joined := false
	t.Cleanup(func() {
		cancel()
		if !joined {
			select {
			case err := <-done:
				assert.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Error("worker did not join during cleanup")
			}
		}
	})
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not commit")
	}
	cancel()
	assertQueueStatus(t, queue, entries[0].ID, durable.Pending)
	request := effectRequest(durable.Delivery{
		ID: entries[0].ID, Codec: entries[0].Codec, Payload: entries[0].Payload,
	})
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	close(release)
	select {
	case err := <-done:
		joined = true
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled worker did not join")
	}
	assertQueueStatus(t, queue, entries[0].ID, durable.Acknowledged)
	assertQueueStatus(t, queue, entries[1].ID, durable.Pending)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	require.NoError(t, store.Close())
	require.NoError(t, queue.Close(t.Context()))
	queue = openRecoveryQueue(t, directory)
	store = openRecoveryStore(t, destination)
	queue.StopAdmission()
	_, err := work(t.Context(), t.Context(), queue, store, io.Discard, false, nil)
	require.NoError(t, err)
	secondRequest := effectRequest(durable.Delivery{
		ID: entries[1].ID, Codec: entries[1].Codec, Payload: entries[1].Payload,
	})
	assertQueueStatus(t, queue, entries[1].ID, durable.Acknowledged)
	assertRecoveryDestination(t, destination, 12,
		expectedReceipt{request, 13}, expectedReceipt{secondRequest, 12})
}
