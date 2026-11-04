//go:build linux && amd64 && cgo

package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/durable"
	"github.com/freshdresch/go-workq/examples/internal/inventory"
)

func mixedEffectEntries() []durable.Entry {
	entries := sourceEntries()
	entries[2].Payload = []byte(`{"item":"widget","delta":-100}`)
	return entries
}

func TestBatchAcknowledgesOnlyEstablishedSuccess(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openRecoveryQueue(t, filepath.Join(root, "queue"))
	store := openRecoveryStore(t, destination)
	entries := mixedEffectEntries()
	submitted, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, durable.Applied, submitted.Decision)
	deliveries, err := receiveEffectBatch(t.Context(), queue, len(entries))
	require.NoError(t, err)
	require.Len(t, deliveries, 3)
	first, unresolved, rejected := deliveries[0], deliveries[1], deliveries[2]
	firstRequest, unresolvedRequest := effectRequest(first), effectRequest(unresolved)
	_, err = store.Apply(t.Context(), firstRequest)
	require.NoError(t, err)
	// Model a lost observation after a real commit, not an injected SQLite failure.
	_, err = store.Apply(t.Context(), unresolvedRequest)
	require.NoError(t, err)
	_, err = store.Apply(t.Context(), effectRequest(rejected))
	require.ErrorIs(t, err, inventory.ErrRejected)
	require.NoError(t, acknowledge(t.Context(), queue, first))
	assertQueueStatus(t, queue, first.ID, durable.Acknowledged)
	assertQueueStatus(t, queue, unresolved.ID, durable.Pending)
	assertQueueStatus(t, queue, rejected.ID, durable.Pending)
	assertRecoveryDestination(t, destination, 12,
		expectedReceipt{firstRequest, 13}, expectedReceipt{unresolvedRequest, 12})

	require.NoError(t, queue.Release(unresolved.Token))
	replay := receiveRecoveryEntry(t, queue)
	assert.Equal(t, unresolved.ID, replay.ID)
	assert.NotEqual(t, unresolved.Token, replay.Token)
	assert.Equal(t, unresolvedRequest, effectRequest(replay))
	result, err := store.Apply(t.Context(), effectRequest(replay))
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	assert.Equal(t, int64(12), result.Quantity)
	require.NoError(t, acknowledge(t.Context(), queue, replay))
	assertQueueStatus(t, queue, unresolved.ID, durable.Acknowledged)
	assertQueueStatus(t, queue, rejected.ID, durable.Pending)
	assertRecoveryDestination(t, destination, 12,
		expectedReceipt{firstRequest, 13}, expectedReceipt{unresolvedRequest, 12})
}

func TestRefusedAckBatchPreservesBothDestinationResults(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openRecoveryQueue(t, directory)
	store := openRecoveryStore(t, destination)
	entries := sourceEntries()[:2]
	submitted, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, durable.Applied, submitted.Decision)
	deliveries, err := receiveEffectBatch(t.Context(), queue, len(entries))
	require.NoError(t, err)
	require.Len(t, deliveries, 2)
	var expected []expectedReceipt
	var tokens []durable.Token
	quantities := []int64{13, 12}
	for index, delivery := range deliveries {
		request := effectRequest(delivery)
		result, err := store.Apply(t.Context(), request)
		require.NoError(t, err)
		assert.Equal(t, quantities[index], result.Quantity)
		expected = append(expected, expectedReceipt{request, quantities[index]})
		tokens = append(tokens, delivery.Token)
	}
	require.NoError(t, queue.Release(tokens[1]))
	refused, err := queue.AckBatch(t.Context(), tokens)
	assert.ErrorIs(t, err, durable.ErrStaleToken)
	assert.Equal(t, durable.NotApplied, refused.Decision)
	for _, entry := range entries {
		assertQueueStatus(t, queue, entry.ID, durable.Pending)
	}
	assertRecoveryDestination(t, destination, 12, expected...)
	require.NoError(t, queue.Close(t.Context()))
	require.NoError(t, store.Close())
	queue = openRecoveryQueue(t, directory)
	store = openRecoveryStore(t, destination)
	queue.StopAdmission()
	for index, original := range deliveries {
		current := receiveRecoveryEntry(t, queue)
		assert.Equal(t, original.ID, current.ID)
		assert.NotEqual(t, original.Token, current.Token)
		assert.Equal(t, expected[index].request, effectRequest(current))
		result, err := store.Apply(t.Context(), effectRequest(current))
		require.NoError(t, err)
		assert.True(t, result.Duplicate)
		assert.Equal(t, expected[index].quantity, result.Quantity)
		require.NoError(t, acknowledge(t.Context(), queue, current))
		assertQueueStatus(t, queue, current.ID, durable.Acknowledged)
	}
	_, err = queue.Receive(t.Context(), 1)
	assert.ErrorIs(t, err, durable.ErrDrained)
	assertRecoveryDestination(t, destination, 12, expected...)
}

func receiveEffectBatch(
	ctx context.Context,
	queue *durable.Queue,
	count int,
) ([]durable.Delivery, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	var deliveries []durable.Delivery
	for len(deliveries) < count {
		batch, err := queue.Receive(ctx, count-len(deliveries))
		if err != nil {
			return deliveries, err
		}
		deliveries = append(deliveries, batch...)
	}
	return deliveries, nil
}
