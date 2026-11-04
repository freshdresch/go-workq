//go:build linux && amd64 && cgo

package main

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/durable"
)

// These tests construct valid public-API histories for possible NORMAL recovery states.
// Omitting an Ack or creating an empty queue models missing durable state. Closing and
// reopening these fixtures does not inject transaction loss or qualify physical power loss.
func openNormalRecoveryQueue(t *testing.T, directory string) *durable.Queue {
	t.Helper()
	config, err := exampleConfig()
	require.NoError(t, err)
	config.Durability = durable.ProcessCrash
	queue, err := durable.Open(t.Context(), directory, config)
	if errors.Is(err, durable.ErrUnsupported) {
		t.Skipf("queue storage profile unsupported: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
	return queue
}

func TestNormalHistorySurvivingPendingAbsorbsLostAck(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openNormalRecoveryQueue(t, directory)
	store := openRecoveryStore(t, destination)
	entry := sourceEntries()[0]
	submitRecoveryEntry(t, queue, entry)
	delivery := receiveRecoveryEntry(t, queue)
	request := effectRequest(delivery)
	original, err := store.Apply(t.Context(), request)
	require.NoError(t, err)
	// The modeled recovered history contains the submission and destination commit,
	// but no Ack. We do not claim that a graceful close rolls back a successful Ack.
	assertQueueStatus(t, queue, entry.ID, durable.Pending)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	require.NoError(t, queue.Close(t.Context()))
	require.NoError(t, store.Close())

	queue = openNormalRecoveryQueue(t, directory)
	store = openRecoveryStore(t, destination)
	queue.StopAdmission()
	replay := receiveRecoveryEntry(t, queue)
	assert.Equal(t, request, effectRequest(replay))
	result, err := store.Apply(t.Context(), effectRequest(replay))
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	assert.Equal(t, original.AdjustmentID, result.AdjustmentID)
	assert.Equal(t, original.Quantity, result.Quantity)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	require.NoError(t, acknowledge(t.Context(), queue, replay))
	_, err = queue.Receive(t.Context(), 1)
	assert.ErrorIs(t, err, durable.ErrDrained)
	assertQueueStatus(t, queue, entry.ID, durable.Acknowledged)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
}

func TestNormalHistoryAbsentSoleIntentCannotRecover(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openNormalRecoveryQueue(t, directory)
	store := openRecoveryStore(t, destination)
	// An empty queue models a lost sole submission before destination execution.
	// The ID is only an oracle. No retained request is supplied to the worker.
	id := sourceIDs()[0]
	assertQueueStatus(t, queue, id, durable.Absent)
	assertRecoveryDestination(t, destination, 10)
	require.NoError(t, queue.Close(t.Context()))
	queue = openNormalRecoveryQueue(t, directory)
	queue.StopAdmission()
	_, err := work(t.Context(), t.Context(), queue, store, io.Discard, false, nil)
	require.NoError(t, err)
	_, err = queue.Receive(t.Context(), 1)
	assert.ErrorIs(t, err, durable.ErrDrained)
	assertQueueStatus(t, queue, id, durable.Absent)
	assertRecoveryDestination(t, destination, 10)
	// Destination deduplication cannot reconstruct an intent absent from every source.
}

func TestNormalHistoryRetainedSourceRebuildsAbsentWork(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openNormalRecoveryQueue(t, filepath.Join(root, "queue"))
	store := openRecoveryStore(t, destination)
	// The checked-in source keeps the original identity, codec, and exact bytes.
	entry := sourceEntries()[0]
	request := effectRequest(durable.Delivery{
		ID: entry.ID, Codec: entry.Codec, Payload: entry.Payload,
	})
	assertQueueStatus(t, queue, entry.ID, durable.Absent)
	assertRecoveryDestination(t, destination, 10)
	submitRecoveryEntry(t, queue, entry)
	queue.StopAdmission()
	delivery := receiveRecoveryEntry(t, queue)
	assert.Equal(t, request, effectRequest(delivery))
	result, err := store.Apply(t.Context(), effectRequest(delivery))
	require.NoError(t, err)
	assert.False(t, result.Duplicate)
	assert.Equal(t, int64(13), result.Quantity)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	require.NoError(t, acknowledge(t.Context(), queue, delivery))
	_, err = queue.Receive(t.Context(), 1)
	assert.ErrorIs(t, err, durable.ErrDrained)
	assertQueueStatus(t, queue, entry.ID, durable.Acknowledged)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
}

func TestNormalHistoryLostFloorStillNeedsDestinationReceipt(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	destination := filepath.Join(root, "inventory.sqlite")
	store := openRecoveryStore(t, destination)
	queue := openNormalRecoveryQueue(t, filepath.Join(root, "original"))
	entry := sourceEntries()[0]
	submitRecoveryEntry(t, queue, entry)
	delivery := receiveRecoveryEntry(t, queue)
	request := effectRequest(delivery)
	original, err := store.Apply(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, acknowledge(t.Context(), queue, delivery))
	retired, err := queue.RetireThrough(t.Context(), entry.ID.Producer, entry.ID.Sequence)
	require.NoError(t, err)
	require.Equal(t, durable.Applied, retired.Decision)
	assertQueueStatus(t, queue, entry.ID, durable.Retired)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	require.NoError(t, queue.Close(t.Context()))
	require.NoError(t, store.Close())

	// A separate valid queue models recovery without the old floor or receipt.
	// Its ability to admit the old key is why retirement grants no receipt TTL.
	recreated := openNormalRecoveryQueue(t, filepath.Join(root, "without-floor"))
	store = openRecoveryStore(t, destination)
	assertQueueStatus(t, recreated, entry.ID, durable.Absent)
	submitRecoveryEntry(t, recreated, entry)
	recreated.StopAdmission()
	replay := receiveRecoveryEntry(t, recreated)
	assert.Equal(t, request, effectRequest(replay))
	result, err := store.Apply(t.Context(), effectRequest(replay))
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	assert.Equal(t, original.AdjustmentID, result.AdjustmentID)
	assert.Equal(t, original.Quantity, result.Quantity)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	require.NoError(t, acknowledge(t.Context(), recreated, replay))
	_, err = recreated.Receive(t.Context(), 1)
	assert.ErrorIs(t, err, durable.ErrDrained)
	assertQueueStatus(t, recreated, entry.ID, durable.Acknowledged)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
}
