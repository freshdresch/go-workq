package durable

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLargeLimitsRealAPI(t *testing.T) {
	policy := DurablePolicy{
		Profile: "sqlite-wal-v2", MaxPayload: 64 << 10, MaxCodec: 256,
		MaxPendingItems: 10000, MaxPendingPayload: 64 << 20,
		MaxReceipts: 20000, MaxProducers: 512, DatabaseLimit: 2 << 30,
	}
	runtime, err := DefaultRuntime(policy)
	require.NoError(t, err)
	config := Config{Policy: policy, Durability: Synchronized, Runtime: runtime}
	queue, directory := openTestQueue(t, config)
	queue.StopDelivery()
	var autoVacuum int
	err = queue.opened.storage.conn.QueryRowContext(t.Context(), "PRAGMA auto_vacuum").
		Scan(&autoVacuum)
	require.NoError(t, err)
	require.Equal(t, 2, autoVacuum)

	submitLargeLimitsWave(t, queue, 0)
	const firstPayload = Bytes(8999*2048 + 32769)
	counts := queue.Snapshot().Store.Counts
	assert.Equal(t, Count(9000), counts.PendingItems)
	assert.Equal(t, firstPayload, counts.PendingPayload)
	assert.Greater(t, counts.PendingPayload, Bytes(16<<20))
	assert.Equal(t, Count(9000), counts.Receipts)
	assert.Equal(t, Count(300), counts.Producers)
	require.NoError(t, queue.Close(t.Context()))

	inspection, err := Inspect(t.Context(), directory, []ID{largeLimitsEntry(0).ID})
	require.NoError(t, err)
	assert.Equal(t, policy, inspection.Store.Policy)
	assert.Equal(t, counts, inspection.Store.Counts)
	assert.Equal(t, Pending, inspection.Statuses[0].Kind)
	queue, err = Open(t.Context(), directory, config)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
	drainLargeLimitsWave(t, queue, 0)
	submitLargeLimitsWave(t, queue, 9000)
	drainLargeLimitsWave(t, queue, 9000)
	counts = queue.Snapshot().Store.Counts
	assert.Zero(t, counts.PendingItems)
	assert.Zero(t, counts.PendingPayload)
	assert.Equal(t, Count(18000), counts.Receipts)
	assert.Equal(t, Count(300), counts.Producers)
	for start := 0; start < 18000; start += 256 {
		ids := make([]ID, min(256, 18000-start))
		for index := range ids {
			ids[index] = largeLimitsEntry(start + index).ID
		}
		statuses, err := queue.Lookup(t.Context(), ids)
		require.NoError(t, err)
		for index, status := range statuses {
			assert.Equal(t, ids[index], status.ID)
			assert.Equal(t, Acknowledged, status.Kind)
		}
	}
	for producer := range 300 {
		id := largeLimitsEntry(producer).ID
		result, err := queue.RetireThrough(t.Context(), id.Producer, 59)
		require.NoError(t, err)
		assert.Equal(t, Applied, result.Decision)
	}
	// Each producer has 60 receipts, so one cleanup turn clears its marker.
	// Ordered Lookups provide progress through the coordinator's service quota.
	barriers := 300 * (int(runtime.Maintenance.MaxCommandTurns) + 1)
	for range barriers {
		_, err := queue.Lookup(t.Context(), nil)
		require.NoError(t, err)
		counts = queue.Snapshot().Store.Counts
		if counts.CleanupMarkers == 0 {
			break
		}
	}
	require.Zero(t, counts.CleanupMarkers, "cleanup exceeded its command-turn bound")
	assert.Zero(t, counts.Receipts)
	assert.Equal(t, Count(300), counts.RetiredProducers)
	require.NoError(t, queue.Close(t.Context()))
	inspection, err = Inspect(t.Context(), directory, []ID{largeLimitsEntry(17999).ID})
	require.NoError(t, err)
	assert.Equal(t, counts, inspection.Store.Counts)
	assert.Equal(t, Retired, inspection.Statuses[0].Kind)
	assert.Equal(t, uint64(59), inspection.Statuses[0].RetiredThrough)
	t.Logf("pending=9000 payload=%d producers=300 retained receipts=18000 cleaned=18000", firstPayload)
}

func largeLimitsEntry(index int) Entry {
	var producer [16]byte
	binary.BigEndian.PutUint16(producer[:2], uint16(index%300+1))
	size := 2048
	if index == 0 {
		size = 32769
	}
	return Entry{
		ID:    ID{Producer: producer, Sequence: uint64(index / 300)},
		Codec: strings.Repeat("c", 65), Payload: bytes.Repeat([]byte{byte(index)}, size),
	}
}

func submitLargeLimitsWave(t *testing.T, queue *Queue, start int) {
	t.Helper()
	for offset := 0; offset < 9000; offset += 256 {
		entries := make([]Entry, min(256, 9000-offset))
		for index := range entries {
			entries[index] = largeLimitsEntry(start + offset + index)
		}
		result, err := queue.SubmitBatch(t.Context(), entries)
		require.NoError(t, err)
		assert.Equal(t, Applied, result.Decision)
	}
}

func drainLargeLimitsWave(t *testing.T, queue *Queue, start int) {
	t.Helper()
	seen := make([]bool, 9000)
	for received := 0; received < len(seen); {
		deliveries, err := queue.Receive(t.Context(), min(32, len(seen)-received))
		require.NoError(t, err)
		require.NotEmpty(t, deliveries)
		tokens := make([]Token, len(deliveries))
		for index, delivery := range deliveries {
			producer := int(binary.BigEndian.Uint16(delivery.ID.Producer[:2])) - 1
			require.GreaterOrEqual(t, producer, 0)
			require.Less(t, producer, 300)
			require.Less(t, delivery.ID.Sequence, uint64(60))
			position := int(delivery.ID.Sequence)*300 + producer
			require.GreaterOrEqual(t, position, start)
			require.Less(t, position, start+len(seen))
			assert.False(t, seen[position-start], "duplicate ID %v", delivery.ID)
			seen[position-start] = true
			expected := largeLimitsEntry(position)
			assert.Equal(t, expected.ID, delivery.ID)
			assert.Equal(
				t,
				entryDigest(expected.Codec, expected.Payload),
				entryDigest(delivery.Codec, delivery.Payload),
			)
			tokens[index] = delivery.Token
		}
		result, err := queue.AckBatch(t.Context(), tokens)
		require.NoError(t, err)
		assert.Equal(t, Applied, result.Decision)
		received += len(deliveries)
	}
	for index, received := range seen {
		assert.True(t, received, "missing entry %d", start+index)
	}
}
