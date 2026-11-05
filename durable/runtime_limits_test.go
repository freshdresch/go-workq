package durable

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubmitCapacityBeforeAddition(t *testing.T) {
	for _, field := range []string{"items", "bytes", "receipts", "producers"} {
		t.Run(field, func(t *testing.T) {
			queue := groupFixture(t, testConfig(t))
			metadata := queue.opened.metadata
			metadata.policy.MaxPendingItems = math.MaxUint32
			metadata.policy.MaxPendingPayload = math.MaxUint64
			metadata.policy.MaxReceipts = math.MaxUint32
			metadata.policy.MaxProducers = math.MaxUint32
			switch field {
			case "items":
				metadata.counts.PendingItems = math.MaxUint32 - 1
			case "bytes":
				metadata.counts.PendingPayload = math.MaxUint64 - 1
			case "receipts":
				metadata.counts.Receipts = math.MaxUint32 - 1
			case "producers":
				metadata.counts.Producers = math.MaxUint32 - 1
			}
			entries := []Entry{
				{ID: ID{Producer: [16]byte{1}}, Payload: []byte{1}},
				{ID: ID{Producer: [16]byte{2}}, Payload: []byte{2}},
				{ID: ID{Producer: [16]byte{3}}, Payload: []byte{3}},
			}
			before := metadata
			next, fresh, err := queue.validateStoredSubmit(t.Context(), entries[:1], metadata)
			require.NoError(t, err)
			assert.Equal(t, []bool{true}, fresh)
			assert.Greater(t, next.lastOrder, before.lastOrder)
			next, fresh, err = queue.validateStoredSubmit(t.Context(), entries, metadata)
			require.ErrorIs(t, err, ErrCapacity)
			assert.Equal(t, storeMetadata{}, next)
			assert.Nil(t, fresh)
			assert.Equal(t, before, metadata)
			for _, table := range []string{"receipts", "payloads", "producers"} {
				var count int
				row := queue.opened.storage.conn.QueryRowContext(
					t.Context(),
					"SELECT count(*) FROM "+table,
				)
				require.NoError(t, row.Scan(&count))
				assert.Zero(t, count)
			}
			stored, err := queue.opened.storage.readMetadata(t.Context())
			require.NoError(t, err)
			assert.Equal(t, queue.opened.metadata, stored)
		})
	}
}

func expandedRuntime(t *testing.T) Config {
	t.Helper()
	config := testConfig(t)
	config.Runtime.Ordinary = CommandBudget{Commands: 65, Retained: 16 << 20}
	config.Runtime.Progress = CommandBudget{Commands: 65, Retained: 512 << 10}
	config.Runtime.Batches = BatchLimits{
		SubmitItems: 513, AckItems: 513, LookupItems: 513, SubmitEncoded: 4 << 20,
	}
	config.Runtime.Group = GroupConfig{
		Commands: 65, Operations: 513, Encoded: 4 << 20, MaxDelay: 11 * time.Millisecond,
	}
	config.Runtime.Delivery = DeliveryConfig{
		Items: 513, ReceiveItems: 513, ScanRows: 513, Retained: 8 << 20, ReadScratch: 64 << 10,
	}
	config.Runtime.Maintenance = MaintenanceConfig{
		CleanupRows: 513, CleanupEncoded: 128 << 10, MaxCommandTurns: 9,
	}
	config.Runtime.SQLite.Cache = 65 << 20
	config.Runtime.SQLite.Mmap = 257 << 20
	config.Runtime.SQLite.CheckpointTrigger = 17 << 20
	config.Runtime.SQLite.CheckpointInterval = 2 * time.Second
	config.Runtime.SQLite.BusyTimeout = 6 * time.Second
	return config
}

func TestRuntimeAboveHistoricalLimits(t *testing.T) {
	config := expandedRuntime(t)
	require.NoError(t, validateConfig(config))
	queue, _ := openTestQueue(t, config)
	entries := make([]Entry, 513)
	ids := make([]ID, len(entries))
	for index := range entries {
		ids[index] = ID{Sequence: uint64(index)}
		entries[index] = Entry{ID: ids[index], Payload: []byte{1}}
	}
	result, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	statuses, err := queue.Lookup(t.Context(), ids)
	require.NoError(t, err)
	assert.Len(t, statuses, len(ids))
	assert.Len(t, queue.ordinary.slots, 65)
	assert.Len(t, queue.progress.slots, 65)
	assert.Len(t, queue.claims, 513)
	assert.Equal(t, 1024, queue.deliveryQueue.Capacity())
	waitDeliveryState(t, queue, func(queue *Queue) bool {
		return queue.claims[512].delivery.Payload != nil
	})
	deliveries, err := queue.Receive(t.Context(), 513)
	require.NoError(t, err)
	require.Len(t, deliveries, 513)
	tokens := make([]Token, len(deliveries))
	for index, delivery := range deliveries {
		tokens[index] = delivery.Token
	}
	result, err = queue.AckBatch(t.Context(), tokens)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	assert.Zero(t, queue.Snapshot().Store.Counts.PendingItems)
	require.NoError(t, queue.Close(t.Context()))
}

func TestRuntimeRepresentationBoundaries(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"cache", func(cfg *Config) { cfg.Runtime.SQLite.Cache = (math.MaxInt32 + 1) * 1024 }},
		{"mmap", func(cfg *Config) { cfg.Runtime.SQLite.Mmap = math.MaxInt64 + 1 }},
		{"busy", func(cfg *Config) {
			cfg.Runtime.SQLite.BusyTimeout = (math.MaxInt32 + 1) * time.Millisecond
		}},
		{"ring", func(cfg *Config) { cfg.Runtime.Delivery.Items = 1<<30 + 1 }},
		{"combined", func(cfg *Config) { cfg.Runtime.Ordinary.Retained = math.MaxUint64 }},
		{"WAL", func(cfg *Config) { cfg.Runtime.SQLite.WALRefusal = math.MaxInt64 + 1 }},
		{"trigger equality", func(cfg *Config) {
			cfg.Runtime.SQLite.CheckpointTrigger = cfg.Runtime.SQLite.WALRefusal
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := testConfig(t)
			tc.change(&config)
			directory := filepath.Join(t.TempDir(), "absent")
			queue, err := Open(t.Context(), directory, config)
			assert.Nil(t, queue)
			assert.ErrorIs(t, err, ErrInvalidInput)
			_, err = os.Stat(directory)
			assert.ErrorIs(t, err, os.ErrNotExist)
		})
	}
	config := testConfig(t)
	config.Runtime.SQLite.Cache = math.MaxInt32*1024 + 1023
	config.Runtime.SQLite.Mmap = math.MaxInt64
	config.Runtime.SQLite.BusyTimeout = math.MaxInt32*time.Millisecond + time.Millisecond - 1
	config.Runtime.SQLite.CheckpointInterval = math.MaxInt64
	config.Runtime.SQLite.WALRefusal = math.MaxInt64
	config.Runtime.Group.MaxDelay = math.MaxInt64
	config.Runtime.Maintenance.CleanupEncoded = math.MaxUint64
	require.NoError(t, validateConfig(config))
	for _, tc := range []struct {
		items    Count
		capacity int
	}{
		{1, 1}, {256, 256}, {257, 512}, {1 << 30, 1 << 30},
	} {
		capacity, err := deliveryRingCapacity(tc.items)
		require.NoError(t, err)
		assert.Equal(t, tc.capacity, capacity)
	}
}

func TestRuntimeSQLiteReadbackAndZeroCompatibility(t *testing.T) {
	config := testConfig(t)
	config.Runtime.SQLite.WALRefusal = 0
	config.Runtime.SQLite.Cache = 2047
	config.Runtime.SQLite.Mmap = 0
	config.Runtime.SQLite.BusyTimeout = time.Millisecond - 1
	config.Runtime.Group.MaxDelay = 0
	queue, _ := openTestQueue(t, config)
	assert.Equal(t, walEnvelopeBytes, queue.config.Runtime.SQLite.WALRefusal)
	assert.Equal(t, walEnvelopeBytes, queue.Snapshot().Disk.WALRefusal)
	assert.Equal(t, Bytes(1024), queue.Snapshot().Disk.Cache)
	require.NoError(t, queue.Close(t.Context()))
	config.Runtime.SQLite.Mmap = math.MaxInt64
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "clamped"), config)
	assert.Nil(t, queue)
	assert.ErrorIs(t, err, ErrUnsupported)
}

func retiredChunkFixture(t *testing.T, config Config) *Queue {
	t.Helper()
	queue := groupFixture(t, config)
	entries := make([]Entry, 513)
	tokens := make([]Token, len(entries))
	for index := range entries {
		id := ID{Sequence: uint64(index)}
		entries[index] = Entry{ID: id}
		token := encodeToken(queue.opened.metadata.uuid, queue.opened.metadata.epoch, id, 1)
		tokens[index] = token
		queue.claims[index] = claim{used: true, acquired: true,
			delivery: Delivery{ID: id, Token: token}}
	}
	group := runGroup(t, queue, submitCommand(entries...))
	require.NoError(t, group.commands[0].err)
	group = runGroup(
		t,
		queue,
		&command{kind: ackCommand, tokens: tokens},
		&command{kind: retireCommand, retirement: ID{Sequence: 512}},
	)
	require.NoError(t, group.commands[0].err)
	require.NoError(t, group.commands[1].err)
	return queue
}

func TestRuntimeCleanupChunks(t *testing.T) {
	for _, mode := range []string{"all", "rows", "encoded", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			config := expandedRuntime(t)
			expected := Count(0)
			switch mode {
			case "all":
				config.Runtime.Maintenance.CleanupEncoded = math.MaxUint64
			case "rows":
				config.Runtime.Maintenance.CleanupRows = 300
				expected = 213
			case "encoded":
				config.Runtime.Maintenance.CleanupEncoded = 26 + 300*121
				expected = 213
			}
			queue := retiredChunkFixture(t, config)
			if mode == "rollback" {
				_, err := queue.opened.storage.conn.ExecContext(
					t.Context(),
					`CREATE TRIGGER reject_second_chunk BEFORE DELETE ON receipts
      WHEN (SELECT count(*) FROM receipts)=257
      BEGIN SELECT RAISE(ABORT,'injected'); END`,
				)
				require.NoError(t, err)
				before := queue.opened.metadata
				require.ErrorIs(t, queue.cleanupTurn(t.Context()), ErrStorage)
				assert.Equal(t, before, queue.opened.metadata)
				var count int
				row := queue.opened.storage.conn.QueryRowContext(
					t.Context(),
					"SELECT count(*) FROM receipts",
				)
				require.NoError(t, row.Scan(&count))
				assert.Equal(t, 513, count)
				return
			}
			require.NoError(t, queue.cleanupTurn(t.Context()))
			assert.Equal(t, expected, queue.opened.metadata.counts.Receipts)
			assert.Equal(t, Count(513)-expected, queue.Snapshot().Cleanup.Deleted)
			assert.Equal(t, expected != 0, queue.opened.metadata.counts.CleanupMarkers != 0)
		})
	}
}

func TestRuntimeIndexedQueryPlans(t *testing.T) {
	queue := retiredChunkFixture(t, expandedRuntime(t))
	runGroup(t, queue, submitCommand(Entry{ID: ID{Producer: [16]byte{1}}}))
	lower, upper := encodeID(ID{}), encodeID(ID{Sequence: 512})
	cases := []struct {
		name, query, index string
		args               []any
	}{
		{"delivery", deliveryKeyQuery, "pending_order", []any{make([]byte, 8), lower[:]}},
		{"producer", cleanupProducerQuery, "cleanup_producers", nil},
		{"receipts", cleanupReceiptQuery, "PRIMARY KEY", []any{lower[:], upper[:], 256}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotContains(t, strings.ToUpper(tc.query), "OFFSET")
			assert.Contains(t, strings.ToUpper(tc.query), "LIMIT")
			rows, err := queue.opened.storage.conn.QueryContext(
				t.Context(),
				"EXPLAIN QUERY PLAN "+tc.query,
				tc.args...,
			)
			require.NoError(t, err)
			var plans []string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
				plans = append(plans, detail)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			plan := strings.Join(plans, "\n")
			t.Log(plan)
			assert.Contains(t, plan, tc.index)
			assert.NotContains(t, plan, "USE TEMP B-TREE")
		})
	}
}

func TestRuntimeFullCountValidationWithoutAllocation(t *testing.T) {
	config := testConfig(t)
	config.Runtime.Ordinary = CommandBudget{Commands: math.MaxUint32, Retained: 1 << 50}
	config.Runtime.Progress = CommandBudget{Commands: math.MaxUint32, Retained: 1 << 50}
	config.Runtime.Batches = BatchLimits{SubmitItems: math.MaxUint32, AckItems: math.MaxUint32,
		LookupItems: math.MaxUint32, SubmitEncoded: 1 << 50}
	config.Runtime.Group.Commands = math.MaxUint32
	config.Runtime.Group.Operations = math.MaxUint32
	config.Runtime.Group.Encoded = 1 << 50
	config.Runtime.Delivery.ScanRows = math.MaxUint32
	config.Runtime.Maintenance.CleanupRows = math.MaxUint32
	config.Runtime.Maintenance.MaxCommandTurns = math.MaxUint32
	require.NoError(t, validateConfig(config))
}
