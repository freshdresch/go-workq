package durable

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func migrationFixture(t *testing.T, config Config, count, payloadSize int) string {
	t.Helper()
	opened, err := startStore(t.Context(), filepath.Join(t.TempDir(), "queue"), config)
	require.NoError(t, err)
	defer func() { require.NoError(t, opened.close()) }()
	queue := &Queue{opened: opened, config: config, changed: make(chan struct{}),
		claims: make([]claim, config.Runtime.Delivery.Items)}
	var autoVacuum int
	require.NoError(t, queue.opened.storage.conn.QueryRowContext(
		t.Context(),
		"PRAGMA auto_vacuum",
	).Scan(&autoVacuum))
	require.Equal(t, 2, autoVacuum)
	for start := 0; start < count; start += 64 {
		entries := make([]Entry, min(64, count-start))
		for i := range entries {
			entries[i] = Entry{ID: ID{Sequence: uint64(start + i)},
				Codec: "raw", Payload: make([]byte, payloadSize)}
		}
		group := runGroup(t, queue, submitCommand(entries...))
		require.NoError(t, group.commands[0].err)
	}
	// Separate producers leave acknowledged history both above and below a retirement floor.
	for i := range 2 {
		id := ID{Producer: [16]byte{byte(i + 1)}, Sequence: 7}
		group := runGroup(t, queue, submitCommand(Entry{ID: id, Payload: []byte("ack")}))
		require.NoError(t, group.commands[0].err)
		token := encodeToken(queue.opened.metadata.uuid, queue.opened.metadata.epoch, id, 3)
		queue.claims[0] = claim{used: true, acquired: true, delivery: Delivery{ID: id, Token: token}}
		group = runGroup(t, queue, &command{kind: ackCommand, tokens: []Token{token}})
		require.NoError(t, group.commands[0].err)
	}
	group := runGroup(t, queue, &command{kind: retireCommand,
		retirement: ID{Producer: [16]byte{2}, Sequence: 7}})
	require.NoError(t, group.commands[0].err)
	require.Equal(t, Count(1), queue.opened.metadata.counts.CleanupMarkers)
	return queue.opened.owner.path
}

func migrationRecords(t *testing.T, directory string) []string {
	t.Helper()
	var records []string
	for _, row := range logicalOfflineSnapshot(t, directory) {
		changed := false
		for _, key := range []int{metaSchema, metaProfile, metaPolicy} {
			if strings.HasPrefix(row, fmt.Sprintf("meta[%d ", key)) {
				changed = true
			}
		}
		if !changed {
			records = append(records, row)
		}
	}
	return records
}

func migrationSchema(t *testing.T, directory string) []string {
	t.Helper()
	opened, err := openOffline(t.Context(), directory)
	require.NoError(t, err)
	rows, err := opened.storage.conn.QueryContext(
		t.Context(),
		"SELECT name,coalesce(sql,'') FROM sqlite_schema ORDER BY name",
	)
	require.NoError(t, err)
	var schema []string
	for rows.Next() {
		var name, ddl string
		require.NoError(t, rows.Scan(&name, &ddl))
		assert.NotContains(t, name, "migration_")
		schema = append(schema, name+ddl)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.NoError(t, opened.close())
	return schema
}

func TestMigrationPreservesHistoryAndRetries(t *testing.T) {
	for _, durability := range []Durability{Synchronized, ProcessCrash} {
		t.Run(durability.String(), func(t *testing.T) {
			t.Parallel()
			config := testConfig(t)
			config.Durability = durability
			directory := migrationFixture(t, config, 513, 17)
			before := migrationRecords(t, directory)
			request := resizeRequest(t, directory)
			request.Next.Profile = profileSQLiteWALV2
			batches := 0
			observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
				if stage == "migration-batch" {
					batches++
				}
				if stage == "migration-created" {
					var synchronous int
					require.NoError(t, opened.storage.conn.QueryRowContext(
						t.Context(),
						"PRAGMA synchronous",
					).Scan(&synchronous))
					assert.Equal(t, 2, synchronous)
				}
				return nil
			}}
			result, err := resizeObserved(t.Context(), directory, request, observer)
			require.NoError(t, err)
			assert.Equal(t, Applied, result.Decision)
			assert.Equal(t, 3, batches)
			assert.Equal(t, before, migrationRecords(t, directory))
			migrationSchema(t, directory)
			result, err = Resize(t.Context(), directory, request)
			require.NoError(t, err)
			assert.Equal(t, Applied, result.Decision)
			assert.Equal(t, before, migrationRecords(t, directory))
			queue, err := Open(t.Context(), directory, config)
			assert.Nil(t, queue)
			assert.ErrorIs(t, err, ErrConfigMismatch)
			config.Policy = request.Next
			queue, err = Open(t.Context(), directory, config)
			require.NoError(t, err)
			require.NoError(t, queue.Close(t.Context()))
		})
	}
}

func TestMigrationCancellationRollsBack(t *testing.T) {
	t.Parallel()
	directory := migrationFixture(t, testConfig(t), 513, 1)
	before := logicalOfflineSnapshot(t, directory)
	schema := migrationSchema(t, directory)
	request := resizeRequest(t, directory)
	request.Next.Profile = profileSQLiteWALV2
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	cause := errors.New("cancel between migration batches")
	batches := 0
	observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
		if stage != "migration-batch" {
			return nil
		}
		batches++
		var copied int
		require.NoError(t, opened.storage.conn.QueryRowContext(
			t.Context(),
			"SELECT count(*) FROM payloads",
		).Scan(&copied))
		assert.Equal(t, 256, copied)
		cancel(cause)
		return nil
	}}
	result, err := resizeObserved(ctx, directory, request, observer)
	assert.Equal(t, NotApplied, result.Decision)
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, cause)
	assert.NotErrorIs(t, err, ErrStorage)
	assert.Equal(t, 1, batches)
	assert.Equal(t, before, logicalOfflineSnapshot(t, directory))
	assert.Equal(t, schema, migrationSchema(t, directory))
	result, err = Resize(t.Context(), directory, request)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
}

func TestMigrationFullRollsBack(t *testing.T) {
	t.Parallel()
	directory := migrationFixture(t, testConfig(t), 64, 16384)
	before := logicalOfflineSnapshot(t, directory)
	schema := migrationSchema(t, directory)
	inspection, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	request := resizeRequest(t, directory)
	request.Next.Profile = profileSQLiteWALV2
	request.Next.DatabaseLimit = Bytes(inspection.Store.PageCount+3) * pageSize
	created := false
	observer := offlineTestObserver{observe: func(stage string, _ *validatedStore) error {
		if stage == "migration-created" {
			created = true
		}
		return nil
	}}
	result, err := resizeObserved(t.Context(), directory, request, observer)
	assert.True(t, created)
	assert.Equal(t, NotApplied, result.Decision)
	assert.ErrorIs(t, err, ErrStorage)
	var sqliteErr sqlite3.Error
	require.ErrorAs(t, err, &sqliteErr)
	assert.Equal(t, sqlite3.ErrFull, sqliteErr.Code)
	assert.Equal(t, before, logicalOfflineSnapshot(t, directory))
	assert.Equal(t, schema, migrationSchema(t, directory))
	queue, err := Open(t.Context(), directory, testConfig(t))
	require.NoError(t, err)
	require.NoError(t, queue.Close(t.Context()))
}

func TestMigrationGrowthMaintainAndShrink(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	config.Policy.DatabaseLimit = 2 << 20
	directory := migrationFixture(t, config, 64, 16384)
	before := migrationRecords(t, directory)
	request := resizeRequest(t, directory)
	request.Next.Profile = profileSQLiteWALV2
	request.Next.DatabaseLimit = 8 << 20
	result, err := Resize(t.Context(), directory, request)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	inspection, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	require.Positive(t, inspection.Store.FreePages)
	shrink := ResizeRequest{UUID: request.UUID, Expected: request.Next, Next: request.Next}
	shrink.Next.DatabaseLimit = Bytes(inspection.Store.PageCount-1) * pageSize
	result, err = Resize(t.Context(), directory, shrink)
	assert.ErrorIs(t, err, ErrCapacity)
	assert.Equal(t, NotApplied, result.Decision)
	_, err = Maintain(t.Context(), directory)
	require.NoError(t, err)
	result, err = Resize(t.Context(), directory, shrink)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	assert.Equal(t, before, migrationRecords(t, directory))
}

func TestMigrationQueryPlans(t *testing.T) {
	t.Parallel()
	directory := migrationFixture(t, testConfig(t), 257, 1)
	request := resizeRequest(t, directory)
	request.Next.Profile = profileSQLiteWALV2
	observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
		if stage != "migration-created" {
			return nil
		}
		key := encodeID(ID{})
		for _, query := range []string{migrationKeysQuery, migrationCopyQuery} {
			assert.NotContains(t, strings.ToUpper(query), "OFFSET")
			rows, err := opened.storage.conn.QueryContext(
				t.Context(),
				"EXPLAIN QUERY PLAN "+query,
				key[:],
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
			assert.Contains(t, plan, "SEARCH migration_payloads USING PRIMARY KEY")
			assert.NotContains(t, plan, "USE TEMP B-TREE")
		}
		return nil
	}}
	result, err := resizeObserved(t.Context(), directory, request, observer)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
}
