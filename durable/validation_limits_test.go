package durable

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidationCountBoundaries(t *testing.T) {
	for _, test := range []struct {
		name       string
		current    Count
		additional uint64
		limit      Count
		want       Count
		invalid    bool
	}{
		{name: "exact maximum", current: math.MaxUint32 - 1, additional: 1,
			limit: math.MaxUint32, want: math.MaxUint32},
		{name: "overflow", current: math.MaxUint32, additional: 1,
			limit: math.MaxUint32, invalid: true},
		{name: "wide addition", additional: math.MaxUint64, limit: math.MaxUint32, invalid: true},
		{name: "invalid current", current: 2, limit: 1, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual, err := validationCount(test.current, test.additional, test.limit)
			if test.invalid {
				assert.ErrorIs(t, err, ErrStorage)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, actual)
		})
	}
}

func TestValidationChunkHelperBoundaries(t *testing.T) {
	storage, _ := newTestStore(t)
	key := encodeID(ID{Sequence: 1})
	// The helper is exercised directly. These are not larger-record v1 fixtures.
	for _, length := range []int{0, 1, validationChunkBytes - 1, validationChunkBytes,
		validationChunkBytes + 1, 2*validationChunkBytes + 7} {
		data := bytes.Repeat([]byte{0x5a}, length)
		digest := sha256.New()
		_, err := storage.conn.ExecContext(t.Context(), "CREATE TEMP TABLE chunk_source(value BLOB)")
		require.NoError(t, err)
		_, err = storage.conn.ExecContext(t.Context(), "INSERT INTO chunk_source VALUES(?)", data)
		require.NoError(t, err)
		err = storage.hashPayloadColumn(
			t.Context(),
			key[:],
			"SELECT substr(value,?,?) FROM chunk_source WHERE ? IS NOT NULL",
			length,
			digest,
		)
		require.NoError(t, err)
		expected := sha256.Sum256(data)
		assert.Equal(t, expected[:], digest.Sum(nil), "length %d", length)
		_, err = storage.conn.ExecContext(t.Context(), "DROP TABLE chunk_source")
		require.NoError(t, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := storage.hashPayloadColumn(ctx, key[:], payloadChunkQuery, 1, sha256.New())
	assert.ErrorIs(t, err, context.Canceled)
	err = storage.hashPayloadColumn(t.Context(), key[:], payloadChunkQuery, 1, sha256.New())
	assert.Error(t, err)
}

func TestValidationV1PayloadDigest(t *testing.T) {
	config := testConfig(t)
	directory := filepath.Join(t.TempDir(), "queue")
	queue, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	entry := Entry{ID: ID{Sequence: 1}, Codec: strings.Repeat("c", int(config.Policy.MaxCodec)),
		Payload: bytes.Repeat([]byte{0x35}, int(config.Policy.MaxPayload))}
	result, err := queue.SubmitBatch(t.Context(), []Entry{entry})
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	require.NoError(t, queue.Close(t.Context()))
	inspection, err := Inspect(t.Context(), directory, []ID{entry.ID})
	require.NoError(t, err)
	assert.Equal(t, Count(1), inspection.Store.Counts.PendingItems)
}

func TestValidationBootstrapDoesNotSetCap(t *testing.T) {
	directory := offlineFixture(t, testConfig(t))
	storage, err := openStoreConnection(t.Context(), filepath.Join(directory, "queue.sqlite3"), false)
	require.NoError(t, err)
	defer func() { assert.NoError(t, storage.close()) }()
	capWrites := 0
	require.NoError(t, storage.conn.Raw(func(raw any) error {
		connection, ok := raw.(*sqlite3.SQLiteConn)
		require.True(t, ok)
		connection.RegisterAuthorizer(func(action int, name, value, _ string) int {
			if action == sqlite3.SQLITE_PRAGMA && name == "max_page_count" && value != "" {
				capWrites++
			}
			return sqlite3.SQLITE_OK
		})
		return nil
	}))
	require.NoError(t, storage.bootstrap(t.Context(), false))
	assert.Zero(t, capWrites)
	settings, err := storage.readSettings(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(-65536), settings.CacheSize)
	assert.Zero(t, settings.MmapSize)
	assert.Equal(t, int64(2), settings.Synchronous)
	metadata, err := storage.readMetadata(t.Context())
	require.NoError(t, err)
	config := testConfig(t)
	config.Policy.DatabaseLimit /= 2
	err = storage.configureExisting(
		t.Context(),
		filepath.Join(directory, "queue.sqlite3"),
		config,
		metadata,
	)
	assert.ErrorIs(t, err, ErrConfigMismatch)
	assert.Zero(t, capWrites)
}

func TestValidationMismatchBeforePhysicalCap(t *testing.T) {
	config := testConfig(t)
	directory := offlineFixture(t, config)
	path := filepath.Join(directory, "queue.sqlite3")
	original, err := os.ReadFile(path)
	require.NoError(t, err)
	config.Policy.DatabaseLimit /= 2
	opened, err := startStore(t.Context(), directory, config)
	require.Nil(t, opened)
	assert.ErrorIs(t, err, ErrConfigMismatch)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, original, after)
}

func TestValidationRejectsOversizedMetadataAndBlobs(t *testing.T) {
	for _, statement := range []string{
		"UPDATE meta SET value=zeroblob(65536) WHERE key=5",
		"UPDATE meta SET value=x'ffffffffffffffff' WHERE key=10",
		"UPDATE meta SET value=CAST(zeroblob(65536) AS TEXT) WHERE key=2",
		"UPDATE producers SET retired_through=zeroblob(65536)",
		"UPDATE producers SET producer=zeroblob(65536) WHERE has_retirement=1",
		"UPDATE receipts SET id=zeroblob(65536)",
		"UPDATE receipts SET accept_order=zeroblob(65536)",
		"UPDATE receipts SET digest=zeroblob(65536)",
		"UPDATE receipts SET ack_token=zeroblob(65536)",
	} {
		t.Run(statement, func(t *testing.T) {
			directory := offlineFixture(t, testConfig(t))
			opened, err := openOffline(t.Context(), directory)
			require.NoError(t, err)
			_, err = opened.storage.conn.ExecContext(t.Context(), "PRAGMA ignore_check_constraints=ON")
			require.NoError(t, err)
			_, err = opened.storage.conn.ExecContext(t.Context(), statement)
			require.NoError(t, err)
			// Call logical/metadata readers directly to exercise bounded projections even
			// when the integrity check would reject the malformed row first.
			if strings.Contains(statement, "UPDATE meta") {
				_, err = opened.storage.readMetadata(t.Context())
			} else {
				_, err = opened.storage.validateLogicalState(t.Context(), opened.metadata)
			}
			assert.ErrorIs(t, err, ErrStorage)
			require.NoError(t, opened.close())
		})
	}
}

func TestValidationQueryPlans(t *testing.T) {
	directory := offlineFixture(t, testConfig(t))
	opened, err := openOffline(t.Context(), directory)
	require.NoError(t, err)
	defer func() { assert.NoError(t, opened.close()) }()
	key := encodeID(ID{})
	for _, query := range []string{codecChunkQuery, payloadChunkQuery} {
		rows, err := opened.storage.conn.QueryContext(
			t.Context(),
			"EXPLAIN QUERY PLAN "+query,
			1,
			validationChunkBytes,
			key[:],
		)
		require.NoError(t, err)
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			t.Log(detail)
			assert.Contains(t, detail, "PRIMARY KEY")
			assert.NotContains(t, detail, "TEMP B-TREE")
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	for _, query := range []string{
		validationProducersQuery,
		validationReceiptsQuery,
	} {
		rows, err := opened.storage.conn.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query)
		require.NoError(t, err)
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			t.Log(detail)
			assert.NotContains(t, detail, "TEMP B-TREE")
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
}

func TestValidationCancellationBetweenChunks(t *testing.T) {
	storage, _ := newTestStore(t)
	_, err := storage.conn.ExecContext(
		t.Context(),
		"CREATE TEMP TABLE chunk_source(value BLOB)",
	)
	require.NoError(t, err)
	_, err = storage.conn.ExecContext(
		t.Context(),
		"INSERT INTO chunk_source VALUES(zeroblob(?))",
		2*validationChunkBytes+1,
	)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reads := 0
	require.NoError(t, storage.conn.Raw(func(raw any) error {
		connection, ok := raw.(*sqlite3.SQLiteConn)
		require.True(t, ok)
		connection.RegisterAuthorizer(func(action int, name, _, _ string) int {
			if action == sqlite3.SQLITE_READ && name == "chunk_source" {
				reads++
				if reads == 2 {
					cancel()
				}
			}
			return sqlite3.SQLITE_OK
		})
		return nil
	}))
	digest := sha256.New()
	err = storage.hashPayloadColumn(
		ctx,
		[]byte{1},
		"SELECT substr(value,?,?) FROM chunk_source WHERE ? IS NOT NULL",
		2*validationChunkBytes+1,
		digest,
	)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 2, reads)
	expected := sha256.Sum256(make([]byte, validationChunkBytes))
	assert.Equal(t, expected[:], digest.Sum(nil))
}
