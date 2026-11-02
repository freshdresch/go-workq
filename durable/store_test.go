package durable

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) (*store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.sqlite3")
	storage, err := connectStore(t.Context(), path, testConfig(t), true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, storage.close()) })
	require.NoError(t, storage.createSchema(t.Context()))
	return storage, path
}

func TestSettingsAndBuildEvidence(t *testing.T) {
	t.Parallel()
	storage, _ := newTestStore(t)
	actual, err := storage.readSettings(t.Context())
	require.NoError(t, err)
	assert.Equal(t, sqliteSettings{
		JournalMode: "wal", Synchronous: 2, AutoVacuum: 2, PageSize: 4096,
		CacheSize: -65536, MmapSize: 268435456, TempStore: 2,
		AutoCheckpoint: 0, BusyTimeout: 5000, MaxPageCount: 262144, AutomaticIndex: 0,
	}, actual)
	version, number, source := sqlite3.Version()
	assert.Equal(t, "3.53.4", version)
	assert.Equal(t, 3053004, number)
	t.Logf("driver=github.com/mattn/go-sqlite3@v1.14.52 SQLite=%s source=%s", version, source)
	t.Logf("effective pragmas: %+v", actual)
	rows, err := storage.conn.QueryContext(t.Context(), "PRAGMA compile_options")
	require.NoError(t, err)
	var options []string
	for rows.Next() {
		var option string
		require.NoError(t, rows.Scan(&option))
		options = append(options, option)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.NotEmpty(t, options)
	t.Logf("compile options: %v", options)
	autoCommit, err := storage.autoCommit()
	require.NoError(t, err)
	assert.True(t, autoCommit)
	_, err = storage.conn.ExecContext(t.Context(), "BEGIN IMMEDIATE")
	require.NoError(t, err)
	autoCommit, err = storage.autoCommit()
	require.NoError(t, err)
	assert.False(t, autoCommit)
	_, err = storage.conn.ExecContext(t.Context(), "ROLLBACK")
	require.NoError(t, err)
}

func TestSchemaUnsignedOrderingAndReopen(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "queue.sqlite3")
	cfg := testConfig(t)
	storage, err := connectStore(t.Context(), path, cfg, true)
	require.NoError(t, err)
	defer func() {
		if storage != nil {
			assert.NoError(t, storage.close())
		}
	}()
	require.NoError(t, storage.createSchema(t.Context()))
	sequences := []uint64{0, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64}
	for _, sequence := range sequences {
		id := ID{Producer: [16]byte{0xff}, Sequence: sequence}
		key := encodeID(id)
		order := encodeUint64(sequence)
		digest := entryDigest("", nil)
		_, err = storage.conn.ExecContext(
			t.Context(),
			"INSERT INTO receipts VALUES(?,?,0,?,NULL)",
			key[:], digest[:], order[:],
		)
		require.NoError(t, err)
		_, err = storage.conn.ExecContext(
			t.Context(),
			"INSERT INTO payloads VALUES(?,?,?,?)",
			key[:], []byte{}, []byte{}, 0,
		)
		require.NoError(t, err)
	}
	require.NoError(t, storage.close())
	storage = nil
	storage, err = connectStore(t.Context(), path, cfg, false)
	require.NoError(t, err)
	rows, err := storage.conn.QueryContext(t.Context(), "SELECT id FROM receipts ORDER BY id")
	require.NoError(t, err)
	var actual []uint64
	for rows.Next() {
		var encoded []byte
		require.NoError(t, rows.Scan(&encoded))
		id, err := decodeID(encoded)
		require.NoError(t, err)
		actual = append(actual, id.Sequence)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	assert.Equal(t, sequences, actual)
	var tableCount, withoutRowID int
	err = storage.conn.QueryRowContext(t.Context(), `
 SELECT count(*),sum(wr) FROM pragma_table_list
 WHERE schema='main' AND name IN ('meta','producers','receipts','payloads')`).Scan(
		&tableCount, &withoutRowID,
	)
	require.NoError(t, err)
	assert.Equal(t, 4, tableCount)
	assert.Equal(t, 4, withoutRowID)
	var indexCount int
	err = storage.conn.QueryRowContext(t.Context(), `
 SELECT count(*) FROM sqlite_schema WHERE type='index'
 AND name IN ('pending_order','pending_producer','cleanup_producers')`).Scan(&indexCount)
	require.NoError(t, err)
	assert.Equal(t, 3, indexCount)
}

func TestSchemaRejectsInvalidRecords(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		query string
	}{
		{"metadata key", "INSERT INTO meta VALUES(16,zeroblob(8))"},
		{"metadata length", "INSERT INTO meta VALUES(1,zeroblob(7))"},
		{"metadata type", "INSERT INTO meta VALUES(1,'12345678')"},
		{"policy length", "INSERT INTO meta VALUES(6,zeroblob(64))"},
		{"producer type", "INSERT INTO producers VALUES('0123456789abcdef',0,zeroblob(8),0)"},
		{"producer flag", "INSERT INTO producers VALUES(zeroblob(16),2,zeroblob(8),0)"},
		{"floor length", "INSERT INTO producers VALUES(zeroblob(16),0,zeroblob(7),0)"},
		{"cleanup flag", "INSERT INTO producers VALUES(zeroblob(16),0,zeroblob(8),2)"},
		{"pending token", `INSERT INTO receipts
 VALUES(zeroblob(24),zeroblob(32),0,zeroblob(8),zeroblob(56))`},
		{"ack missing token", `INSERT INTO receipts
 VALUES(zeroblob(24),zeroblob(32),1,zeroblob(8),NULL)`},
		{"ack token type", `INSERT INTO receipts
 VALUES(zeroblob(24),zeroblob(32),1,zeroblob(8),printf('%056d',0))`},
		{"receipt state", `INSERT INTO receipts
 VALUES(zeroblob(24),zeroblob(32),2,zeroblob(8),NULL)`},
		{"digest length", `INSERT INTO receipts
 VALUES(zeroblob(24),zeroblob(31),0,zeroblob(8),NULL)`},
		{"payload bound", `INSERT INTO payloads
 VALUES(zeroblob(24),zeroblob(0),zeroblob(16385),16385)`},
		{"codec bound", `INSERT INTO payloads
 VALUES(zeroblob(24),zeroblob(65),zeroblob(0),0)`},
		{"payload length", `INSERT INTO payloads
 VALUES(zeroblob(24),zeroblob(0),zeroblob(1),0)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			storage, _ := newTestStore(t)
			_, err := storage.conn.ExecContext(t.Context(), tc.query)
			var sqliteErr sqlite3.Error
			require.ErrorAs(t, err, &sqliteErr)
			assert.Equal(t, sqlite3.ErrConstraint, sqliteErr.Code)
		})
	}
}

func TestConnectionRejectsMissingExistingStore(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "missing.sqlite3")
	storage, err := connectStore(t.Context(), path, testConfig(t), false)
	assert.Nil(t, storage)
	assert.ErrorIs(t, err, ErrStorage)
	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist)
	//nolint:staticcheck // The contract explicitly rejects nil contexts.
	storage, err = connectStore(nil, path, testConfig(t), true)
	assert.Nil(t, storage)
	assert.ErrorIs(t, err, ErrInvalidInput)
}

func TestSQLiteEffectiveUnitConversions(t *testing.T) {
	t.Parallel()
	for _, timeout := range []time.Duration{time.Nanosecond, time.Millisecond + time.Nanosecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Runtime.SQLite.Cache = (8 << 20) + 1023
			cfg.Runtime.SQLite.BusyTimeout = timeout
			cfg.Runtime.SQLite.Mmap = 0
			path := filepath.Join(t.TempDir(), "queue.sqlite3")
			storage, err := connectStore(t.Context(), path, cfg, true)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, storage.close()) })
			settings, err := storage.readSettings(t.Context())
			require.NoError(t, err)
			assert.Equal(t, int64(-8192), settings.CacheSize)
			assert.Equal(t, int64(timeout/time.Millisecond), settings.BusyTimeout)
			assert.Zero(t, settings.MmapSize)
		})
	}
}

func TestSchemaValidationBoundsGoAllocation(t *testing.T) {
	// Keep this test sequential because TotalAlloc includes all Go goroutines.
	storage, _ := newTestStore(t)
	statement := "CREATE TABLE z_extra (x TEXT DEFAULT '" + strings.Repeat("x", 1<<20) + "')"
	_, err := storage.conn.ExecContext(t.Context(), statement)
	require.NoError(t, err)
	var length int
	err = storage.conn.QueryRowContext(t.Context(),
		"SELECT length(CAST(sql AS BLOB)) FROM sqlite_schema WHERE name='z_extra'").Scan(&length)
	require.NoError(t, err)
	require.Greater(t, length, 1<<20)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = storage.validateSchema(t.Context())
	runtime.ReadMemStats(&after)
	assert.ErrorIs(t, err, ErrUnsupported)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("SQL bytes=%d, Go allocation bytes=%d", length, allocated)
	// This catches full SQL transfer/copies, not native SQLite schema allocations.
	assert.Less(t, allocated, uint64(256<<10))
}

func TestStoreLookupCancellationAndStorageCategories(t *testing.T) {
	t.Parallel()
	storage, _ := newTestStore(t)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, cancelDeadline := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	for _, ctx := range []context.Context{canceled, expired} {
		statuses, err := storage.lookup(ctx, []ID{{Sequence: 1}})
		assert.Nil(t, statuses)
		assert.ErrorIs(t, err, ctx.Err())
		assert.NotErrorIs(t, err, ErrStorage)
	}
	_, err := storage.conn.ExecContext(t.Context(), "DROP TABLE producers")
	require.NoError(t, err)
	statuses, err := storage.lookup(t.Context(), []ID{{Sequence: 1}})
	assert.Nil(t, statuses)
	assert.ErrorIs(t, err, ErrStorage)
	var sqliteErr sqlite3.Error
	assert.ErrorAs(t, err, &sqliteErr)
}

func TestIntegrityDiagnosticTransferBound(t *testing.T) {
	// Keep process allocation sampling isolated from this package's parallel tests.
	storage, _ := newTestStore(t)
	require.NoError(t, storage.validateSchema(t.Context()))
	name := "extra_" + strings.Repeat("x", 1<<20)
	_, err := storage.conn.ExecContext(
		t.Context(),
		`CREATE TABLE "`+name+`" (x INTEGER CHECK(x>0))`,
	)
	require.NoError(t, err)
	require.ErrorIs(t, storage.validateSchema(t.Context()), ErrUnsupported)
	_, err = storage.conn.ExecContext(t.Context(), "PRAGMA ignore_check_constraints=ON")
	require.NoError(t, err)
	_, err = storage.conn.ExecContext(t.Context(), `INSERT INTO "`+name+`" VALUES(-1)`)
	require.NoError(t, err)
	_, err = storage.conn.ExecContext(t.Context(), "PRAGMA ignore_check_constraints=OFF")
	require.NoError(t, err)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = storage.validateSchema(t.Context())
	runtime.ReadMemStats(&after)
	assert.ErrorIs(t, err, ErrStorage)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("integrity diagnostic Go allocation bytes=%d", allocated)
	assert.Less(t, allocated, uint64(256<<10))
}

func TestSetupSQLCancellationCategory(t *testing.T) {
	for _, operation := range []string{"configure", "read-settings", "schema", "receipt-producer"} {
		t.Run(operation, func(t *testing.T) {
			config := testConfig(t)
			directory := offlineFixture(t, config)
			opened, err := openOffline(t.Context(), directory)
			require.NoError(t, err)
			defer func() { assert.NoError(t, opened.close()) }()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			matched := 0
			require.NoError(t, opened.storage.conn.Raw(func(raw any) error {
				connection, ok := raw.(*sqlite3.SQLiteConn)
				require.True(t, ok)
				connection.RegisterAuthorizer(func(action int, name, column, _ string) int {
					if operation == "receipt-producer" {
						if action != sqlite3.SQLITE_READ || name != "producers" || column != "has_retirement" {
							return sqlite3.SQLITE_OK
						}
						matched++
						if matched != 2 {
							return sqlite3.SQLITE_OK
						}
					} else {
						matched++
					}
					cancel()
					return sqlite3.SQLITE_OK
				})
				return nil
			}))
			switch operation {
			case "configure":
				err = opened.storage.configure(
					ctx,
					filepath.Join(directory, "queue.sqlite3"),
					config,
					false,
				)
			case "read-settings":
				_, err = opened.storage.readSettings(ctx)
			case "schema":
				err = opened.storage.validateSchema(ctx)
			case "receipt-producer":
				_, err = opened.storage.validateLogicalState(ctx, opened.metadata)
			}
			require.Positive(t, matched)
			assert.ErrorIs(t, err, context.Canceled)
			assert.NotErrorIs(t, err, ErrStorage)
		})
	}
}
