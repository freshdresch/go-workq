//go:build linux && amd64 && cgo

package inventory

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func inventoryPath(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	var filesystem syscall.Statfs_t
	require.NoError(t, syscall.Statfs(directory, &filesystem))
	if filesystem.Type != 0xef53 {
		t.Skip("inventory storage tests require local ext4")
	}
	return filepath.Join(directory, "inventory ?#.db")
}

func openInventory(t *testing.T, path string, limit int) *Store {
	t.Helper()
	store, err := Open(t.Context(), path, limit)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	return store
}

func testRequest(sequence uint64, delta int64) Request {
	return Request{
		Key: Key{Namespace: "test", Producer: [16]byte{1}, Sequence: sequence,
			Effect: "inventory.adjust"},
		Codec:   Codec,
		Payload: []byte(fmt.Sprintf(`{"item":"widget","delta":%d}`, delta)),
	}
}

// assertDatabase uses separate SQL reads rather than Store results as the durable oracle.
func assertDatabase(t *testing.T, path string, want Inventory, requests ...Request) {
	t.Helper()
	address := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite3", address.String())
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	var got Inventory
	require.NoError(t, db.QueryRow(
		"SELECT quantity, adjustments FROM inventory WHERE item = 'widget'",
	).Scan(&got.Quantity, &got.Adjustments))
	assert.Equal(t, want, got)
	var count, incomplete int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM receipts").Scan(&count))
	require.NoError(t, db.QueryRow(
		"SELECT count(*) FROM receipts WHERE result_quantity IS NULL",
	).Scan(&incomplete))
	assert.Equal(t, len(requests), count)
	assert.Zero(t, incomplete)
	for _, request := range requests {
		var version int
		var namespace, effect, codec string
		var producer, sequence, payload []byte
		var quantity int64
		require.NoError(t, db.QueryRow(`SELECT version, namespace, producer, sequence,
   effect, codec, request, result_quantity FROM receipts WHERE `+receiptWhere,
			keyArguments(request.Key)...,
		).Scan(&version, &namespace, &producer, &sequence, &effect, &codec, &payload, &quantity))
		assert.Equal(t, keyVersion, version)
		assert.Equal(t, request.Key.Namespace, namespace)
		assert.Equal(t, request.Key.Producer[:], producer)
		assert.Equal(t, binary.BigEndian.AppendUint64(nil, request.Key.Sequence), sequence)
		assert.Equal(t, request.Key.Effect, effect)
		assert.Equal(t, request.Codec, codec)
		assert.Equal(t, request.Payload, payload)
		assert.GreaterOrEqual(t, quantity, int64(0))
	}
}

func TestStoreInitializationAndReopen(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 3)
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
	assert.Equal(t, 1, store.db.Stats().MaxOpenConnections)
	for _, setting := range []struct {
		name string
		want string
	}{
		{"journal_mode", "wal"}, {"synchronous", "2"}, {"busy_timeout", "250"},
		{"foreign_keys", "1"},
	} {
		var got string
		require.NoError(t, store.conn.QueryRowContext(
			t.Context(), "PRAGMA "+setting.name,
		).Scan(&got))
		assert.Equal(t, setting.want, got)
	}
	assertDatabase(t, path, Inventory{Quantity: 10})
	first := testRequest(math.MaxUint64, 3)
	result, err := store.Apply(t.Context(), first)
	require.NoError(t, err)
	assert.False(t, result.Duplicate)
	assert.Equal(t, int64(13), result.Quantity)
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
	reopened := openInventory(t, path, 3)
	snapshot, err := reopened.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, Inventory{Quantity: 13, Adjustments: 1}, snapshot)
	receipt, found, err := reopened.Lookup(t.Context(), first.Key)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, result, receipt.Result)
	assert.Equal(t, first.Payload, receipt.Payload)
	assertDatabase(t, path, snapshot, first)
	require.NoError(t, reopened.Close())
	mismatch, err := Open(t.Context(), path, 4)
	assert.Error(t, err)
	assert.Nil(t, mismatch)
}

func TestStoreDuplicateKeepsOriginalResult(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 2)
	first, second := testRequest(1, 3), testRequest(2, 3)
	original, err := store.Apply(t.Context(), first)
	require.NoError(t, err)
	later, err := store.Apply(t.Context(), second)
	require.NoError(t, err)
	assert.Equal(t, int64(16), later.Quantity)
	duplicate, err := store.Apply(t.Context(), first)
	require.NoError(t, err)
	assert.True(t, duplicate.Duplicate)
	duplicate.Duplicate = false
	assert.Equal(t, original, duplicate)
	for _, changed := range []Request{
		{Key: first.Key, Codec: "wrong", Payload: first.Payload},
		{Key: first.Key, Codec: first.Codec, Payload: []byte("invalid JSON")},
		{Key: first.Key, Codec: first.Codec, Payload: append([]byte(" "), first.Payload...)},
	} {
		result, err := store.Apply(t.Context(), changed)
		assert.ErrorIs(t, err, ErrConflict)
		assert.Zero(t, result)
	}
	result, err := store.Apply(t.Context(), testRequest(3, 1))
	assert.ErrorIs(t, err, ErrCapacity)
	assert.Zero(t, result)
	receipt, found, err := store.Lookup(t.Context(), first.Key)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, original, receipt.Result)
	receipt.Payload[0] = '!'
	receipt, found, err = store.Lookup(t.Context(), first.Key)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, first.Payload, receipt.Payload)
	_, found, err = store.Lookup(t.Context(), testRequest(3, 1).Key)
	require.NoError(t, err)
	assert.False(t, found)
	assertDatabase(t, path, Inventory{Quantity: 16, Adjustments: 2}, first, second)
}

func TestStoreRejectedRequestsRollback(t *testing.T) {
	for _, test := range []struct {
		name    string
		request Request
		setup   string
	}{
		{name: "negative result", request: testRequest(1, -11)},
		{name: "quantity overflow", request: testRequest(1, math.MaxInt64)},
		{name: "minimum delta", request: testRequest(1, math.MinInt64)},
		{name: "count overflow", request: testRequest(1, 1),
			setup: "UPDATE inventory SET adjustments = 9223372036854775807"},
		{name: "unknown item", request: Request{Key: testRequest(1, 1).Key, Codec: Codec,
			Payload: []byte(`{"item":"missing","delta":1}`)}},
		{name: "payload bound", request: Request{Key: testRequest(1, 1).Key, Codec: Codec,
			Payload: []byte(strings.Repeat("x", MaxPayload+1))}},
		{name: "codec bound", request: Request{Key: testRequest(1, 1).Key,
			Codec: strings.Repeat("x", MaxCodec+1)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			store := openInventory(t, path, 1)
			want := Inventory{Quantity: 10}
			if test.setup != "" {
				_, err := store.conn.ExecContext(t.Context(), test.setup)
				require.NoError(t, err)
				want.Adjustments = math.MaxInt64
			}
			result, err := store.Apply(t.Context(), test.request)
			assert.ErrorIs(t, err, ErrRejected)
			assert.Zero(t, result)
			assertDatabase(t, path, want)
		})
	}
}

func TestStoreMissingItemDoesNotLeaveReceipt(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 1)
	_, err := store.conn.ExecContext(t.Context(), "DELETE FROM inventory")
	require.NoError(t, err)
	_, err = store.Apply(t.Context(), testRequest(1, 1))
	assert.ErrorIs(t, err, ErrRejected)
	var count int
	require.NoError(t, store.conn.QueryRowContext(
		t.Context(), "SELECT count(*) FROM receipts",
	).Scan(&count))
	assert.Zero(t, count)
	_, err = store.conn.ExecContext(t.Context(), "INSERT INTO inventory VALUES ('widget', 10, 0)")
	require.NoError(t, err)
	_, err = store.Apply(t.Context(), testRequest(1, 1))
	require.NoError(t, err)
	assertDatabase(t, path, Inventory{Quantity: 11, Adjustments: 1}, testRequest(1, 1))
}

func TestStoreReceiptFailureRollsBackBusinessUpdate(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 1)
	_, err := store.conn.ExecContext(t.Context(), `CREATE TRIGGER reject_result
  BEFORE UPDATE ON receipts BEGIN SELECT RAISE(ABORT, 'test rejection'); END`)
	require.NoError(t, err)
	_, err = store.Apply(t.Context(), testRequest(1, 3))
	assert.ErrorIs(t, err, ErrUnresolved)
	assertDatabase(t, path, Inventory{Quantity: 10})
	_, err = store.conn.ExecContext(t.Context(), "DROP TRIGGER reject_result")
	require.NoError(t, err)
	_, err = store.Apply(t.Context(), testRequest(1, 3))
	require.NoError(t, err)
	assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, testRequest(1, 3))
}

func TestStoreCommitFailureFencesReuse(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 2)
	_, err := store.conn.ExecContext(t.Context(), `
 CREATE TABLE guard_parent(id INTEGER PRIMARY KEY);
 CREATE TABLE guard_child(parent_id INTEGER REFERENCES guard_parent(id)
  DEFERRABLE INITIALLY DEFERRED);
 CREATE TRIGGER reject_commit AFTER UPDATE ON inventory
 BEGIN INSERT INTO guard_child VALUES (1); END;`)
	require.NoError(t, err)
	_, err = store.Apply(t.Context(), testRequest(1, 3))
	assert.ErrorIs(t, err, ErrUnresolved)
	_, err = store.Apply(t.Context(), testRequest(2, 3))
	assert.ErrorIs(t, err, ErrUnresolved)
	receipt, found, err := store.Lookup(t.Context(), testRequest(1, 3).Key)
	assert.ErrorIs(t, err, ErrUnresolved)
	assert.False(t, found)
	assert.Zero(t, receipt)
	snapshot, err := store.Snapshot(t.Context())
	assert.ErrorIs(t, err, ErrUnresolved)
	assert.Zero(t, snapshot)
	require.NoError(t, store.Close())
	assertDatabase(t, path, Inventory{Quantity: 10})
	reopened := openInventory(t, path, 2)
	_, err = reopened.conn.ExecContext(t.Context(), "DROP TRIGGER reject_commit")
	require.NoError(t, err)
	_, err = reopened.Apply(t.Context(), testRequest(1, 3))
	require.NoError(t, err)
	assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, testRequest(1, 3))
}

func TestStoreRejectsForeignAndDamagedFiles(t *testing.T) {
	for _, test := range []struct{ name, sql string }{
		{name: "empty"},
		{name: "foreign", sql: "CREATE TABLE foreign_table(value TEXT)"},
		{name: "missing tables", sql: `CREATE TABLE metadata(singleton, version, receipt_limit);
   INSERT INTO metadata VALUES (1, 1, 2)`},
		{name: "unknown version", sql: `CREATE TABLE metadata(singleton, version, receipt_limit);
   INSERT INTO metadata VALUES (1, 99, 2)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			require.NoError(t, os.WriteFile(path, nil, 0600))
			if test.sql != "" {
				address := url.URL{Scheme: "file", Path: path}
				db, err := sql.Open("sqlite3", address.String())
				require.NoError(t, err)
				_, err = db.Exec(test.sql)
				require.NoError(t, err)
				require.NoError(t, db.Close())
			}
			store, err := Open(t.Context(), path, 2)
			assert.Error(t, err)
			assert.Nil(t, store)
			_, err = os.Stat(path)
			assert.NoError(t, err, "rejected files must remain for inspection")
		})
	}
}

func TestStoreCancellationAndClosed(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := store.Apply(ctx, testRequest(1, 1))
	assert.ErrorIs(t, err, context.Canceled)
	assertDatabase(t, path, Inventory{Quantity: 10})
	require.NoError(t, store.Close())
	_, err = store.Apply(t.Context(), testRequest(1, 1))
	assert.ErrorIs(t, err, ErrUnresolved)
}

func TestStoreRejectsChangedTableDefinitions(t *testing.T) {
	for _, test := range []struct{ name, original, replacement string }{
		{
			name:        "case-insensitive namespace",
			original:    "namespace TEXT NOT NULL",
			replacement: "namespace TEXT COLLATE NOCASE NOT NULL",
		},
		{
			name:     "missing receipt primary key",
			original: ",\n PRIMARY KEY(version, namespace, producer, sequence, effect)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			address := url.URL{Scheme: "file", Path: path}
			db, err := sql.Open("sqlite3", address.String())
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, db.Close()) })
			foreign := strings.Replace(schema, test.original, test.replacement, 1)
			require.NotEqual(t, schema, foreign)
			_, err = db.Exec(foreign)
			require.NoError(t, err)
			_, err = db.Exec(`INSERT INTO metadata VALUES (1, 1, 2);
    INSERT INTO inventory VALUES ('widget', 10, 0)`)
			require.NoError(t, err)
			require.NoError(t, db.Close())

			store, err := Open(t.Context(), path, 2)
			if store != nil {
				require.NoError(t, store.Close())
			}
			assert.Error(t, err, "changed identity semantics must not be admitted")
			assert.Nil(t, store)
			assertDatabase(t, path, Inventory{Quantity: 10})
		})
	}
}

func TestStoreRollbackFailureDoesNotReportRejection(t *testing.T) {
	for _, cause := range []error{ErrConflict, ErrRejected, ErrCapacity} {
		t.Run(cause.Error(), func(t *testing.T) {
			t.Parallel()
			store := openInventory(t, inventoryPath(t), 2)
			tx, err := store.conn.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())
			store.opMu.Lock()
			cleanupErr := store.rollback(tx, cause)
			store.opMu.Unlock()
			_, applyErr := store.Apply(t.Context(), testRequest(1, 1))
			_, found, lookupErr := store.Lookup(t.Context(), testRequest(1, 1).Key)
			assert.False(t, found)
			for _, observed := range []error{cleanupErr, applyErr, lookupErr} {
				assert.ErrorIs(t, observed, ErrUnresolved)
				assert.ErrorIs(t, observed, sql.ErrTxDone)
				assert.NotErrorIs(t, observed, ErrConflict)
				assert.NotErrorIs(t, observed, ErrRejected)
				assert.NotErrorIs(t, observed, ErrCapacity)
			}
		})
	}
}
