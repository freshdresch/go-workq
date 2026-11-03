//go:build linux && amd64 && cgo

package inventory

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The concrete embedding preserves SQLite's optional connection and rows interfaces.
type cancellationConnector struct {
	dsn   string
	conn  *cancellationConn
	opens atomic.Int64
}

func (c *cancellationConnector) Driver() driver.Driver { return &sqlite3.SQLiteDriver{} }

func (c *cancellationConnector) Connect(context.Context) (driver.Conn, error) {
	native, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	conn, ok := native.(*sqlite3.SQLiteConn)
	if !ok {
		return nil, errors.Join(errors.New("unexpected SQLite connection type"), native.Close())
	}
	c.opens.Add(1)
	c.conn = &cancellationConn{SQLiteConn: conn}
	return c.conn, nil
}

type cancellationConn struct {
	*sqlite3.SQLiteConn
	beginEntered, beginRelease chan struct{}
	beginErr                   error
	closeEntered, closeRelease chan struct{}
	closes                     atomic.Int64
	commits                    atomic.Int64
	rollbacks                  atomic.Int64
	finalExec                  string
	cancelExec                 context.CancelFunc
	finalQuery                 string
	cancelRows                 context.CancelFunc
	rowsClosed                 atomic.Int64
}

func (c *cancellationConn) BeginTx(
	ctx context.Context,
	options driver.TxOptions,
) (driver.Tx, error) {
	if c.beginErr != nil {
		return nil, c.beginErr
	}
	tx, err := c.SQLiteConn.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	if c.beginEntered != nil {
		close(c.beginEntered)
		<-c.beginRelease
	}
	return &cancellationTx{Tx: tx, conn: c}, nil
}

func (c *cancellationConn) Close() error {
	if c.closeEntered != nil {
		close(c.closeEntered)
		<-c.closeRelease
	}
	err := c.SQLiteConn.Close()
	c.closes.Add(1)
	return err
}

type cancellationTx struct {
	driver.Tx
	conn *cancellationConn
}

func (tx *cancellationTx) Commit() error {
	err := tx.Tx.Commit()
	tx.conn.commits.Add(1)
	return err
}

func (tx *cancellationTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.conn.rollbacks.Add(1)
	return err
}

func (c *cancellationConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	result, err := c.SQLiteConn.ExecContext(ctx, query, args)
	if err == nil && query == c.finalExec {
		c.cancelExec()
	}
	return result, err
}

func (c *cancellationConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	rows, err := c.SQLiteConn.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if query != c.finalQuery {
		return rows, nil
	}
	native, ok := rows.(*sqlite3.SQLiteRows)
	if !ok {
		return nil, errors.Join(errors.New("unexpected SQLite rows type"), rows.Close())
	}
	return &cancellationRows{SQLiteRows: native, conn: c}, nil
}

type cancellationRows struct {
	*sqlite3.SQLiteRows
	conn *cancellationConn
}

func (r *cancellationRows) Close() error {
	err := r.SQLiteRows.Close()
	if err == nil && r.conn.rowsClosed.Add(1) == 1 {
		r.conn.cancelRows()
	}
	return err
}

func unpublishedCancellationStore(t *testing.T, path string) (*Store, *cancellationConnector) {
	t.Helper()
	address := url.URL{Scheme: "file", Path: path}
	address.RawQuery = url.Values{
		"_journal_mode": {"WAL"}, "_synchronous": {"FULL"},
		"_busy_timeout": {"250"}, "_foreign_keys": {"on"}, "_txlock": {"immediate"},
	}.Encode()
	connector := &cancellationConnector{dsn: address.String()}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	store := &Store{db: db, conn: conn, receiptLimit: 2, failed: make(chan struct{})}
	// ErrBadConn discards the dedicated connection before owner Close.
	t.Cleanup(func() {
		err := store.Close()
		if errors.Is(connector.conn.beginErr, driver.ErrBadConn) {
			assert.ErrorIs(t, err, sql.ErrConnDone)
			return
		}
		assert.NoError(t, err)
	})
	return store, connector
}

func openCancellationStore(t *testing.T, path string) (*Store, *cancellationConnector) {
	t.Helper()
	seed := openInventory(t, path, 2)
	require.NoError(t, seed.Close())
	store, connector := unpublishedCancellationStore(t, path)
	require.NoError(t, store.configureAndInitialize(t.Context(), false))
	connector.conn.commits.Store(0)
	return store, connector
}

func TestStoreCancellationAfterBeginPreventsBusinessSQL(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store, connector := openCancellationStore(t, path)
	native := connector.conn
	native.beginEntered = make(chan struct{})
	native.beginRelease = make(chan struct{})
	release := sync.OnceFunc(func() { close(native.beginRelease) })
	defer release()
	var businessSQL atomic.Int64
	native.RegisterAuthorizer(func(action int, _, _, _ string) int {
		if action != sqlite3.SQLITE_TRANSACTION {
			businessSQL.Add(1)
		}
		return sqlite3.SQLITE_OK
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var result Result
	owner := healthCall(t, func() error {
		var err error
		result, err = store.Apply(ctx, testRequest(1, 3))
		return err
	})
	healthSignal(t, native.beginEntered)
	cancel()
	release()
	err := healthResult(t, owner)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, ErrUnresolved)
	assert.Zero(t, result)
	assert.Zero(t, businessSQL.Load())
	assert.EqualValues(t, 1, native.rollbacks.Load())
	assert.Zero(t, native.commits.Load())
	assertHealthyCancellationStore(t, store)
	require.NoError(t, store.Close())
	assertDatabase(t, path, Inventory{Quantity: 10})
}

func assertHealthyCancellationStore(t *testing.T, store *Store) {
	t.Helper()
	select {
	case <-store.Failed():
		t.Error("successful cancellation cleanup poisoned Store")
	default:
	}
}

func TestStoreCancellationAfterCommitRetainsSuccess(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 2)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store.stageHook = func(stage applyStage) {
		if stage == destinationCommitSucceeded {
			cancel()
		}
	}
	request := testRequest(1, 3)
	result, err := store.Apply(ctx, request)
	require.NoError(t, err)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	encoded, err := request.Key.Encode()
	require.NoError(t, err)
	assert.Equal(t, Result{AdjustmentID: encoded, Quantity: 13}, result)
	assertHealthyCancellationStore(t, store)
	require.NoError(t, store.Close())
	assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, request)
}

func TestStoreCancellationAfterFinalRowsClose(t *testing.T) {
	for _, operation := range []string{"lookup", "snapshot"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			store, connector := openCancellationStore(t, path)
			request := testRequest(1, 3)
			_, err := store.Apply(t.Context(), request)
			require.NoError(t, err)
			native := connector.conn
			native.commits.Store(0)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			native.cancelRows = cancel
			if operation == "lookup" {
				native.finalQuery = "SELECT codec, request, result_quantity FROM receipts WHERE " + receiptWhere
				receipt, found, err := store.Lookup(ctx, request.Key)
				assert.ErrorIs(t, err, context.Canceled)
				assert.Zero(t, receipt)
				assert.False(t, found)
			} else {
				native.finalQuery = "SELECT quantity, adjustments FROM inventory WHERE item = 'widget'"
				snapshot, err := store.Snapshot(ctx)
				assert.ErrorIs(t, err, context.Canceled)
				assert.Zero(t, snapshot)
			}
			assert.EqualValues(t, 1, native.rowsClosed.Load())
			assert.EqualValues(t, 1, native.rollbacks.Load())
			assert.Zero(t, native.commits.Load())
			assertHealthyCancellationStore(t, store)
			require.NoError(t, store.Close())
			assert.EqualValues(t, 1, native.closes.Load())
			assert.Zero(t, store.db.Stats().OpenConnections)
			assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, request)
		})
	}
}

func TestStoreCancellationInitializationFinalRowsClose(t *testing.T) {
	for _, created := range []bool{true, false} {
		name := "validate"
		if created {
			name = "create"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			if !created {
				seed := openInventory(t, path, 2)
				_, err := seed.Apply(t.Context(), testRequest(1, 3))
				require.NoError(t, err)
				require.NoError(t, seed.Close())
			}
			store, connector := unpublishedCancellationStore(t, path)
			native := connector.conn
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			native.finalQuery = "SELECT count(*) FROM receipts WHERE result_quantity IS NULL"
			native.cancelRows = cancel
			initialize := func() error {
				if err := store.configureAndInitialize(ctx, created); err != nil {
					return errors.Join(err, store.Close())
				}
				return nil
			}
			assert.ErrorIs(t, initialize(), context.Canceled)
			assert.EqualValues(t, 1, native.rowsClosed.Load())
			assert.EqualValues(t, 1, native.rollbacks.Load())
			assert.Zero(t, native.commits.Load())
			assert.EqualValues(t, 1, native.closes.Load())
			assert.Zero(t, store.db.Stats().OpenConnections)
			assert.ErrorIs(t, store.conn.Raw(func(any) error { return nil }), sql.ErrConnDone)
			if !created {
				assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, testRequest(1, 3))
				return
			}
			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			defer func() { assert.NoError(t, db.Close()) }()
			var tables int
			err = db.QueryRowContext(t.Context(),
				"SELECT count(*) FROM sqlite_schema WHERE type = 'table'").Scan(&tables)
			require.NoError(t, err)
			assert.Zero(t, tables, "canceled creation must roll back its schema")
		})
	}
}

func TestStoreBeginBusyRemainsHealthyForRetry(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 2)
	writer := openInventory(t, path, 2)
	tx, err := writer.conn.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() {
		if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
			assert.NoError(t, err)
		}
	}()
	request := testRequest(1, 3)
	result, err := store.Apply(t.Context(), request)
	assert.ErrorIs(t, err, ErrUnresolved)
	var sqliteErr sqlite3.Error
	require.ErrorAs(t, err, &sqliteErr)
	assert.Equal(t, sqlite3.ErrBusy, sqliteErr.Code)
	assert.Zero(t, result)
	assertHealthyCancellationStore(t, store)
	require.NoError(t, tx.Rollback())
	result, err = store.Apply(t.Context(), request)
	require.NoError(t, err)
	assert.False(t, result.Duplicate)
	assert.EqualValues(t, 13, result.Quantity)
	require.NoError(t, store.Close())
	require.NoError(t, writer.Close())
	assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, request)
}

func TestStoreBeginBadConnCleanupPrecedesFailure(t *testing.T) {
	path := inventoryPath(t)
	store, connector := openCancellationStore(t, path)
	native := connector.conn
	var statements atomic.Int64
	native.RegisterAuthorizer(func(int, string, string, string) int {
		statements.Add(1)
		return sqlite3.SQLITE_OK
	})
	synctest.Test(t, func(t *testing.T) {
		native.beginErr = driver.ErrBadConn
		native.closeEntered = make(chan struct{})
		native.closeRelease = make(chan struct{})
		release := sync.OnceFunc(func() { close(native.closeRelease) })
		defer release()
		done := make(chan struct{})
		var result Result
		var applyErr error
		go func() {
			result, applyErr = store.Apply(t.Context(), testRequest(1, 3))
			close(done)
		}()
		defer func() { release(); <-done }()
		<-native.closeEntered
		synctest.Wait()
		select {
		case <-done:
			t.Error("Apply returned before native discard completed")
		default:
		}
		assert.Zero(t, native.closes.Load())
		release()
		synctest.Wait()
		<-done
		assert.Zero(t, result)
		assert.ErrorIs(t, applyErr, driver.ErrBadConn)
		assert.ErrorIs(t, applyErr, ErrUnresolved)
		assert.EqualValues(t, 1, native.closes.Load())
	})
	healthSignal(t, store.Failed())
	for _, operation := range healthOperations(t, store) {
		assert.ErrorIs(t, operation.call(t.Context()), driver.ErrBadConn, operation.name)
	}
	assert.Zero(t, statements.Load())
	assert.EqualValues(t, 1, connector.opens.Load())
	assert.Zero(t, store.db.Stats().OpenConnections)
	assert.ErrorIs(t, store.Close(), sql.ErrConnDone)
	assertDatabase(t, path, Inventory{Quantity: 10})
}

func TestStoreCancellationDuringStatementJoinsCleanup(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store, connector := openCancellationStore(t, path)
	native := connector.conn
	entered, release, callbackDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseStatement := sync.OnceFunc(func() { close(release) })
	defer releaseStatement()
	require.NoError(t, native.RegisterFunc("hold_inventory_update", func() int {
		close(entered)
		<-release
		close(callbackDone)
		return 0
	}, false))
	_, err := store.conn.ExecContext(t.Context(), `CREATE TRIGGER hold_update
 AFTER UPDATE ON inventory BEGIN SELECT hold_inventory_update(); END`)
	require.NoError(t, err)
	var rollbackAfterCallback atomic.Bool
	native.RegisterRollbackHook(func() {
		select {
		case <-callbackDone:
			rollbackAfterCallback.Store(true)
		default:
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var result Result
	var applyErr error
	owner := healthCall(t, func() error {
		result, applyErr = store.Apply(ctx, testRequest(1, 3))
		return store.Close()
	})
	healthSignal(t, entered)
	cancel()
	releaseStatement()
	require.NoError(t, healthResult(t, owner))
	assert.Zero(t, result)
	assert.ErrorIs(t, applyErr, context.Canceled)
	assert.True(t, rollbackAfterCallback.Load())
	assert.EqualValues(t, 1, native.rollbacks.Load())
	assert.EqualValues(t, 1, native.closes.Load())
	assert.Zero(t, native.commits.Load())
	assert.Zero(t, store.db.Stats().OpenConnections)
	assertHealthyCancellationStore(t, store)
	assertDatabase(t, path, Inventory{Quantity: 10})
}

func TestStoreCleanupFailureFencesWaitingOperations(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 2)
	var statements atomic.Int64
	var rollbackDenied atomic.Bool
	require.NoError(t, store.conn.Raw(func(raw any) error {
		native, ok := raw.(*sqlite3.SQLiteConn)
		require.True(t, ok)
		native.RegisterAuthorizer(func(action int, operation, _, _ string) int {
			if action == sqlite3.SQLITE_TRANSACTION && operation == "ROLLBACK" {
				rollbackDenied.Store(true)
				return sqlite3.SQLITE_DENY
			}
			if rollbackDenied.Load() {
				statements.Add(1)
			}
			return sqlite3.SQLITE_OK
		})
		return nil
	}))
	atStage, release := make(chan struct{}), make(chan struct{})
	releaseStage := sync.OnceFunc(func() { close(release) })
	defer releaseStage()
	store.stageHook = func(stage applyStage) {
		if stage == inventoryUpdated {
			close(atStage)
			<-release
		}
	}
	admitted := make(chan struct{}, 1)
	var admissions atomic.Int64
	store.observeAdmission = func() {
		if admissions.Add(1) > 1 {
			admitted <- struct{}{}
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var result Result
	owner := healthCall(t, func() error {
		var err error
		result, err = store.Apply(ctx, testRequest(1, 3))
		return err
	})
	healthSignal(t, atStage)
	var waiters []<-chan error
	for _, operation := range healthOperations(t, store) {
		waiters = append(waiters, healthCall(t, func() error {
			return operation.call(t.Context())
		}))
		healthSignal(t, admitted)
	}
	cancel()
	releaseStage()
	err := healthResult(t, owner)
	assert.Zero(t, result)
	assert.ErrorIs(t, err, ErrUnresolved)
	assert.NotErrorIs(t, err, context.Canceled)
	var sqliteErr sqlite3.Error
	require.ErrorAs(t, err, &sqliteErr)
	assert.Equal(t, sqlite3.ErrAuth, sqliteErr.Code)
	assert.True(t, rollbackDenied.Load())
	healthSignal(t, store.Failed())
	for _, waiter := range waiters {
		assert.ErrorIs(t, healthResult(t, waiter), ErrUnresolved)
	}
	store.observeAdmission = nil
	for _, operation := range healthOperations(t, store) {
		assert.ErrorIs(t, operation.call(t.Context()), ErrUnresolved, operation.name)
		assert.ErrorIs(t, operation.call(ctx), ErrUnresolved, operation.name)
	}
	assert.Zero(t, statements.Load(), "waiting and future callers must issue no SQL after failure")
	require.NoError(t, store.Close())
	assert.Zero(t, store.db.Stats().OpenConnections)
	assertDatabase(t, path, Inventory{Quantity: 10})
	reopened := openInventory(t, path, 2)
	_, err = reopened.Apply(t.Context(), testRequest(1, 3))
	require.NoError(t, err)
	assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, testRequest(1, 3))
}

func TestStoreCancellationBeforeCommitRollsBack(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store, connector := openCancellationStore(t, path)
	native := connector.conn
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Cancel only after the final real statement succeeds, so Apply reaches its commit check.
	native.finalExec = "UPDATE receipts SET result_quantity = ? WHERE " + receiptWhere
	native.cancelExec = cancel
	result, err := store.Apply(ctx, testRequest(1, 3))
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, result)
	assert.EqualValues(t, 1, native.rollbacks.Load())
	assert.Zero(t, native.commits.Load())
	assertHealthyCancellationStore(t, store)
	require.NoError(t, store.Close())
	assertDatabase(t, path, Inventory{Quantity: 10})
}
