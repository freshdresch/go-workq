//go:build linux && amd64 && cgo

package inventory

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeGateConnector struct {
	dsn  string
	conn *closeGateConn
}

func (c *closeGateConnector) Connect(context.Context) (driver.Conn, error) {
	native, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	sqliteConn, ok := native.(*sqlite3.SQLiteConn)
	if !ok {
		return nil, errors.Join(errors.New("unexpected SQLite connection type"), native.Close())
	}
	c.conn = &closeGateConn{SQLiteConn: sqliteConn}
	return c.conn, nil
}

func (c *closeGateConnector) Driver() driver.Driver { return &sqlite3.SQLiteDriver{} }

type closeGateConn struct {
	*sqlite3.SQLiteConn
	entered   chan struct{}
	release   chan struct{}
	completed chan struct{}
}

func (c *closeGateConn) Close() error {
	if c.entered != nil {
		close(c.entered)
		<-c.release
	}
	err := c.SQLiteConn.Close()
	if c.completed != nil {
		close(c.completed)
	}
	return err
}

func openCloseGateInventory(t *testing.T, path string) (*Store, *closeGateConn) {
	t.Helper()
	seed := openInventory(t, path, 2)
	require.NoError(t, seed.Close())
	address := url.URL{Scheme: "file", Path: path}
	address.RawQuery = url.Values{
		"_journal_mode": {"WAL"}, "_synchronous": {"FULL"},
		"_busy_timeout": {"250"}, "_foreign_keys": {"on"}, "_txlock": {"immediate"},
	}.Encode()
	connector := &closeGateConnector{dsn: address.String()}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	store := &Store{db: db, conn: conn, receiptLimit: 2, failed: make(chan struct{})}
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	require.NoError(t, store.configureAndInitialize(t.Context(), false))
	return store, connector.conn
}

func TestStoreCancellationNativeCloseJoinsOwner(t *testing.T) {
	path := inventoryPath(t)
	store, native := openCloseGateInventory(t, path)
	synctest.Test(t, func(t *testing.T) {
		atStage, releaseStage := make(chan struct{}), make(chan struct{})
		native.entered = make(chan struct{})
		native.release = make(chan struct{})
		native.completed = make(chan struct{})
		ownerDone := make(chan struct{})
		type outcome struct {
			result             Result
			applyErr, closeErr error
			nativeCompleted    bool
		}
		results := make(chan outcome, 1)
		releaseApply := sync.OnceFunc(func() { close(releaseStage) })
		releaseClose := sync.OnceFunc(func() { close(native.release) })
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		store.stageHook = func(stage applyStage) {
			if stage == inventoryUpdated {
				close(atStage)
				<-releaseStage
			}
		}
		go func() {
			result, applyErr := store.Apply(ctx, testRequest(1, 3))
			closeErr := store.Close()
			completed := false
			select {
			case <-native.completed:
				completed = true
			default:
			}
			results <- outcome{
				result: result, applyErr: applyErr, closeErr: closeErr,
				nativeCompleted: completed,
			}
			close(ownerDone)
		}()
		defer func() {
			releaseApply()
			releaseClose()
			<-ownerDone
			<-native.completed
		}()
		<-atStage
		cancel()
		synctest.Wait()
		releaseApply()
		synctest.Wait()
		<-native.entered
		select {
		case <-ownerDone:
			t.Error("owner returned before native Close completed")
		default:
		}
		releaseClose()
		synctest.Wait()
		<-ownerDone
		got := <-results
		assert.Zero(t, got.result)
		assert.ErrorIs(t, got.applyErr, context.Canceled)
		assert.NoError(t, got.closeErr)
		assert.True(t, got.nativeCompleted, "owner must observe native Close completion")
		assert.Zero(t, store.db.Stats().OpenConnections)
	})
	assertDatabase(t, path, Inventory{Quantity: 10})
	reopened := openInventory(t, path, 2)
	_, err := reopened.Apply(t.Context(), testRequest(1, 3))
	require.NoError(t, err)
	assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, testRequest(1, 3))
}

// This wrapper records completion after native Rollback returns, outside the SQLite hook.
type rollbackCompletionTx struct {
	driver.Tx
	completed chan struct{}
}

func (tx *rollbackCompletionTx) Rollback() error {
	err := tx.Tx.Rollback()
	if tx.completed != nil {
		close(tx.completed)
	}
	return err
}

type rollbackCompletionConn struct {
	*sqlite3.SQLiteConn
	completed chan struct{}
}

func (c *rollbackCompletionConn) BeginTx(
	ctx context.Context,
	options driver.TxOptions,
) (driver.Tx, error) {
	tx, err := c.SQLiteConn.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return &rollbackCompletionTx{Tx: tx, completed: c.completed}, nil
}

type rollbackCompletionConnector struct {
	dsn  string
	conn *rollbackCompletionConn
}

func (c *rollbackCompletionConnector) Connect(context.Context) (driver.Conn, error) {
	native, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	sqliteConn, ok := native.(*sqlite3.SQLiteConn)
	if !ok {
		return nil, errors.Join(errors.New("unexpected SQLite connection type"), native.Close())
	}
	c.conn = &rollbackCompletionConn{SQLiteConn: sqliteConn}
	return c.conn, nil
}

func (c *rollbackCompletionConnector) Driver() driver.Driver { return &sqlite3.SQLiteDriver{} }

func TestStoreCancellationCleanupJoinsBeforeClose(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	seed := openInventory(t, path, 2)
	require.NoError(t, seed.Close())
	address := url.URL{Scheme: "file", Path: path}
	address.RawQuery = url.Values{
		"_journal_mode": {"WAL"}, "_synchronous": {"FULL"},
		"_busy_timeout": {"250"}, "_foreign_keys": {"on"}, "_txlock": {"immediate"},
	}.Encode()
	connector := &rollbackCompletionConnector{dsn: address.String()}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	store := &Store{db: db, conn: conn, receiptLimit: 2, failed: make(chan struct{})}
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	require.NoError(t, store.configureAndInitialize(t.Context(), false))
	rollbackStarted, release := make(chan struct{}), make(chan struct{})
	releaseRollback := sync.OnceFunc(func() { close(release) })
	defer releaseRollback()
	nativeCompleted := make(chan struct{})
	connector.conn.completed = nativeCompleted
	connector.conn.RegisterRollbackHook(func() {
		close(rollbackStarted)
		<-release
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store.stageHook = func(stage applyStage) {
		if stage == inventoryUpdated {
			cancel()
		}
	}
	type outcome struct {
		result          Result
		applyErr        error
		nativeCompleted bool
	}
	results := make(chan outcome, 1)
	owner := healthCall(t, func() error {
		result, applyErr := store.Apply(ctx, testRequest(1, 3))
		completed := false
		select {
		case <-nativeCompleted:
			completed = true
		default:
		}
		results <- outcome{result: result, applyErr: applyErr, nativeCompleted: completed}
		return store.Close()
	})
	healthSignal(t, rollbackStarted)
	releaseRollback()
	require.NoError(t, healthResult(t, owner))
	got := <-results
	assert.Zero(t, got.result)
	assert.ErrorIs(t, got.applyErr, context.Canceled)
	assert.True(t, got.nativeCompleted, "Apply must observe native rollback completion")
	healthSignal(t, nativeCompleted)
	select {
	case <-store.Failed():
		t.Error("successful cancellation cleanup poisoned Store")
	default:
	}
	assert.Zero(t, store.db.Stats().OpenConnections)
	assertDatabase(t, path, Inventory{Quantity: 10})
	reopened := openInventory(t, path, 2)
	_, err = reopened.Apply(t.Context(), testRequest(1, 3))
	require.NoError(t, err)
	assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, testRequest(1, 3))
}
