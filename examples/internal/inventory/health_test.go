//go:build linux && amd64 && cgo

package inventory

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func healthCall(t *testing.T, call func() error) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- call()
	}()
	t.Cleanup(func() { healthSignal(t, done) })
	return result
}

func healthSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("store health handshake did not complete")
	}
}

func healthResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("store health operation did not join")
		return nil
	}
}

func healthOperations(t *testing.T, store *Store) []struct {
	name string
	call func(context.Context) error
} {
	t.Helper()
	return []struct {
		name string
		call func(context.Context) error
	}{
		{"apply", func(ctx context.Context) error {
			result, err := store.Apply(ctx, testRequest(2, 2))
			assert.Zero(t, result)
			return err
		}},
		{"lookup", func(ctx context.Context) error {
			receipt, found, err := store.Lookup(ctx, testRequest(1, 3).Key)
			assert.Zero(t, receipt)
			assert.False(t, found)
			return err
		}},
		{"snapshot", func(ctx context.Context) error {
			snapshot, err := store.Snapshot(ctx)
			assert.Zero(t, snapshot)
			return err
		}},
	}
}

func countHealthSQL(t *testing.T, store *Store) *atomic.Int64 {
	t.Helper()
	var statements atomic.Int64
	require.NoError(t, store.conn.Raw(func(raw any) error {
		conn, ok := raw.(*sqlite3.SQLiteConn)
		require.True(t, ok)
		conn.RegisterAuthorizer(func(int, string, string, string) int {
			statements.Add(1)
			return sqlite3.SQLITE_OK
		})
		return nil
	}))
	return &statements
}

func TestStoreFailureFencesWaitingOperations(t *testing.T) {
	for _, committed := range []bool{true, false} {
		name := "rolled back"
		if committed {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			store := openInventory(t, path, 2)
			failed := store.Failed()
			require.NotNil(t, failed)
			assert.Equal(t, failed, store.Failed())
			select {
			case <-failed:
				t.Fatal("healthy store signaled failure")
			default:
			}
			statements := countHealthSQL(t, store)
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
				// The first Apply owns opMu. Each subsequent signal is a caller
				// inside the Store at the ownership boundary while it is held.
				if admissions.Add(1) > 1 {
					select {
					case admitted <- struct{}{}:
					default:
					}
				}
			}
			unknown := errors.New("commit observation lost")
			store.observeCommit = func(err error) error {
				if committed {
					assert.NoError(t, err)
				} else {
					assert.ErrorIs(t, err, sql.ErrTxDone)
				}
				statements.Store(0)
				return unknown
			}
			request := testRequest(1, 3)
			first := healthCall(t, func() error {
				if committed {
					result, err := store.Apply(t.Context(), request)
					assert.Zero(t, result)
					return err
				}
				// Model the other real SQL history without changing the observation seam.
				store.lockOperation()
				defer store.opMu.Unlock()
				tx, err := store.begin(t.Context())
				if err != nil {
					return err
				}
				if _, err := store.apply(t.Context(), tx, request); err != nil {
					return errors.Join(err, tx.Rollback())
				}
				if err := tx.Rollback(); err != nil {
					return err
				}
				return store.commit(t.Context(), tx)
			})
			healthSignal(t, atStage)
			var waiters []<-chan error
			for _, operation := range healthOperations(t, store) {
				waiters = append(waiters, healthCall(t, func() error {
					return operation.call(t.Context())
				}))
				healthSignal(t, admitted)
			}
			releaseStage()
			require.ErrorIs(t, healthResult(t, first), unknown)
			healthSignal(t, failed)
			for _, waiter := range waiters {
				assert.ErrorIs(t, healthResult(t, waiter), ErrUnresolved)
			}
			// All callers joined, so hooks can be removed before synchronous calls.
			store.observeAdmission = nil
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			for _, operation := range healthOperations(t, store) {
				assert.ErrorIs(t, operation.call(t.Context()), unknown, operation.name)
				assert.ErrorIs(t, operation.call(canceled), unknown, operation.name)
			}
			assert.Zero(t, statements.Load(), "fenced callers must issue no SQL")
			store.opMu.Lock()
			assert.ErrorIs(t, store.poison(errors.New("later failure")), unknown)
			assert.Equal(t, failed, store.Failed())
			store.opMu.Unlock()
			require.NoError(t, store.Close())
			if committed {
				assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, request)
			} else {
				assertDatabase(t, path, Inventory{Quantity: 10})
			}
			reopened := openInventory(t, path, 2)
			receipt, found, err := reopened.Lookup(t.Context(), request.Key)
			require.NoError(t, err)
			assert.Equal(t, committed, found)
			if found {
				assert.Equal(t, request.Payload, receipt.Payload)
			}
			result, err := reopened.Apply(t.Context(), request)
			require.NoError(t, err)
			assert.Equal(t, committed, result.Duplicate)
			assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, request)
		})
	}
}

func TestStoreWaitingCancellation(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 2)
	statements := countHealthSQL(t, store)
	admitted := make(chan struct{}, 1)
	store.observeAdmission = func() {
		select {
		case admitted <- struct{}{}:
		default:
		}
	}
	for _, operation := range healthOperations(t, store) {
		ctx, cancel := context.WithCancel(t.Context())
		store.opMu.Lock()
		unlock := sync.OnceFunc(store.opMu.Unlock)
		defer unlock()
		result := healthCall(t, func() error { return operation.call(ctx) })
		healthSignal(t, admitted)
		cancel()
		unlock()
		assert.ErrorIs(t, healthResult(t, result), context.Canceled, operation.name)
	}
	store.observeAdmission = nil
	assert.Zero(t, statements.Load(), "cancellation after waiting must prevent SQL")
	require.NoError(t, store.Close())
	select {
	case <-store.Failed():
		t.Error("ordinary cancellation and Close must not signal failure")
	default:
	}
	assertDatabase(t, path, Inventory{Quantity: 10})
}

func TestStoreConcurrentApplyAndCloseComplete(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store := openInventory(t, path, 1)
	atCommit, release := make(chan struct{}), make(chan struct{})
	releaseCommit := sync.OnceFunc(func() { close(release) })
	defer releaseCommit()
	store.stageHook = func(stage applyStage) {
		if stage == destinationCommitSucceeded {
			close(atCommit)
			<-release
		}
	}
	admitted := make(chan struct{}, 1)
	var admissions atomic.Int64
	store.observeAdmission = func() {
		if admissions.Add(1) == 2 {
			admitted <- struct{}{}
		}
	}
	applied := healthCall(t, func() error {
		_, err := store.Apply(t.Context(), testRequest(1, 3))
		return err
	})
	healthSignal(t, atCommit)
	closed := healthCall(t, store.Close)
	healthSignal(t, admitted)
	releaseCommit()
	require.NoError(t, healthResult(t, applied))
	require.NoError(t, healthResult(t, closed))
	require.NoError(t, store.Close())
	for _, operation := range healthOperations(t, store) {
		assert.ErrorIs(t, operation.call(t.Context()), ErrUnresolved, operation.name)
	}
	assert.Zero(t, store.db.Stats().OpenConnections)
	assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, testRequest(1, 3))
}

func TestStoreCloseRetainsCleanupError(t *testing.T) {
	t.Parallel()
	path := inventoryPath(t)
	store, err := Open(t.Context(), path, 1)
	require.NoError(t, err)
	t.Cleanup(func() { assert.ErrorIs(t, store.Close(), sql.ErrConnDone) })
	// A real already-closed connection makes Close report an error. It must still close db.
	require.NoError(t, store.conn.Close())
	first := store.Close()
	assert.ErrorIs(t, first, sql.ErrConnDone)
	assert.ErrorIs(t, store.Close(), first)
	assert.Zero(t, store.db.Stats().OpenConnections)
	assertDatabase(t, path, Inventory{Quantity: 10})
}
