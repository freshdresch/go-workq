//go:build linux && amd64 && cgo

package inventory

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreUnknownCommitRecovery(t *testing.T) {
	for _, committed := range []bool{true, false} {
		name := "rolled back"
		if committed {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			store := openInventory(t, path, 2)
			request := testRequest(1, 3)
			unknown := errors.New("commit observation lost")
			observed := false
			store.observeCommit = func(err error) error {
				observed = true
				if committed {
					assert.NoError(t, err, "success must follow a real commit")
				} else {
					assert.ErrorIs(t, err, sql.ErrTxDone, "fixture already rolled back")
				}
				acquired := store.opMu.TryLock()
				if acquired {
					store.opMu.Unlock()
				}
				assert.False(t, acquired, "observation must retain ownership")
				return unknown
			}
			var outcome error
			if committed {
				result, err := store.Apply(t.Context(), request)
				assert.Zero(t, result)
				outcome = err
			} else {
				outcome = rolledBackCommitObservation(t, store, request)
			}
			require.True(t, observed)
			require.ErrorIs(t, outcome, ErrUnresolved)
			assert.ErrorIs(t, outcome, unknown)
			store.opMu.Lock()
			assert.ErrorIs(t, store.failure, unknown)
			store.opMu.Unlock()

			// A lost observation never permits reuse of this connection for recovery.
			_, err := store.Apply(t.Context(), request)
			assert.ErrorIs(t, err, ErrUnresolved)
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
				assert.Equal(t, int64(13), receipt.Result.Quantity)
			}
			result, err := reopened.Apply(t.Context(), request)
			require.NoError(t, err)
			assert.Equal(t, committed, result.Duplicate)
			assert.Equal(t, int64(13), result.Quantity)
			retry, err := reopened.Apply(t.Context(), request)
			require.NoError(t, err)
			assert.True(t, retry.Duplicate)
			result.Duplicate = true
			assert.Equal(t, result, retry)
			assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, request)
		})
	}
}

// This fixture changes the SQL history explicitly. The observation seam only changes the
// returned error, and production commit handling must poison before ownership is released.
func rolledBackCommitObservation(t *testing.T, store *Store, request Request) error {
	t.Helper()
	store.opMu.Lock()
	defer store.opMu.Unlock()
	tx, err := store.conn.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = store.apply(t.Context(), tx, request)
	if err != nil {
		require.NoError(t, tx.Rollback())
		t.Fatalf("prepare rolled-back history: %v", err)
	}
	require.NoError(t, tx.Rollback())
	outcome := store.commit(tx)
	assert.ErrorIs(t, store.failure, ErrUnresolved, "poison must precede unlock")
	return outcome
}

func TestCommitObservationCannotClearFailure(t *testing.T) {
	t.Parallel()
	store := openInventory(t, inventoryPath(t), 1)
	store.observeCommit = func(error) error { return nil }
	store.opMu.Lock()
	defer store.opMu.Unlock()
	tx, err := store.conn.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	err = store.commit(tx)
	assert.ErrorIs(t, err, ErrUnresolved)
	assert.ErrorIs(t, err, sql.ErrTxDone)
}
