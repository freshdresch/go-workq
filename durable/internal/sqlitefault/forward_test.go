//go:build durable_faulttest && linux && amd64 && cgo

package sqlitefault

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForwarding(t *testing.T) {
	if os.Getenv("WORKQ_FAULT_CHILD") != "1" {
		command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestForwarding$", "-test.v")
		command.Env = append(os.Environ(), "WORKQ_FAULT_CHILD=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		t.Log(string(output))
		return
	}
	root := t.TempDir()
	name, controller, err := Attach(root, Limits{Events: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { assert.ErrorIs(t, controller.Close(), ErrObservation) })
	path := filepath.Join(root, "queue.sqlite3")
	location := url.URL{Scheme: "file", Path: path}
	query := url.Values{"vfs": {name}, "_synchronous": {"FULL"}}
	location.RawQuery = query.Encode()
	open := func() *sql.DB {
		db, err := sql.Open("sqlite3", location.String())
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		return db
	}
	db := open()
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0",
		"CREATE TABLE records(value TEXT)", "INSERT INTO records VALUES ('retained')",
	} {
		_, err := db.ExecContext(t.Context(), statement)
		require.NoError(t, err, statement)
	}
	var value string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT value FROM records").Scan(&value))
	assert.Equal(t, "retained", value)

	require.NoError(t, controller.Arm("native-sync", "main", 1, "before", SyncError))
	var busy, frames, checkpointed int
	err = db.QueryRowContext(t.Context(), "PRAGMA wal_checkpoint(TRUNCATE)").Scan(
		&busy, &frames, &checkpointed,
	)
	var sqliteErr sqlite3.Error
	require.ErrorAs(t, err, &sqliteErr)
	assert.Equal(t, sqlite3.ErrIoErrFsync, sqliteErr.ExtendedCode)
	events := controller.Events()
	require.NotEmpty(t, events)
	last := events[len(events)-1]
	assert.Equal(t, "native-sync", last.Operation)
	assert.Equal(t, "after", last.Phase)
	assert.Equal(t, "main", last.Role)
	assert.Equal(t, 1034, last.Result)
	require.NoError(t, db.Close())

	db = open()
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT value FROM records").Scan(&value))
	assert.Equal(t, "retained", value)
	require.NoError(t, db.QueryRowContext(t.Context(), "PRAGMA wal_checkpoint(TRUNCATE)").Scan(
		&busy, &frames, &checkpointed,
	))
	assert.Zero(t, busy)
	// A replacement name must disagree with the still-open native descriptor.
	var opened Event
	for _, event := range controller.Events() {
		if event.Operation == "native-open" && event.Role == "main" {
			opened = event
		}
	}
	require.NotZero(t, opened.Actual)
	require.NoError(t, os.Rename(path, path+".held"))
	require.NoError(t, os.WriteFile(path, []byte("replacement"), 0600))
	actual, err := controller.DescriptorEvent(path, opened.Descriptor)
	require.NoError(t, err)
	assert.Equal(t, opened.Actual, actual.Actual)
	assert.NotEqual(t, actual.Expected, actual.Actual)
	actual.Operation = "native-sync"
	require.ErrorIs(t, controller.Before(actual), ErrObservation)
	require.ErrorIs(t, controller.After(actual, ErrObservation), ErrObservation)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Rename(path+".held", path))

	require.NoError(t, db.Close())
	require.ErrorIs(t, controller.Err(), ErrObservation)
	events = controller.Events()
	for i, event := range events {
		assert.Equal(t, uint64(i+1), event.Sequence)
		assert.NotZero(t, event.Parent)
	}
}

// This unit check supplies synthetic outcomes. It does not claim a real syscall failed.
func TestOutcomeMetadata(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome error
		errno   syscall.Errno
	}{
		{"wrapped errno", &os.PathError{Op: "sync", Path: "fixture", Err: syscall.ENOSPC},
			syscall.ENOSPC},
		{"injected without syscall", ErrInjected, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller := &Controller{limits: Limits{Events: 1}}
			require.ErrorIs(t, controller.After(Event{Operation: "owned-sync"}, tc.outcome), tc.outcome)
			events := controller.Events()
			require.Len(t, events, 1)
			assert.Equal(t, tc.errno, events[0].Errno)
			assert.NotZero(t, events[0].Result)
		})
	}
}

func TestObservationFailureSticky(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			controller := &Controller{limits: Limits{Events: 16}}
			event := Event{Actual: Identity{Inode: 1}, Expected: Identity{Inode: 2}}
			var err error
			if phase == "before" {
				err = controller.Before(event)
			} else {
				err = controller.After(event, nil)
			}
			require.ErrorIs(t, err, ErrObservation)
			first := controller.Err()
			require.ErrorIs(t, first, ErrObservation)
			for range 20 {
				assert.ErrorIs(t, controller.Before(event), ErrObservation)
				assert.ErrorIs(t, controller.After(event, nil), ErrObservation)
			}
			assert.Same(t, first, controller.Err())
		})
	}
}

func TestOwnedObservationFailureSticky(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			_, controller, err := Attach(root, Limits{Events: 16})
			require.NoError(t, err)
			t.Cleanup(func() { assert.ErrorIs(t, controller.Close(), ErrObservation) })
			path := filepath.Join(root, "queue.sqlite3")
			file, err := os.Create(path)
			require.NoError(t, err)
			if phase == "before" {
				require.NoError(t, file.Close())
			}
			after, beforeErr := controller.ObserveOwned(file, path)
			if phase == "after" {
				require.NoError(t, beforeErr)
				require.NoError(t, file.Close())
			} else {
				assert.ErrorIs(t, beforeErr, ErrObservation)
			}
			assert.ErrorIs(t, after(beforeErr), ErrObservation)
			assert.ErrorIs(t, controller.Err(), ErrObservation)
		})
	}
}

func TestSuppressedCloseObservation(t *testing.T) {
	if os.Getenv("WORKQ_FAULT_CLOSE_CHILD") != "1" {
		command := exec.CommandContext(
			t.Context(), os.Args[0], "-test.run=^TestSuppressedCloseObservation$", "-test.v",
		)
		command.Env = append(os.Environ(), "WORKQ_FAULT_CLOSE_CHILD=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		t.Log(string(output))
		return
	}
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			root := t.TempDir()
			name, controller, err := Attach(root, Limits{Events: 4096})
			require.NoError(t, err)
			t.Cleanup(func() { assert.ErrorIs(t, controller.Close(), ErrObservation) })
			path := filepath.Join(root, "queue.sqlite3")
			location := url.URL{Scheme: "file", Path: path}
			location.RawQuery = url.Values{"vfs": {name}}.Encode()
			db, err := sql.Open("sqlite3", location.String())
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, db.Close()) })
			db.SetMaxOpenConns(1)
			for _, statement := range []string{
				"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0",
				"PRAGMA synchronous=NORMAL", "CREATE TABLE x(a)", "INSERT INTO x VALUES(1)",
			} {
				_, err := db.ExecContext(t.Context(), statement)
				require.NoError(t, err)
			}
			require.NoError(t, controller.Err())
			wal := path + "-wal"
			require.NoError(t, os.Rename(wal, wal+".held"))
			if replacement {
				require.NoError(t, os.WriteFile(wal, []byte("replacement"), 0600))
			}
			assert.NoError(t, db.Close(), "SQLite suppresses its close checkpoint error")
			assert.ErrorIs(t, controller.Err(), ErrObservation)
		})
	}
}
