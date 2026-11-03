//go:build durable_faulttest && linux && amd64 && cgo

package sqlitefault

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func effectDatabase(t *testing.T, journal string) (*sql.DB, *Controller) {
	t.Helper()
	root := os.Getenv("WORKQ_EFFECT_ROOT")
	require.NotEmpty(t, root)
	name, controller, err := Attach(root, Limits{Events: 8192})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, controller.Close()) })
	t.Cleanup(func() { saveEffectReplay(t, controller) })
	location := url.URL{Scheme: "file", Path: filepath.Join(root, "queue.sqlite3")}
	location.RawQuery = url.Values{"vfs": {name}}.Encode()
	db, err := sql.Open("sqlite3", location.String())
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	for _, statement := range []string{
		"PRAGMA journal_mode=" + journal, "PRAGMA wal_autocheckpoint=0",
		"PRAGMA synchronous=FULL", "CREATE TABLE records(value)", "INSERT INTO records VALUES(1)",
	} {
		_, err := db.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}
	return db, controller
}

func injectedEvents(controller *Controller) []Event {
	var result []Event
	for _, event := range controller.Events() {
		if event.Injected {
			result = append(result, event)
		}
	}
	return result
}

func TestNativeWriteEffects(t *testing.T) {
	for _, tc := range []struct {
		name      string
		effect    Effect
		code      sqlite3.ErrNoExtended
		performed bool
		prefix    int64
		errno     syscall.Errno
		tail      string
	}{
		{"full", FullError, sqlite3.ErrNoExtended(sqlite3.ErrFull), false, 0, syscall.ENOSPC, ""},
		{"short", ShortWrite(8), sqlite3.ErrIoErrWrite, true, 8, syscall.EIO, ""},
		{"short-existing-tail", ShortWrite(8), sqlite3.ErrIoErrWrite, true, 8, syscall.EIO,
			"uncommitted-WAL-tail-bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if replayEffectChild(t) {
				return
			}
			db, controller := effectDatabase(t, "WAL")
			wal := filepath.Join(controller.root, "queue.sqlite3-wal")
			before, err := os.ReadFile(wal)
			require.NoError(t, err)
			var rootPage, pageCount uint32
			rootQuery := "SELECT rootpage FROM sqlite_schema WHERE name='records'"
			require.NoError(t, db.QueryRowContext(t.Context(), rootQuery).Scan(&rootPage))
			require.NoError(t, db.QueryRowContext(t.Context(), "PRAGMA page_count").Scan(&pageCount))
			// This tiny insert changes only the table root, so its frame is the commit frame.
			expectedPrefix := binary.BigEndian.AppendUint32(nil, rootPage)
			expectedPrefix = binary.BigEndian.AppendUint32(expectedPrefix, pageCount)
			expected := append(slices.Clone(before), expectedPrefix[:tc.prefix]...)
			if tc.tail != "" {
				// Bytes beyond SQLite's logical WAL end must survive a bounded overwrite.
				file, err := os.OpenFile(wal, os.O_WRONLY, 0)
				require.NoError(t, err)
				_, err = file.WriteAt([]byte(tc.tail), int64(len(before)))
				require.NoError(t, err)
				require.NoError(t, file.Close())
				expected = append(expected, tc.tail[tc.prefix:]...)
			}
			require.NoError(t, controller.Arm("native-write", "wal", 1, "before", tc.effect))
			_, err = db.ExecContext(t.Context(), "INSERT INTO records VALUES(2)")
			after, readErr := os.ReadFile(wal)
			require.NoError(t, readErr)
			assert.Equal(t, expected, after, "exact prefix and unchanged surrounding bytes and length")
			var nativeErr sqlite3.Error
			require.ErrorAs(t, err, &nativeErr)
			assert.Equal(t, tc.code, nativeErr.ExtendedCode)
			assert.False(t, controller.Armed())
			events := injectedEvents(controller)
			require.Len(t, events, 1)
			event := events[0]
			assert.Equal(t, int64(len(before)), event.Offset, "next logical WAL frame")
			assert.Equal(t, int64(24), event.Length, "WAL frame header")
			assert.Equal(t, tc.performed, event.Performed)
			assert.Zero(t, event.ActualResult)
			assert.Zero(t, event.ActualErrno)
			assert.Equal(t, tc.errno, event.Errno)
			assert.Equal(t, tc.prefix, event.Prefix)
			assert.Equal(t, event.Expected, event.Actual)
			assert.NotZero(t, event.Actual)
			t.Logf("native effect: %+v", event)
		})
	}
}

func TestNativeSuppressedErrors(t *testing.T) {
	for _, tc := range []struct {
		name, operation, role, phase string
		effect                       Effect
	}{
		{"close", "native-close", "main", "after", CloseError},
		{"close-checkpoint", "native-sync", "main", "before", SyncError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if replayEffectChild(t) {
				return
			}
			db, controller := effectDatabase(t, "WAL")
			require.NoError(t, controller.Arm(tc.operation, tc.role, 1, tc.phase, tc.effect))
			assert.NoError(t, db.Close(), "pinned SQLite suppresses native destructor errors")
			assert.False(t, controller.Armed())
			events := injectedEvents(controller)
			require.Len(t, events, 1)
			assert.NotZero(t, events[0].Result)
			assert.Equal(t, tc.operation, events[0].Operation)
			assert.Zero(t, events[0].ActualResult)
			assert.Equal(t, tc.name == "close", events[0].Performed)
			t.Logf("native destructor error, driver nil: %+v", events[0])
		})
	}
}

func TestFaultMatching(t *testing.T) {
	if replayEffectChild(t) {
		return
	}
	db, controller := effectDatabase(t, "WAL")
	require.NoError(t, controller.Arm("owned-sync", "parent", 1, "after", SyncError))
	_, err := db.ExecContext(t.Context(), "INSERT INTO records VALUES(2)")
	require.NoError(t, err)
	assert.True(t, controller.Armed())
	assert.Empty(t, injectedEvents(controller))
	require.ErrorIs(t, controller.Arm("native-sync", "wal", 1, "before", SyncError), ErrObservation)
}

// sqlite3RollbackAll discards sqlite3BtreeRollback's return in the pinned amalgamation.
// This rollback-journal control establishes native finalization failure only.
// The bounded image oracle does not model journal recovery.
func TestNativeRollbackFailure(t *testing.T) {
	if replayEffectChild(t) {
		return
	}
	db, controller := effectDatabase(t, "DELETE")
	_, err := db.ExecContext(t.Context(), "PRAGMA cache_size=1; BEGIN; UPDATE records SET value=2")
	require.NoError(t, err)
	require.NoError(t, controller.Arm("native-delete", "journal", 1, "before", IOError))
	_, err = db.ExecContext(t.Context(), "ROLLBACK")
	events := injectedEvents(controller)
	t.Logf("rollback result %v armed %v injected %+v", err, controller.Armed(), events)
	require.Len(t, events, 1)
	assert.Equal(t, "native-delete", events[0].Operation)
	assert.Equal(t, "journal", events[0].Role)
	assert.Equal(t, 10, events[0].Result)
	assert.False(t, events[0].Performed)
	assert.False(t, controller.Armed())
	assert.NoError(t, err, "SQLite suppresses rollback finalization failure")
}

type effectReplay struct {
	Events []Event `json:"events"`
	Values []int   `json:"values"`
}

// Each replay gets a fresh process and parent-owned paths, including on timeout.
func replayEffectChild(t *testing.T) bool {
	t.Helper()
	if os.Getenv("WORKQ_EFFECT_CHILD") == "1" {
		return false
	}
	var previous effectReplay
	for attempt := range 2 {
		root, err := os.MkdirTemp("", "effect-replay-")
		require.NoError(t, err)
		t.Logf("replay fixture %s", root)
		t.Cleanup(func() {
			if !t.Failed() {
				assert.NoError(t, os.RemoveAll(root))
			}
		})
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		pattern := "^" + strings.ReplaceAll(regexp.QuoteMeta(t.Name()), "/", "$/^") + "$"
		command := exec.CommandContext(ctx, os.Args[0], "-test.run="+pattern, "-test.v")
		command.Env = append(os.Environ(), "WORKQ_EFFECT_CHILD=1", "WORKQ_EFFECT_ROOT="+root)
		output, err := command.CombinedOutput()
		cancel()
		require.NoError(t, err, "%s", output)
		data, err := os.ReadFile(filepath.Join(root, "replay.json"))
		require.NoError(t, err)
		var replay effectReplay
		require.NoError(t, json.Unmarshal(data, &replay))
		require.NotEmpty(t, replay.Events)
		replay.Events = semanticEvents(replay.Events)
		if attempt == 1 {
			assert.Equal(t, previous, replay, "semantic events and reopened records")
		}
		previous = replay
	}
	return true
}

func saveEffectReplay(t *testing.T, controller *Controller) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	replay := effectReplay{Events: controller.Events()}
	db, err := sql.Open("sqlite3", filepath.Join(controller.root, "queue.sqlite3"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	rows, err := db.QueryContext(ctx, "SELECT value FROM records ORDER BY value")
	require.NoError(t, err)
	for rows.Next() {
		var value int
		require.NoError(t, rows.Scan(&value))
		replay.Values = append(replay.Values, value)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	data, err := json.Marshal(replay)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(controller.root, "replay.json"), data, 0600))
}

func TestNativeSyncOccurrence(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			if replayEffectChild(t) {
				return
			}
			db, controller := effectDatabase(t, "WAL")
			require.NoError(t, controller.Arm("native-sync", "wal", 2, phase, SyncError))
			_, err := db.ExecContext(t.Context(), "INSERT INTO records VALUES(2)")
			require.NoError(t, err)
			assert.True(t, controller.Armed())
			assert.Empty(t, injectedEvents(controller))
			_, err = db.ExecContext(t.Context(), "INSERT INTO records VALUES(3)")
			var nativeErr sqlite3.Error
			require.ErrorAs(t, err, &nativeErr)
			assert.Equal(t, sqlite3.ErrIoErrFsync, nativeErr.ExtendedCode)
			assert.False(t, controller.Armed())
			events := injectedEvents(controller)
			require.Len(t, events, 1)
			assert.Equal(t, phase == "after", events[0].Performed)
			_, err = db.ExecContext(t.Context(), "INSERT INTO records VALUES(4)")
			require.NoError(t, err, "the fault fires only once")
			assert.Len(t, injectedEvents(controller), 1)
		})
	}
}

func TestNativeTruncateFailure(t *testing.T) {
	if replayEffectChild(t) {
		return
	}
	db, controller := effectDatabase(t, "WAL")
	require.NoError(t, controller.Arm("native-truncate", "wal", 1, "before", IOError))
	_, err := db.ExecContext(t.Context(), "PRAGMA wal_checkpoint(TRUNCATE)")
	var nativeErr sqlite3.Error
	require.ErrorAs(t, err, &nativeErr)
	assert.Equal(t, sqlite3.ErrIoErr, nativeErr.Code)
	events := injectedEvents(controller)
	require.Len(t, events, 1)
	assert.False(t, events[0].Performed)
	assert.Equal(t, syscall.EIO, events[0].Errno)
	assert.False(t, controller.Armed())
	_, err = db.ExecContext(t.Context(), "PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, err)
	assert.Len(t, injectedEvents(controller), 1)
}

func semanticEvents(events []Event) []Event {
	result := slices.Clone(events)
	// OS identities and descriptor allocation differ across independent processes.
	for i := range result {
		event := &result[i]
		event.Actual, event.Expected, event.Parent = Identity{}, Identity{}, Identity{}
		event.Descriptor = 0
	}
	return result
}
