//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable/internal/sqlitefault"
)

type resizeFaultCase struct {
	name      string
	stage     string
	operation string
	role      string
	effect    sqlitefault.Effect
	decision  Decision
}

func resizeFaultCases() []resizeFaultCase {
	return []resizeFaultCase{
		{"initial-main", "before-barrier", "owned-sync", "main", sqlitefault.SyncError, NotApplied},
		{
			"initial-directory",
			"before-barrier",
			"owned-sync",
			"directory",
			sqlitefault.SyncError,
			NotApplied,
		},
		{"commit-header-write", "before-commit", "native-write", "wal", sqlitefault.FullError, Unknown},
		{"commit-header-sync", "before-commit", "native-sync", "wal", sqlitefault.SyncError, Unknown},
		{
			"checkpoint-write",
			"before-final-barrier",
			"native-write",
			"main",
			sqlitefault.FullError,
			Unknown,
		},
		{
			"checkpoint-sync",
			"before-final-barrier",
			"native-sync",
			"main",
			sqlitefault.SyncError,
			Unknown,
		},
		{"final-main", "before-final-barrier", "owned-sync", "main", sqlitefault.SyncError, Unknown},
		{
			"final-directory",
			"before-final-barrier",
			"owned-sync",
			"directory",
			sqlitefault.SyncError,
			Unknown,
		},
	}
}

func resizeFaultRequest(t *testing.T, directory, transition string) ResizeRequest {
	t.Helper()
	request := resizeRequest(t, directory)
	request.Next.MaxPendingItems--
	if transition == "migration" {
		request.Next.Profile = profileSQLiteWALV2
	}
	return request
}

func resizeFaultConfig(t *testing.T, transition string, mode Durability) Config {
	t.Helper()
	config := testConfig(t)
	config.Durability = mode
	if transition == "v2" {
		config.Policy.Profile = profileSQLiteWALV2
	}
	return config
}

func attachResizeFault(t *testing.T, directory string) *sqlitefault.Controller {
	t.Helper()
	_, controller, err := sqlitefault.Attach(directory, sqlitefault.Limits{Events: 8192})
	require.NoError(t, err)
	faultController.Store(controller)
	t.Cleanup(func() {
		faultController.Store(nil)
		assert.NoError(t, controller.Close())
	})
	return controller
}

func TestFaultResizeStorageBoundaries(t *testing.T) {
	for _, transition := range []string{"v1", "v2", "migration"} {
		for _, mode := range []Durability{Synchronized, ProcessCrash} {
			for _, fault := range resizeFaultCases() {
				t.Run(fmt.Sprintf("%s/%s/%s", transition, mode, fault.name), func(t *testing.T) {
					config := resizeFaultConfig(t, transition, mode)
					directory := migrationFixture(t, config, 3, 128)
					request := resizeFaultRequest(t, directory, transition)
					before := migrationRecords(t, directory)
					controller := attachResizeFault(t, directory)
					observer := offlineTestObserver{observe: func(stage string, _ *validatedStore) error {
						if stage != fault.stage {
							return nil
						}
						return controller.Arm(fault.operation, fault.role, 1, "before", fault.effect)
					}}
					result, err := resizeObserved(t.Context(), directory, request, observer)
					assert.ErrorIs(t, err, ErrStorage)
					assert.Equal(t, fault.decision, result.Decision)
					require.False(t, controller.Armed(), "selected storage failure must occur")
					require.NoError(t, controller.Err(), "evidence overflow is a test failure")
					var injected int
					for _, event := range controller.Events() {
						if event.Injected {
							injected++
							assert.NotZero(t, event.Result)
							assert.Equal(t, event.Expected, event.Actual)
						}
					}
					require.Equal(t, 1, injected)
					faultController.Store(nil)
					inspection, err := Inspect(t.Context(), directory, nil)
					require.NoError(t, err)
					expected := request.Expected
					if fault.stage == "before-final-barrier" {
						expected = request.Next
					}
					assert.Equal(t, expected, inspection.Store.Policy)
					assert.Equal(t, mode, inspection.Store.Durability)
					assert.Equal(t, before, migrationRecords(t, directory))
					migrationSchema(t, directory)
					result, err = Resize(t.Context(), directory, request)
					require.NoError(t, err)
					assert.Equal(t, Applied, result.Decision)
					assert.Equal(t, before, migrationRecords(t, directory))
				})
			}
		}
	}
}

func TestFaultResizeSQLBoundaries(t *testing.T) {
	for _, transition := range []string{"v1", "v2", "migration"} {
		for _, mode := range []Durability{Synchronized, ProcessCrash} {
			for _, failure := range []string{"policy", "rollback", "readback", "rename", "copy", "drop"} {
				migrationOnly := failure == "rename" || failure == "copy" || failure == "drop"
				if transition != "migration" && migrationOnly {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", transition, mode, failure), func(t *testing.T) {
					directory := migrationFixture(t, resizeFaultConfig(t, transition, mode), 3, 128)
					request := resizeFaultRequest(t, directory, transition)
					before := migrationRecords(t, directory)
					var denied, rollbackDenied bool
					observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
						target := "before-mutation"
						if failure == "readback" {
							target = "after-commit"
						}
						if transition == "migration" && (failure == "policy" || failure == "rollback") {
							target = "migration-metadata"
						}
						if stage != target {
							return nil
						}
						return opened.storage.conn.Raw(func(raw any) error {
							connection, ok := raw.(*sqlite3.SQLiteConn)
							if !ok {
								return ErrStorage
							}
							connection.RegisterAuthorizer(func(action int, name, _, _ string) int {
								reject := false
								switch failure {
								case "policy", "rollback":
									reject = action == sqlite3.SQLITE_UPDATE && name == "meta"
								case "readback":
									reject = action == sqlite3.SQLITE_READ && name == "meta"
								case "rename":
									reject = action == sqlite3.SQLITE_ALTER_TABLE
								case "copy":
									reject = action == sqlite3.SQLITE_INSERT && name == "payloads"
								case "drop":
									reject = action == sqlite3.SQLITE_DROP_TABLE && name == "migration_payloads"
								}
								if reject {
									denied = true
									return sqlite3.SQLITE_DENY
								}
								if failure == "rollback" && action == sqlite3.SQLITE_TRANSACTION && name == "ROLLBACK" {
									rollbackDenied = true
									return sqlite3.SQLITE_DENY
								}
								return sqlite3.SQLITE_OK
							})
							return nil
						})
					}}
					result, err := resizeObserved(t.Context(), directory, request, observer)
					require.True(t, denied)
					assert.ErrorIs(t, err, ErrStorage)
					decision := NotApplied
					expected := request.Expected
					if failure == "rollback" || failure == "readback" {
						decision = Unknown
					}
					if failure == "readback" {
						expected = request.Next
					}
					assert.Equal(t, decision, result.Decision)
					assert.Equal(t, failure == "rollback", rollbackDenied)
					if failure == "rollback" {
						assert.Equal(t, 2, countNativeAuthErrors(err))
					}
					inspection, err := Inspect(t.Context(), directory, nil)
					require.NoError(t, err)
					assert.Equal(t, expected, inspection.Store.Policy)
					assert.Equal(t, mode, inspection.Store.Durability)
					assert.Equal(t, before, migrationRecords(t, directory))
					migrationSchema(t, directory)
					result, err = Resize(t.Context(), directory, request)
					require.NoError(t, err)
					assert.Equal(t, Applied, result.Decision)
				})
			}
		}
	}
}

func TestFaultResizeDescriptorIdentity(t *testing.T) {
	directory := migrationFixture(t, testConfig(t), 3, 1)
	request := resizeFaultRequest(t, directory, "migration")
	controller := attachResizeFault(t, directory)
	observed := false
	observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
		if stage != "before-barrier" {
			return nil
		}
		observed = true
		// A valid retained descriptor paired with the wrong pathname must fail before Sync.
		return syncOwnedDescriptor(opened.main, filepath.Join(directory, "LOCK"))
	}}
	result, err := resizeObserved(t.Context(), directory, request, observer)
	assert.True(t, observed)
	assert.ErrorIs(t, err, ErrStorage)
	assert.Equal(t, NotApplied, result.Decision)
	for _, event := range controller.Events() {
		assert.NotEqual(t, "owned-sync", event.Operation)
	}
	require.NoError(t, controller.Err())
	faultController.Store(nil)
	inspection, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	assert.Equal(t, request.Expected, inspection.Store.Policy)
}

func TestFaultResizeCancellationDuringCommit(t *testing.T) {
	for _, transition := range []string{"v1", "v2", "migration"} {
		for _, mode := range []Durability{Synchronized, ProcessCrash} {
			t.Run(fmt.Sprintf("%s/%s", transition, mode), func(t *testing.T) {
				directory := migrationFixture(t, resizeFaultConfig(t, transition, mode), 3, 128)
				request := resizeFaultRequest(t, directory, transition)
				before := readCrashLogicalSnapshot(t, directory)
				controller := attachResizeFault(t, directory)
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				cause := errors.New("resize canceled inside commit")
				var calls, commitStart int
				var retained *validatedStore
				committed := false
				observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
					switch stage {
					case "before-commit":
						checkResizeCommitStart(t, opened)
						commitStart = len(controller.Events())
						return installResizeCommitHook(opened, func() int {
							calls++
							cancel(cause)
							return 0
						})
					case "after-commit":
						committed = true
						require.Equal(t, 1, calls)
						require.ErrorIs(t, context.Cause(ctx), cause)
						checkPolicyCommitSync(t, controller.Events()[commitStart:])
					case "before-close":
						retained = opened
					}
					return nil
				}}
				result, err := resizeObserved(ctx, directory, request, observer)
				require.NoError(t, err)
				require.Equal(t, Applied, result.Decision)
				require.True(t, committed)
				require.Equal(t, 1, calls)
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.ErrorIs(t, context.Cause(ctx), cause)
				require.NoError(t, controller.Err())
				require.NotNil(t, retained)
				require.ErrorIs(t, retained.storage.conn.PingContext(t.Context()), sql.ErrConnDone)
				for _, file := range []*os.File{
					retained.main, retained.owner.lock, retained.owner.directory, retained.owner.parent,
				} {
					_, err := file.Stat()
					require.ErrorIs(t, err, os.ErrClosed)
				}
				faultController.Store(nil)
				inspection, err := Inspect(t.Context(), directory, nil)
				require.NoError(t, err)
				require.Equal(t, request.Next, inspection.Store.Policy)
				require.Equal(t, mode, inspection.Store.Durability)
				checkResizeSnapshot(t, directory, before, request.Next)
				result, err = Resize(t.Context(), directory, request)
				require.NoError(t, err)
				require.Equal(t, Applied, result.Decision)
				checkResizeSnapshot(t, directory, before, request.Next)
			})
		}
	}
}

func installResizeCommitHook(opened *validatedStore, hook func() int) error {
	return opened.storage.conn.Raw(func(raw any) error {
		connection, ok := raw.(*sqlite3.SQLiteConn)
		if !ok {
			return ErrStorage
		}
		connection.RegisterCommitHook(hook)
		return nil
	})
}

func checkResizeCommitStart(t *testing.T, opened *validatedStore) {
	t.Helper()
	size, err := opened.offlineWALBytes()
	require.NoError(t, err)
	require.Zero(t, size, "initial TRUNCATE must leave an empty WAL")
	var synchronous int
	require.NoError(t, opened.storage.conn.QueryRowContext(
		t.Context(),
		"PRAGMA synchronous",
	).Scan(&synchronous))
	require.Equal(t, 2, synchronous)
}

func TestFaultResizeFrameSync(t *testing.T) {
	for _, transition := range []string{"v1", "v2", "migration"} {
		for _, mode := range []Durability{Synchronized, ProcessCrash} {
			for _, phase := range []string{"before", "after"} {
				t.Run(fmt.Sprintf("%s/%s/%s", transition, mode, phase), func(t *testing.T) {
					directory := migrationFixture(t, resizeFaultConfig(t, transition, mode), 3, 128)
					request := resizeFaultRequest(t, directory, transition)
					before := readCrashLogicalSnapshot(t, directory)
					controller := attachResizeFault(t, directory)
					var commitStart int
					observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
						if stage != "before-commit" {
							return nil
						}
						checkResizeCommitStart(t, opened)
						commitStart = len(controller.Events())
						return controller.Arm("native-sync", "wal", 2, phase, sqlitefault.SyncError)
					}}
					result, err := resizeObserved(t.Context(), directory, request, observer)
					require.ErrorIs(t, err, ErrStorage)
					require.Equal(t, Unknown, result.Decision)
					require.False(t, controller.Armed())
					require.NoError(t, controller.Err())
					checkResizeFrameSyncFailure(t, controller.Events()[commitStart:], phase)
					faultController.Store(nil)
					inspection, err := Inspect(t.Context(), directory, nil)
					require.NoError(t, err)
					require.Contains(t, []DurablePolicy{request.Expected, request.Next}, inspection.Store.Policy)
					require.Equal(t, mode, inspection.Store.Durability)
					checkResizeSnapshot(t, directory, before, inspection.Store.Policy)
					result, err = Resize(t.Context(), directory, request)
					require.NoError(t, err)
					require.Equal(t, Applied, result.Decision)
					checkResizeSnapshot(t, directory, before, request.Next)
				})
			}
		}
	}
}

// A fresh WAL synchronizes its 32-byte header before writing transaction frames.
// Count completed sync operations so the injected completion is exactly the second one.
func checkResizeFrameSyncFailure(t *testing.T, events []sqlitefault.Event, phase string) {
	t.Helper()
	var headerWrite, headerSync, frameWrite uint64
	var syncs, injected int
	var wal sqlitefault.Identity
	for _, event := range events {
		if event.Injected {
			injected++
			require.Equal(t, "after", event.Phase)
			require.Equal(t, "native-sync", event.Operation)
			require.Equal(t, "wal", event.Role)
		}
		if event.Role != "wal" || event.Phase != "after" {
			continue
		}
		if event.Operation != "native-write" && event.Operation != "native-sync" {
			continue
		}
		require.Equal(t, "queue.sqlite3-wal", event.Name)
		require.Equal(t, event.Expected, event.Actual)
		require.NotZero(t, event.Actual.Inode)
		if wal.Inode == 0 {
			wal = event.Actual
		}
		require.Equal(t, wal, event.Actual)
		if event.Operation == "native-write" {
			require.True(t, event.Performed)
			require.Zero(t, event.ActualResult)
			require.Zero(t, event.Result)
			if event.Offset == 0 && event.Length == 32 {
				require.Zero(t, headerWrite)
				headerWrite = event.Sequence
			} else {
				require.Greater(t, event.Offset+event.Length, int64(32))
				require.Positive(t, headerSync)
				frameWrite = event.Sequence
			}
			continue
		}
		syncs++
		if syncs == 1 {
			require.Positive(t, headerWrite)
			require.Zero(t, frameWrite)
			require.True(t, event.Performed)
			require.Zero(t, event.ActualResult)
			require.Zero(t, event.Result)
			headerSync = event.Sequence
			continue
		}
		if event.Injected {
			require.Equal(t, 2, syncs)
			require.Positive(t, frameWrite)
			require.Greater(t, event.Sequence, frameWrite)
			require.Equal(t, phase == "after", event.Performed)
			require.Zero(t, event.ActualResult)
			require.Zero(t, event.ActualErrno)
			require.Equal(t, int(sqlite3.ErrIoErrFsync), event.Result)
		}
	}
	require.Equal(t, 1, injected)
	require.GreaterOrEqual(t, syncs, 2)
}
