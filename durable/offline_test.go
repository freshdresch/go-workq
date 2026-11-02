package durable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type offlineTestObserver struct {
	observe func(string, *validatedStore) error
}

func (o offlineTestObserver) ObserveOffline(stage string, opened *validatedStore) error {
	return o.observe(stage, opened)
}

func offlineFixture(t *testing.T, config Config) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "queue")
	queue, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
	entries := []Entry{{ID: ID{Sequence: 1}, Codec: "raw", Payload: []byte("pending")}}
	result, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, Applied, result.Decision)
	result, err = queue.RetireThrough(t.Context(), [16]byte{1}, 0)
	require.NoError(t, err)
	require.Equal(t, Applied, result.Decision)
	require.NoError(t, queue.Close(t.Context()))
	return directory
}

func TestInspectBoundsPolicyAndEpoch(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	config.Policy.MaxPendingItems = 32
	config.Policy.MaxReceipts = 64
	config.Policy.DatabaseLimit = 4 << 20
	config.Durability = ProcessCrash
	directory := offlineFixture(t, config)
	for _, ids := range [][]ID{nil, {{Sequence: 1}, {Sequence: 1}, {Sequence: 9}}, make([]ID, 256)} {
		inspection, err := Inspect(t.Context(), directory, ids)
		require.NoError(t, err)
		assert.Equal(t, config.Policy, inspection.Store.Policy)
		assert.Equal(t, ProcessCrash, inspection.Store.Durability)
		assert.Equal(t, uint64(1), inspection.Store.Epoch)
		assert.Equal(t, Count(1), inspection.Store.Counts.PendingItems)
		assert.Len(t, inspection.Statuses, len(ids))
		assert.Positive(t, inspection.Validation.LogicalRecords)
		assert.Zero(t, inspection.Store.WALBytes)
		for i, id := range ids {
			expected := Absent
			if id.Sequence == 1 {
				expected = Pending
			}
			assert.Equal(t, Status{ID: id, Kind: expected}, inspection.Statuses[i])
		}
	}
	absent := filepath.Join(t.TempDir(), "never-created")
	inspection, err := Inspect(t.Context(), absent, make([]ID, 257))
	assert.ErrorIs(t, err, ErrInvalidInput)
	assert.Equal(t, Inspection{}, inspection)
	_, err = os.Stat(absent)
	assert.ErrorIs(t, err, os.ErrNotExist)
	opened, err := openOffline(t.Context(), directory)
	require.NoError(t, err)
	settings, err := opened.storage.readSettings(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(config.Policy.DatabaseLimit/pageSize), settings.MaxPageCount)
	assert.Equal(t, int64(2), settings.Synchronous)
	require.NoError(t, opened.close())
}

func TestOfflineExcludesLiveOwner(t *testing.T) {
	t.Parallel()
	queue, directory := openTestQueue(t, testConfig(t))
	inspection, err := Inspect(t.Context(), directory, nil)
	assert.ErrorIs(t, err, ErrInUse)
	assert.Equal(t, Inspection{}, inspection)
	maintenance, err := Maintain(t.Context(), directory)
	assert.ErrorIs(t, err, ErrInUse)
	assert.False(t, maintenance.HasStore)
	changed, err := ChangeDurability(t.Context(), directory, Synchronized, ProcessCrash)
	assert.ErrorIs(t, err, ErrInUse)
	assert.Equal(t, NotApplied, changed.Decision)
	require.NoError(t, queue.Close(t.Context()))
}

func TestInspectRejectsCancellationAndObservableFailures(t *testing.T) {
	for _, stage := range []string{"before-barrier", "after-barrier", "before-close"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			directory := offlineFixture(t, testConfig(t))
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			cause := errors.New("inspection canceled")
			observer := offlineTestObserver{observe: func(actual string, _ *validatedStore) error {
				if actual == stage {
					cancel(cause)
				}
				return nil
			}}
			inspection, err := inspectObserved(ctx, directory, []ID{{Sequence: 1}}, observer)
			assert.ErrorIs(t, err, context.Canceled)
			assert.ErrorIs(t, err, cause)
			assert.NotErrorIs(t, err, ErrStorage)
			assert.Equal(t, Inspection{}, inspection)
			_, err = Inspect(t.Context(), directory, nil)
			require.NoError(t, err)
		})
	}
	for _, failure := range []string{"identity", "descriptor", "sync", "closure", "close-descriptor"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			directory := offlineFixture(t, testConfig(t))
			injected := errors.New("observable closure")
			observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
				if failure == "closure" && stage == "before-close" {
					return injected
				}
				if failure == "close-descriptor" && stage == "before-close" {
					require.NoError(t, opened.main.Close())
				}
				if stage != "before-barrier" {
					return nil
				}
				switch failure {
				case "identity":
					path := filepath.Join(directory, "queue.sqlite3")
					require.NoError(t, os.Rename(path, path+".original"))
					require.NoError(t, os.WriteFile(path, nil, 0600))
				case "descriptor":
					require.NoError(t, opened.main.Close())
				case "sync":
					require.NoError(t, opened.main.Close())
					path := filepath.Join(directory, "queue.sqlite3")
					// Linux O_PATH supports identity checks but rejects the real fsync.
					const openPath = 0x200000
					fd, err := syscall.Open(path, openPath|syscall.O_CLOEXEC, 0)
					require.NoError(t, err)
					opened.main = os.NewFile(uintptr(fd), path)
					require.NoError(t, verifyOwnedDescriptor(opened.main, path))
				}
				return nil
			}}
			inspection, err := inspectObserved(t.Context(), directory, nil, observer)
			assert.Error(t, err)
			assert.Equal(t, Inspection{}, inspection)
			if failure == "closure" {
				assert.ErrorIs(t, err, injected)
			}
			if failure == "sync" {
				assert.ErrorIs(t, err, syscall.EBADF)
			}
			if failure == "close-descriptor" {
				assert.ErrorIs(t, err, os.ErrClosed)
			}
		})
	}
}

func TestChangeDurabilityDirectionsAndIdempotency(t *testing.T) {
	for _, initial := range []Durability{Synchronized, ProcessCrash} {
		t.Run(initial.String(), func(t *testing.T) {
			t.Parallel()
			config := testConfig(t)
			config.Durability = initial
			directory := offlineFixture(t, config)
			ids := []ID{
				{Sequence: 1}, {Producer: [16]byte{1}}, {Producer: [16]byte{1}, Sequence: 9},
			}
			before, err := Inspect(t.Context(), directory, ids)
			require.NoError(t, err)
			next := Durability(1 - initial)
			result, err := ChangeDurability(t.Context(), directory, next, next)
			assert.ErrorIs(t, err, ErrConfigMismatch)
			assert.Equal(t, NotApplied, result.Decision)
			result, err = ChangeDurability(t.Context(), directory, initial, next)
			require.NoError(t, err)
			assert.Equal(t, Applied, result.Decision)
			result, err = ChangeDurability(t.Context(), directory, initial, next)
			require.NoError(t, err)
			assert.Equal(t, Applied, result.Decision)
			after, err := Inspect(t.Context(), directory, ids)
			require.NoError(t, err)
			assert.Equal(t, next, after.Store.Durability)
			assert.Equal(t, before.Store.UUID, after.Store.UUID)
			assert.Equal(t, before.Store.Epoch, after.Store.Epoch)
			assert.Equal(t, before.Store.Counts, after.Store.Counts)
			assert.Equal(t, before.Statuses, after.Statuses)
			refused, err := Open(t.Context(), directory, config)
			assert.Nil(t, refused)
			assert.ErrorIs(t, err, ErrConfigMismatch)
			config.Durability = next
			queue, err := Open(t.Context(), directory, config)
			require.NoError(t, err)
			require.NoError(t, queue.Close(t.Context()))
		})
	}
}

func TestChangeDurabilityCancellationBoundaryAndClose(t *testing.T) {
	stages := []string{"before-mutation", "before-commit", "after-commit", "before-close"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			directory := offlineFixture(t, testConfig(t))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("closure failed")
			observer := offlineTestObserver{observe: func(actual string, _ *validatedStore) error {
				if actual != stage {
					return nil
				}
				if stage == "before-close" {
					return injected
				}
				cancel()
				return nil
			}}
			result, err := changeDurabilityObserved(ctx, directory, Synchronized, ProcessCrash, observer)
			switch stage {
			case "before-mutation":
				assert.ErrorIs(t, err, context.Canceled)
				assert.Equal(t, NotApplied, result.Decision)
			case "before-close":
				assert.ErrorIs(t, err, injected)
				assert.Equal(t, Unknown, result.Decision)
			default:
				assert.NoError(t, err)
				assert.Equal(t, Applied, result.Decision)
			}
			inspection, err := Inspect(t.Context(), directory, nil)
			require.NoError(t, err)
			expected := ProcessCrash
			if stage == "before-mutation" {
				expected = Synchronized
			}
			assert.Equal(t, expected, inspection.Store.Durability)
		})
	}
}

func logicalOfflineSnapshot(t *testing.T, directory string) []string {
	t.Helper()
	opened, err := openOffline(t.Context(), directory)
	require.NoError(t, err)
	var snapshot []string
	for _, table := range []string{"meta", "producers", "receipts", "payloads"} {
		rows, err := opened.storage.conn.QueryContext(t.Context(), "SELECT * FROM "+table+" ORDER BY 1")
		require.NoError(t, err)
		columns, err := rows.Columns()
		require.NoError(t, err)
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		for rows.Next() {
			require.NoError(t, rows.Scan(pointers...))
			snapshot = append(snapshot, table+fmt.Sprint(values))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	require.NoError(t, opened.close())
	return snapshot
}

func TestMaintainReclaimsBoundedStepsAndPreservesLogicalState(t *testing.T) {
	t.Parallel()
	queue, directory := openTestQueue(t, testConfig(t))
	payload := make([]byte, 16<<10)
	for batch := range 16 {
		entries := make([]Entry, 64)
		for i := range entries {
			entries[i] = Entry{ID: ID{Sequence: uint64(batch*64 + i)}, Payload: payload}
		}
		_, err := queue.SubmitBatch(t.Context(), entries)
		require.NoError(t, err)
	}
	for remaining := 1024; remaining > 0; {
		deliveries, err := queue.Receive(t.Context(), 32)
		require.NoError(t, err)
		tokens := make([]Token, len(deliveries))
		for i := range deliveries {
			tokens[i] = deliveries[i].Token
		}
		_, err = queue.AckBatch(t.Context(), tokens)
		require.NoError(t, err)
		remaining -= len(deliveries)
	}
	require.NoError(t, queue.Close(t.Context()))
	before := logicalOfflineSnapshot(t, directory)
	identity, err := os.Stat(filepath.Join(directory, "queue.sqlite3"))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var freeBefore, freeAfter uint64
	observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
		if stage == "before-step" {
			require.NoError(t, opened.storage.conn.QueryRowContext(t.Context(),
				"PRAGMA freelist_count").Scan(&freeBefore))
		}
		if stage == "after-step" {
			require.NoError(t, opened.storage.conn.QueryRowContext(t.Context(),
				"PRAGMA freelist_count").Scan(&freeAfter))
			cancel()
		}
		return nil
	}}
	partial, err := maintainObserved(ctx, directory, observer)
	assert.ErrorIs(t, err, context.Canceled)
	assert.True(t, partial.HasStore)
	assert.True(t, partial.HasSizes)
	assert.True(t, partial.MoreWork)
	assert.Equal(t, uint32(1), partial.Steps)
	assert.Positive(t, partial.ReclaimedPages)
	assert.LessOrEqual(t, freeBefore-freeAfter, uint64(256))
	assert.Equal(t, before, logicalOfflineSnapshot(t, directory))
	var callFreeBefore uint64
	opened, err := openOffline(t.Context(), directory)
	require.NoError(t, err)
	require.NoError(t, opened.storage.conn.QueryRowContext(t.Context(),
		"PRAGMA freelist_count").Scan(&callFreeBefore))
	require.NoError(t, opened.close())
	complete, err := Maintain(t.Context(), directory)
	require.NoError(t, err)
	assert.LessOrEqual(t, complete.Steps, uint32(16))
	observed, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	assert.LessOrEqual(t, callFreeBefore-observed.Store.FreePages, uint64(4096))
	assert.Less(t, complete.DatabaseAfter, complete.DatabaseBefore)
	assert.Zero(t, complete.WALAfter)
	assert.Equal(t, before, logicalOfflineSnapshot(t, directory))
	afterIdentity, err := os.Stat(filepath.Join(directory, "queue.sqlite3"))
	require.NoError(t, err)
	assert.True(t, os.SameFile(identity, afterIdentity))
	if complete.MoreWork {
		complete, err = Maintain(t.Context(), directory)
		require.NoError(t, err)
	}
	assert.False(t, complete.MoreWork)
	noop, err := Maintain(t.Context(), directory)
	require.NoError(t, err)
	assert.Zero(t, noop.Steps)
	assert.Zero(t, noop.ReclaimedPages)
	assert.False(t, noop.MoreWork)
	assert.Equal(t, before, logicalOfflineSnapshot(t, directory))
}

func TestInspectSurvivingWAL(t *testing.T) {
	if directory := os.Getenv("DURABLE_OFFLINE_WAL_CHILD"); directory != "" {
		config := testConfig(t)
		config.Durability = ProcessCrash
		queue, err := Open(t.Context(), directory, config)
		require.NoError(t, err)
		entries := []Entry{{ID: ID{Sequence: 7}, Payload: []byte("wal")}}
		result, err := queue.SubmitBatch(t.Context(), entries)
		require.NoError(t, err)
		require.Equal(t, Applied, result.Decision)
		// Exit without running Go cleanup or SQLite destructors, preserving the WAL.
		os.Exit(0)
	}
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "queue")
	executable, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), executable, "-test.run=^TestInspectSurvivingWAL$")
	command.Env = append(os.Environ(), "DURABLE_OFFLINE_WAL_CHILD="+directory)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	wal, err := os.Stat(filepath.Join(directory, "queue.sqlite3-wal"))
	require.NoError(t, err)
	require.Positive(t, wal.Size())
	inspection, err := Inspect(t.Context(), directory, []ID{{Sequence: 7}})
	require.NoError(t, err)
	assert.Equal(t, Pending, inspection.Statuses[0].Kind)
	assert.Equal(t, ProcessCrash, inspection.Store.Durability)
	assert.Equal(t, uint64(1), inspection.Store.Epoch)
	assert.Zero(t, inspection.Store.WALBytes)
	again, err := Inspect(t.Context(), directory, []ID{{Sequence: 7}})
	require.NoError(t, err)
	assert.Equal(t, inspection.Store, again.Store)
	assert.Equal(t, inspection.Statuses, again.Statuses)
}

func TestOfflinePinnedReaderCategory(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		t.Run(fmt.Sprintf("close-failure=%t", failClose), func(t *testing.T) {
			t.Parallel()
			testOfflinePinnedReader(t, failClose)
		})
	}
}

func testOfflinePinnedReader(t *testing.T, failClose bool) {
	t.Helper()
	config := testConfig(t)
	directory := offlineFixture(t, config)
	var reader *store
	observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
		if stage == "before-close" && failClose {
			require.NoError(t, opened.main.Close())
		}
		if stage != "before-barrier" {
			return nil
		}
		var err error
		reader, err = connectStore(t.Context(), filepath.Join(directory, "queue.sqlite3"), config, false)
		require.NoError(t, err)
		_, err = reader.conn.ExecContext(t.Context(), "BEGIN")
		require.NoError(t, err)
		var count int
		row := reader.conn.QueryRowContext(t.Context(), "SELECT count(*) FROM meta")
		require.NoError(t, row.Scan(&count))
		order := encodeUint64(2)
		_, err = opened.storage.conn.ExecContext(t.Context(),
			"UPDATE meta SET value=? WHERE key=?", order[:], metaLastOrder)
		require.NoError(t, err)
		_, err = opened.storage.conn.ExecContext(t.Context(), "PRAGMA busy_timeout=0")
		require.NoError(t, err)
		return nil
	}}
	inspection, err := inspectObserved(t.Context(), directory, nil, observer)
	assert.ErrorIs(t, err, ErrMaintenanceRequired)
	var diagnostic *MaintenanceError
	require.ErrorAs(t, err, &diagnostic)
	assert.Equal(t, CheckpointOperation, diagnostic.Operation)
	assert.Equal(t, CheckpointBlocked, diagnostic.Reason)
	assert.True(t, diagnostic.WALValid)
	assert.Positive(t, diagnostic.WALBytes)
	assert.True(t, diagnostic.Checkpoint.Valid)
	assert.Equal(t, int64(1), diagnostic.Checkpoint.Busy)
	assert.Greater(t, diagnostic.Checkpoint.Frames, diagnostic.Checkpoint.Completed)
	assert.Positive(t, diagnostic.Checkpoint.Duration)
	assert.Less(t, len(diagnostic.Error()), 256)
	if failClose {
		assert.ErrorIs(t, err, ErrStorage)
		assert.ErrorIs(t, err, os.ErrClosed)
	} else {
		assert.NotErrorIs(t, err, ErrStorage)
	}
	assert.Equal(t, Inspection{}, inspection)
	t.Logf("checkpoint category: maintenance=%t storage=%t error=%v",
		errors.Is(err, ErrMaintenanceRequired), errors.Is(err, ErrStorage), err)
	require.NotNil(t, reader)
	_, rollbackErr := reader.conn.ExecContext(t.Context(), "ROLLBACK")
	require.NoError(t, rollbackErr)
	require.NoError(t, reader.close())
	_, err = Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
}

func TestInspectRejectsStoredDatabaseCapViolation(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	directory := offlineFixture(t, config)
	storage, err := connectStore(t.Context(), filepath.Join(directory, "queue.sqlite3"), config, false)
	require.NoError(t, err)
	policy := config.Policy
	policy.DatabaseLimit = pageSize
	encoded := encodePolicy(policy)
	_, err = storage.conn.ExecContext(t.Context(),
		"UPDATE meta SET value=? WHERE key=?", encoded[:], metaPolicy)
	require.NoError(t, err)
	require.NoError(t, storage.close())
	inspection, err := Inspect(t.Context(), directory, nil)
	assert.Equal(t, Inspection{}, inspection)
	assert.ErrorIs(t, err, ErrStorage)
	assert.NotErrorIs(t, err, ErrConfigMismatch)
}

func TestInspectRejectsBootstrapDatabaseCapViolation(t *testing.T) {
	t.Parallel()
	directory := offlineFixture(t, testConfig(t))
	path := filepath.Join(directory, "queue.sqlite3")
	original, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(path, (1<<30)+int64(pageSize)))
	t.Cleanup(func() { assert.NoError(t, os.Truncate(path, original.Size())) })
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(1<<30)+int64(pageSize), info.Size())
	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	assert.Less(t, stat.Blocks*512, int64(1<<20), "fixture must remain sparse")
	inspection, err := Inspect(t.Context(), directory, nil)
	assert.Equal(t, Inspection{}, inspection)
	assert.ErrorIs(t, err, ErrStorage)
	assert.NotErrorIs(t, err, ErrConfigMismatch)
	require.NoError(t, os.Truncate(path, original.Size()))
}

func TestOfflineConfigurationErrorPreservesStorageCauses(t *testing.T) {
	t.Parallel()
	cause := &os.PathError{Op: "close", Path: "queue.sqlite3", Err: syscall.EIO}
	translated := offlineConfigurationError(cause)
	assert.NotErrorIs(t, translated, ErrConfigMismatch)
	assert.ErrorIs(t, translated, syscall.EIO)
	var actual *os.PathError
	require.ErrorAs(t, translated, &actual)
	assert.Same(t, cause, actual)
	assert.ErrorIs(t, offlineConfigurationError(ErrUnsupported), ErrUnsupported)
}

func TestInspectSyncFailureAndCancellation(t *testing.T) {
	for _, target := range []string{"main", "directory", "parent"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			directory := offlineFixture(t, testConfig(t))
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			cause := errors.New("inspection canceled during closure")
			observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
				if stage == "before-close" {
					cancel(cause)
				}
				if stage != "before-barrier" {
					return nil
				}
				file := &opened.main
				path := filepath.Join(directory, "queue.sqlite3")
				switch target {
				case "directory":
					file = &opened.owner.directory
					path = directory
				case "parent":
					file = &opened.owner.parent
					path = filepath.Dir(directory)
				}
				require.NoError(t, (*file).Close())
				// O_PATH retains same-file identity but rejects the actual fsync.
				const openPath = 0x200000
				fd, err := syscall.Open(path, openPath|syscall.O_CLOEXEC, 0)
				require.NoError(t, err)
				*file = os.NewFile(uintptr(fd), path)
				require.NoError(t, verifyOwnedDescriptor(*file, path))
				return nil
			}}
			inspection, err := inspectObserved(ctx, directory, nil, observer)
			assert.Equal(t, Inspection{}, inspection)
			assert.ErrorIs(t, err, syscall.EBADF)
			assert.ErrorIs(t, err, context.Canceled)
			assert.ErrorIs(t, err, cause)
			assert.ErrorIs(t, err, ErrStorage)
		})
	}
}

func TestOpenAndOfflineRejectInvalidStoredState(t *testing.T) {
	for _, test := range []struct {
		name     string
		mutation string
		category error
	}{
		{name: "count", mutation: "UPDATE meta SET value=zeroblob(8) WHERE key=10",
			category: ErrStorage},
		{name: "digest", mutation: "UPDATE payloads SET payload=zeroblob(payload_length)",
			category: ErrStorage},
		{name: "missing-payload", mutation: "DELETE FROM payloads", category: ErrStorage},
		{name: "extra-schema", mutation: "CREATE TABLE z_extra(value BLOB)",
			category: ErrUnsupported},
		{name: "wrong-schema", mutation: "DROP INDEX pending_order;" +
			"CREATE INDEX pending_order ON receipts(id)", category: ErrUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := testConfig(t)
			directory := offlineFixture(t, config)
			opened, err := openOffline(t.Context(), directory)
			require.NoError(t, err)
			_, err = opened.storage.conn.ExecContext(t.Context(), test.mutation)
			require.NoError(t, err)
			require.NoError(t, opened.close())

			queue, err := Open(t.Context(), directory, config)
			assert.ErrorIs(t, err, test.category)
			if queue != nil {
				assert.NoError(t, queue.Close(t.Context()))
			}
			assert.Nil(t, queue)
			inspection, err := Inspect(t.Context(), directory, nil)
			assert.ErrorIs(t, err, test.category)
			assert.Equal(t, Inspection{}, inspection)
			maintenance, err := Maintain(t.Context(), directory)
			assert.ErrorIs(t, err, test.category)
			assert.False(t, maintenance.HasStore)
			changed, err := ChangeDurability(t.Context(), directory, Synchronized, ProcessCrash)
			assert.ErrorIs(t, err, test.category)
			assert.Equal(t, NotApplied, changed.Decision)
		})
	}
}

func TestOfflineDoesNotCreateArtifacts(t *testing.T) {
	operations := []struct {
		name string
		call func(context.Context, string) error
	}{
		{"inspect", func(ctx context.Context, path string) error {
			_, err := Inspect(ctx, path, nil)
			return err
		}},
		{"maintain", func(ctx context.Context, path string) error {
			_, err := Maintain(ctx, path)
			return err
		}},
		{"change", func(ctx context.Context, path string) error {
			_, err := ChangeDurability(ctx, path, Synchronized, ProcessCrash)
			return err
		}},
	}
	for _, operation := range operations {
		for _, state := range []string{"absent", "empty", "invalid"} {
			t.Run(operation.name+"/"+state, func(t *testing.T) {
				directory := filepath.Join(t.TempDir(), "queue")
				if state != "absent" {
					require.NoError(t, os.Mkdir(directory, 0700))
				}
				if state == "invalid" {
					require.NoError(t, os.WriteFile(filepath.Join(directory, "LOCK"), []byte("lock"), 0600))
					require.NoError(t, os.WriteFile(filepath.Join(directory, "unknown"), []byte("keep"), 0600))
				}
				require.ErrorIs(t, operation.call(t.Context(), directory), ErrStorage)
				if state == "absent" {
					_, err := os.Stat(directory)
					require.ErrorIs(t, err, os.ErrNotExist)
				} else {
					entries, err := os.ReadDir(directory)
					require.NoError(t, err)
					if state == "empty" {
						assert.Empty(t, entries)
					} else {
						require.Len(t, entries, 2)
						for name, contents := range map[string]string{"LOCK": "lock", "unknown": "keep"} {
							actual, err := os.ReadFile(filepath.Join(directory, name))
							require.NoError(t, err)
							assert.Equal(t, contents, string(actual))
						}
						return
					}
				}
				queue, err := Open(t.Context(), directory, testConfig(t))
				require.NoError(t, err)
				require.NoError(t, queue.Close(t.Context()))
			})
		}
	}
}

func TestOfflineCheckpointRetainedReader(t *testing.T) {
	for _, failure := range []string{"none", "wal", "close"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			directory := offlineFixture(t, testConfig(t))
			opened, err := openOffline(t.Context(), directory)
			require.NoError(t, err)
			closed := false
			t.Cleanup(func() {
				if !closed {
					assert.NoError(t, opened.close())
				}
			})
			rows, err := opened.storage.conn.QueryContext(t.Context(), "SELECT * FROM meta")
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, rows.Close()) })
			require.True(t, rows.Next())
			if failure == "wal" {
				loop := filepath.Join(t.TempDir(), "loop")
				require.NoError(t, os.Symlink(loop, loop))
				opened.owner.path = loop
			}
			err = opened.offlineCheckpoint(t.Context())
			opened.owner.path = directory
			require.NoError(t, rows.Close())
			if failure == "close" {
				require.NoError(t, opened.main.Close())
				err = errors.Join(err, closeOffline(opened, nil))
				closed = true
			}
			assert.ErrorIs(t, err, ErrMaintenanceRequired)
			var sqliteErr sqlite3.Error
			require.ErrorAs(t, err, &sqliteErr)
			assert.Equal(t, sqlite3.ErrLocked, sqliteErr.Code)
			if failure == "none" {
				assert.NotErrorIs(t, err, ErrStorage)
			} else {
				assert.ErrorIs(t, err, ErrStorage)
			}
			if failure == "wal" {
				assert.ErrorIs(t, err, syscall.ELOOP)
			}
			if failure == "close" {
				assert.ErrorIs(t, err, os.ErrClosed)
			}
			var diagnostic *MaintenanceError
			require.ErrorAs(t, err, &diagnostic)
			assert.Equal(t, CheckpointOperation, diagnostic.Operation)
			assert.Equal(t, CheckpointBlocked, diagnostic.Reason)
			assert.Equal(t, failure != "wal", diagnostic.WALValid)
			assert.False(t, diagnostic.Checkpoint.Valid)
			assert.Zero(t, diagnostic.Checkpoint.Busy)
			assert.Zero(t, diagnostic.Checkpoint.Frames)
			assert.Zero(t, diagnostic.Checkpoint.Completed)
			assert.Positive(t, diagnostic.Checkpoint.Duration)
		})
	}
}
