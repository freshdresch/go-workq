package durable

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartupPreservesInterruptedLockOnlyDirectory(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "LOCK"), nil, 0600))
	opened, err := startStore(t.Context(), directory, testConfig(t))
	if opened != nil {
		require.NoError(t, opened.close())
	}
	assert.Nil(t, opened)
	assert.ErrorIs(t, err, ErrStorage)
	_, statErr := os.Stat(filepath.Join(directory, "queue.sqlite3"))
	assert.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestStartupCreateReopenAndPolicy(t *testing.T) {
	for _, durability := range []Durability{Synchronized, ProcessCrash} {
		t.Run(durability.String(), func(t *testing.T) {
			t.Parallel()
			directory := filepath.Join(t.TempDir(), "queue")
			config := testConfig(t)
			config.Durability = durability
			first, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			assert.Equal(t, uint64(1), first.metadata.epoch)
			uuid := first.metadata.uuid
			settings, err := first.storage.readSettings(t.Context())
			require.NoError(t, err)
			expected := int64(2)
			if durability == ProcessCrash {
				expected = 1
			}
			assert.Equal(t, expected, settings.Synchronous)
			require.NoError(t, first.close())
			_, err = os.Stat(filepath.Join(directory, "queue.sqlite3-wal"))
			require.ErrorIs(t, err, os.ErrNotExist)
			second, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			assert.Equal(t, uuid, second.metadata.uuid)
			assert.Equal(t, uint64(2), second.metadata.epoch)
			wal, err := os.Stat(filepath.Join(directory, "queue.sqlite3-wal"))
			require.NoError(t, err)
			assert.Positive(t, wal.Size())
			require.NoError(t, second.close())
			other := config
			other.Durability = Durability(1 - byte(durability))
			refused, err := startStore(t.Context(), directory, other)
			assert.Nil(t, refused)
			assert.ErrorIs(t, err, ErrConfigMismatch)
			reopened, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			assert.Equal(t, uint64(3), reopened.metadata.epoch)
			require.NoError(t, reopened.close())
		})
	}
}

func TestStartupOwnershipAndCanonicalAlias(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	directory := filepath.Join(parent, "queue")
	opened, err := startStore(t.Context(), directory, testConfig(t))
	require.NoError(t, err)
	alias := filepath.Join(parent, "alias")
	require.NoError(t, os.Symlink(directory, alias))
	duplicate, err := startStore(t.Context(), alias, testConfig(t))
	assert.Nil(t, duplicate)
	assert.ErrorIs(t, err, ErrInUse)
	require.NoError(t, opened.close())
	reopened, err := startStore(t.Context(), alias, testConfig(t))
	require.NoError(t, err)
	assert.Equal(t, directory, reopened.owner.path)
	require.NoError(t, reopened.close())
}

func TestStartupChildExclusion(t *testing.T) {
	if directory := os.Getenv("DURABLE_LOCK_CHILD"); directory != "" {
		opened, err := startStore(t.Context(), directory, testConfig(t))
		assert.Nil(t, opened)
		require.ErrorIs(t, err, ErrInUse)
		return
	}
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "queue")
	opened, err := startStore(t.Context(), directory, testConfig(t))
	require.NoError(t, err)
	executable, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), executable, "-test.run=^TestStartupChildExclusion$")
	command.Env = append(os.Environ(), "DURABLE_LOCK_CHILD="+directory)
	output, err := command.CombinedOutput()
	assert.NoError(t, err, "%s", output)
	require.NoError(t, opened.close())
	reopened, err := startStore(t.Context(), directory, testConfig(t))
	require.NoError(t, err)
	require.NoError(t, reopened.close())
}

type cancelStartup struct {
	stage  startupStage
	cancel context.CancelFunc
}

func (c *cancelStartup) Observe(stage startupStage) {
	if stage == c.stage {
		c.cancel()
	}
}

func TestStartupCancellationAtEpochBoundary(t *testing.T) {
	for _, stage := range []startupStage{startupBeforeEpoch, startupAfterEpoch} {
		t.Run(stage.String(), func(t *testing.T) {
			t.Parallel()
			directory := filepath.Join(t.TempDir(), "queue")
			config := testConfig(t)
			initial, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			require.NoError(t, initial.close())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			observer := &cancelStartup{stage: stage, cancel: cancel}
			refused, err := startStoreObserved(ctx, directory, config, observer)
			assert.Nil(t, refused)
			require.ErrorIs(t, err, context.Canceled)
			recovered, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			expected := uint64(2)
			if stage == startupAfterEpoch {
				expected = 3
			}
			assert.Equal(t, expected, recovered.metadata.epoch)
			require.NoError(t, recovered.close())
		})
	}
}

func TestStartupCorruptionAndUnsupportedSchema(t *testing.T) {
	tests := []struct {
		name     string
		mutate   string
		args     []any
		expected error
	}{
		{name: "count", mutate: "UPDATE meta SET value=? WHERE key=10",
			args: []any{[]byte{0, 0, 0, 0, 0, 0, 0, 1}}, expected: ErrStorage},
		{name: "schema-version", mutate: "UPDATE meta SET value=? WHERE key=1",
			args: []any{[]byte{0, 0, 0, 0, 0, 0, 0, 2}}, expected: ErrUnsupported},
		{name: "metadata-length", mutate: "UPDATE meta SET value=x'00' WHERE key=8",
			expected: ErrStorage},
		{name: "extra-table", mutate: "CREATE TABLE extra(value BLOB)", expected: ErrUnsupported},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			directory := filepath.Join(t.TempDir(), "queue")
			config := testConfig(t)
			opened, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			_, err = opened.storage.conn.ExecContext(t.Context(), "PRAGMA ignore_check_constraints=ON")
			require.NoError(t, err)
			_, err = opened.storage.conn.ExecContext(t.Context(), test.mutate, test.args...)
			require.NoError(t, err)
			require.NoError(t, opened.close())
			refused, err := startStore(t.Context(), directory, config)
			assert.Nil(t, refused)
			assert.ErrorIs(t, err, test.expected)
			owner, err := acquireDirectory(directory)
			require.NoError(t, err)
			require.NoError(t, owner.close())
		})
	}
}

func TestOwnedDescriptorRejectsPathReplacement(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "original")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0600))
	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { assert.NoError(t, file.Close()) }()
	require.NoError(t, os.Rename(path, path+".retained"))
	require.NoError(t, os.WriteFile(path, []byte("new"), 0600))
	assert.ErrorIs(t, syncOwnedDescriptor(file, path), ErrStorage)
}

func seedPending(t *testing.T, opened *validatedStore) {
	t.Helper()
	id := encodeID(ID{Producer: [16]byte{0xff}, Sequence: ^uint64(0)})
	order := encodeUint64(1)
	floor := encodeUint64(0)
	payload := []byte("payload")
	digest := entryDigest("raw", payload)
	_, err := opened.storage.conn.ExecContext(t.Context(),
		"INSERT INTO producers VALUES(?,0,?,0)", id[:16], floor[:])
	require.NoError(t, err)
	_, err = opened.storage.conn.ExecContext(t.Context(),
		"INSERT INTO receipts VALUES(?,?,0,?,NULL)", id[:], digest[:], order[:])
	require.NoError(t, err)
	_, err = opened.storage.conn.ExecContext(t.Context(),
		"INSERT INTO payloads VALUES(?,?,?,?)", id[:], []byte("raw"), payload, len(payload))
	require.NoError(t, err)
	for _, key := range []int{metaLastOrder, metaPendingItems, metaReceipts, metaProducers} {
		_, err = opened.storage.conn.ExecContext(t.Context(),
			"UPDATE meta SET value=? WHERE key=?", order[:], key)
		require.NoError(t, err)
	}
	length := encodeUint64(uint64(len(payload)))
	_, err = opened.storage.conn.ExecContext(t.Context(),
		"UPDATE meta SET value=? WHERE key=?", length[:], metaPendingPayload)
	require.NoError(t, err)
}

func TestStartupValidatesPendingPayloadAndRelationships(t *testing.T) {
	tests := []struct{ name, mutation string }{
		{name: "valid"},
		{name: "digest", mutation: "UPDATE receipts SET digest=zeroblob(32)"},
		{name: "payload-length", mutation: "UPDATE payloads SET payload_length=2"},
		{name: "payload-limit", mutation: "UPDATE payloads SET payload=zeroblob(16385)"},
		{name: "missing-payload", mutation: "DELETE FROM payloads"},
		{name: "missing-producer", mutation: "DELETE FROM producers"},
		{name: "payload-type", mutation: "UPDATE payloads SET payload='payload'"},
		{name: "receipt-length", mutation: "UPDATE receipts SET id=x'00'"},
		{name: "zero-order", mutation: "UPDATE receipts SET accept_order=zeroblob(8)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := testConfig(t)
			directory := filepath.Join(t.TempDir(), "queue")
			opened, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			seedPending(t, opened)
			if test.mutation != "" {
				_, err = opened.storage.conn.ExecContext(t.Context(), "PRAGMA ignore_check_constraints=ON")
				require.NoError(t, err)
				_, err = opened.storage.conn.ExecContext(t.Context(), test.mutation)
				require.NoError(t, err)
			}
			require.NoError(t, opened.close())
			recovered, err := startStore(t.Context(), directory, config)
			if test.mutation == "" {
				require.NoError(t, err)
				assert.Equal(t, Count(1), recovered.metadata.counts.PendingItems)
				assert.Equal(t, Bytes(7), recovered.metadata.counts.PendingPayload)
				assert.Equal(t, uint64(18), recovered.validation.LogicalRecords)
				require.NoError(t, recovered.close())
				return
			}
			assert.Nil(t, recovered)
			assert.ErrorIs(t, err, ErrStorage)
			owner, err := acquireDirectory(directory)
			require.NoError(t, err)
			require.NoError(t, owner.close())
		})
	}
}

func TestStartupCanceledBeforeCreation(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "queue")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	opened, err := startStore(ctx, directory, testConfig(t))
	assert.Nil(t, opened)
	assert.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(directory)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestStartupRejectsUnexpectedArtifact(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "unrelated"), []byte("retain"), 0600))
	opened, err := startStore(t.Context(), directory, testConfig(t))
	assert.Nil(t, opened)
	assert.ErrorIs(t, err, ErrStorage)
	contents, err := os.ReadFile(filepath.Join(directory, "unrelated"))
	require.NoError(t, err)
	assert.Equal(t, "retain", string(contents))
}

func TestStartupSmallerDatabasePolicyMismatch(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "queue")
	config := testConfig(t)
	opened, err := startStore(t.Context(), directory, config)
	require.NoError(t, err)
	require.NoError(t, opened.close())
	smaller := config
	smaller.Policy.DatabaseLimit = 4096
	require.NoError(t, validateConfig(smaller))
	refused, err := startStore(t.Context(), directory, smaller)
	assert.Nil(t, refused)
	assert.ErrorIs(t, err, ErrConfigMismatch)
	reopened, reopenErr := startStore(t.Context(), directory, config)
	require.NoError(t, reopenErr)
	assert.Equal(t, uint64(2), reopened.metadata.epoch)
	require.NoError(t, reopened.close())
}

func TestRecordValidationClosesEarlierRowsAfterLaterQueryError(t *testing.T) {
	t.Parallel()
	storage, _ := newTestStore(t)
	_, err := storage.conn.ExecContext(t.Context(), "DROP TABLE receipts")
	require.NoError(t, err)
	require.NotPanics(t, func() {
		_, _, err := storage.validateRecords(t.Context(), storeMetadata{policy: testConfig(t).Policy})
		assert.Error(t, err)
	})
}

func TestOwnershipReleaseWithDuplicatedDescriptor(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	owner, err := acquireDirectory(directory)
	require.NoError(t, err)
	duplicate, err := syscall.Dup(int(owner.lock.Fd()))
	require.NoError(t, err)
	defer func() { assert.NoError(t, syscall.Close(duplicate)) }()
	require.NoError(t, owner.close())
	replacement, err := acquireDirectory(directory)
	require.NoError(t, err)
	require.NoError(t, replacement.close())
}

func TestStartupClassifiesMissingParent(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "absent", "queue")
	opened, err := startStore(t.Context(), directory, testConfig(t))
	assert.Nil(t, opened)
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.ErrorIs(t, err, ErrStorage)
}

func TestStartupClassifiesInitializationFull(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	config.Policy.DatabaseLimit = 4096
	directory := filepath.Join(t.TempDir(), "queue")
	opened, err := startStore(t.Context(), directory, config)
	assert.Nil(t, opened)
	assert.ErrorIs(t, err, ErrStorage)
	var sqliteErr sqlite3.Error
	require.ErrorAs(t, err, &sqliteErr)
	assert.Equal(t, sqlite3.ErrFull, sqliteErr.Code)
	owner, err := acquireDirectory(directory)
	require.NoError(t, err)
	require.NoError(t, owner.close())
}

type observeStartup func(startupStage)

func (observe observeStartup) Observe(stage startupStage) { observe(stage) }

func TestStartupCanceledCreationReopenSynchronizesParent(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "queue")
	config := testConfig(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	refused, err := startStoreObserved(ctx, directory, config, &cancelStartup{
		stage: startupBeforeEpoch, cancel: cancel,
	})
	assert.Nil(t, refused)
	require.ErrorIs(t, err, context.Canceled)
	parentSynced := false
	observer := observeStartup(func(stage startupStage) {
		if stage == startupAfterParentSync {
			parentSynced = true
		}
	})
	opened, err := startStoreObserved(t.Context(), directory, config, observer)
	require.NoError(t, err)
	assert.True(t, parentSynced, "successful reopen must synchronize the queue's parent entry")
	assert.Equal(t, uint64(1), opened.metadata.epoch)
	require.NoError(t, opened.close())
}

func TestStartupConcurrentCreatorSynchronizesParent(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "queue")
	config := testConfig(t)
	created := make(chan struct{})
	resume := make(chan struct{})
	result := make(chan error, 1)
	observer := observeStartup(func(stage startupStage) {
		if stage == startupDirectoryReady {
			close(created)
			<-resume
		}
	})
	go func() {
		opened, err := startStoreObserved(t.Context(), directory, config, observer)
		if opened != nil {
			err = errors.Join(err, opened.close())
		}
		result <- err
	}()
	<-created
	parentSynced := false
	winnerObserver := observeStartup(func(stage startupStage) {
		if stage == startupAfterParentSync {
			parentSynced = true
		}
	})
	winner, err := startStoreObserved(t.Context(), directory, config, winnerObserver)
	close(resume)
	loserErr := <-result
	require.NoError(t, err)
	assert.ErrorIs(t, loserErr, ErrInUse)
	assert.True(t, parentSynced, "the flock winner must synchronize the creator's parent entry")
	assert.Equal(t, uint64(1), winner.metadata.epoch)
	require.NoError(t, winner.close())
	reopened, err := startStore(t.Context(), directory, config)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), reopened.metadata.epoch)
	require.NoError(t, reopened.close())
}

func TestStartupExistingParentDescriptorFailure(t *testing.T) {
	for _, invalid := range []string{"wrong", "closed"} {
		t.Run(invalid, func(t *testing.T) {
			t.Parallel()
			directory := filepath.Join(t.TempDir(), "queue")
			config := testConfig(t)
			initial, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			require.NoError(t, initial.close())
			opened, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			parent := opened.owner.parent
			badParent, err := os.Open(t.TempDir())
			require.NoError(t, err)
			if invalid == "closed" {
				require.NoError(t, badParent.Close())
			} else {
				defer func() { assert.NoError(t, badParent.Close()) }()
			}
			opened.owner.parent = badParent
			err = opened.synchronizeCreation(nil)
			opened.owner.parent = parent
			if invalid == "closed" {
				assert.ErrorIs(t, err, os.ErrClosed)
			} else {
				assert.ErrorIs(t, err, ErrStorage)
			}
			require.NoError(t, opened.close())
		})
	}
}

func TestStartupStoredProfileErrorCategories(t *testing.T) {
	for _, unknown := range []bool{true, false} {
		name := "supported-malformed-limits"
		if unknown {
			name = "unknown-profile"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			directory := filepath.Join(t.TempDir(), "queue")
			config := testConfig(t)
			opened, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			key := metaPolicy
			policy := encodePolicy(config.Policy)
			value := make([]byte, len(policy))
			if unknown {
				key = metaProfile
				value = []byte("sqlite-wal-v2")
			}
			_, err = opened.storage.conn.ExecContext(t.Context(),
				"UPDATE meta SET value=? WHERE key=?", value, key)
			require.NoError(t, err)
			require.NoError(t, opened.close())
			refused, err := startStore(t.Context(), directory, config)
			assert.Nil(t, refused)
			if unknown {
				assert.ErrorIs(t, err, ErrUnsupported)
				assert.NotErrorIs(t, err, ErrStorage)
				assert.NotErrorIs(t, err, ErrInvalidInput)
			} else {
				assert.ErrorIs(t, err, ErrStorage)
				assert.NotErrorIs(t, err, ErrUnsupported)
				assert.NotErrorIs(t, err, ErrInvalidInput)
			}
			owner, err := acquireDirectory(directory)
			require.NoError(t, err)
			storage, err := connectStore(t.Context(),
				filepath.Join(directory, "queue.sqlite3"), config, false)
			require.NoError(t, err)
			if unknown {
				value = []byte(config.Policy.Profile)
			} else {
				value = policy[:]
			}
			_, err = storage.conn.ExecContext(t.Context(),
				"UPDATE meta SET value=? WHERE key=?", value, key)
			require.NoError(t, err)
			require.NoError(t, storage.close())
			require.NoError(t, owner.close())
			reopened, err := startStore(t.Context(), directory, config)
			require.NoError(t, err)
			assert.Equal(t, uint64(2), reopened.metadata.epoch, "refusal must not advance epoch")
			require.NoError(t, reopened.close())
		})
	}
}
