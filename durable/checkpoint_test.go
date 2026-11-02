package durable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReviewCheckpointTriggerBeforeNextGroup(t *testing.T) {
	config := testConfig(t)
	config.Runtime.SQLite.CheckpointTrigger = 4 << 20
	queue, directory := openTestQueue(t, config)
	queue.StopDelivery()
	for batch := range 3 {
		entries := make([]Entry, 120)
		for index := range entries {
			entries[index] = Entry{ID: ID{Sequence: uint64(batch*120 + index)}, Payload: make([]byte, 16384)}
		}
		_, err := queue.SubmitBatch(t.Context(), entries)
		require.NoError(t, err)
		info, err := os.Stat(filepath.Join(directory, "queue.sqlite3-wal"))
		require.NoError(t, err)
		if info.Size() > int64(config.Runtime.SQLite.CheckpointTrigger) {
			break
		}
	}
	before, err := os.Stat(filepath.Join(directory, "queue.sqlite3-wal"))
	require.NoError(t, err)
	require.Greater(t, before.Size(), int64(config.Runtime.SQLite.CheckpointTrigger))
	_, err = queue.SubmitBatch(t.Context(), nil)
	require.NoError(t, err)
	after, err := os.Stat(filepath.Join(directory, "queue.sqlite3-wal"))
	require.NoError(t, err)
	t.Logf("WAL before next group=%d, after=%d, trigger=%d",
		before.Size(), after.Size(), config.Runtime.SQLite.CheckpointTrigger)
	assert.Less(t, after.Size(), int64(config.Runtime.SQLite.CheckpointTrigger))
}

func TestPinnedReaderStopsWritesAndPreservesReconciliation(t *testing.T) {
	config := testConfig(t)
	config.Runtime.SQLite.BusyTimeout = 0
	config.Runtime.SQLite.CheckpointTrigger = 4 << 20
	directory := filepath.Join(t.TempDir(), "queue")
	queue, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	defer func() { _ = queue.Close(context.Background()) }()
	_, err = queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 9000}}})
	require.NoError(t, err)
	deliveries, err := queue.Receive(t.Context(), 1)
	require.NoError(t, err)
	reader, err := sql.Open("sqlite3", filepath.Join(directory, "queue.sqlite3"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	tx, err := reader.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var count int
	require.NoError(t, tx.QueryRow("SELECT count(*) FROM receipts").Scan(&count))
	for batch := range 2 {
		entries := make([]Entry, 120)
		for index := range entries {
			entries[index] = Entry{ID: ID{Sequence: uint64(batch*120 + index)}, Payload: make([]byte, 16384)}
		}
		_, err = queue.SubmitBatch(t.Context(), entries)
		require.NoError(t, err)
	}
	result, err := queue.SubmitBatch(t.Context(), nil)
	require.ErrorIs(t, err, ErrMaintenanceRequired)
	assert.Equal(t, NotApplied, result.Decision)
	<-queue.Stopping()
	assert.Equal(t, MaintenanceRequired, queue.Snapshot().State)
	assert.True(t, queue.Snapshot().Cause.Maintenance)
	stoppedCommits := queue.Snapshot().Outcomes.Commits
	stoppedWAL := queue.Snapshot().Store.WALBytes
	_, err = queue.Receive(t.Context(), 1)
	assert.ErrorIs(t, err, ErrMaintenanceRequired)
	result, err = queue.AckBatch(t.Context(), nil)
	assert.ErrorIs(t, err, ErrMaintenanceRequired)
	assert.Equal(t, NotApplied, result.Decision)
	result, err = queue.RetireThrough(t.Context(), [16]byte{1}, 0)
	assert.ErrorIs(t, err, ErrMaintenanceRequired)
	assert.Equal(t, NotApplied, result.Decision)
	statuses, err := queue.Lookup(t.Context(), []ID{{Sequence: 9000}})
	require.NoError(t, err)
	assert.Equal(t, Pending, statuses[0].Kind)
	require.NoError(t, queue.Release(deliveries[0].Token))
	assert.Equal(t, stoppedCommits, queue.Snapshot().Outcomes.Commits)
	assert.Equal(t, stoppedWAL, queue.Snapshot().Store.WALBytes)
	require.NoError(t, tx.Rollback())
	// Removing pressure never reactivates a session.
	result, err = queue.SubmitBatch(t.Context(), nil)
	assert.ErrorIs(t, err, ErrMaintenanceRequired)
	assert.Equal(t, NotApplied, result.Decision)
	require.ErrorIs(t, queue.Close(t.Context()), ErrMaintenanceRequired)
	assert.False(t, queue.Snapshot().Cause.Storage)
}

type fixedWAL struct{ size Bytes }

func (w fixedWAL) WALBytes() (Bytes, error) { return w.size, nil }

func TestEnvelopeComparedBeforeCheckpointAndNeverRecovers(t *testing.T) {
	for _, size := range []Bytes{99, 100, 101} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			queue := groupFixture(t, testConfig(t))
			queue.stopping = make(chan struct{})
			queue.walEnvelope = 100
			queue.walObserver = fixedWAL{size: size}
			err := queue.beforeWrite(t.Context())
			if size <= 100 {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrMaintenanceRequired)
			assert.Zero(t, queue.Snapshot().Outcomes.Checkpoints)
			queue.walObserver = fixedWAL{}
			require.ErrorIs(t, queue.beforeWrite(t.Context()), ErrMaintenanceRequired)
		})
	}
}

func TestPostCommitPressurePreservesApplied(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	queue.stopping = make(chan struct{})
	require.NoError(t, queue.sampleStorage(t.Context()))
	queue.walEnvelope = queue.Snapshot().Store.WALBytes
	group := runGroup(t, queue, submitCommand(Entry{Payload: make([]byte, 16384)}))
	assert.Equal(t, Applied, group.commands[0].result.Decision)
	assert.NoError(t, group.commands[0].err)
	assert.Equal(t, MaintenanceRequired, queue.Snapshot().State)
}

func TestFinalSnapshotDoesNotWaitForResponseConsumption(t *testing.T) {
	queue, _ := openTestQueue(t, testConfig(t))
	slot, err := queue.reserve(t.Context(), &queue.ordinary, 33024, false)
	require.NoError(t, err)
	slot.ids = make([]ID, 256)
	require.NoError(t, queue.handoff(t.Context(), slot))
	for {
		queue.mu.Lock()
		finished, changed := slot.finished, queue.changed
		queue.mu.Unlock()
		if finished {
			break
		}
		<-changed
	}
	require.NoError(t, queue.Close(t.Context()))
	before := queue.Snapshot()
	assert.Equal(t, Closed, before.State)
	assert.Equal(t, Count(1), before.Ordinary.Commands)
	queue.mu.Lock()
	assert.Nil(t, slot.ids)
	assert.Len(t, slot.statuses, 256)
	assert.Equal(t, Bytes(33024), queue.ordinary.retained)
	queue.mu.Unlock()
	_, statuses, err := queue.await(t.Context(), &queue.ordinary, slot)
	require.NoError(t, err)
	assert.Len(t, statuses, 256)
	assert.Equal(t, before, queue.Snapshot())
	queue.mu.Lock()
	assert.Zero(t, queue.ordinary.retained)
	assert.Equal(t, command{}, *slot)
	queue.mu.Unlock()
}

func TestIdleCheckpoint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, _ := openTestQueue(t, testConfig(t))
		_, err := queue.SubmitBatch(t.Context(), []Entry{{}})
		require.NoError(t, err)
		queue.StopDelivery()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		snapshot := queue.Snapshot()
		assert.Positive(t, snapshot.Outcomes.Checkpoints)
		assert.Zero(t, snapshot.Store.WALBytes)
	})
}

func TestSnapshotDuringStorageAndAbortJoin(t *testing.T) {
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), testConfig(t))
	require.NoError(t, err)
	entered, resume := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(t.Context(), nil,
			executionGate{entered: entered, release: resume})
		result <- err
	}()
	<-entered
	assert.Equal(t, Active, queue.Snapshot().State)
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New(strings.Repeat("x", 300))
	cancel(cause)
	closed := make(chan error, 1)
	go func() { closed <- queue.Close(ctx) }()
	<-queue.Stopping()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before storage joined: %v", err)
	default:
	}
	close(resume)
	require.NoError(t, <-result)
	require.ErrorIs(t, <-closed, cause)
	snapshot := queue.Snapshot()
	assert.True(t, snapshot.Cause.Truncated)
	assert.Len(t, snapshot.Cause.Detail, 256)
	assert.Zero(t, snapshot.Outcomes.Checkpoints)
}

func TestAttemptExhaustionPreservesAcquiredResult(t *testing.T) {
	config := testConfig(t)
	config.Runtime.Delivery.Items = 1
	config.Runtime.Delivery.ReceiveItems = 1
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), config)
	require.NoError(t, err)
	defer func() { queue.Abort(nil); _ = queue.Wait() }()
	queue.mu.Lock()
	queue.lastAttempt = math.MaxUint64 - 1
	queue.mu.Unlock()
	_, err = queue.SubmitBatch(t.Context(), []Entry{{}})
	require.NoError(t, err)
	gate := &acquisitionGate{acquired: make(chan struct{}), resume: make(chan struct{})}
	type response struct {
		deliveries []Delivery
		err        error
	}
	received := make(chan response, 1)
	go func() {
		deliveries, err := queue.receiveObserved(t.Context(), 1, gate)
		received <- response{deliveries: deliveries, err: err}
	}()
	<-gate.acquired
	queue.mu.Lock()
	token := queue.claims[0].delivery.Token
	assert.Equal(t, uint64(math.MaxUint64), queue.lastAttempt)
	queue.mu.Unlock()
	require.NoError(t, queue.Release(token))
	<-queue.Stopping()
	assert.True(t, queue.Snapshot().Cause.Exhausted)
	close(gate.resume)
	result := <-received
	require.NoError(t, result.err)
	require.Len(t, result.deliveries, 1)
	assert.Equal(t, token, result.deliveries[0].Token)
	require.ErrorIs(t, queue.Wait(), ErrExhausted)
	assert.Zero(t, queue.Snapshot().Delivery.Claims)
}

func TestStorageFaultSupersedesMaintenance(t *testing.T) {
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), testConfig(t))
	require.NoError(t, err)
	queue.StopDelivery()
	_, err = queue.Lookup(t.Context(), nil)
	require.NoError(t, err)
	queue.mu.Lock()
	_ = queue.enterMaintenanceLocked(WALEnvelopeExceeded)
	queue.mu.Unlock()
	require.NoError(t, queue.opened.storage.conn.Close())
	statuses, err := queue.Lookup(t.Context(), []ID{{}})
	assert.Nil(t, statuses)
	require.ErrorIs(t, err, ErrStorage)
	require.ErrorIs(t, queue.Wait(), ErrStorage)
	assert.True(t, queue.Snapshot().Cause.Storage)
}

func TestDiagnosticCountersSaturate(t *testing.T) {
	queue := &Queue{}
	queue.snapshot.Outcomes.Commits = math.MaxUint64
	queue.addCounterLocked(&queue.snapshot.Outcomes.Commits, 1)
	queue.snapshot.CommitLatency.Total = time.Duration(math.MaxInt64)
	queue.addLatencyLocked(&queue.snapshot.CommitLatency, time.Second)
	assert.Equal(t, uint64(math.MaxUint64), queue.snapshot.Outcomes.Commits)
	assert.Equal(t, time.Duration(math.MaxInt64), queue.snapshot.CommitLatency.Total)
	assert.True(t, queue.snapshot.Overflow)
	assert.Less(t, unsafe.Sizeof(Queue{})+unsafe.Sizeof(Snapshot{}), uintptr(1<<20))
	t.Logf("queue=%d snapshot=%d command=%d claim=%d groupedCommand=%d bytes",
		unsafe.Sizeof(Queue{}), unsafe.Sizeof(Snapshot{}), unsafe.Sizeof(command{}),
		unsafe.Sizeof(claim{}), unsafe.Sizeof(groupedCommand{}))
}

func TestCloseCutoffEveryCommandClass(t *testing.T) {
	for _, kind := range []string{"submit", "empty-submit", "ack", "empty-ack", "retire",
		"lookup", "empty-lookup", "release"} {
		t.Run(kind, func(t *testing.T) {
			queue, _ := openTestQueue(t, testConfig(t))
			_, err := queue.SubmitBatch(t.Context(), []Entry{{}})
			require.NoError(t, err)
			deliveries, err := queue.Receive(t.Context(), 1)
			require.NoError(t, err)
			queue.StopDelivery()
			invoke := func() error {
				switch kind {
				case "submit":
					_, err := queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 1}}})
					return err
				case "empty-submit":
					_, err := queue.SubmitBatch(t.Context(), nil)
					return err
				case "ack":
					_, err := queue.AckBatch(t.Context(), []Token{deliveries[0].Token})
					return err
				case "empty-ack":
					_, err := queue.AckBatch(t.Context(), nil)
					return err
				case "retire":
					_, err := queue.RetireThrough(t.Context(), [16]byte{1}, 0)
					return err
				case "lookup":
					_, err := queue.Lookup(t.Context(), []ID{{}})
					return err
				case "empty-lookup":
					_, err := queue.Lookup(t.Context(), nil)
					return err
				default:
					return queue.Release(deliveries[0].Token)
				}
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			gateDone := make(chan error, 1)
			go func() {
				_, err := queue.submitBatchObserved(t.Context(), nil,
					executionGate{entered: entered, release: resume})
				gateDone <- err
			}()
			<-entered
			admitted := make(chan error, 1)
			go func() { admitted <- invoke() }()
			waitForHandoff(t, queue, 3)
			closed := make(chan error, 1)
			go func() { closed <- queue.Close(t.Context()) }()
			<-queue.Stopping()
			require.ErrorIs(t, invoke(), ErrStopped)
			close(resume)
			require.NoError(t, <-gateDone)
			require.NoError(t, <-admitted)
			require.NoError(t, <-closed)
		})
	}
}

type blockingCause struct{ entered, resume chan struct{} }

func (c blockingCause) Error() string {
	close(c.entered)
	<-c.resume
	return "caller cause"
}

func TestSnapshotDoesNotCallApplicationError(t *testing.T) {
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), testConfig(t))
	require.NoError(t, err)
	cause := blockingCause{entered: make(chan struct{}), resume: make(chan struct{})}
	aborted := make(chan struct{})
	go func() { queue.Abort(cause); close(aborted) }()
	<-cause.entered
	assert.Equal(t, Active, queue.Snapshot().State)
	close(cause.resume)
	<-aborted
	require.Error(t, queue.Wait())
	assert.Equal(t, "caller cause", queue.Snapshot().Cause.Detail)
}

type sequenceWAL struct {
	values []Bytes
	next   int
}

func (w *sequenceWAL) WALBytes() (Bytes, error) {
	value := w.values[min(w.next, len(w.values)-1)]
	w.next++
	return value, nil
}

func TestTriggerBoundary(t *testing.T) {
	trigger := Bytes(4 << 20)
	for _, size := range []Bytes{trigger - 1, trigger, trigger + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			config := testConfig(t)
			config.Runtime.SQLite.CheckpointTrigger = trigger
			queue := groupFixture(t, config)
			queue.walObserver = &sequenceWAL{values: []Bytes{size, 0}}
			require.NoError(t, queue.beforeWrite(t.Context()))
			if size < trigger {
				assert.Zero(t, queue.Snapshot().Outcomes.Checkpoints)
			} else {
				assert.Equal(t, uint64(1), queue.Snapshot().Outcomes.Checkpoints)
			}
		})
	}
}

func TestCleanupUsesPressurePath(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	runGroup(t, queue, &command{kind: retireCommand, retirement: ID{Producer: [16]byte{1}}})
	queue.snapshot.Store.WALBytes = queue.config.Runtime.SQLite.CheckpointTrigger
	require.NoError(t, queue.cleanupTurn(t.Context()))
	assert.Equal(t, uint64(1), queue.Snapshot().Outcomes.Checkpoints)
	assert.Zero(t, queue.opened.metadata.counts.CleanupMarkers)
	queue.stopping = make(chan struct{})
	queue.walEnvelope = 100
	queue.walObserver = fixedWAL{size: 101}
	require.ErrorIs(t, queue.cleanupTurn(t.Context()), ErrMaintenanceRequired)
	assert.Equal(t, uint64(1), queue.Snapshot().Outcomes.CleanupTurns)
}

type blockingIsCause struct {
	once            sync.Once
	entered, resume chan struct{}
}

func (c *blockingIsCause) Error() string { return "custom Is" }
func (c *blockingIsCause) Is(error) bool {
	c.once.Do(func() { close(c.entered) })
	<-c.resume
	return false
}

type blockingUnwrapCause struct {
	once            sync.Once
	entered, resume chan struct{}
}

func (c *blockingUnwrapCause) Error() string { return "custom Unwrap" }
func (c *blockingUnwrapCause) Unwrap() error {
	c.once.Do(func() { close(c.entered) })
	<-c.resume
	return nil
}

func TestSnapshotDuringApplicationErrorClassification(t *testing.T) {
	for _, method := range []string{"Is", "Unwrap"} {
		t.Run(method, func(t *testing.T) {
			queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), testConfig(t))
			require.NoError(t, err)
			entered, resume := make(chan struct{}), make(chan struct{})
			var cause error = &blockingIsCause{entered: entered, resume: resume}
			if method == "Unwrap" {
				cause = &blockingUnwrapCause{entered: entered, resume: resume}
			}
			aborted := make(chan struct{})
			go func() { queue.Abort(cause); close(aborted) }()
			<-entered
			assert.Equal(t, Active, queue.Snapshot().State)
			close(resume)
			<-aborted
			require.Error(t, queue.Wait())
			assert.Equal(t, Closed, queue.Snapshot().State)
		})
	}
}

func TestCheckpointDurabilitySelection(t *testing.T) {
	config := testConfig(t)
	config.Durability = ProcessCrash
	queue := groupFixture(t, config)
	require.NoError(t, queue.checkpoint(t.Context()))
	var synchronous int
	require.NoError(t, queue.opened.storage.conn.QueryRowContext(t.Context(),
		"PRAGMA synchronous").Scan(&synchronous))
	assert.Equal(t, 1, synchronous, "live checkpoint restores NORMAL")
	require.NoError(t, queue.shutdownBarrier(t.Context()))
	require.NoError(t, queue.opened.storage.conn.QueryRowContext(t.Context(),
		"PRAGMA synchronous").Scan(&synchronous))
	assert.Equal(t, 2, synchronous, "shutdown keeps FULL through resource closure")
}

func TestPinnedReaderClosePreservesMaintenanceCategoryAndJoins(t *testing.T) {
	config := testConfig(t)
	config.Runtime.SQLite.BusyTimeout = 0
	directory := filepath.Join(t.TempDir(), "queue")
	queue, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	queue.StopDelivery()
	_, err = queue.SubmitBatch(t.Context(), []Entry{{}})
	require.NoError(t, err)
	reader, err := sql.Open("sqlite3", filepath.Join(directory, "queue.sqlite3"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	tx, err := reader.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var count int
	require.NoError(t, tx.QueryRow("SELECT count(*) FROM receipts").Scan(&count))
	_, err = queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 1}}})
	require.NoError(t, err)
	err = queue.Close(t.Context())
	require.ErrorIs(t, err, ErrMaintenanceRequired)
	assert.NotErrorIs(t, err, ErrStorage)
	assert.Equal(t, Closed, queue.Snapshot().State)
	assert.False(t, queue.Snapshot().Cause.Storage)
	require.NoError(t, tx.Rollback())
	reopened, err := Open(t.Context(), directory, config)
	require.NoError(t, err, "completed join released the permanent lock")
	require.NoError(t, reopened.Close(t.Context()))
}
