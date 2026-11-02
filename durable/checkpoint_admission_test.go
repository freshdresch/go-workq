package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type idleSampleGate struct {
	calls           atomic.Int32
	entered, resume chan struct{}
}

func (w *idleSampleGate) WALBytes() (Bytes, error) {
	switch w.calls.Add(1) {
	case 1:
		return 4096, nil
	case 2:
		close(w.entered)
		<-w.resume
		return 4096, nil
	default:
		return 0, nil
	}
}
func TestAbortDuringIdleSample(t *testing.T) {
	config := testConfig(t)
	config.Runtime.SQLite.CheckpointInterval = 100 * time.Millisecond
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), config)
	require.NoError(t, err)
	queue.StopDelivery()
	entered, resume := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(
			t.Context(),
			nil,
			executionGate{entered: entered, release: resume},
		)
		result <- err
	}()
	<-entered
	observer := &idleSampleGate{entered: make(chan struct{}), resume: make(chan struct{})}
	queue.walObserver = observer
	close(resume)
	require.NoError(t, <-result)
	<-observer.entered
	before := queue.Snapshot().Outcomes.Checkpoints
	cause := errors.New("abort during idle observation")
	queue.Abort(cause)
	close(observer.resume)
	require.ErrorIs(t, queue.Wait(), cause)
	after := queue.Snapshot().Outcomes.Checkpoints
	t.Logf("explicit checkpoints before Abort=%d after joined Abort=%d", before, after)
	require.Equal(t, before, after, "Abort must not start a new explicit checkpoint after observation")
}

func TestFailedCheckpointObservationCounter(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	queue.walObserver = failedWALObservation{}
	err := queue.checkpoint(t.Context())
	require.ErrorIs(t, err, ErrStorage)
	snapshot := queue.Snapshot()
	t.Logf("checkpoints=%d failures=%d",
		snapshot.Outcomes.Checkpoints, snapshot.Outcomes.CheckpointFailures)
	assert.Equal(t, uint64(1), snapshot.Outcomes.Checkpoints)
	assert.Equal(t, uint64(1), snapshot.Outcomes.CheckpointFailures)
	assert.True(t, snapshot.Checkpoint.Valid)
}

type failedWALObservation struct{}

func (failedWALObservation) WALBytes() (Bytes, error) { return 0, ErrStorage }

func TestSnapshotUnpublishedReservation(t *testing.T) {
	queue, _ := openTestQueue(t, testConfig(t))
	_, err := queue.Lookup(t.Context(), nil)
	require.NoError(t, err)
	gate := &deliveryReadGate{entered: make(chan struct{}), resume: make(chan struct{})}
	queue.mu.Lock()
	queue.deliveryObserver = gate
	queue.mu.Unlock()
	slot, err := queue.reserve(t.Context(), &queue.ordinary, 512, true)
	require.NoError(t, err)
	slot.entries = []Entry{{}}
	require.NoError(t, queue.handoff(t.Context(), slot))
	<-gate.entered
	snapshot := queue.Snapshot()
	queue.mu.Lock()
	actual := queue.deliveryRetained
	reserved := queue.claims[0].used
	queue.mu.Unlock()
	t.Logf("reserved read: used=%v retained=%d snapshot=%+v", reserved, actual, snapshot.Delivery)
	queue.StopDelivery()
	close(gate.resume)
	_, _, err = queue.await(t.Context(), &queue.ordinary, slot)
	require.NoError(t, err)
	assert.Equal(t, actual, snapshot.Delivery.Retained)
	assert.Equal(t, Count(1), snapshot.Delivery.Claims)
	assert.Equal(t, Count(1), snapshot.Delivery.Unpublished)
	_, err = queue.Lookup(t.Context(), nil)
	require.NoError(t, err)
	assert.Zero(t, queue.Snapshot().Delivery)
}

func TestAbortAndIndependentSampleClosureFault(t *testing.T) {
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), testConfig(t))
	require.NoError(t, err)
	queue.StopDelivery()
	entered, resume := make(chan struct{}), make(chan struct{})
	type response struct {
		result CommitResult
		err    error
	}
	completed := make(chan response, 1)
	go func() {
		result, err := queue.submitBatchObserved(
			t.Context(),
			[]Entry{{}},
			executionGate{entered: entered, release: resume},
		)
		completed <- response{result, err}
	}()
	<-entered
	require.NoError(t, queue.opened.main.Close())
	cause := errors.New("operator abort")
	queue.Abort(cause)
	close(resume)
	result := <-completed
	require.NoError(t, result.err)
	require.Equal(t, Applied, result.result.Decision)
	terminal := queue.Wait()
	require.ErrorIs(t, terminal, cause)
	require.ErrorIs(t, terminal, ErrStorage)
	require.ErrorIs(t, terminal, os.ErrClosed)
	require.True(t, queue.Snapshot().Cause.Storage)
	t.Logf("joined terminal: %v", terminal)
}

func TestFinalSnapshotPreHandoffReservation(t *testing.T) {
	queue, _ := openTestQueue(t, testConfig(t))
	slot, err := queue.reserve(t.Context(), &queue.ordinary, 256, true)
	require.NoError(t, err)
	require.NoError(t, queue.Close(t.Context()))
	before := queue.Snapshot()
	require.Equal(t, Count(1), before.Ordinary.Commands)
	require.ErrorIs(t, queue.handoff(t.Context(), slot), ErrStopped)
	queue.mu.Lock()
	queue.clearLocked(&queue.ordinary, slot)
	queue.mu.Unlock()
	require.Equal(t, before, queue.Snapshot())
	t.Logf("final admission retained=%d remains frozen", before.Ordinary.Retained)
}
func TestAbortDuringPreWriteSample(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	queue.stopping = make(chan struct{})
	observer := &preWriteSampleGate{entered: make(chan struct{}), resume: make(chan struct{})}
	queue.walObserver = observer
	completed := make(chan error, 1)
	go func() { completed <- queue.beforeWrite(t.Context()) }()
	<-observer.entered
	cause := errors.Join(ErrStorage, errors.New("abort before explicit checkpoint"))
	queue.Abort(cause)
	close(observer.resume)
	err := <-completed
	snapshot := queue.Snapshot()
	t.Logf("pre-write gate: error=%v checkpoints=%d", err, snapshot.Outcomes.Checkpoints)
	assert.Zero(t, snapshot.Outcomes.Checkpoints)
	assert.Zero(t, snapshot.Outcomes.CheckpointFailures)
	assert.ErrorIs(t, err, ErrStopped)
	assert.NotErrorIs(t, err, cause)
	assert.NotErrorIs(t, err, ErrStorage)
}

type preWriteSampleGate struct {
	calls           int
	entered, resume chan struct{}
}

func (w *preWriteSampleGate) WALBytes() (Bytes, error) {
	w.calls++
	if w.calls == 1 {
		close(w.entered)
		<-w.resume
		return 16 << 20, nil
	}
	return 0, nil
}

func TestCheckpointProtocolFailureAccounting(t *testing.T) {
	phases := []string{"full-selection", "full-readback", "truncate", "normal-restoration"}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			config := testConfig(t)
			config.Durability = ProcessCrash
			queue := groupFixture(t, config)
			require.NoError(t, queue.checkpoint(t.Context()))
			require.NoError(t, queue.opened.storage.conn.Raw(func(raw any) error {
				connection, ok := raw.(*sqlite3.SQLiteConn)
				require.True(t, ok)
				connection.RegisterAuthorizer(func(action int, name, value, _ string) int {
					if action != sqlite3.SQLITE_PRAGMA {
						return sqlite3.SQLITE_OK
					}
					deny := phase == "full-selection" && name == "synchronous" && value == "2"
					deny = deny || phase == "full-readback" && name == "synchronous" && value == ""
					deny = deny || phase == "truncate" && name == "wal_checkpoint"
					deny = deny || phase == "normal-restoration" && name == "synchronous" && value == "1"
					if deny {
						return sqlite3.SQLITE_DENY
					}
					return sqlite3.SQLITE_OK
				})
				return nil
			}))
			err := queue.checkpoint(t.Context())
			assert.ErrorIs(t, err, ErrStorage)
			snapshot := queue.Snapshot()
			assert.Equal(t, uint64(2), snapshot.Outcomes.Checkpoints)
			assert.Equal(t, uint64(1), snapshot.Outcomes.CheckpointFailures)
			assert.Equal(t, phase == "normal-restoration", snapshot.Checkpoint.Valid)
		})
	}
}

type checkpointObservationGate struct {
	calls           int
	entered, resume chan struct{}
	err             error
}

func (w *checkpointObservationGate) WALBytes() (Bytes, error) {
	w.calls++
	if w.calls == 1 {
		return 16 << 20, nil
	}
	close(w.entered)
	<-w.resume
	return 0, w.err
}

func TestAbortDuringAdmittedCheckpoint(t *testing.T) {
	for _, storageFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(storageFailure), func(t *testing.T) {
			queue := groupFixture(t, testConfig(t))
			queue.stopping = make(chan struct{})
			observer := &checkpointObservationGate{
				entered: make(chan struct{}), resume: make(chan struct{}),
			}
			if storageFailure {
				observer.err = ErrStorage
			}
			queue.walObserver = observer
			group := commandGroup{count: 1, commands: []groupedCommand{{slot: submitCommand(Entry{})}}}
			completed := make(chan error, 1)
			go func() { completed <- queue.executeGroup(t.Context(), &group) }()
			<-observer.entered
			cause := errors.New("abort during admitted checkpoint")
			queue.Abort(cause)
			close(observer.resume)
			err := <-completed
			if storageFailure {
				assert.ErrorIs(t, err, ErrStorage)
			} else {
				assert.ErrorIs(t, err, ErrStopped)
			}
			assert.NotErrorIs(t, err, cause)
			assert.Equal(t, NotApplied, group.commands[0].result.Decision)
			assert.Zero(t, queue.opened.metadata.counts.PendingItems)
			snapshot := queue.Snapshot()
			assert.Equal(t, uint64(1), snapshot.Outcomes.Checkpoints)
			assert.Equal(t, storageFailure, snapshot.Outcomes.CheckpointFailures == 1)
			assert.True(t, snapshot.Checkpoint.Valid)
		})
	}
}

func TestDeliveryReadErrorReclaimsSnapshotReservation(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	runGroup(t, queue, submitCommand(Entry{}))
	_, err := queue.opened.storage.conn.ExecContext(t.Context(),
		"UPDATE receipts SET digest=zeroblob(32)")
	require.NoError(t, err)
	gate := &deliveryReadGate{entered: make(chan struct{}), resume: make(chan struct{})}
	queue.deliveryObserver = gate
	completed := make(chan error, 1)
	go func() { completed <- queue.loadDelivery(t.Context(), 0, ID{}) }()
	<-gate.entered
	assert.Equal(t, Count(1), queue.Snapshot().Delivery.Unpublished)
	close(gate.resume)
	require.ErrorIs(t, <-completed, ErrStorage)
	assert.Zero(t, queue.Snapshot().Delivery)
	assert.Zero(t, queue.deliveryRetained)
	assert.False(t, queue.claims[0].used)
}

func TestShutdownCheckpointFailureCountedOnce(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	queue.walObserver = failedWALObservation{}
	require.ErrorIs(t, queue.shutdownBarrier(t.Context()), ErrStorage)
	snapshot := queue.Snapshot()
	assert.Equal(t, uint64(1), snapshot.Outcomes.Checkpoints)
	assert.Equal(t, uint64(1), snapshot.Outcomes.CheckpointFailures)
	assert.True(t, snapshot.Checkpoint.Valid)
}

func TestCloseDuringIdleObservation(t *testing.T) {
	config := testConfig(t)
	config.Runtime.SQLite.CheckpointInterval = 100 * time.Millisecond
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), config)
	require.NoError(t, err)
	queue.StopDelivery()
	entered, resume := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(
			t.Context(), nil, executionGate{entered: entered, release: resume},
		)
		result <- err
	}()
	<-entered
	observer := &idleSampleGate{entered: make(chan struct{}), resume: make(chan struct{})}
	queue.walObserver = observer
	close(resume)
	require.NoError(t, <-result)
	<-observer.entered
	before := queue.Snapshot().Outcomes.Checkpoints
	closed := make(chan error, 1)
	go func() { closed <- queue.Close(t.Context()) }()
	<-queue.Stopping()
	close(observer.resume)
	require.NoError(t, <-closed)
	after := queue.Snapshot().Outcomes.Checkpoints
	t.Logf("checkpoints before Close=%d after join=%d", before, after)
	require.Equal(
		t,
		before+1,
		after,
		"only shutdown barrier should start after Close cut off idle work",
	)
}

func TestCommittedCountsAfterObservationFailure(t *testing.T) {
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), testConfig(t))
	require.NoError(t, err)
	queue.StopDelivery()
	entered, resume := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		decision, err := queue.submitBatchObserved(
			t.Context(),
			[]Entry{{}},
			executionGate{entered: entered, release: resume},
		)
		if decision.Decision != Applied {
			result <- errors.New("not Applied")
			return
		}
		result <- err
	}()
	<-entered
	before := queue.Snapshot()
	queue.walObserver = failedWALObservation{}
	close(resume)
	require.NoError(t, <-result)
	require.ErrorIs(t, queue.Wait(), ErrStorage)
	snapshot := queue.Snapshot()
	assert.Equal(t, StoreCounts{PendingItems: 1, Receipts: 1, Producers: 1}, snapshot.Store.Counts)
	assert.Equal(t, uint64(1), snapshot.Outcomes.Commits)
	assert.Equal(t, before.Disk, snapshot.Disk)
	assert.Equal(t, before.Store.DatabaseBytes, snapshot.Store.DatabaseBytes)
	assert.Equal(t, before.Store.WALBytes, snapshot.Store.WALBytes)
	assert.Equal(t, before.Store.PageCount, snapshot.Store.PageCount)
	assert.Equal(t, before.Store.FreePages, snapshot.Store.FreePages)
}

type failCleanupSample struct {
	calls  int
	queue  *Queue
	before Snapshot
}

func (w *failCleanupSample) WALBytes() (Bytes, error) {
	w.calls++
	if w.calls == 3 {
		w.before = w.queue.Snapshot()
		return 0, ErrStorage
	}
	return 0, nil
}

func TestCleanupCountsAfterObservationFailure(t *testing.T) {
	queue, err := Open(t.Context(), filepath.Join(t.TempDir(), "queue"), testConfig(t))
	require.NoError(t, err)
	_, err = queue.SubmitBatch(t.Context(), []Entry{{}})
	require.NoError(t, err)
	deliveries, err := queue.Receive(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	_, err = queue.AckBatch(t.Context(), []Token{deliveries[0].Token})
	require.NoError(t, err)
	queue.StopDelivery()
	entered, resume := make(chan struct{}), make(chan struct{})
	slot, err := queue.reserve(t.Context(), &queue.progress, 384, false)
	require.NoError(t, err)
	slot.kind = retireCommand
	slot.observer = executionGate{entered: entered, release: resume}
	require.NoError(t, queue.handoff(t.Context(), slot))
	<-entered
	observer := &failCleanupSample{queue: queue}
	queue.walObserver = observer
	close(resume)
	result, _, err := queue.await(t.Context(), &queue.progress, slot)
	require.NoError(t, err)
	require.Equal(t, Applied, result.Decision)
	require.ErrorIs(t, queue.Wait(), ErrStorage)
	snapshot := queue.Snapshot()
	assert.Equal(t, StoreCounts{Producers: 1, RetiredProducers: 1}, snapshot.Store.Counts)
	assert.Equal(t, uint64(1), snapshot.Outcomes.CleanupTurns)
	assert.Equal(t, CleanupStats{Deleted: 1, MarkerCleared: true}, snapshot.Cleanup)
	assert.Equal(t, observer.before.Disk, snapshot.Disk)
	assert.Equal(t, observer.before.Store.DatabaseBytes, snapshot.Store.DatabaseBytes)
	assert.Equal(t, observer.before.Store.WALBytes, snapshot.Store.WALBytes)
	assert.Equal(t, observer.before.Store.PageCount, snapshot.Store.PageCount)
	assert.Equal(t, observer.before.Store.FreePages, snapshot.Store.FreePages)
}
