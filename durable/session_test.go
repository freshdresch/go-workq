package durable

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestQueue(t *testing.T, config Config) (*Queue, string) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "queue")
	queue, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
	return queue, directory
}

func TestSubmitRetriesAtomicityAndReopen(t *testing.T) {
	t.Parallel()
	for _, durability := range []Durability{Synchronized, ProcessCrash} {
		t.Run(durability.String(), func(t *testing.T) {
			t.Parallel()
			config := testConfig(t)
			config.Durability = durability
			queue, directory := openTestQueue(t, config)
			entries := []Entry{
				{ID: ID{Sequence: 0}},
				{ID: ID{Sequence: 1}, Codec: string([]byte{0, 255, 128}), Payload: []byte("value")},
				{ID: ID{Sequence: 2}, Codec: "", Payload: []byte{}},
			}
			result, err := queue.SubmitBatch(t.Context(), entries)
			require.NoError(t, err)
			assert.Equal(t, Applied, result.Decision)
			entries[0].Payload = []byte{}
			entries[2].Payload = nil
			result, err = queue.SubmitBatch(t.Context(), entries)
			require.NoError(t, err)
			assert.Equal(t, Applied, result.Decision)
			result, err = queue.SubmitBatch(t.Context(), []Entry{
				{ID: ID{Sequence: 3}}, {ID: ID{Sequence: 1}, Payload: []byte("conflict")},
			})
			assert.ErrorIs(t, err, ErrConflict)
			assert.Equal(t, NotApplied, result.Decision)
			result, err = queue.SubmitBatch(t.Context(), []Entry{entries[0], entries[0]})
			assert.ErrorIs(t, err, ErrInvalidInput)
			assert.Equal(t, NotApplied, result.Decision)
			ids := []ID{{Sequence: 2}, {Sequence: 3}, {Sequence: 0}, {Sequence: 2}}
			statuses, err := queue.Lookup(t.Context(), ids)
			require.NoError(t, err)
			require.Len(t, statuses, 4)
			for index, expected := range []StatusKind{Pending, Absent, Pending, Pending} {
				assert.Equal(t, ids[index], statuses[index].ID)
				assert.Equal(t, expected, statuses[index].Kind)
			}
			require.NoError(t, queue.Close(t.Context()))
			reopened, err := Open(t.Context(), directory, config)
			require.NoError(t, err)
			defer func() { assert.NoError(t, reopened.Close(context.Background())) }()
			statuses, err = reopened.Lookup(t.Context(), ids)
			require.NoError(t, err)
			assert.Equal(t, Pending, statuses[0].Kind)
			assert.Equal(t, Absent, statuses[1].Kind)
		})
	}
}

func waitForHandoff(t *testing.T, queue *Queue, sequence uint64) {
	t.Helper()
	for {
		queue.mu.Lock()
		reached := queue.lastSequence >= sequence
		changed := queue.changed
		queue.mu.Unlock()
		if reached {
			return
		}
		select {
		case <-changed:
		case <-t.Context().Done():
			t.Fatal("handoff did not arrive")
		}
	}
}

func TestCanceledHandoffCopiesAndOrderedEmptyLookup(t *testing.T) {
	t.Parallel()
	queue, _ := openTestQueue(t, testConfig(t))
	entered, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("caller stopped")
	entries := []Entry{{ID: ID{Sequence: 10}, Codec: "codec", Payload: []byte("original")}}
	type answer struct {
		result CommitResult
		err    error
	}
	response := make(chan answer, 1)
	go func() {
		result, err := queue.submitBatchObserved(
			ctx,
			entries,
			executionGate{entered: entered, release: release},
		)
		response <- answer{result: result, err: err}
	}()
	<-entered
	cancel(cause)
	returned := <-response
	assert.Equal(t, Unknown, returned.result.Decision)
	assert.ErrorIs(t, returned.err, context.Canceled)
	assert.ErrorIs(t, returned.err, cause)
	entries[0].Payload[0] = 'X'
	entries[0].ID.Sequence = 999
	entries[0].Codec = "changed"
	barrier := make(chan error, 1)
	go func() {
		statuses, err := queue.Lookup(t.Context(), nil)
		if len(statuses) != 0 {
			err = errors.New("nonempty barrier result")
		}
		barrier <- err
	}()
	waitForHandoff(t, queue, 2)
	select {
	case err := <-barrier:
		t.Fatalf("barrier overtook submit: %v", err)
	default:
	}
	close(release)
	require.NoError(t, <-barrier)
	result, err := queue.SubmitBatch(t.Context(), []Entry{
		{ID: ID{Sequence: 10}, Codec: "codec", Payload: []byte("original")},
	})
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	queue.mu.Lock()
	assert.Zero(t, queue.ordinary.retained)
	for _, slot := range queue.ordinary.slots {
		assert.False(t, slot.used)
	}
	queue.mu.Unlock()
}

func TestBeforeHandoffAndEmptySubmitGates(t *testing.T) {
	t.Parallel()
	queue, _ := openTestQueue(t, testConfig(t))
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("pre-handoff")
	cancel(cause)
	result, err := queue.SubmitBatch(ctx, []Entry{{ID: ID{Sequence: 1}}})
	assert.Equal(t, NotApplied, result.Decision)
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, cause)
	queue.StopAdmission()
	result, err = queue.SubmitBatch(t.Context(), nil)
	assert.Equal(t, NotApplied, result.Decision)
	assert.ErrorIs(t, err, ErrStopped)
	statuses, err := queue.Lookup(t.Context(), []ID{{Sequence: 1}})
	require.NoError(t, err)
	assert.Equal(t, Absent, statuses[0].Kind)
	require.NoError(t, queue.Close(t.Context()))
	result, err = queue.SubmitBatch(t.Context(), nil)
	assert.Equal(t, NotApplied, result.Decision)
	assert.ErrorIs(t, err, ErrStopped)
	statuses, err = queue.Lookup(t.Context(), nil)
	assert.Nil(t, statuses)
	assert.ErrorIs(t, err, ErrStopped)
	select {
	case <-queue.Done():
	default:
		t.Fatal("Close returned before join")
	}
}

func TestLogicalCapacityKeepsSessionHealthy(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	config.Policy.MaxPendingItems = 1
	queue, _ := openTestQueue(t, config)
	result, err := queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 1}}})
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	result, err = queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 2}}})
	assert.Equal(t, NotApplied, result.Decision)
	assert.ErrorIs(t, err, ErrCapacity)
	assert.NotErrorIs(t, err, ErrStorage)
	statuses, err := queue.Lookup(t.Context(), []ID{{Sequence: 1}, {Sequence: 2}})
	require.NoError(t, err)
	assert.Equal(t, Pending, statuses[0].Kind)
	assert.Equal(t, Absent, statuses[1].Kind)
	result, err = queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 1}}})
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
}

func TestReservationClassesAndCutoff(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	config.Runtime.Ordinary.Commands = 1
	config.Runtime.Progress.Commands = 1
	queue, _ := openTestQueue(t, config)
	ordinary, err := queue.reserve(t.Context(), &queue.ordinary, 256, false)
	require.NoError(t, err)
	progress, err := queue.reserve(t.Context(), &queue.progress, 384, false)
	require.NoError(t, err)
	errorsReturned := make(chan error, 2) // One result for each independent reservation class.
	for _, pool := range []*admissionPool{&queue.ordinary, &queue.progress} {
		go func() {
			_, err := queue.reserve(t.Context(), pool, 384, false)
			errorsReturned <- err
		}()
	}
	for {
		queue.mu.Lock()
		waiting := queue.ordinary.waiters == 1 && queue.progress.waiters == 1
		changed := queue.changed
		queue.mu.Unlock()
		if waiting {
			break
		}
		// Reserve waiters notify test observers through the same shared change channel.
		select {
		case <-changed:
		case <-t.Context().Done():
			t.Fatal("reservation waiters did not arrive")
		}
	}
	require.NoError(t, queue.Close(t.Context()))
	assert.ErrorIs(t, <-errorsReturned, ErrStopped)
	assert.ErrorIs(t, <-errorsReturned, ErrStopped)
	queue.mu.Lock()
	queue.clearLocked(&queue.ordinary, ordinary)
	queue.clearLocked(&queue.progress, progress)
	queue.mu.Unlock()
}

func TestStorageFailureIsNotLogicalCapacity(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	directory := filepath.Join(t.TempDir(), "queue")
	queue, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	t.Cleanup(func() { queue.Abort(nil); _ = queue.Wait() })
	setup := make(chan error, 1)
	result, err := queue.submitBatchObserved(t.Context(), []Entry{
		{ID: ID{Sequence: 1}, Payload: make([]byte, 16<<10)},
	}, databaseLimitObserver{queue: queue, setup: setup})
	require.NoError(t, <-setup)
	assert.NotEqual(t, Applied, result.Decision)
	assert.ErrorIs(t, err, ErrStorage)
	assert.NotErrorIs(t, err, ErrCapacity)
	var sqliteErr sqlite3.Error
	assert.ErrorAs(t, err, &sqliteErr)
	assert.Equal(t, sqlite3.ErrFull, sqliteErr.Code)
	assert.ErrorIs(t, queue.Wait(), ErrStorage)
	statuses, err := queue.Lookup(t.Context(), nil)
	assert.Nil(t, statuses)
	assert.ErrorIs(t, err, ErrStopped)
	reopened, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	require.NoError(t, reopened.Close(t.Context()))
}

type executionGate struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (g executionGate) BeforeExecute() {
	close(g.entered)
	<-g.release
}

type databaseLimitObserver struct {
	queue *Queue
	setup chan<- error
}

func (o databaseLimitObserver) BeforeExecute() {
	var pages int64
	conn := o.queue.opened.storage.conn
	ctx := context.Background()
	err := conn.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages)
	if err == nil {
		_, err = conn.ExecContext(ctx, "PRAGMA max_page_count="+strconv.FormatInt(pages, 10))
	}
	o.setup <- err
}

func TestSharedOrderAcrossReservationClasses(t *testing.T) {
	t.Parallel()
	queue, _ := openTestQueue(t, testConfig(t))
	entered, release := make(chan struct{}), make(chan struct{})
	submitted := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(t.Context(), []Entry{{ID: ID{Sequence: 7}}},
			executionGate{entered: entered, release: release})
		submitted <- err
	}()
	<-entered
	slot, err := queue.reserve(t.Context(), &queue.progress, 384, false)
	require.NoError(t, err)
	slot.ids = []ID{{Sequence: 7}}
	require.NoError(t, queue.handoff(t.Context(), slot))
	queue.mu.Lock()
	assert.Equal(t, uint64(2), slot.sequence)
	assert.Equal(t, Bytes(384), queue.progress.retained)
	queue.mu.Unlock()
	close(release)
	_, statuses, err := queue.await(t.Context(), &queue.progress, slot)
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.Equal(t, Pending, statuses[0].Kind)
	require.NoError(t, <-submitted)
}

func TestCloseSettlesQueuedCommands(t *testing.T) {
	t.Parallel()
	queue, directory := openTestQueue(t, testConfig(t))
	entered, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(t.Context(), []Entry{{ID: ID{Sequence: 1}}},
			executionGate{entered: entered, release: release})
		first <- err
	}()
	<-entered
	second := make(chan error, 1)
	go func() {
		_, err := queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 2}}})
		second <- err
	}()
	waitForHandoff(t, queue, 2)
	closed := make(chan error, 1)
	go func() { closed <- queue.Close(t.Context()) }()
	<-queue.Stopping()
	close(release)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.NoError(t, <-closed)
	reopened, err := Open(t.Context(), directory, testConfig(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, reopened.Close(context.Background())) }()
	statuses, err := reopened.Lookup(t.Context(), []ID{{Sequence: 1}, {Sequence: 2}})
	require.NoError(t, err)
	assert.Equal(t, Pending, statuses[0].Kind)
	assert.Equal(t, Pending, statuses[1].Kind)
}

func TestCanceledCloseJoinsAndRejectsUnstartedCommands(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	directory := filepath.Join(t.TempDir(), "queue")
	queue, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(t.Context(), []Entry{{ID: ID{Sequence: 1}}},
			executionGate{entered: entered, release: release})
		first <- err
	}()
	<-entered
	type answer struct {
		result CommitResult
		err    error
	}
	second := make(chan answer, 1)
	go func() {
		result, err := queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 2}}})
		second <- answer{result: result, err: err}
	}()
	waitForHandoff(t, queue, 2)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	closed := make(chan error, 1)
	go func() { closed <- queue.Close(ctx) }()
	<-queue.Stopping()
	// Observe Abort's publication so the second command cannot start first.
	for {
		queue.mu.Lock()
		aborting := queue.aborting
		changed := queue.changed
		queue.mu.Unlock()
		if aborting {
			break
		}
		<-changed
	}
	select {
	case err := <-closed:
		t.Fatalf("Close detached execution: %v", err)
	default:
	}
	_, err = Open(t.Context(), directory, config)
	assert.ErrorIs(t, err, ErrInUse)
	close(release)
	require.NoError(t, <-first)
	refused := <-second
	assert.Equal(t, NotApplied, refused.result.Decision)
	assert.ErrorIs(t, refused.err, ErrStopped)
	assert.ErrorIs(t, <-closed, context.Canceled)
	assert.ErrorIs(t, queue.Wait(), context.Canceled)
	reopened, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	require.NoError(t, reopened.Close(t.Context()))
}

func TestSubmitArenaReservation(t *testing.T) {
	queue, _ := openTestQueue(t, testConfig(t))
	entered, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	response := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(ctx,
			[]Entry{{Payload: make([]byte, 8193)}},
			executionGate{entered: entered, release: release})
		response <- err
	}()
	<-entered
	queue.mu.Lock()
	charge := queue.ordinary.retained
	queue.mu.Unlock()
	assert.GreaterOrEqual(t, charge, Bytes(9472), "payload allocation alone")
	assert.Equal(t, Bytes(16832), charge, "complete rounded reservation")
	cancel()
	assert.ErrorIs(t, <-response, context.Canceled)
	queue.mu.Lock()
	assert.Equal(t, charge, queue.ordinary.retained, "abandoned input is still owned")
	queue.mu.Unlock()
	close(release)
	require.NoError(t, queue.Close(t.Context()))
	queue.mu.Lock()
	assert.Zero(t, queue.ordinary.retained)
	queue.mu.Unlock()
}

func TestTightRetainedBudgetWaitsForRelease(t *testing.T) {
	config := testConfig(t)
	config.Policy.MaxPayload = 8193
	config.Policy.MaxCodec = 0
	config.Runtime.Batches.SubmitItems = 1
	config.Runtime.Batches.SubmitEncoded = 88 + 8193
	config.Runtime.Batches.LookupItems = 1
	config.Runtime.Ordinary.Retained = 16832
	queue, _ := openTestQueue(t, config)
	entered, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(t.Context(), []Entry{{Payload: make([]byte, 8193)}},
			executionGate{entered: entered, release: release})
		first <- err
	}()
	<-entered
	second := make(chan error, 1)
	go func() {
		_, err := queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 1},
			Payload: make([]byte, 8193)}})
		second <- err
	}()
	for {
		queue.mu.Lock()
		waiting := queue.ordinary.waiters == 1
		changed := queue.changed
		assert.Equal(t, Bytes(16832), queue.ordinary.retained)
		queue.mu.Unlock()
		if waiting {
			break
		}
		<-changed
	}
	close(release)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.NoError(t, queue.Close(t.Context()))
	queue.mu.Lock()
	assert.Zero(t, queue.ordinary.retained)
	queue.mu.Unlock()
}

type storageFaultGate struct {
	gate  executionGate
	limit databaseLimitObserver
}

func (g storageFaultGate) BeforeExecute() {
	g.gate.BeforeExecute()
	g.limit.BeforeExecute()
}

func TestStorageFaultRejectsQueuedCommandsWithoutErrorGrowth(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(strconv.FormatBool(abort), func(t *testing.T) {
			t.Parallel()
			config := testConfig(t)
			directory := filepath.Join(t.TempDir(), "queue")
			queue, err := Open(t.Context(), directory, config)
			require.NoError(t, err)
			t.Cleanup(func() { queue.Abort(nil); _ = queue.Wait() })
			entered, release := make(chan struct{}), make(chan struct{})
			setup := make(chan error, 1)
			type answer struct {
				result CommitResult
				err    error
			}
			first := make(chan answer, 1)
			go func() {
				result, err := queue.submitBatchObserved(t.Context(), []Entry{
					{Payload: make([]byte, 16<<10)},
				}, storageFaultGate{
					gate:  executionGate{entered: entered, release: release},
					limit: databaseLimitObserver{queue: queue, setup: setup},
				})
				first <- answer{result: result, err: err}
			}()
			<-entered
			const queued = 16
			responses := make([]chan answer, queued)
			for index := range responses {
				responses[index] = make(chan answer, 1)
				go func() {
					result, err := queue.SubmitBatch(t.Context(), []Entry{
						{ID: ID{Sequence: uint64(index + 1)}},
					})
					responses[index] <- answer{result: result, err: err}
				}()
				waitForHandoff(t, queue, uint64(index+2))
			}
			abandoned, err := queue.reserve(t.Context(), &queue.ordinary, 256, true)
			require.NoError(t, err)
			require.NoError(t, queue.handoff(t.Context(), abandoned))
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			result, _, err := queue.await(ctx, &queue.ordinary, abandoned)
			assert.Equal(t, Unknown, result.Decision)
			assert.ErrorIs(t, err, context.Canceled)
			cause := errors.New("operator stopped")
			if abort {
				queue.Abort(cause)
			}
			close(release)
			require.NoError(t, <-setup)
			failed := <-first
			assert.NotEqual(t, Applied, failed.result.Decision)
			assert.ErrorIs(t, failed.err, ErrStorage)
			var sqliteErr sqlite3.Error
			require.ErrorAs(t, failed.err, &sqliteErr)
			assert.Equal(t, sqlite3.ErrFull, sqliteErr.Code)
			for index, response := range responses {
				refused := <-response
				assert.Equal(t, NotApplied, refused.result.Decision, "queued %d", index)
				assert.ErrorIs(t, refused.err, ErrStopped)
				assert.ErrorIs(t, refused.err, ErrStorage)
				assert.Less(t, len(refused.err.Error()), 4096, "queued %d", index)
			}
			terminal := queue.Wait()
			assert.ErrorIs(t, terminal, ErrStorage)
			assert.NotErrorIs(t, terminal, ErrStopped)
			if abort {
				assert.ErrorIs(t, terminal, cause)
			}
			t.Logf("abort=%t queued=%d terminal diagnostic bytes=%d",
				abort, queued, len(terminal.Error()))
			assert.Less(t, len(terminal.Error()), 4096)
			assert.LessOrEqual(t, len(terminal.Error()),
				len(failed.err.Error())+len(cause.Error())+1)
			assert.ErrorIs(t, queue.Close(t.Context()), ErrStorage)
			queue.mu.Lock()
			assert.Zero(t, queue.ordinary.retained)
			for _, slot := range queue.ordinary.slots {
				assert.False(t, slot.used)
			}
			queue.mu.Unlock()
			reopened, err := Open(t.Context(), directory, config)
			require.NoError(t, err)
			require.NoError(t, reopened.Close(t.Context()))
		})
	}
}
