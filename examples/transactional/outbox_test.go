//go:build linux && amd64 && cgo

package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable"
)

const outboxBatchLimit = 4

var errOutboxRetired = errors.New("outbox has no completion frontier for retired occurrence")

type outboxIntent struct {
	entry  durable.Entry
	key    []byte
	amount int64
}

type outboxQueue interface {
	SubmitBatch(context.Context, []durable.Entry) (durable.CommitResult, error)
	Lookup(context.Context, []durable.ID) ([]durable.Status, error)
}

func openOutbox(t *testing.T, path string) *sql.DB {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: path}
	query := url.Values{
		"_journal_mode": {"WAL"}, "_synchronous": {"FULL"},
		"_busy_timeout": {"5000"}, "_txlock": {"immediate"},
	}
	uri.RawQuery = query.Encode()
	database, err := sql.Open("sqlite3", uri.String())
	require.NoError(t, err)
	database.SetMaxOpenConns(1)
	t.Cleanup(func() { assert.NoError(t, database.Close()) })
	var mode string
	var synchronous, timeout int
	require.NoError(t, database.QueryRow("PRAGMA journal_mode").Scan(&mode))
	require.NoError(t, database.QueryRow("PRAGMA synchronous").Scan(&synchronous))
	require.NoError(t, database.QueryRow("PRAGMA busy_timeout").Scan(&timeout))
	assert.Equal(t, "wal", mode)
	assert.Equal(t, 2, synchronous)
	assert.Equal(t, 5000, timeout)
	_, err = database.Exec(`
 CREATE TABLE IF NOT EXISTS source_changes(sequence BLOB PRIMARY KEY, amount INTEGER);
 CREATE TABLE IF NOT EXISTS outbox(
  sequence BLOB PRIMARY KEY, key BLOB NOT NULL, producer BLOB NOT NULL,
  codec TEXT NOT NULL, payload BLOB NOT NULL,
  handed_off INTEGER NOT NULL DEFAULT 0 CHECK(handed_off IN (0,1))
 );`)
	require.NoError(t, err)
	return database
}

func newOutboxIntent(t *testing.T, sequence uint64) outboxIntent {
	t.Helper()
	entry := sourceEntries()[0]
	entry.ID.Sequence = sequence
	key, err := effectRequest(durable.Delivery{
		ID: entry.ID, Codec: entry.Codec, Payload: entry.Payload,
	}).Key.Encode()
	require.NoError(t, err)
	return outboxIntent{entry: entry, key: []byte(key), amount: 3}
}

func outboxSequence(sequence uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, sequence)
	return encoded
}

// The gate runs inside the transaction so a killed process leaves neither source row.
func commitOutboxIntent(
	ctx context.Context,
	database *sql.DB,
	intent outboxIntent,
	beforeCommit func(),
) (err error) {
	transaction, err := database.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		return err
	}
	defer func() {
		rollbackErr := transaction.Rollback()
		if !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	sequence := outboxSequence(intent.entry.ID.Sequence)
	_, err = transaction.ExecContext(ctx,
		"INSERT INTO source_changes(sequence, amount) VALUES (?, ?)", sequence, intent.amount)
	if err != nil {
		return err
	}
	_, err = transaction.ExecContext(ctx, `
 INSERT INTO outbox(sequence, key, producer, codec, payload) VALUES (?, ?, ?, ?, ?)`,
		sequence, intent.key, intent.entry.ID.Producer[:], intent.entry.Codec, intent.entry.Payload)
	if err != nil {
		return err
	}
	if beforeCommit != nil {
		beforeCommit()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return transaction.Commit()
}

func scanOutbox(
	ctx context.Context,
	database *sql.DB,
) (intents []outboxIntent, err error) {
	rows, err := database.QueryContext(ctx, `
 SELECT sequence, key, producer, codec, payload FROM outbox
 WHERE handed_off = 0 ORDER BY sequence LIMIT ?`, outboxBatchLimit)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			intents = nil
			err = errors.Join(err, closeErr)
		}
	}()
	for rows.Next() {
		var intent outboxIntent
		var sequence, producer []byte
		if err := rows.Scan(&sequence, &intent.key, &producer,
			&intent.entry.Codec, &intent.entry.Payload); err != nil {
			return nil, err
		}
		if len(sequence) != 8 || len(producer) != 16 {
			return nil, errors.New("invalid persisted outbox identity")
		}
		intent.entry.ID.Sequence = binary.BigEndian.Uint64(sequence)
		copy(intent.entry.ID.Producer[:], producer)
		intents = append(intents, intent)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return intents, nil
}

func establishOutboxAcceptance(
	ctx context.Context,
	queue outboxQueue,
	entry durable.Entry,
) error {
	for range maxAttempts {
		result, err := queue.SubmitBatch(ctx, []durable.Entry{entry})
		if result.Decision == durable.Applied {
			return err
		}
		if result.Decision != durable.Unknown {
			return fmt.Errorf("outbox submission %s: %w", result.Decision, err)
		}
		statuses, err := queue.Lookup(ctx, []durable.ID{entry.ID})
		if err != nil {
			return err
		}
		switch statuses[0].Kind {
		case durable.Pending, durable.Acknowledged:
			return nil
		case durable.Absent:
		case durable.Retired:
			// Real applications need durable completion frontier history to accept this status.
			return errOutboxRetired
		default:
			return errors.New("unresolved outbox acceptance")
		}
	}
	return errors.New("outbox submission attempts exhausted")
}

func markOutboxHandoff(
	ctx context.Context,
	database *sql.DB,
	sequence uint64,
	beforeCommit func(),
) (err error) {
	transaction, err := database.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		return err
	}
	defer func() {
		rollbackErr := transaction.Rollback()
		if !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	_, err = transaction.ExecContext(ctx,
		"UPDATE outbox SET handed_off = 1 WHERE sequence = ?", outboxSequence(sequence))
	if err != nil {
		return err
	}
	if beforeCommit != nil {
		beforeCommit()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return transaction.Commit()
}

func dispatchOutbox(ctx context.Context, database *sql.DB, queue outboxQueue) error {
	intents, err := scanOutbox(ctx, database)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		if err := establishOutboxAcceptance(ctx, queue, intent.entry); err != nil {
			return err
		}
		if err := markOutboxHandoff(ctx, database, intent.entry.ID.Sequence, nil); err != nil {
			return err
		}
	}
	return nil
}

// This oracle uses an independent connection and includes handed-off rows, which remain retained.
func assertOutboxRows(t *testing.T, path string, handedOff int, expected ...outboxIntent) {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	database, err := sql.Open("sqlite3", uri.String())
	require.NoError(t, err)
	defer func() { assert.NoError(t, database.Close()) }()
	var changes, outboxRows int
	require.NoError(t, database.QueryRow("SELECT count(*) FROM source_changes").Scan(&changes))
	require.NoError(t, database.QueryRow("SELECT count(*) FROM outbox").Scan(&outboxRows))
	assert.Equal(t, len(expected), changes)
	assert.Equal(t, len(expected), outboxRows)
	for _, intent := range expected {
		var key, producer, payload []byte
		var codec string
		var amount int64
		var marked int
		err := database.QueryRow(`
 SELECT s.amount, o.key, o.producer, o.codec, o.payload, o.handed_off
 FROM source_changes s JOIN outbox o USING(sequence) WHERE sequence = ?`,
			outboxSequence(intent.entry.ID.Sequence)).Scan(&amount, &key, &producer,
			&codec, &payload, &marked)
		require.NoError(t, err)
		assert.Equal(t, intent.amount, amount)
		assert.Equal(t, intent.key, key)
		assert.Equal(t, intent.entry.ID.Producer[:], producer)
		assert.Equal(t, intent.entry.Codec, codec)
		assert.Equal(t, intent.entry.Payload, payload)
		assert.Equal(t, handedOff, marked)
	}
}

func finishOutbox(
	t *testing.T,
	queue *durable.Queue,
	destination string,
	intents ...outboxIntent,
) {
	t.Helper()
	store := openRecoveryStore(t, destination)
	var receipts []expectedReceipt
	quantity := int64(10)
	expected := make(map[durable.ID]outboxIntent, len(intents))
	for _, intent := range intents {
		expected[intent.entry.ID] = intent
	}
	for range len(intents) {
		delivery := receiveRecoveryEntry(t, queue)
		intent, exists := expected[delivery.ID]
		require.True(t, exists, "unexpected delivery ID: %v", delivery.ID)
		delete(expected, delivery.ID)
		assert.Equal(t, intent.entry.Codec, delivery.Codec)
		assert.Equal(t, intent.entry.Payload, delivery.Payload)
		request := effectRequest(delivery)
		encodedKey, err := request.Key.Encode()
		require.NoError(t, err)
		assert.Equal(t, intent.key, []byte(encodedKey))
		_, err = store.Apply(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, acknowledge(t.Context(), queue, delivery))
		quantity += 3
		receipts = append(receipts, expectedReceipt{request: request, quantity: quantity})
		assertQueueStatus(t, queue, delivery.ID, durable.Acknowledged)
	}
	assertRecoveryDestination(t, destination, quantity, receipts...)
}

func TestOutboxBoundedScanFindsLowerLateSequence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "source.sqlite")
	database := openOutbox(t, path)
	queue := openRecoveryQueue(t, filepath.Join(root, "queue"))
	// Allocation precedes the higher IDs, but its SQLite write transaction starts later.
	lower := newOutboxIntent(t, 1)
	var higher []outboxIntent
	for sequence := uint64(2); sequence <= 6; sequence++ {
		intent := newOutboxIntent(t, sequence)
		require.NoError(t, commitOutboxIntent(t.Context(), database, intent, nil))
		higher = append(higher, intent)
	}
	require.NoError(t, dispatchOutbox(t.Context(), database, queue))
	for _, intent := range higher[:outboxBatchLimit] {
		assertQueueStatus(t, queue, intent.entry.ID, durable.Pending)
	}
	assertQueueStatus(t, queue, higher[4].entry.ID, durable.Absent)
	require.NoError(t, commitOutboxIntent(t.Context(), database, lower, nil))
	require.NoError(t, dispatchOutbox(t.Context(), database, queue))
	all := append(higher, lower)
	assertOutboxRows(t, path, 1, all...)
	finishOutbox(t, queue, filepath.Join(root, "inventory.sqlite"), all...)
}

// lostOutboxReply models lost observations, not native SQLite or queue storage faults.
// Skipping one invocation models an Unknown operation that did not commit.
type lostOutboxReply struct {
	queue     *durable.Queue
	skip      bool
	lost      bool
	lookupErr error
}

func (q *lostOutboxReply) SubmitBatch(
	ctx context.Context,
	entries []durable.Entry,
) (durable.CommitResult, error) {
	if q.lost {
		return q.queue.SubmitBatch(ctx, entries)
	}
	q.lost = true
	if !q.skip {
		result, err := q.queue.SubmitBatch(ctx, entries)
		if err != nil {
			return result, err
		}
	}
	return durable.CommitResult{Decision: durable.Unknown}, io.ErrUnexpectedEOF
}

func (q *lostOutboxReply) Lookup(
	ctx context.Context,
	ids []durable.ID,
) ([]durable.Status, error) {
	if q.lookupErr != nil {
		return nil, q.lookupErr
	}
	return q.queue.Lookup(ctx, ids)
}

func TestOutboxUnknownHandoff(t *testing.T) {
	for _, history := range []durable.StatusKind{
		durable.Absent, durable.Pending, durable.Acknowledged, durable.Retired,
	} {
		t.Run(history.String(), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			path := filepath.Join(root, "source.sqlite")
			database := openOutbox(t, path)
			queue := openRecoveryQueue(t, filepath.Join(root, "queue"))
			intent := newOutboxIntent(t, 1)
			require.NoError(t, commitOutboxIntent(t.Context(), database, intent, nil))
			destination := filepath.Join(root, "inventory.sqlite")
			if history == durable.Acknowledged {
				submitRecoveryEntry(t, queue, intent.entry)
				finishOutbox(t, queue, destination, intent)
			}
			if history == durable.Retired {
				result, err := queue.RetireThrough(t.Context(), intent.entry.ID.Producer, 1)
				require.NoError(t, err)
				require.Equal(t, durable.Applied, result.Decision)
			}
			lost := &lostOutboxReply{queue: queue,
				skip: history == durable.Absent || history == durable.Retired}
			err := dispatchOutbox(t.Context(), database, lost)
			if history == durable.Retired {
				require.ErrorIs(t, err, errOutboxRetired)
				assertOutboxRows(t, path, 0, intent)
				store := openRecoveryStore(t, destination)
				assertRecoveryDestination(t, destination, 10)
				require.NoError(t, store.Close())
				// Operator recovery uses the original identity in a fresh queue, not a new effect key.
				require.NoError(t, queue.Close(t.Context()))
				queue = openRecoveryQueue(t, filepath.Join(root, "recovered-queue"))
				require.NoError(t, dispatchOutbox(t.Context(), database, queue))
			} else {
				require.NoError(t, err)
			}
			assertOutboxRows(t, path, 1, intent)
			if history != durable.Acknowledged {
				assertQueueStatus(t, queue, intent.entry.ID, durable.Pending)
				finishOutbox(t, queue, destination, intent)
			}
			assertQueueStatus(t, queue, intent.entry.ID, durable.Acknowledged)
		})
	}
}

func TestOutboxErrorsDoNotMarkHandoff(t *testing.T) {
	for _, failure := range []string{"submit", "lookup", "marker"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			path := filepath.Join(root, "source.sqlite")
			database := openOutbox(t, path)
			directory := filepath.Join(root, "queue")
			queue := openRecoveryQueue(t, directory)
			intent := newOutboxIntent(t, 1)
			require.NoError(t, commitOutboxIntent(t.Context(), database, intent, nil))
			var handoff outboxQueue = queue
			switch failure {
			case "submit":
				require.NoError(t, queue.Close(t.Context()))
			case "lookup":
				handoff = &lostOutboxReply{queue: queue, lookupErr: io.ErrUnexpectedEOF}
			case "marker":
				_, err := database.Exec(`CREATE TRIGGER refuse_marker BEFORE UPDATE ON outbox
     BEGIN SELECT RAISE(ABORT, 'marker unavailable'); END`)
				require.NoError(t, err)
			}
			require.Error(t, dispatchOutbox(t.Context(), database, handoff))
			assertOutboxRows(t, path, 0, intent)
			destination := filepath.Join(root, "inventory.sqlite")
			store := openRecoveryStore(t, destination)
			assertRecoveryDestination(t, destination, 10)
			require.NoError(t, store.Close())
			if failure == "submit" {
				queue = openRecoveryQueue(t, directory)
				assertQueueStatus(t, queue, intent.entry.ID, durable.Absent)
			} else {
				assertQueueStatus(t, queue, intent.entry.ID, durable.Pending)
			}
			if failure == "marker" {
				_, err := database.Exec("DROP TRIGGER refuse_marker")
				require.NoError(t, err)
			}
			require.NoError(t, dispatchOutbox(t.Context(), database, queue))
			assertOutboxRows(t, path, 1, intent)
			finishOutbox(t, queue, destination, intent)
		})
	}
}

type gatedOutboxQueue struct {
	queue   *durable.Queue
	entered chan<- durable.Entry
	release <-chan struct{}
}

func (q *gatedOutboxQueue) SubmitBatch(
	ctx context.Context,
	entries []durable.Entry,
) (durable.CommitResult, error) {
	select {
	case q.entered <- entries[0]:
	case <-ctx.Done():
		return durable.CommitResult{Decision: durable.NotApplied}, ctx.Err()
	}
	select {
	case <-q.release:
	case <-ctx.Done():
		return durable.CommitResult{Decision: durable.NotApplied}, ctx.Err()
	}
	return q.queue.SubmitBatch(ctx, entries)
}

func (q *gatedOutboxQueue) Lookup(
	ctx context.Context,
	ids []durable.ID,
) ([]durable.Status, error) {
	return q.queue.Lookup(ctx, ids)
}

func TestOutboxConcurrentDispatchersPreserveMapping(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "source.sqlite")
	first := openOutbox(t, path)
	second := openOutbox(t, path)
	queue := openRecoveryQueue(t, filepath.Join(root, "queue"))
	intent := newOutboxIntent(t, ^uint64(0))
	require.NoError(t, commitOutboxIntent(t.Context(), first, intent, nil))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	entered := make(chan durable.Entry)
	release := make(chan struct{})
	gated := &gatedOutboxQueue{queue: queue, entered: entered, release: release}
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() { firstDone <- dispatchOutbox(ctx, first, gated) }()
	go func() { secondDone <- dispatchOutbox(ctx, second, gated) }()
	completions := []<-chan error{firstDone, secondDone}
	joined := 0
	t.Cleanup(func() {
		cancel()
		for _, done := range completions[joined:] {
			select {
			case err := <-done:
				if err != nil {
					assert.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(5 * time.Second):
				t.Error("dispatcher did not join")
			}
		}
	})
	for range 2 {
		select {
		case entry := <-entered:
			assert.Equal(t, intent.entry, entry)
		case <-ctx.Done():
			t.Fatal("dispatchers did not both scan the unmarked intent")
		}
	}
	assertOutboxRows(t, path, 0, intent)
	close(release)
	for _, done := range completions {
		select {
		case err := <-done:
			joined++
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("dispatcher did not finish before deadline")
		}
	}
	assertOutboxRows(t, path, 1, intent)
	assertQueueStatus(t, queue, intent.entry.ID, durable.Pending)
	finishOutbox(t, queue, filepath.Join(root, "inventory.sqlite"), intent)
}

func TestOutboxCrashRecovery(t *testing.T) {
	for _, stage := range []string{
		"before-source-commit", "after-source-commit", "queue-accepted",
		"before-marker-commit", "after-marker-commit",
	} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			path := filepath.Join(root, "source.sqlite")
			directory := filepath.Join(root, "queue")
			database := openOutbox(t, path)
			queue := openRecoveryQueue(t, directory)
			require.NoError(t, database.Close())
			require.NoError(t, queue.Close(t.Context()))
			destination := filepath.Join(root, "inventory.sqlite")
			store := openRecoveryStore(t, destination)
			require.NoError(t, store.Close())
			killOutboxChild(t, root, stage)
			database = openOutbox(t, path)
			queue = openRecoveryQueue(t, directory)
			intent := newOutboxIntent(t, 1)
			assertRecoveryDestination(t, destination, 10)
			if stage == "before-source-commit" {
				assertOutboxRows(t, path, 0)
				assertQueueStatus(t, queue, intent.entry.ID, durable.Absent)
				require.NoError(t, dispatchOutbox(t.Context(), database, queue))
				assertQueueStatus(t, queue, intent.entry.ID, durable.Absent)
				return
			}
			marked := 0
			if stage == "after-marker-commit" {
				marked = 1
			}
			assertOutboxRows(t, path, marked, intent)
			status := durable.Pending
			if stage == "after-source-commit" {
				status = durable.Absent
			}
			assertQueueStatus(t, queue, intent.entry.ID, status)
			require.NoError(t, dispatchOutbox(t.Context(), database, queue))
			assertOutboxRows(t, path, 1, intent)
			finishOutbox(t, queue, destination, intent)
			assertOutboxRows(t, path, 1, intent)
		})
	}
}

func TestOutboxCrashChild(t *testing.T) {
	stage := os.Getenv("OUTBOX_CRASH_STAGE")
	if stage == "" {
		return
	}
	root := os.Getenv("OUTBOX_CRASH_ROOT")
	stagePipe := os.NewFile(3, "outbox-stage")
	releasePipe := os.NewFile(4, "outbox-release")
	require.NotNil(t, stagePipe)
	require.NotNil(t, releasePipe)
	t.Cleanup(func() {
		assert.NoError(t, stagePipe.Close())
		assert.NoError(t, releasePipe.Close())
	})
	stop := func(current string) {
		if current != stage {
			return
		}
		_, err := fmt.Fprintln(stagePipe, current)
		require.NoError(t, err)
		var release [1]byte
		_, err = io.ReadFull(releasePipe, release[:])
		require.NoError(t, err)
		t.Fatal("parent released crash point instead of killing child")
	}
	database := openOutbox(t, filepath.Join(root, "source.sqlite"))
	queue := openRecoveryQueue(t, filepath.Join(root, "queue"))
	intent := newOutboxIntent(t, 1)
	require.NoError(t, commitOutboxIntent(t.Context(), database, intent, func() {
		stop("before-source-commit")
	}))
	stop("after-source-commit")
	require.NoError(t, establishOutboxAcceptance(t.Context(), queue, intent.entry))
	stop("queue-accepted")
	require.NoError(t, markOutboxHandoff(t.Context(), database, intent.entry.ID.Sequence, func() {
		stop("before-marker-commit")
	}))
	stop("after-marker-commit")
	t.Fatal("child did not reach requested crash stage")
}

func killOutboxChild(t *testing.T, root, stage string) {
	t.Helper()
	stageReader, stageWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stageReader.Close() }) // Covers failed Start and earlier assertions.
	t.Cleanup(func() { _ = stageWriter.Close() })
	releaseReader, releaseWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = releaseReader.Close() })
	t.Cleanup(func() { _ = releaseWriter.Close() })
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, executable, "-test.run=^TestOutboxCrashChild$")
	command.Env = append(os.Environ(), "OUTBOX_CRASH_ROOT="+root, "OUTBOX_CRASH_STAGE="+stage)
	command.ExtraFiles = []*os.File{stageWriter, releaseReader}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	require.NoError(t, command.Start())
	joined := false
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		if joined {
			return
		}
		// Kill can race with timeout or early exit. Wait still owns reaping.
		_ = command.Process.Kill()
		select {
		case err := <-done:
			var exitErr *exec.ExitError
			if err != nil && !errors.As(err, &exitErr) {
				t.Errorf("wait for outbox child: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("outbox child did not exit during cleanup")
		}
	})
	require.NoError(t, stageWriter.Close())
	require.NoError(t, releaseReader.Close())
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.NoError(t, stageReader.SetReadDeadline(deadline))
	record, err := bufio.NewReader(stageReader).ReadString('\n')
	require.NoError(t, err, "child did not report stage")
	require.Equal(t, stage+"\n", record)
	require.NoError(t, command.Process.Kill())
	select {
	case err := <-done:
		joined = true
		var exitErr *exec.ExitError
		require.True(t, errors.As(err, &exitErr), "child exit: %v\n%s", err, output.String())
		assert.False(t, exitErr.Success())
	case <-ctx.Done():
		t.Fatal("outbox child did not exit after kill")
	}
}
