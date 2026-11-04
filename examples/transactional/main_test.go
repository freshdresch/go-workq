package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/durable"
	"github.com/freshdresch/go-workq/examples/internal/inventory"
)

func runExample(t *testing.T, args []string, output io.Writer) error {
	t.Helper()
	err := run(t.Context(), args, nil, nil, output, io.Discard)
	if errors.Is(err, durable.ErrUnsupported) {
		t.Skipf("queue storage profile unsupported: %v", err)
	}
	return err
}

func TestRunAndRepeat(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	args := []string{"transactional", "-directory", directory, "-destination", destination}
	var output bytes.Buffer
	require.NoError(t, runExample(t, args, &output))
	assert.Contains(t, output.String(), "attempts=3 commits=3 duplicates=0 acknowledged=3")
	assertDestination(t, destination, 14, 3)
	assertCompleted(t, directory)
	output.Reset()
	require.NoError(t, runExample(t, args, &output))
	assert.Contains(t, output.String(), "attempts=0 commits=0 duplicates=0 acknowledged=0")
	assert.Contains(t, output.String(), "quantity=14 adjustments=3")
	assertDestination(t, destination, 14, 3)
	assertCompleted(t, directory)
}

func TestStopAfterCommitAndRestart(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	args := []string{"transactional", "-directory", directory, "-destination", destination}
	var output bytes.Buffer
	require.ErrorIs(t, runExample(t, append(args, "-stop-after-commit"), &output), errInterrupted)
	assertDestination(t, destination, 13, 1)
	inspection, err := durable.Inspect(t.Context(), directory, sourceIDs())
	require.NoError(t, err)
	assert.Equal(t, durable.Pending, inspection.Statuses[0].Kind)
	assert.Equal(t, durable.Count(3), inspection.Store.Counts.PendingItems)
	output.Reset()
	require.NoError(t, runExample(t, args, &output))
	assert.Contains(t, output.String(), "attempts=3 commits=2 duplicates=1 acknowledged=3")
	assertDestination(t, destination, 14, 3)
	assertCompleted(t, directory)
}

func assertDestination(t *testing.T, path string, quantity, adjustments int64) {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	database, err := sql.Open("sqlite3", uri.String())
	require.NoError(t, err)
	defer func() { assert.NoError(t, database.Close()) }()
	var actualQuantity, actualAdjustments, receipts, incomplete int64
	err = database.QueryRowContext(t.Context(),
		"SELECT quantity, adjustments FROM inventory WHERE item = 'widget'",
	).Scan(&actualQuantity, &actualAdjustments)
	require.NoError(t, err)
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT COUNT(*), COUNT(*) - COUNT(result_quantity) FROM receipts",
	).Scan(&receipts, &incomplete))
	assert.Equal(t, quantity, actualQuantity)
	assert.Equal(t, adjustments, actualAdjustments)
	assert.Equal(t, adjustments, receipts)
	assert.Zero(t, incomplete)
	rows, err := database.QueryContext(t.Context(),
		"SELECT namespace, producer, sequence, effect, codec, request FROM receipts ORDER BY sequence")
	require.NoError(t, err)
	defer func() { assert.NoError(t, rows.Close()) }()
	entries := sourceEntries()
	index := 0
	for rows.Next() {
		var namespace, effect, codec string
		var producer, sequence, payload []byte
		require.NoError(t, rows.Scan(&namespace, &producer, &sequence, &effect, &codec, &payload))
		require.Less(t, index, len(entries))
		assert.Equal(t, sourceNamespace, namespace)
		assert.Equal(t, entries[index].ID.Producer[:], producer)
		require.Len(t, sequence, 8)
		assert.Equal(t, entries[index].ID.Sequence, binary.BigEndian.Uint64(sequence))
		assert.Equal(t, "inventory.adjust", effect)
		assert.Equal(t, entries[index].Codec, codec)
		assert.Equal(t, entries[index].Payload, payload)
		index++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, int(adjustments), index)
}

func assertCompleted(t *testing.T, directory string) {
	t.Helper()
	inspection, err := durable.Inspect(t.Context(), directory, sourceIDs())
	require.NoError(t, err)
	assert.Zero(t, inspection.Store.Counts.PendingItems)
	for _, status := range inspection.Statuses {
		assert.Equal(t, durable.Acknowledged, status.Kind)
	}
}

func TestSeparatePaths(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	queuePath := filepath.Join(root, "queue")
	require.NoError(t, os.Mkdir(queuePath, 0700))
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(queuePath, alias))
	for _, destination := range []string{queuePath, filepath.Join(queuePath, "db"),
		filepath.Join(alias, "db")} {
		_, _, err := separatePaths(queuePath, destination)
		assert.Error(t, err)
	}
	_, _, err := separatePaths(queuePath, filepath.Join(root, "queue-other"))
	assert.NoError(t, err)
}

func TestRunInvalidArguments(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{nil, {"transactional"}, {"transactional", "-unknown"}} {
		assert.Error(t, run(t.Context(), args, nil, nil, io.Discard, io.Discard))
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestOutputFailureClosesOwners(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	args := []string{"transactional", "-directory", directory, "-destination", destination}
	require.ErrorIs(t, runExample(t, args, failingWriter{}), io.ErrClosedPipe)
	// Reopening both databases proves run released their owners after output failed.
	require.NoError(t, runExample(t, args, io.Discard))
	assertDestination(t, destination, 14, 3)
	assertCompleted(t, directory)
}

func TestCanceledRun(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	args := []string{"transactional", "-directory", filepath.Join(root, "queue"),
		"-destination", filepath.Join(root, "inventory.sqlite")}
	assert.ErrorIs(t, run(ctx, args, nil, nil, io.Discard, io.Discard), context.Canceled)
}

func TestApplyRetriesHealthyBusyStore(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	args := []string{"transactional", "-directory", directory, "-destination", destination}
	require.ErrorIs(t, runExample(t, append(args, "-stop-after-commit"), io.Discard), errInterrupted)
	store, err := inventory.Open(t.Context(), destination, 64)
	require.NoError(t, err)
	defer func() { assert.NoError(t, store.Close()) }()
	uri := url.URL{Scheme: "file", Path: destination, RawQuery: "_txlock=immediate"}
	database, err := sql.Open("sqlite3", uri.String())
	require.NoError(t, err)
	defer func() { assert.NoError(t, database.Close()) }()
	transaction, err := database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() {
		// Explicit rollback below releases contention before recovery. This covers early failure.
		err := transaction.Rollback()
		if !errors.Is(err, sql.ErrTxDone) {
			assert.NoError(t, err)
		}
	}()
	entry := sourceEntries()[1]
	request := effectRequest(durable.Delivery{
		ID: entry.ID, Codec: entry.Codec, Payload: entry.Payload,
	})
	result, attempts, err := apply(t.Context(), store, request)
	assert.Error(t, err)
	assert.Equal(t, maxAttempts, attempts)
	assert.Equal(t, inventory.Result{}, result)
	select {
	case <-store.Failed():
		t.Fatal("busy begin poisoned the healthy destination")
	default:
	}
	require.NoError(t, transaction.Rollback())
	result, attempts, err = apply(t.Context(), store, request)
	require.NoError(t, err)
	assert.Equal(t, 1, attempts)
	assert.False(t, result.Duplicate)
	assert.Equal(t, int64(12), result.Quantity)
	require.NoError(t, store.Close())
	require.NoError(t, runExample(t, args, io.Discard))
	assertDestination(t, destination, 14, 3)
	assertCompleted(t, directory)
}
