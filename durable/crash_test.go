//go:build linux

package durable

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This campaign kills only the application. Kernel caches survive SIGKILL.
type crashRequest struct {
	Directory  string     `json:"directory"`
	Operation  string     `json:"operation"`
	Phase      string     `json:"phase"`
	Durability Durability `json:"durability"`
	Seed       uint64     `json:"seed"`
}

type crashMessage struct {
	Phase  string     `json:"phase"`
	Tokens [][56]byte `json:"tokens,omitempty"`
	Epoch  uint64     `json:"epoch"`
}

type crashObserver struct {
	phase   string
	encoder *json.Encoder
	reader  io.Reader
}

func (o *crashObserver) stop(phase string) {
	if phase != o.phase {
		return
	}
	// A protocol error must terminate the child, not masquerade as the selected boundary.
	if err := o.encoder.Encode(crashMessage{Phase: phase}); err != nil {
		panic(err)
	}
	var release [1]byte
	if _, err := io.ReadFull(o.reader, release[:]); err != nil {
		panic(err)
	}
	panic("parent resumed a crash boundary instead of killing the child")
}

func (o *crashObserver) BeforeExecute()                  { o.stop("before-begin") }
func (o *crashObserver) ObserveTransaction(phase string) { o.stop(phase) }
func (o *crashObserver) Observe(stage startupStage)      { o.stop(stage.String()) }
func (o *crashObserver) ObserveOffline(stage string, _ *validatedStore) error {
	o.stop(stage)
	return nil
}

func crashEntries(seed uint64) []Entry {
	entries := make([]Entry, 4)
	for i := range entries {
		entries[i] = Entry{
			ID:      ID{Producer: [16]byte{byte(i + 1)}, Sequence: seed},
			Codec:   fmt.Sprintf("codec-%d", i),
			Payload: bytes.Repeat([]byte{byte(seed), byte(i)}, 8192),
		}
	}
	return entries
}

func receiveCrashTokens(t *testing.T, queue *Queue, count int) []Token {
	t.Helper()
	var tokens []Token
	for len(tokens) < count {
		deliveries, err := queue.Receive(t.Context(), count-len(tokens))
		require.NoError(t, err)
		for _, delivery := range deliveries {
			tokens = append(tokens, delivery.Token)
		}
	}
	return tokens
}

func seedCrashStore(t *testing.T, request crashRequest) [][56]byte {
	t.Helper()
	config := testConfig(t)
	config.Durability = request.Durability
	queue, err := Open(t.Context(), request.Directory, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		queue.Abort(ErrStopped)
		// Abort supplies a terminal cause. Cleanup only needs to join the owner.
		_ = queue.Wait()
	})
	entries := crashEntries(request.Seed)
	result, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, Applied, result.Decision)
	tokens := receiveCrashTokens(t, queue, len(entries))
	if request.Operation == "retire" {
		result, err = queue.AckBatch(t.Context(), tokens)
		require.NoError(t, err)
		require.Equal(t, Applied, result.Decision)
	}
	if request.Operation == "maintain" {
		result, err = queue.AckBatch(t.Context(), tokens[:1])
		require.NoError(t, err)
		require.Equal(t, Applied, result.Decision)
		result, err = queue.RetireThrough(t.Context(), [16]byte{9}, request.Seed)
		require.NoError(t, err)
		require.Equal(t, Applied, result.Decision)
	}
	require.NoError(t, queue.Close(t.Context()))
	wire := make([][56]byte, len(tokens))
	for i, token := range tokens {
		wire[i] = token.bytes
	}
	return wire
}

func TestCrashChild(t *testing.T) {
	if os.Getenv("DURABLE_CRASH_CHILD") != "1" {
		return
	}
	input := json.NewDecoder(os.Stdin)
	var request crashRequest
	require.NoError(t, input.Decode(&request))
	output := os.NewFile(3, "crash-events")
	require.NotNil(t, output)
	observer := &crashObserver{
		phase: request.Phase, encoder: json.NewEncoder(output), reader: os.Stdin,
	}
	config := testConfig(t)
	config.Durability = request.Durability
	switch request.Operation {
	case "initialize", "epoch":
		opened, err := startStoreObserved(t.Context(), request.Directory, config, observer)
		require.NoError(t, err)
		// Keep the live owner until SIGKILL. Never close the store before the kill.
		require.NotNil(t, opened)
	case "maintain":
		_, err := maintainObserved(t.Context(), request.Directory, observer)
		require.NoError(t, err)
	case "policy":
		next := ProcessCrash
		if request.Durability == ProcessCrash {
			next = Synchronized
		}
		result, err := changeDurabilityObserved(
			t.Context(), request.Directory, request.Durability, next, observer,
		)
		require.NoError(t, err)
		require.Equal(t, Applied, result.Decision)
	default:
		crashChildCommands(t, request, config, observer)
	}
	t.Fatal("selected crash boundary was not reached")
}

func crashChildCommands(
	t *testing.T,
	request crashRequest,
	config Config,
	observer *crashObserver,
) {
	t.Helper()
	config.Runtime.Group.MaxDelay = 0
	queue, err := Open(t.Context(), request.Directory, config)
	require.NoError(t, err)
	entries := crashEntries(request.Seed)
	var tokens []Token
	if request.Operation == "ack" {
		tokens = receiveCrashTokens(t, queue, len(entries))
	}
	wire := make([][56]byte, len(tokens))
	for i, token := range tokens {
		wire[i] = token.bytes
	}
	require.NoError(t, observer.encoder.Encode(crashMessage{
		Phase: "ready", Tokens: wire, Epoch: queue.Snapshot().Store.Epoch,
	}))

	// An earlier empty command holds the real coordinator while both public calls
	// hand off. With MaxDelay=0, their complete commands form one outer group.
	entered, release := make(chan struct{}), make(chan struct{})
	gateDone := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(
			t.Context(), nil, executionGate{entered: entered, release: release},
		)
		gateDone <- err
	}()
	<-entered
	queue.mu.Lock()
	target := queue.lastSequence + 2
	queue.mu.Unlock()
	type outcome struct {
		result CommitResult
		err    error
	}
	results := make(chan outcome, 2) // Exactly two public commands belong to the tested group.
	for index := range 2 {
		go func() {
			var result CommitResult
			var err error
			switch request.Operation {
			case "submit":
				fresh := crashEntries(request.Seed + 1)
				result, err = queue.submitBatchObserved(t.Context(), fresh[index*2:index*2+2], observer)
			case "ack":
				result, err = queue.ackBatchObserved(t.Context(), tokens[index*2:index*2+2], observer)
			case "retire":
				result, err = queue.retireThroughObserved(
					t.Context(), entries[index].ID.Producer, request.Seed+1, observer,
				)
			}
			results <- outcome{result: result, err: err}
		}()
	}
	queue.mu.Lock()
	for queue.lastSequence < target {
		changed := queue.changed
		queue.mu.Unlock()
		<-changed
		queue.mu.Lock()
	}
	queue.mu.Unlock()
	close(release)
	require.NoError(t, <-gateDone)
	for range 2 {
		result := <-results
		require.NoError(t, result.err)
		require.Equal(t, Applied, result.result.Decision)
	}
	require.Equal(t, Count(2), queue.Snapshot().LastGroup.Commands)
	observer.stop("applied")
}

func newCrashFixture(t *testing.T, request crashRequest) crashRequest {
	t.Helper()
	root, err := os.MkdirTemp("", "durable-crash-")
	require.NoError(t, err)
	request.Directory = filepath.Join(root, "queue")
	encoded, err := json.MarshalIndent(request, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "request.json"), encoded, 0600))
	t.Logf("crash fixture seed=%d phase=%s: %s", request.Seed, request.Phase, root)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("preserved crash artifacts: %s", root)
			return
		}
		assert.NoError(t, os.RemoveAll(root))
	})
	return request
}

func killCrashChild(t *testing.T, request crashRequest) crashMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	events, childEvents, err := os.Pipe()
	require.NoError(t, err)
	defer func() { assert.NoError(t, events.Close()) }()
	// The parent closes this pipe immediately after Start. Cleanup also covers a failed Start.
	defer func() { _ = childEvents.Close() }()
	binary, err := os.Executable()
	require.NoError(t, err)
	child := exec.CommandContext(ctx, binary, "-test.run=^TestCrashChild$", "-test.timeout=20s")
	child.Env = append(os.Environ(), "DURABLE_CRASH_CHILD=1")
	child.ExtraFiles = []*os.File{childEvents}
	input, err := child.StdinPipe()
	require.NoError(t, err)
	// Wait closes StdinPipe. This also covers an early failure before Start.
	defer func() { _ = input.Close() }()
	log, err := os.Create(filepath.Join(filepath.Dir(request.Directory), "child.log"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, log.Close()) }()
	child.Stdout, child.Stderr = log, log
	require.NoError(t, child.Start())
	joined := false
	defer func() {
		if !joined {
			// Failure cleanup must join even if the child already exited.
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	require.NoError(t, childEvents.Close())
	require.NoError(t, json.NewEncoder(input).Encode(request))
	require.NoError(t, events.SetReadDeadline(time.Now().Add(15*time.Second)))
	decoder := json.NewDecoder(events)
	var ready crashMessage
	for {
		var message crashMessage
		require.NoError(t, decoder.Decode(&message), "child log: %s", log.Name())
		if message.Phase == "ready" {
			ready = message
			continue
		}
		require.Equal(t, request.Phase, message.Phase)
		break
	}
	// Every selected boundary still owns the permanent lock, including offline calls.
	_, err = Inspect(t.Context(), request.Directory, nil)
	require.ErrorIs(t, err, ErrInUse)
	require.NoError(t, child.Process.Kill())
	err = child.Wait()
	joined = true
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, status.Signal())
	// Preserve the pre-recovery DB/WAL even if a later assertion fails after Inspect checkpoints.
	raw := filepath.Join(filepath.Dir(request.Directory), "after-kill")
	require.NoError(t, os.Mkdir(raw, 0700))
	for _, name := range []string{"queue.sqlite3", "queue.sqlite3-wal", "queue.sqlite3-shm", "LOCK"} {
		data, err := os.ReadFile(filepath.Join(request.Directory, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(raw, name), data, 0600))
	}
	return ready
}

func TestProcessCrashGroups(t *testing.T) {
	for _, durability := range []Durability{Synchronized, ProcessCrash} {
		for _, operation := range []string{"submit", "ack", "retire"} {
			for _, phase := range []string{"before-begin", "after-mutation", "after-commit", "applied"} {
				t.Run(durability.String()+"/"+operation+"/"+phase, func(t *testing.T) {
					request := newCrashFixture(t, crashRequest{
						Operation: operation, Phase: phase, Durability: durability, Seed: 41,
					})
					oldTokens := seedCrashStore(t, request)
					ready := killCrashChild(t, request)
					ready.Tokens = append(ready.Tokens, oldTokens...)
					checkCrashGroup(t, request, ready)
				})
			}
		}
	}
}

func checkCrashGroup(t *testing.T, request crashRequest, ready crashMessage) {
	t.Helper()
	committed := request.Phase == "after-commit" || request.Phase == "applied"
	entries := crashEntries(request.Seed)
	fresh := crashEntries(request.Seed + 1)
	ids := make([]ID, 0, 8)
	for _, entry := range append(entries, fresh...) {
		ids = append(ids, entry.ID)
	}
	inspection, err := Inspect(t.Context(), request.Directory, ids)
	require.NoError(t, err)
	require.Equal(t, request.Durability, inspection.Store.Durability)
	require.Equal(t, ready.Epoch, inspection.Store.Epoch)
	pending := Count(4)
	if request.Operation == "submit" && committed {
		pending = 8
	}
	if request.Operation == "retire" || (request.Operation == "ack" && committed) {
		pending = 0
	}
	assert.Equal(t, pending, inspection.Store.Counts.PendingItems)
	assert.Equal(t, Bytes(pending)*16384, inspection.Store.Counts.PendingPayload)
	assert.Equal(t, Count(4), inspection.Store.Counts.Producers)
	wantReceipts := Count(4)
	if request.Operation == "submit" && committed {
		wantReceipts = 8
	}
	if request.Operation == "retire" && committed {
		assert.Equal(t, Count(2), inspection.Store.Counts.RetiredProducers)
		assert.GreaterOrEqual(t, inspection.Store.Counts.Receipts, Count(2))
		assert.LessOrEqual(t, inspection.Store.Counts.Receipts, wantReceipts)
	} else {
		assert.Zero(t, inspection.Store.Counts.RetiredProducers)
		assert.Equal(t, wantReceipts, inspection.Store.Counts.Receipts)
	}
	require.Len(t, inspection.Statuses, len(ids))
	for i, status := range inspection.Statuses {
		assert.Equal(t, ids[i], status.ID)
		want := Pending
		if i >= 4 {
			want = Absent
		}
		if request.Operation == "submit" && committed {
			want = Pending
		}
		if i < 4 && (request.Operation == "retire" || (request.Operation == "ack" && committed)) {
			want = Acknowledged
		}
		retired := request.Operation == "retire" && committed && i%4 < 2
		if retired {
			want = Retired
		}
		assert.Equal(t, want, status.Kind, "id=%+v", status.ID)
		assert.Equal(t, retired, status.HasRetirement)
		if retired {
			assert.Equal(t, request.Seed+1, status.RetiredThrough)
		}
	}
	checkCrashReceipts(t, request)
	config := testConfig(t)
	config.Durability = request.Durability
	// Startup's production validator checks receipt digests, payload absence, exact
	// counts, ordering, and floors before the fresh FULL epoch is published.
	queue, err := Open(t.Context(), request.Directory, config)
	require.NoError(t, err)
	defer func() { assert.NoError(t, queue.Close(context.Background())) }()
	require.Greater(t, queue.Snapshot().Store.Epoch, ready.Epoch)
	for _, wire := range ready.Tokens {
		result, err := queue.AckBatch(t.Context(), []Token{{bytes: wire}})
		assert.ErrorIs(t, err, ErrStaleToken)
		assert.Equal(t, NotApplied, result.Decision)
	}
	expected := make(map[ID]Entry)
	if pending > 0 {
		for _, entry := range entries {
			expected[entry.ID] = entry
		}
		if pending == 8 {
			for _, entry := range fresh {
				expected[entry.ID] = entry
			}
		}
	}
	for len(expected) > 0 {
		deliveries, err := queue.Receive(t.Context(), len(expected))
		require.NoError(t, err)
		for _, delivery := range deliveries {
			entry, found := expected[delivery.ID]
			require.True(t, found)
			assert.Equal(t, entry.Codec, delivery.Codec)
			assert.Equal(t, entry.Payload, delivery.Payload)
			delete(expected, delivery.ID)
		}
	}
	if request.Operation == "retire" && committed {
		result, err := queue.SubmitBatch(t.Context(), fresh[:2])
		assert.ErrorIs(t, err, ErrRetired)
		assert.Equal(t, NotApplied, result.Decision)
	}
}

func TestProcessCrashOfflineBoundaries(t *testing.T) {
	cases := []struct {
		operation string
		phases    []string
	}{
		{"initialize", []string{"before-initialize", "during-initialize", "after-initialize"}},
		{"epoch", []string{"before-epoch", "during-epoch", "after-epoch"}},
		{"maintain", []string{"before-step", "after-vacuum", "after-step"}},
		{"policy", []string{"before-mutation", "before-commit", "after-commit"}},
	}
	for _, durability := range []Durability{Synchronized, ProcessCrash} {
		for _, tc := range cases {
			for _, phase := range tc.phases {
				t.Run(durability.String()+"/"+tc.operation+"/"+phase, func(t *testing.T) {
					request := newCrashFixture(t, crashRequest{
						Operation: tc.operation, Phase: phase, Durability: durability, Seed: 73,
					})
					var before Inspection
					var logicalBefore crashLogicalSnapshot
					if tc.operation != "initialize" {
						seedCrashStore(t, request)
						var err error
						before, err = Inspect(t.Context(), request.Directory, nil)
						require.NoError(t, err)
						if tc.operation == "maintain" {
							require.Positive(t, before.Store.FreePages)
							require.Equal(t, Count(3), before.Store.Counts.PendingItems)
							require.Equal(t, Count(4), before.Store.Counts.Receipts)
							require.Equal(t, Count(1), before.Store.Counts.RetiredProducers)
						}
						logicalBefore = readCrashLogicalSnapshot(t, request.Directory)
					}
					killCrashChild(t, request)
					after, err := Inspect(t.Context(), request.Directory, nil)
					incomplete := tc.operation == "initialize" && phase != "after-initialize"
					if incomplete {
						require.ErrorIs(t, err, ErrUnsupported)
						assert.Equal(t, Inspection{}, after)
						// Interrupted initialization must not be silently replaced by a fresh queue.
						config := testConfig(t)
						config.Durability = durability
						queue, openErr := Open(t.Context(), request.Directory, config)
						assert.Nil(t, queue)
						assert.ErrorIs(t, openErr, ErrUnsupported)
						_, statErr := os.Stat(filepath.Join(request.Directory, "queue.sqlite3"))
						require.NoError(t, statErr)
						return
					}
					require.NoError(t, err)
					wantDurability := durability
					if tc.operation == "policy" && phase == "after-commit" {
						if durability == Synchronized {
							wantDurability = ProcessCrash
						} else {
							wantDurability = Synchronized
						}
					}
					assert.Equal(t, wantDurability, after.Store.Durability)
					if tc.operation == "initialize" {
						assert.Equal(t, StoreCounts{}, after.Store.Counts)
						return
					}
					assert.Equal(t, before.Store.UUID, after.Store.UUID)
					assert.Equal(t, before.Store.Counts, after.Store.Counts)
					wantEpoch := before.Store.Epoch
					if tc.operation == "epoch" && phase == "after-epoch" {
						wantEpoch++
					}
					assert.Equal(t, wantEpoch, after.Store.Epoch)
					if tc.operation == "epoch" && phase == "after-epoch" {
						wantEpochBytes := encodeUint64(wantEpoch)
						logicalBefore.metadata[metaEpoch] = wantEpochBytes[:]
					}
					if tc.operation == "policy" && phase == "after-commit" {
						logicalBefore.metadata[metaDurability] = []byte{byte(wantDurability)}
					}
					afterLogical := readCrashLogicalSnapshot(t, request.Directory)
					assert.Equal(t, logicalBefore, afterLogical, "offline logical preservation")
				})
			}
		}
	}
}

func checkCrashReceipts(t *testing.T, request crashRequest) {
	t.Helper()
	opened, err := openOffline(t.Context(), request.Directory)
	require.NoError(t, err)
	defer func() { assert.NoError(t, opened.close()) }()
	expected := make(map[ID]Entry)
	for _, entry := range append(crashEntries(request.Seed), crashEntries(request.Seed+1)...) {
		expected[entry.ID] = entry
	}
	rows, err := opened.storage.conn.QueryContext(t.Context(), "SELECT id,digest FROM receipts")
	require.NoError(t, err)
	var receipts Count
	for rows.Next() {
		var key, digest []byte
		require.NoError(t, rows.Scan(&key, &digest))
		id, err := decodeID(key)
		require.NoError(t, err)
		entry, found := expected[id]
		require.True(t, found)
		want := entryDigest(entry.Codec, entry.Payload)
		assert.Equal(t, want[:], digest)
		receipts++
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	assert.Equal(t, receipts, opened.metadata.counts.Receipts)
}

// The fixture has at most five producers and four receipts/payloads. SQL reads every
// logical column independently of the production validator, with a fixed row bound.
type crashLogicalSnapshot struct {
	metadata  map[int][]byte
	producers []string
	receipts  []string
	payloads  []string
}

func readCrashLogicalSnapshot(t *testing.T, directory string) crashLogicalSnapshot {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(directory, "queue.sqlite3"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	rows, err := db.QueryContext(t.Context(), "SELECT key,value FROM meta ORDER BY key LIMIT 16")
	require.NoError(t, err)
	snapshot := crashLogicalSnapshot{metadata: make(map[int][]byte)}
	for rows.Next() {
		var key int
		var value []byte
		require.NoError(t, rows.Scan(&key, &value))
		snapshot.metadata[key] = value
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Len(t, snapshot.metadata, 15)
	// quote preserves BLOB bytes, integer values, and NULL distinctly. Physical
	// page layout and freelist changes are deliberately outside this comparison.
	snapshot.producers = readCrashLogicalRows(t, db, `SELECT
	 quote(producer)||','||quote(has_retirement)||','||quote(retired_through)||','||
	 quote(cleanup_needed) FROM producers ORDER BY producer LIMIT 6`)
	snapshot.receipts = readCrashLogicalRows(t, db, `SELECT
	 quote(id)||','||quote(digest)||','||quote(state)||','||quote(accept_order)||','||
	 quote(ack_token) FROM receipts ORDER BY id LIMIT 6`)
	snapshot.payloads = readCrashLogicalRows(t, db, `SELECT
	 quote(id)||','||quote(codec)||','||quote(payload)||','||quote(payload_length)
	 FROM payloads ORDER BY id LIMIT 6`)
	return snapshot
}

func readCrashLogicalRows(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	require.NoError(t, err)
	var records []string
	for rows.Next() {
		var record string
		require.NoError(t, rows.Scan(&record))
		records = append(records, record)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.LessOrEqual(t, len(records), 5, "fixture exceeded logical snapshot row bound")
	return records
}
