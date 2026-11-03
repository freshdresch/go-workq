//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable/internal/sqlitefault"
)

// These fixtures model unsynchronized loss, not physical hardware power loss.
// The oracle caps images at 64 MiB, events at 8192, and identities at eight.
func TestFaultQueueImages(t *testing.T) {
	if mode := os.Getenv("WORKQ_QUEUE_IMAGE"); mode != "" {
		if os.Getenv("WORKQ_QUEUE_RECOVER") != "" {
			data, err := os.ReadFile(os.Getenv("WORKQ_QUEUE_RECOVER"))
			require.NoError(t, err)
			var boundary queueImageBoundary
			require.NoError(t, json.Unmarshal(data, &boundary))
			checkQueueImage(t, mode, boundary)
			return
		}
		queueImageChild(t, mode)
		return
	}
	for _, mode := range []string{
		"normal/submit", "normal/ack", "normal/retire",
		"full/submit", "full/ack", "full/retire", "normal/reopen",
		"normal/inspect", "normal/upgrade", "full/downgrade",
	} {
		t.Run(mode, func(t *testing.T) { runQueueImage(t, mode) })
	}
}

type queueImageBoundary struct {
	UUID       UUID                 `json:"uuid"`
	Manifest   sqlitefault.Manifest `json:"manifest"`
	Epoch      uint64               `json:"epoch"`
	PriorEpoch uint64               `json:"prior_epoch"`
	OldTokens  [][56]byte           `json:"old_tokens"`
	OpenTokens [][56]byte           `json:"open_tokens"`
	AckTokens  [][56]byte           `json:"ack_tokens"`
}

func runQueueImage(t *testing.T, mode string) {
	t.Helper()
	scratchBase := os.TempDir()
	parent, err := os.MkdirTemp(scratchBase, "queue-fault-")
	require.NoError(t, err)
	scratch, err := os.MkdirTemp(scratchBase, "queue-evidence-")
	require.NoError(t, err)
	root := filepath.Join(parent, "queue")
	require.NoError(t, os.Mkdir(root, 0700))
	t.Logf("fixture %s evidence %s", root, scratch)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		assert.NoError(t, os.RemoveAll(parent))
		assert.NoError(t, os.RemoveAll(scratch))
	})
	durability := ProcessCrash
	if strings.HasPrefix(mode, "full/") {
		durability = Synchronized
	}
	operation := strings.Split(mode, "/")[1]
	var expected queueImageBoundary
	if operation != "submit" {
		expected.OldTokens = seedCrashStore(t, crashRequest{Directory: root, Operation: operation,
			Durability: durability, Seed: 41})
		baseline, err := Inspect(t.Context(), root, nil)
		require.NoError(t, err)
		expected.PriorEpoch = baseline.Store.Epoch
		if operation == "retire" {
			expected.AckTokens = expected.OldTokens
		}
	}
	expectationsPath := filepath.Join(scratch, "expectations.json")
	expectationsData, err := json.Marshal(expected)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(expectationsPath, expectationsData, 0600))
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	defer func() { _ = writer.Close() }() // Also covers a failed Start.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultQueueImages$")
	command.Env = append(os.Environ(), "WORKQ_QUEUE_IMAGE="+mode,
		"WORKQ_QUEUE_ROOT="+root, "WORKQ_QUEUE_SCRATCH="+scratch,
		"WORKQ_QUEUE_EXPECTATIONS="+expectationsPath)
	command.ExtraFiles = []*os.File{writer}
	log, err := os.Create(filepath.Join(scratch, "child.log"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, log.Close()) }()
	command.Stdout, command.Stderr = log, log
	require.NoError(t, command.Start())
	require.NoError(t, writer.Close())
	joined := false
	defer func() {
		if !joined {
			_ = command.Process.Kill() // Join before any fixture cleanup, even on assertion failure.
			_ = command.Wait()
		}
	}()
	require.NoError(t, reader.SetReadDeadline(time.Now().Add(25*time.Second)))
	var boundary queueImageBoundary
	require.NoError(t, json.NewDecoder(reader).Decode(&boundary), "child log: %s", log.Name())
	require.NoError(t, command.Process.Kill())
	err = command.Wait()
	joined = true
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, status.Signal())
	wantEpoch := expected.PriorEpoch + 1
	if operation == "inspect" || operation == "upgrade" || operation == "downgrade" {
		wantEpoch = expected.PriorEpoch
	}
	require.Equal(t, wantEpoch, boundary.Epoch)
	require.Equal(t, expected.PriorEpoch, boundary.PriorEpoch)
	require.Equal(t, expected.OldTokens, boundary.OldTokens)
	if operation == "retire" {
		require.Equal(t, expected.AckTokens, boundary.AckTokens)
	}
	require.True(t, boundary.Manifest.DirectoryDurable)
	var lastWALWrite, lastWALSync uint64
	roles := make(map[string]bool)
	for _, event := range boundary.Manifest.Events {
		if event.Phase != "after" || !event.Performed || event.ActualResult != 0 {
			continue
		}
		if event.Operation == "owned-sync" {
			require.Equal(t, event.Expected, event.Actual)
			roles[event.Role] = true
		}
		if event.Role != "wal" {
			continue
		}
		if event.Operation == "native-write" {
			lastWALWrite = event.Sequence
		}
		if event.Operation == "native-sync" {
			lastWALSync = event.Sequence
		}
	}
	require.True(t, roles["main"])
	require.True(t, roles["directory"])
	require.True(t, roles["parent"])
	if operation == "submit" || operation == "ack" || operation == "retire" {
		require.NotZero(t, lastWALWrite)
		require.NotZero(t, lastWALSync)
		if durability == ProcessCrash {
			require.Greater(t, lastWALWrite, lastWALSync)
		} else {
			require.Greater(t, lastWALSync, lastWALWrite)
		}
	}
	require.Contains(t, boundary.Manifest.Synchronized, "queue.sqlite3")
	if operation == "submit" || operation == "ack" || operation == "retire" || operation == "reopen" {
		require.Contains(t, boundary.Manifest.Synchronized, "queue.sqlite3-wal")
	}
	_, err = sqlitefault.Restore(boundary.Manifest)
	require.NoError(t, err)
	boundaryPath := filepath.Join(scratch, "boundary.json")
	data, err := json.Marshal(boundary)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(boundaryPath, data, 0600))
	recovery := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultQueueImages$")
	recovery.Env = append(os.Environ(), "WORKQ_QUEUE_IMAGE="+mode,
		"WORKQ_QUEUE_RECOVER="+boundaryPath)
	output, err := recovery.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func checkQueueImage(t *testing.T, mode string, boundary queueImageBoundary) {
	t.Helper()
	root := boundary.Manifest.Root
	durability := ProcessCrash
	if strings.HasPrefix(mode, "full/") {
		durability = Synchronized
	}
	operation := strings.Split(mode, "/")[1]
	ids := make([]ID, 4)
	for i, entry := range crashEntries(41) {
		ids[i] = entry.ID
	}
	inspection, err := Inspect(t.Context(), root, ids)
	require.NoError(t, err)
	wantEpoch := boundary.PriorEpoch + 1
	if operation == "inspect" || operation == "upgrade" || operation == "downgrade" {
		wantEpoch = boundary.PriorEpoch
	}
	assert.Equal(t, wantEpoch, inspection.Store.Epoch)
	assert.Equal(t, boundary.UUID, inspection.Store.UUID)
	assert.Equal(t, testConfig(t).Policy, inspection.Store.Policy)
	require.Len(t, inspection.Statuses, len(ids))
	expectedPolicy := durability
	if operation == "upgrade" {
		expectedPolicy = Synchronized
	}
	if operation == "downgrade" {
		expectedPolicy = ProcessCrash
	}
	assert.Equal(t, expectedPolicy, inspection.Store.Durability)
	for i, status := range inspection.Statuses {
		want := Pending
		switch operation {
		case "submit":
			if durability == ProcessCrash {
				want = Absent
			}
		case "ack":
			if durability == Synchronized {
				want = Acknowledged
			}
		case "retire":
			want = Acknowledged
			if durability == Synchronized && i < 2 {
				want = Retired
			}
		}
		assert.Equal(t, ids[i], status.ID)
		assert.Equal(t, want, status.Kind, "id %v", status.ID)
		retired := operation == "retire" && durability == Synchronized && i < 2
		assert.Equal(t, retired, status.HasRetirement)
		floor := uint64(0)
		if retired {
			floor = 42
		}
		assert.Equal(t, floor, status.RetiredThrough)
	}
	pending := Count(4)
	if operation == "retire" || (operation == "ack" && durability == Synchronized) ||
		(operation == "submit" && durability == ProcessCrash) {
		pending = 0
	}
	counts := StoreCounts{PendingItems: pending, PendingPayload: Bytes(pending) * 16384,
		Receipts: 4, Producers: 4}
	if operation == "submit" && durability == ProcessCrash {
		counts = StoreCounts{}
	}
	if operation == "retire" && durability == Synchronized {
		counts.RetiredProducers, counts.CleanupMarkers = 2, 2
	}
	assert.Equal(t, counts, inspection.Store.Counts)
	checkCrashReceipts(t, crashRequest{Directory: root, Seed: 41})
	checkQueueImageAckTokens(t, boundary, operation, durability)
	config := testConfig(t)
	config.Durability = expectedPolicy
	queue, err := Open(t.Context(), root, config)
	require.NoError(t, err)
	require.Equal(t, wantEpoch+1, queue.Snapshot().Store.Epoch)
	if pending > 0 {
		expected := make(map[ID]Entry)
		for _, entry := range crashEntries(41) {
			expected[entry.ID] = entry
		}
		for range pending {
			deliveries, err := queue.Receive(t.Context(), 1)
			require.NoError(t, err)
			require.Len(t, deliveries, 1)
			delivery := deliveries[0]
			if operation == "reopen" {
				checkQueueImageFencing(t, queue, delivery.Token, boundary.OldTokens)
				checkQueueImageFencing(t, queue, delivery.Token, boundary.OpenTokens)
			}
			entry, found := expected[delivery.ID]
			require.True(t, found)
			assert.Equal(t, entry.Codec, delivery.Codec)
			assert.Equal(t, entry.Payload, delivery.Payload)
			result, err := queue.AckBatch(t.Context(), []Token{delivery.Token})
			require.NoError(t, err)
			require.Equal(t, Applied, result.Decision, "current delivery token")
			delete(expected, delivery.ID)
		}
		assert.Empty(t, expected)
	}
	require.NoError(t, queue.Close(t.Context()))
}

func queueImageChild(t *testing.T, mode string) {
	data, err := os.ReadFile(os.Getenv("WORKQ_QUEUE_EXPECTATIONS"))
	require.NoError(t, err)
	var boundary queueImageBoundary
	require.NoError(t, json.Unmarshal(data, &boundary))
	root := os.Getenv("WORKQ_QUEUE_ROOT")
	scratch := filepath.Join(os.Getenv("WORKQ_QUEUE_SCRATCH"), "images")
	require.NoError(t, os.Mkdir(scratch, 0700))
	_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{Events: 8192, Scratch: scratch})
	require.NoError(t, err)
	faultController.Store(controller)
	config := testConfig(t)
	config.Durability = ProcessCrash
	if strings.HasPrefix(mode, "full/") {
		config.Durability = Synchronized
	}
	config.Runtime.Group.MaxDelay = 0
	config.Runtime.Group.Commands = 2
	// The tested group and its predecessor cannot trigger retirement cleanup.
	config.Runtime.Maintenance.MaxCommandTurns = 8
	operation := strings.Split(mode, "/")[1]
	var epoch uint64
	var uuid UUID
	switch operation {
	case "inspect", "upgrade", "downgrade":
		baseline, err := openOffline(t.Context(), root)
		require.NoError(t, err)
		uuid = baseline.metadata.uuid
		require.NoError(t, baseline.close())
		// Offline operations must preserve the independently observed baseline epoch.
		epoch = boundary.PriorEpoch
		if operation == "inspect" {
			inspection, err := Inspect(t.Context(), root, nil)
			require.NoError(t, err)
			require.Equal(t, epoch, inspection.Store.Epoch)
		} else {
			next := Synchronized
			if operation == "downgrade" {
				next = ProcessCrash
			}
			result, err := ChangeDurability(t.Context(), root, config.Durability, next)
			require.NoError(t, err)
			require.Equal(t, Applied, result.Decision)
		}
	default:
		queue, err := Open(t.Context(), root, config)
		require.NoError(t, err)
		epoch = queue.Snapshot().Store.Epoch
		require.Equal(t, boundary.PriorEpoch+1, epoch)
		uuid = queue.Snapshot().Store.UUID
		if operation == "reopen" {
			tokens := receiveCrashTokens(t, queue, 4)
			for _, token := range tokens {
				checkQueueImageFencing(t, queue, token, boundary.OldTokens)
				boundary.OpenTokens = append(boundary.OpenTokens, token.bytes)
			}
			parkQueueImage(t, queue)
		} else {
			tokens := queueImageGroup(t, queue, operation)
			if operation == "ack" {
				boundary.AckTokens = tokens
			}
		}
	}
	manifest, err := controller.Freeze()
	require.NoError(t, err)
	pipe := os.NewFile(3, "freeze")
	boundary.Manifest, boundary.Epoch, boundary.UUID = manifest, epoch, uuid
	require.NoError(t, json.NewEncoder(pipe).Encode(boundary))
	for {
		_ = syscall.Pause()
	} // Parent kills and joins without allowing clean Close.
}

func waitQueueSequence(t *testing.T, queue *Queue, target uint64) {
	t.Helper()
	queue.mu.Lock()
	for queue.lastSequence < target {
		changed := queue.changed
		queue.mu.Unlock()
		select {
		case <-changed:
		case <-time.After(10 * time.Second):
			t.Fatal("command did not hand off")
		}
		queue.mu.Lock()
	}
	queue.mu.Unlock()
}

func parkQueueImage(t *testing.T, queue *Queue) {
	t.Helper()
	entered := make(chan struct{})
	go func() {
		_, _ = queue.submitBatchObserved(t.Context(), nil,
			executionGate{entered: entered, release: make(chan struct{})})
	}() // The recording child owns this goroutine until SIGKILL.
	<-entered
}

func queueImageGroup(t *testing.T, queue *Queue, operation string) [][56]byte {
	t.Helper()
	var tokens []Token
	if operation == "ack" {
		tokens = receiveCrashTokens(t, queue, 4)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	gateDone := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(t.Context(), nil,
			executionGate{entered: entered, release: release})
		gateDone <- err
	}()
	<-entered
	queue.mu.Lock()
	target := queue.lastSequence
	queue.mu.Unlock()
	type outcome struct {
		result CommitResult
		err    error
	}
	results := make(chan outcome, 2) // Exactly two commands form this transaction group.
	entries := crashEntries(41)
	for index := range 2 {
		go func() {
			var result CommitResult
			var err error
			switch operation {
			case "submit":
				result, err = queue.SubmitBatch(t.Context(), entries[index*2:index*2+2])
			case "ack":
				result, err = queue.AckBatch(t.Context(), tokens[index*2:index*2+2])
			case "retire":
				result, err = queue.RetireThrough(t.Context(), entries[index].ID.Producer, 42)
			}
			results <- outcome{result: result, err: err}
		}()
	}
	waitQueueSequence(t, queue, target+2)
	parked := make(chan struct{})
	go func() {
		_, _ = queue.submitBatchObserved(t.Context(), nil,
			executionGate{entered: parked, release: make(chan struct{})})
	}() // Group.Commands forces this admitted gate into the following group.
	waitQueueSequence(t, queue, target+3)
	close(release)
	require.NoError(t, <-gateDone)
	for range 2 {
		outcome := <-results
		require.NoError(t, outcome.err)
		require.Equal(t, Applied, outcome.result.Decision)
	}
	<-parked
	require.Equal(t, Count(2), queue.Snapshot().LastGroup.Commands)
	wire := make([][56]byte, len(tokens))
	for i, token := range tokens {
		wire[i] = token.bytes
	}
	return wire
}

// Attempts reset on Open. Matching every other field makes epoch the sole fence.
func checkQueueImageFencing(t *testing.T, queue *Queue, current Token, old [][56]byte) {
	t.Helper()
	require.Len(t, old, 4)
	var matched bool
	for _, wire := range old {
		if !bytes.Equal(wire[24:48], current.bytes[24:48]) {
			continue
		}
		matched = true
		require.Equal(t, wire[:16], current.bytes[:16])
		require.Equal(t, wire[24:], current.bytes[24:])
		require.Less(
			t,
			binary.BigEndian.Uint64(wire[16:24]),
			binary.BigEndian.Uint64(current.bytes[16:24]),
		)
		token := Token{bytes: wire}
		require.ErrorIs(t, queue.Release(token), ErrStaleToken)
		result, err := queue.AckBatch(t.Context(), []Token{token})
		require.ErrorIs(t, err, ErrStaleToken)
		require.Equal(t, NotApplied, result.Decision)
		id, err := decodeID(current.bytes[24:48])
		require.NoError(t, err)
		queue.mu.Lock()
		claimed := queue.currentClaimLocked(id, current) >= 0
		queue.mu.Unlock()
		require.True(t, claimed, "old token must not clear the current claim")
	}
	require.True(t, matched, "no previous token for current claim")
}

func checkQueueImageAckTokens(
	t *testing.T,
	boundary queueImageBoundary,
	operation string,
	durability Durability,
) {
	t.Helper()
	require.LessOrEqual(t, len(boundary.AckTokens), 4)
	expected := make(map[[24]byte][56]byte, len(boundary.AckTokens))
	for _, token := range boundary.AckTokens {
		expected[[24]byte(token[24:48])] = token
	}
	opened, err := openOffline(t.Context(), boundary.Manifest.Root)
	require.NoError(t, err)
	defer func() { assert.NoError(t, opened.close()) }()
	rows, err := opened.storage.conn.QueryContext(
		t.Context(),
		"SELECT id,ack_token FROM receipts WHERE state=1 LIMIT 5",
	)
	require.NoError(t, err)
	var count int
	for rows.Next() {
		var id, token []byte
		require.NoError(t, rows.Scan(&id, &token))
		require.Len(t, id, 24)
		want, found := expected[[24]byte(id)]
		require.True(t, found, "unexpected acknowledged receipt %x", id)
		assert.Equal(t, want[:], token, "acknowledged receipt %x", id)
		count++
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	wantCount := 0
	if operation == "retire" || (operation == "ack" && durability == Synchronized) {
		wantCount = 4
	}
	assert.Equal(t, wantCount, count)
}
