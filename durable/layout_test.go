package durable

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring/frontier"
	"github.com/freshdresch/go-data-structures/workq"
)

func TestLayoutFixedStorage(t *testing.T) {
	// These are the allocations retained by Open, plus the cleanup key array.
	// Backend objects, path-resolution scratch, and caller-lifetime notifications are separate.
	sizes := []uintptr{
		unsafe.Sizeof(Queue{}), 64 * unsafe.Sizeof(command{}), 64 * unsafe.Sizeof(command{}),
		256 * unsafe.Sizeof(claim{}), unsafe.Sizeof(workq.Queue[int]{}),
		unsafe.Sizeof(frontier.MPMCQueue[int]{}), 3 * 256 * 8,
		unsafe.Sizeof(validatedStore{}), unsafe.Sizeof(store{}), unsafe.Sizeof(directoryOwner{}),
		unsafe.Sizeof([256][24]byte{}),
	}
	var total Bytes
	for _, size := range sizes {
		charge, err := allocationCharge(Bytes(size))
		require.NoError(t, err)
		total += charge
	}
	structural := total
	file := reflect.TypeFor[os.File]().Field(0).Type.Elem()
	// Pinned Go 1.27.3 runtime sources give hchan=112 and timeTimer=112.
	// A timer also owns a channel and one 24-byte time.Time buffer.
	auxiliary := []struct {
		name  string
		count int
		size  int
	}{
		{"file wrappers", 4, int(unsafe.Sizeof(os.File{}))},
		{"file internals", 4, int(file.Size())},
		{"current lifecycle channels", 4, 112},
		{"coordinator-held old notification", 1, 112},
		{"timer objects and channels", 4, 112},
		{"timer values", 2, 24},
		{"directory reader", 1, 32},
		{"directory buffer header", 1, 24},
		{"directory buffer", 1, 8192},
		{"canonical and two child paths", 3, 4096},
		{"retained escaped DSN capacity", 1, 32768},
		{"one temporary file path", 1, 4096},
		{"identity comparison file stats", 2, 200},
		{"decoded profile", 1, 13},
		{"cached cause detail", 1, 256},
		{"directory result capacity", 1, 8 * 16},
		{"directory entries", 4, 56},
		{"recognized artifact names", 4, 16},
		{"directory entry stats", 4, 200},
	}
	for _, object := range auxiliary {
		charge := Bytes(object.count) * expectedAllocation(object.size)
		total += charge
		t.Logf("%s: %d", object.name, charge)
	}
	// Error-reporting graphs/formatting and caller-lifetime objects have explicit
	// exclusions and ownership boundaries, as does the preceding path-resolution phase.
	// This tests working storage after resolution, not all live memory.
	assert.LessOrEqual(t, total, Bytes(1<<20))
	t.Logf("fixed structures=%d auxiliaries=%d combined=%d budget=%d",
		structural, total-structural, total, 1<<20)
	group, err := allocationCharge(64 * Bytes(unsafe.Sizeof(groupedCommand{})))
	require.NoError(t, err)
	assert.LessOrEqual(t, group+Bytes(unsafe.Sizeof(commandGroup{})), Bytes(48<<10))
	t.Logf("group references=%d header=%d budget=%d", group, unsafe.Sizeof(commandGroup{}), 48<<10)
}

func TestLayoutMaximumSubmitCopy(t *testing.T) {
	config := testConfig(t)
	queue := &Queue{config: config}
	entries := make([]Entry, 256)
	remaining := int(config.Runtime.Batches.SubmitEncoded) - len(entries)*(88+64)
	for index := range entries {
		size := min(remaining, int(config.Policy.MaxPayload))
		entries[index] = Entry{ID: ID{Sequence: uint64(index)},
			Codec: strings.Repeat("c", 64), Payload: make([]byte, size)}
		remaining -= size
	}
	require.Zero(t, remaining)
	allocation, err := queue.validateSubmit(entries)
	require.NoError(t, err)
	outputs := make([][]Entry, 64)
	actual := allocationProbe(func(index int) {
		outputs[index] = copyEntries(entries, allocation.payload)
	})
	assert.LessOrEqual(t, actual, uint64(allocation.charge))
	copied := outputs[0]
	require.Equal(t, 256, cap(copied))
	offset := 0
	arena := unsafe.SliceData(copied[0].Payload)
	for index, entry := range copied {
		assert.Equal(t, len(entry.Payload), cap(entry.Payload))
		if len(entry.Payload) > 0 {
			assert.Equal(t, unsafe.Add(unsafe.Pointer(arena), offset),
				unsafe.Pointer(unsafe.SliceData(entry.Payload)))
			assert.NotSame(t, unsafe.SliceData(entries[index].Payload), unsafe.SliceData(entry.Payload))
		}
		assert.NotSame(t, unsafe.StringData(entries[index].Codec), unsafe.StringData(entry.Codec))
		offset += len(entry.Payload)
	}
	maximum, err := maximumSubmitCharge(config.Policy, config.Runtime.Batches)
	require.NoError(t, err)
	assert.Equal(t, Bytes(4223872), maximum)
	assert.LessOrEqual(t, allocation.charge, maximum)
	t.Logf("maximum encoded input copy=%d bytes reservation=%d profile maximum=%d payload=%d",
		actual, allocation.charge, maximum, allocation.payload)
	runtime.KeepAlive(outputs)
}

func TestLayoutReceiveDescriptors(t *testing.T) {
	for _, count := range []int{32, 256} {
		config := testConfig(t)
		config.Runtime.Delivery.ReceiveItems = Count(count)
		require.NoError(t, validateConfig(config))
		outputs := make([][]Delivery, 64)
		actual := allocationProbe(func(index int) { outputs[index] = make([]Delivery, count) })
		charge, err := allocationCharge(Bytes(count) * Bytes(unsafe.Sizeof(Delivery{})))
		require.NoError(t, err)
		assert.LessOrEqual(t, charge, Bytes(count)*256)
		assert.LessOrEqual(t, actual, uint64(count)*256)
		t.Logf("Receive=%d descriptor=%d allocated=%d A-bound=%d allowance=%d",
			count, unsafe.Sizeof(Delivery{}), actual, charge, count*256)
		runtime.KeepAlive(outputs)
	}
}

func TestLayoutReservationCleanup(t *testing.T) {
	for _, abandoned := range []bool{false, true} {
		queue := &Queue{changed: make(chan struct{}), done: make(chan struct{})}
		queue.ordinary = admissionPool{slots: make([]command, 1),
			budget: CommandBudget{Commands: 1, Retained: 33024}}
		queue.progress = admissionPool{slots: make([]command, 1),
			budget: CommandBudget{Commands: 1, Retained: 33024}}
		ordinary, err := queue.reserve(t.Context(), &queue.ordinary, 33024, false)
		require.NoError(t, err)
		progress, err := queue.reserve(t.Context(), &queue.progress, 33024, false)
		require.NoError(t, err)
		ordinary.ids = make([]ID, 256)
		progress.tokens = make([]Token, 256)
		progress.kind = ackCommand
		ordinary.abandoned, progress.abandoned = abandoned, abandoned
		queue.mu.Lock()
		queue.finishLocked(&queue.ordinary, ordinary, CommitResult{}, make([]Status, 256), nil)
		queue.finishLocked(&queue.progress, progress, CommitResult{Decision: Applied}, nil, nil)
		queue.mu.Unlock()
		if !abandoned {
			_, statuses, err := queue.await(t.Context(), &queue.ordinary, ordinary)
			require.NoError(t, err)
			assert.Len(t, statuses, 256)
			_, _, err = queue.await(t.Context(), &queue.progress, progress)
			require.NoError(t, err)
		}
		assert.Zero(t, queue.ordinary.retained)
		assert.Zero(t, queue.progress.retained)
		assert.Equal(t, command{}, *ordinary)
		assert.Equal(t, command{}, *progress)
	}
}

func TestLayoutHandoffFailureCleanup(t *testing.T) {
	for _, mode := range []string{"canceled", "cutoff", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			queue := &Queue{changed: make(chan struct{}), done: make(chan struct{}), config: testConfig(t)}
			queue.ordinary = admissionPool{slots: make([]command, 1),
				budget: CommandBudget{Commands: 1, Retained: 8 << 20}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mode {
			case "canceled":
				cancel()
			case "cutoff":
				queue.closing = true
			case "exhausted":
				queue.lastSequence = math.MaxUint64
			}
			result, err := queue.SubmitBatch(ctx, []Entry{{Payload: make([]byte, 16384)}})
			require.Error(t, err)
			assert.Equal(t, NotApplied, result.Decision)
			assert.Zero(t, queue.ordinary.retained)
			assert.Equal(t, command{}, queue.ordinary.slots[0])
		})
	}
}

func TestLayoutMaximumDeliveryWindow(t *testing.T) {
	config := testConfig(t)
	config.Runtime.Delivery.ReceiveItems = 256
	queue, _ := openTestQueue(t, config)
	entries := make([]Entry, 256)
	for index := range entries {
		entries[index].ID.Sequence = uint64(index)
	}
	result, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, Applied, result.Decision)
	var deliveries []Delivery
	for len(deliveries) < 256 {
		acquired, err := queue.Receive(t.Context(), 256-len(deliveries))
		require.NoError(t, err)
		require.NotEmpty(t, acquired)
		deliveries = append(deliveries, acquired...)
	}
	require.Len(t, deliveries, 256)
	queue.mu.Lock()
	assert.Equal(t, 64, cap(queue.ordinary.slots))
	assert.Equal(t, 64, cap(queue.progress.slots))
	assert.Equal(t, 256, cap(queue.claims))
	assert.Equal(t, 256, queue.deliveryQueue.Capacity())
	assert.Equal(t, Bytes(256*256), queue.deliveryRetained)
	for _, slot := range queue.claims {
		assert.Nil(t, slot.delivery.Payload)
		assert.Empty(t, slot.delivery.Codec)
	}
	queue.mu.Unlock()
	tokens := make([]Token, len(deliveries))
	for index, delivery := range deliveries {
		tokens[index] = delivery.Token
	}
	result, err = queue.AckBatch(t.Context(), tokens)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	require.NoError(t, queue.Close(t.Context()))
	queue.mu.Lock()
	defer queue.mu.Unlock()
	assert.Zero(t, queue.ordinary.retained)
	assert.Zero(t, queue.progress.retained)
	assert.Zero(t, queue.deliveryRetained)
	for _, slot := range queue.claims {
		assert.Equal(t, claim{}, slot)
	}
}

func TestLayoutMaximumSubmitPublicOwnership(t *testing.T) {
	config := testConfig(t)
	queue, directory := openTestQueue(t, config)
	queue.StopDelivery()
	entered, release := make(chan struct{}), make(chan struct{})
	blocker, err := queue.reserve(t.Context(), &queue.ordinary, 256, false)
	require.NoError(t, err)
	blocker.observer = executionGate{entered: entered, release: release}
	require.NoError(t, queue.handoff(t.Context(), blocker))
	<-entered

	entries := make([]Entry, 256)
	digests := make([][32]byte, len(entries))
	remaining := (2 << 20) - len(entries)*(88+64)
	for index := range entries {
		size := min(remaining, 16<<10)
		entries[index] = Entry{ID: ID{Sequence: uint64(index)},
			Codec: strings.Repeat("c", 64), Payload: bytes.Repeat([]byte{byte(index)}, size)}
		digests[index] = entryDigest(entries[index].Codec, entries[index].Payload)
		remaining -= size
	}
	require.Zero(t, remaining)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type answer struct {
		result CommitResult
		err    error
	}
	response := make(chan answer, 1)
	go func() {
		result, err := queue.SubmitBatch(ctx, entries)
		response <- answer{result: result, err: err}
	}()
	waitForHandoff(t, queue, 2)
	queue.mu.Lock()
	assert.Equal(t, Bytes(4191104+256), queue.ordinary.retained)
	owned := &queue.ordinary.slots[1]
	assert.Len(t, owned.entries, 256)
	assert.Equal(t, 256, cap(owned.entries))
	for index, entry := range owned.entries {
		assert.Equal(t, len(entry.Payload), cap(entry.Payload))
		if len(entry.Payload) != 0 {
			assert.NotSame(t, unsafe.SliceData(entries[index].Payload), unsafe.SliceData(entry.Payload))
		}
		assert.NotSame(t, unsafe.StringData(entries[index].Codec), unsafe.StringData(entry.Codec))
	}
	queue.mu.Unlock()
	cancel()
	returned := <-response
	require.ErrorIs(t, returned.err, context.Canceled)
	assert.Equal(t, Unknown, returned.result.Decision)
	for index := range entries {
		clear(entries[index].Payload)
		entries[index].Codec = "reused"
	}
	close(release)
	_, _, err = queue.await(t.Context(), &queue.ordinary, blocker)
	require.NoError(t, err)
	_, err = queue.Lookup(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, queue.Close(t.Context()))
	queue.mu.Lock()
	assert.Zero(t, queue.ordinary.retained)
	for _, slot := range queue.ordinary.slots {
		assert.Equal(t, command{}, slot)
	}
	queue.mu.Unlock()

	reopened, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, reopened.Close(context.Background())) })
	assert.Equal(t, Count(256), reopened.Snapshot().Store.Counts.PendingItems)
	assert.Equal(t, Bytes(2058240), reopened.Snapshot().Store.Counts.PendingPayload)
	seen := make([]bool, 256)
	for received := 0; received < 256; {
		deliveries, err := reopened.Receive(t.Context(), min(32, 256-received))
		require.NoError(t, err)
		require.NotEmpty(t, deliveries)
		tokens := make([]Token, len(deliveries))
		for index, delivery := range deliveries {
			require.Less(t, delivery.ID.Sequence, uint64(len(seen)))
			assert.False(t, seen[delivery.ID.Sequence])
			seen[delivery.ID.Sequence] = true
			assert.Equal(t, digests[delivery.ID.Sequence], entryDigest(delivery.Codec, delivery.Payload))
			tokens[index] = delivery.Token
		}
		result, err := reopened.AckBatch(t.Context(), tokens)
		require.NoError(t, err)
		assert.Equal(t, Applied, result.Decision)
		received += len(deliveries)
	}
	require.NoError(t, reopened.Close(t.Context()))
	t.Log("public Submit: 2097152 encoded bytes, 4191104 reserved, detached after cancellation")
}

func TestLayoutStartupValidationScratch(t *testing.T) {
	// Schema and logical validation run sequentially before active pools exist.
	// Count all expected-field arrays together even though only one is live.
	expected := strings.Split(strings.TrimSuffix(strings.TrimSpace(schemaDDL), ";"), ";")
	schemaSizes := []int{len(expected) * 16, len(schemaDDL), ((len(schemaDDL) + 1) / 2) * 16,
		len(schemaDDL), 24, 16, 8, 8, 8, 24, 24, 16, 8}
	for _, statement := range expected {
		schemaSizes = append(schemaSizes, len(strings.Fields(statement))*16,
			len(strings.Join(strings.Fields(statement), " ")))
	}
	var schema Bytes
	for _, size := range schemaSizes {
		schema += expectedAllocation(size)
	}
	// One driver source and one Scan clone of the maximum payload coexist.
	// The pinned allocator uses the exact 16384 class for each noscan object.
	// Small objects use the more conservative A bound, including both row layers.
	logical := Bytes(2 * 16384)
	logicalSizes := []int{
		64, 64, 64, 64, // source codec, Scan codec, string conversion, hash conversion
		24, 32, 8, 56, 8, 24, 32, 8, 56, 8, // receipt and producer source/Scan copies
		int(reflect.TypeOf(sha256.New()).Elem().Size()), 32, 32, 8, 8,
		136, 32, 48, // metadata, counts, decoded policy integers
		5 * 16, 5 * 16, 2 * 16, 5 * 8, 4 * 24, 2 * 24, 2 * 16, // scan arguments and headers
	}
	for _, size := range logicalSizes {
		logical += expectedAllocation(size)
	}
	metadata := expectedAllocation(15*24) + expectedAllocation(56) + expectedAllocation(13)
	for _, size := range []int{136, 48, 56, 56, 2 * 8, 16, 24, 4 * 16, 2 * 16} {
		metadata += expectedAllocation(size)
	}
	for index := range 15 {
		size := 8
		switch index + 1 {
		case metaProfile:
			size = 13
		case metaUUID:
			size = 16
		case metaPolicy:
			size = 56
		case metaDurability:
			size = 1
		}
		metadata += expectedAllocation(size)
	}
	assert.LessOrEqual(t, schema, Bytes(64<<10))
	assert.LessOrEqual(t, logical, Bytes(64<<10))
	assert.LessOrEqual(t, metadata, Bytes(64<<10))
	t.Logf("startup separate phase bounds: schema=%d metadata=%d logical=%d budget=%d",
		schema, metadata, logical, 64<<10)
}
