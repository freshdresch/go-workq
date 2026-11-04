//go:build linux

package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/durable"
)

func TestMaximumSizes(t *testing.T) {
	sizes := maximumSizes()
	require.Len(t, sizes, 127)
	total := 0
	for i, size := range sizes {
		assert.Positive(t, size)
		assert.LessOrEqual(t, size, 16<<10)
		if i < 126 {
			assert.Equal(t, 16<<10, size)
		}
		total += 88 + 64 + size
	}
	assert.Equal(t, 2<<20, total)
}

func TestSmallCapacityBookkeeping(t *testing.T) {
	config, err := productionConfig(durable.Synchronized, 8, 0)
	require.NoError(t, err)
	config.Policy.MaxPendingItems = 8
	config.Policy.MaxReceipts = 16
	config.Policy.MaxProducers = 2
	harness := &harness{
		directory: filepath.Join(t.TempDir(), "queue"), config: config,
		recorder: &recorder{encoder: json.NewEncoder(io.Discard)},
	}
	require.NoError(t, harness.open(t.Context()))
	t.Cleanup(func() {
		if harness.queue != nil {
			harness.queue.Abort(nil)
			assert.NoError(t, harness.queue.Wait())
		}
	})
	for cycle := range 2 {
		require.NoError(t, harness.capacity(t.Context(), 2, 8))
		// Producer zero also retires the hole left by the refused fresh ID.
		assert.Equal(t, map[[16]byte]uint64{
			producer(0): uint64((cycle + 1) * 9), producer(1): uint64((cycle + 1) * 8),
		}, harness.retiredThrough)
	}
	snapshot := harness.queue.Snapshot()
	assert.Equal(t, durable.Count(2), snapshot.Store.Counts.RetiredProducers)
	assert.Zero(t, snapshot.Store.Counts.CleanupMarkers)
	require.NoError(t, harness.close(t.Context()))
	inspection, err := harness.inspect(t.Context(), harness.frontierStatuses(durable.Retired))
	require.NoError(t, err)
	assert.Zero(t, inspection.Store.Counts.Receipts)
	require.Len(t, inspection.Statuses, 2)
	for _, status := range inspection.Statuses {
		assert.Equal(t, durable.Retired, status.Kind)
	}
}

func TestSmallMixedPayloadBookkeeping(t *testing.T) {
	config, err := productionConfig(durable.ProcessCrash, 8, 0)
	require.NoError(t, err)
	harness := &harness{
		directory: filepath.Join(t.TempDir(), "queue"), config: config,
		recorder: &recorder{encoder: json.NewEncoder(io.Discard)},
	}
	require.NoError(t, harness.open(t.Context()))
	t.Cleanup(func() {
		if harness.queue != nil {
			harness.queue.Abort(nil)
			assert.NoError(t, harness.queue.Wait())
		}
	})
	for range 2 {
		expected, err := harness.fill(t.Context(), []int{256, 4096, 16 << 10})
		require.NoError(t, err)
		require.NoError(t, harness.drain(t.Context(), expected))
		require.NoError(t, harness.retire(t.Context()))
	}
	require.NoError(t, harness.close(t.Context()))
}

func TestResourceSample(t *testing.T) {
	sample, err := readResources(t.TempDir())
	require.NoError(t, err)
	assert.Positive(t, sample.RSS)
	assert.Positive(t, sample.HeapAlloc)
	assert.False(t, sample.Main.Exists)
	assert.False(t, sample.WAL.Exists)
}

func TestMaximumBatchPublicValidation(t *testing.T) {
	config, err := productionConfig(durable.Synchronized, 8, 0)
	require.NoError(t, err)
	harness := &harness{
		directory: filepath.Join(t.TempDir(), "queue"), config: config,
		recorder: &recorder{encoder: json.NewEncoder(io.Discard)},
	}
	require.NoError(t, harness.open(t.Context()))
	harness.queue.StopAdmission()
	entries := harness.records(maximumSizes())
	result, err := harness.queue.SubmitBatch(t.Context(), entries)
	assert.Equal(t, durable.NotApplied, result.Decision)
	// Public input validation precedes the admission gate.
	assert.ErrorIs(t, err, durable.ErrStopped)
	last := len(entries) - 1
	entries[last].Payload = append(entries[last].Payload, 0)
	result, err = harness.queue.SubmitBatch(t.Context(), entries)
	assert.Equal(t, durable.NotApplied, result.Decision)
	assert.ErrorIs(t, err, durable.ErrInvalidInput)
	require.NoError(t, harness.close(t.Context()))
}

func TestExecuteSmallCapacity(t *testing.T) {
	config, err := productionConfig(durable.Synchronized, 8, 0)
	require.NoError(t, err)
	config.Policy.MaxPendingItems = 4
	config.Policy.MaxReceipts = 16
	config.Policy.MaxProducers = 2
	require.NoError(t, execute(
		t.Context(), filepath.Join(t.TempDir(), "queue"), "capacity", config,
		&recorder{encoder: json.NewEncoder(io.Discard)},
	))
}
func TestMappingPath(t *testing.T) {
	for _, name := range []string{"plain", "with space", "symlink"} {
		t.Run(name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), name)
			require.NoError(t, os.Mkdir(directory, 0700))
			for _, file := range []string{
				"queue.sqlite3", "queue.sqlite3-wal", "queue.sqlite3-shm", "queue.sqlite3-other",
			} {
				touchMapping(t, filepath.Join(directory, file))
			}
			if name == "symlink" {
				alias := filepath.Join(t.TempDir(), "alias")
				require.NoError(t, os.Symlink(directory, alias))
				directory = alias
			}
			sample, err := readResources(directory)
			require.NoError(t, err)
			require.True(t, sample.MappingValid, sample.MappingUnavailable)
			assert.Equal(t, uint64(3*os.Getpagesize()), sample.MappingRSS)
		})
	}
}

func touchMapping(t *testing.T, path string) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, file.Close()) })
	size := os.Getpagesize()
	require.NoError(t, file.Truncate(int64(size)))
	mapping, err := syscall.Mmap(
		int(file.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, syscall.Munmap(mapping)) })
	mapping[0] = 42
}

func TestMappingUnavailable(t *testing.T) {
	for _, name := range []string{"newline\n", `escaped\012`, "deleted", "missing"} {
		t.Run(name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), name)
			if name != "missing" {
				require.NoError(t, os.Mkdir(directory, 0700))
				path := filepath.Join(directory, "queue.sqlite3")
				touchMapping(t, path)
				if name == "deleted" {
					require.NoError(t, os.Remove(path))
				}
			}
			sample, err := readResources(directory)
			require.NoError(t, err)
			assert.False(t, sample.MappingValid)
			assert.NotEmpty(t, sample.MappingUnavailable)
		})
	}
}

func TestValidateStatuses(t *testing.T) {
	expected := []durable.Status{
		{ID: durable.ID{Producer: producer(0), Sequence: 3}, Kind: durable.Retired,
			HasRetirement: true, RetiredThrough: 3},
		{ID: durable.ID{Producer: producer(1), Sequence: 7}, Kind: durable.Retired,
			HasRetirement: true, RetiredThrough: 7},
	}
	require.NoError(t, validateStatuses(expected, expected))
	for _, name := range []string{
		"nil", "missing", "extra", "reordered", "id", "kind", "retirement", "floor",
	} {
		t.Run(name, func(t *testing.T) {
			actual := slices.Clone(expected)
			switch name {
			case "nil":
				actual = nil
			case "missing":
				actual = actual[:1]
			case "extra":
				actual = append(actual, expected[0])
			case "reordered":
				actual[0], actual[1] = actual[1], actual[0]
			case "id":
				actual[0].ID.Sequence++
			case "kind":
				actual[0].Kind = durable.Absent
			case "retirement":
				actual[0].HasRetirement = false
			case "floor":
				actual[0].RetiredThrough++
			}
			assert.Error(t, validateStatuses(actual, expected))
		})
	}
	absent := []durable.Status{{ID: expected[0].ID, Kind: durable.Absent}}
	require.NoError(t, validateStatuses(absent, absent))
	actual := slices.Clone(absent)
	actual[0].RetiredThrough = 1
	assert.Error(t, validateStatuses(actual, absent))
}

func TestFrontierStatuses(t *testing.T) {
	harness := harness{
		producers: 2, sequences: [256]uint64{3, 7},
		retiredThrough: map[[16]byte]uint64{producer(0): 3, producer(1): 7},
	}
	assert.Equal(t, []durable.Status{
		{ID: durable.ID{Producer: producer(0), Sequence: 3}, Kind: durable.Retired,
			HasRetirement: true, RetiredThrough: 3},
		{ID: durable.ID{Producer: producer(1), Sequence: 7}, Kind: durable.Retired,
			HasRetirement: true, RetiredThrough: 7},
	}, harness.frontierStatuses(durable.Retired))
	harness.retiredThrough = nil
	assert.Equal(t, []durable.Status{
		{ID: durable.ID{Producer: producer(0), Sequence: 3}, Kind: durable.Absent},
		{ID: durable.ID{Producer: producer(1), Sequence: 7}, Kind: durable.Absent},
	}, harness.frontierStatuses(durable.Absent))
}

func TestOfflineOracles(t *testing.T) {
	for _, name := range []string{"lifecycle", "small-main"} {
		t.Run(name, func(t *testing.T) {
			config, err := productionConfig(durable.Synchronized, 8, 0)
			require.NoError(t, err)
			if name == "small-main" {
				config.Policy.DatabaseLimit = 256 << 10
			}
			harness := &harness{
				directory: filepath.Join(t.TempDir(), "queue"), config: config,
				recorder: &recorder{encoder: json.NewEncoder(io.Discard)},
			}
			require.NoError(t, harness.open(t.Context()))
			t.Cleanup(func() {
				if harness.queue != nil {
					harness.queue.Abort(nil)
					assert.NoError(t, harness.queue.Wait())
				}
			})
			if name == "small-main" {
				require.NoError(t, harness.smallMain(t.Context()))
				return
			}
			expected, err := harness.fill(t.Context(), []int{256, 4096, 16384})
			require.NoError(t, err)
			require.NoError(t, harness.drain(t.Context(), expected))
			require.NoError(t, harness.retire(t.Context()))
			require.NoError(t, harness.lifecycle(t.Context()))
			require.NoError(t, harness.close(t.Context()))
		})
	}
}

func TestCommittedRetirementAcrossCycles(t *testing.T) {
	config, err := productionConfig(durable.Synchronized, 8, 0)
	require.NoError(t, err)
	config.Policy.MaxProducers = 2
	harness := &harness{
		directory: filepath.Join(t.TempDir(), "queue"), config: config,
		recorder: &recorder{encoder: json.NewEncoder(io.Discard)},
	}
	require.NoError(t, harness.open(t.Context()))
	t.Cleanup(func() {
		if harness.queue != nil {
			harness.queue.Abort(nil)
			assert.NoError(t, harness.queue.Wait())
		}
	})
	for cycle := range 3 {
		previous := map[[16]byte]uint64(nil)
		if cycle > 0 {
			previous = map[[16]byte]uint64{
				producer(0): uint64(cycle * 2), producer(1): uint64(cycle),
			}
		}
		expected, err := harness.fill(t.Context(), []int{256, 4096, 256})
		require.NoError(t, err)
		assert.Equal(t, previous, harness.retiredThrough)
		// A pending record refuses retirement, so generation cannot advance the ledger.
		require.ErrorIs(t, harness.retire(t.Context()), durable.ErrConflict)
		assert.Equal(t, previous, harness.retiredThrough)
		require.NoError(t, harness.check(t.Context(), expected, durable.Pending))
		require.NoError(t, harness.drain(t.Context(), expected))
		assert.Equal(t, previous, harness.retiredThrough)
		require.NoError(t, harness.retire(t.Context()))
		assert.Equal(t, map[[16]byte]uint64{
			producer(0): uint64((cycle + 1) * 2), producer(1): uint64(cycle + 1),
		}, harness.retiredThrough)
	}
	require.NoError(t, harness.close(t.Context()))
}

func TestExpectedStatusRetirementMetadata(t *testing.T) {
	harness := harness{retiredThrough: map[[16]byte]uint64{
		producer(0): 0, producer(1): 7,
	}}
	for _, kind := range []durable.StatusKind{durable.Pending, durable.Acknowledged, durable.Absent} {
		for _, tc := range []struct {
			name    string
			index   int
			retired bool
			floor   uint64
		}{
			{name: "retired-through-zero", index: 0, retired: true},
			{name: "retired-through-seven", index: 1, retired: true, floor: 7},
			{name: "unretired", index: 2},
		} {
			t.Run(kind.String()+"/"+tc.name, func(t *testing.T) {
				id := durable.ID{Producer: producer(tc.index), Sequence: 10}
				assert.Equal(t, durable.Status{
					ID: id, Kind: kind, HasRetirement: tc.retired, RetiredThrough: tc.floor,
				}, harness.expectedStatus(id, kind))
			})
		}
	}
}

func TestValidateReopened(t *testing.T) {
	config, err := productionConfig(durable.Synchronized, 8, 0)
	require.NoError(t, err)
	inspected := durable.StoreInfo{
		UUID: durable.UUID{1}, Epoch: 7, Policy: config.Policy, Durability: config.Durability,
		Counts:        durable.StoreCounts{Producers: 2, RetiredProducers: 2},
		DatabaseBytes: 4096, PageCount: 1,
	}
	reopened := inspected
	reopened.Epoch++
	require.NoError(t, validateReopened(reopened, inspected))
	for _, name := range []string{
		"uuid", "policy", "durability", "reused-epoch", "skipped-epoch", "pending-items",
		"pending-payload", "receipts", "producers", "retired-producers", "cleanup-markers",
	} {
		t.Run(name, func(t *testing.T) {
			actual := reopened
			switch name {
			case "uuid":
				actual.UUID[0]++
			case "policy":
				actual.Policy.MaxReceipts++
			case "durability":
				actual.Durability = durable.ProcessCrash
			case "reused-epoch":
				actual.Epoch--
			case "skipped-epoch":
				actual.Epoch++
			case "pending-items":
				actual.Counts.PendingItems++
			case "pending-payload":
				actual.Counts.PendingPayload++
			case "receipts":
				actual.Counts.Receipts++
			case "producers":
				actual.Counts.Producers++
			case "retired-producers":
				actual.Counts.RetiredProducers++
			case "cleanup-markers":
				actual.Counts.CleanupMarkers++
			}
			assert.Error(t, validateReopened(actual, inspected))
		})
	}
	reopened.DatabaseBytes++
	reopened.WALBytes++
	reopened.PageCount++
	reopened.FreePages++
	assert.NoError(t, validateReopened(reopened, inspected))
}
