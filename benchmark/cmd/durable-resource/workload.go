//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"

	"github.com/freshdresch/go-workq/durable"
)

type expectedRecord struct {
	id   durable.ID
	size int
}

type harness struct {
	lastWAL        int64
	haveWAL        bool
	directory      string
	config         durable.Config
	recorder       *recorder
	queue          *durable.Queue
	sequences      [256]uint64
	retiredThrough map[[16]byte]uint64
	producers      int
	receipts       int
}

func (h *harness) boundary(phase string, started time.Time) error {
	value := event{Kind: "boundary", Phase: phase, Duration: time.Since(started)}
	if h.queue != nil {
		snapshot := h.queue.Snapshot()
		value.Snapshot = &snapshot
	}
	sample, err := readResources(h.directory)
	if err != nil {
		return err
	}
	value.Sample = &sample
	if h.haveWAL {
		delta := sample.WAL.Length - h.lastWAL
		value.WALBoundaryDelta = &delta
	}
	h.lastWAL = sample.WAL.Length
	h.haveWAL = true
	return h.recorder.write(value)
}

func (h *harness) open(ctx context.Context) error {
	if err := h.recorder.sample(h.directory, "before-open"); err != nil {
		return err
	}
	started := time.Now()
	queue, err := durable.Open(ctx, h.directory, h.config)
	if err != nil {
		return err
	}
	h.queue = queue
	if err := h.boundary("open", started); err != nil {
		return err
	}
	return h.recorder.write(event{
		Kind: "effective-settings", Config: &h.config,
		Detail: "Successful Open verifies exact readback via store.configure and selectDurability. " +
			"This is not an independent harness pragma query. " +
			"WAL, page_size=4096, auto_vacuum=2, temp_store=2, wal_autocheckpoint=0, " +
			"automatic_index=0; synchronous=2 for synchronized, 1 for process-crash. " +
			"Offline Inspect/Maintain use fixed 64MiB cache and 256MiB mmap.",
	})
}

func (h *harness) close(ctx context.Context) error {
	started := time.Now()
	err := h.queue.Close(ctx)
	boundaryErr := h.boundary("close", started)
	h.queue = nil
	return errors.Join(err, boundaryErr)
}

func producer(index int) [16]byte {
	var result [16]byte
	binary.BigEndian.PutUint64(result[8:], uint64(index))
	return result
}

func payload(id durable.ID, size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = byte(uint64(i)*31 + id.Sequence*17 + uint64(id.Producer[15]))
	}
	return result
}

func (h *harness) records(sizes []int) []durable.Entry {
	entries := make([]durable.Entry, len(sizes))
	for i, size := range sizes {
		index := i % int(h.config.Policy.MaxProducers)
		h.sequences[index]++
		h.producers = max(h.producers, index+1)
		id := durable.ID{Producer: producer(index), Sequence: h.sequences[index]}
		entries[i] = durable.Entry{ID: id, Codec: strings.Repeat("c", 64), Payload: payload(id, size)}
	}
	return entries
}

func maximumSizes() []int {
	const limit = 2 << 20
	const overhead = 88 + 64
	const fullSize = 16 << 10
	remaining := limit
	var sizes []int
	for remaining > overhead {
		size := min(fullSize, remaining-overhead)
		sizes = append(sizes, size)
		remaining -= overhead + size
	}
	return sizes
}

func (h *harness) submit(ctx context.Context, entries []durable.Entry) error {
	started := time.Now()
	result, err := h.queue.SubmitBatch(ctx, entries)
	boundaryErr := h.boundary("submit", started)
	if err != nil {
		return errors.Join(err, boundaryErr)
	}
	if result.Decision != durable.Applied {
		return errors.New("submit did not return Applied")
	}
	h.receipts += len(entries)
	return boundaryErr
}

func (h *harness) fill(ctx context.Context, sizes []int) ([]expectedRecord, error) {
	entries := h.records(sizes)
	expected := make([]expectedRecord, len(entries))
	for i, entry := range entries {
		expected[i] = expectedRecord{id: entry.ID, size: len(entry.Payload)}
	}
	for start := 0; start < len(entries); {
		end, encoded := start, 0
		for end < len(entries) && end-start < 256 {
			next := 88 + len(entries[end].Codec) + len(entries[end].Payload)
			if encoded+next > 2<<20 {
				break
			}
			encoded += next
			end++
		}
		if end == start {
			return nil, errors.New("record cannot fit a legal submit batch")
		}
		if err := h.submit(ctx, entries[start:end]); err != nil {
			return nil, err
		}
		start = end
	}
	if err := h.check(ctx, expected, durable.Pending); err != nil {
		return nil, err
	}
	var total int
	for _, size := range sizes {
		total += size
	}
	if err := h.counts(len(entries), total, h.receipts); err != nil {
		return nil, err
	}
	return expected, nil
}

func (h *harness) counts(pending, payloadBytes, receipts int) error {
	actual := h.queue.Snapshot().Store.Counts
	if actual.PendingItems != durable.Count(pending) ||
		actual.PendingPayload != durable.Bytes(payloadBytes) ||
		actual.Receipts != durable.Count(receipts) || actual.Producers != durable.Count(h.producers) {
		return fmt.Errorf("counts %+v, want pending=%d bytes=%d receipts=%d producers=%d",
			actual, pending, payloadBytes, receipts, h.producers)
	}
	return nil
}

func (h *harness) check(
	ctx context.Context,
	expected []expectedRecord,
	kind durable.StatusKind,
) error {
	for start := 0; start < len(expected); start += 256 {
		batch := expected[start:min(start+256, len(expected))]
		want := make([]durable.Status, len(batch))
		for i, record := range batch {
			want[i] = h.expectedStatus(record.id, kind)
		}
		ids := statusIDs(want)
		started := time.Now()
		statuses, err := h.queue.Lookup(ctx, ids)
		if err != nil {
			return err
		}
		if err := h.boundary("lookup", started); err != nil {
			return err
		}
		if err := validateStatuses(statuses, want); err != nil {
			return fmt.Errorf("lookup: %w", err)
		}
	}
	return nil
}

func (h *harness) drain(ctx context.Context, expected []expectedRecord) error {
	remaining := make(map[durable.ID]int, len(expected))
	for _, record := range expected {
		remaining[record.id] = record.size
	}
	for len(remaining) > 0 {
		started := time.Now()
		deliveries, err := h.queue.Receive(ctx, 256)
		if err != nil {
			return err
		}
		if err := h.boundary("receive", started); err != nil {
			return err
		}
		if len(deliveries) == 0 {
			return errors.New("empty successful receive")
		}
		tokens := make([]durable.Token, len(deliveries))
		for i, delivery := range deliveries {
			size, exists := remaining[delivery.ID]
			if !exists {
				return fmt.Errorf("unexpected or duplicate delivery %+v", delivery.ID)
			}
			if delivery.Codec != strings.Repeat("c", 64) ||
				!bytes.Equal(delivery.Payload, payload(delivery.ID, size)) {
				return fmt.Errorf("payload mismatch for %+v", delivery.ID)
			}
			delete(remaining, delivery.ID)
			tokens[i] = delivery.Token
		}
		started = time.Now()
		result, err := h.queue.AckBatch(ctx, tokens)
		boundaryErr := h.boundary("ack", started)
		if err != nil {
			return errors.Join(err, boundaryErr)
		}
		if result.Decision != durable.Applied {
			return errors.New("ack did not return Applied")
		}
		if boundaryErr != nil {
			return boundaryErr
		}
	}
	if err := h.check(ctx, expected, durable.Acknowledged); err != nil {
		return err
	}
	return h.counts(0, 0, h.receipts)
}

func (h *harness) retire(ctx context.Context) error {
	for i := range h.producers {
		started := time.Now()
		result, err := h.queue.RetireThrough(ctx, producer(i), h.sequences[i])
		boundaryErr := h.boundary("retire", started)
		if err != nil {
			return errors.Join(err, boundaryErr)
		}
		if result.Decision != durable.Applied {
			return errors.New("retire did not return Applied")
		}
		if h.retiredThrough == nil {
			h.retiredThrough = make(map[[16]byte]uint64)
		}
		h.retiredThrough[producer(i)] = h.sequences[i]
		if boundaryErr != nil {
			return boundaryErr
		}
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		started := time.Now()
		if _, err := h.queue.Lookup(cleanupCtx, nil); err != nil {
			return err
		}
		if err := h.boundary("cleanup-lookup", started); err != nil {
			return err
		}
		counts := h.queue.Snapshot().Store.Counts
		if counts.Receipts == 0 && counts.CleanupMarkers == 0 {
			break
		}
		select {
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		case <-ticker.C:
		}
	}
	h.receipts = 0
	if err := h.counts(0, 0, 0); err != nil {
		return err
	}
	expected := h.frontierStatuses(durable.Retired)
	statuses, err := h.queue.Lookup(ctx, statusIDs(expected))
	if err != nil {
		return err
	}
	if err := validateStatuses(statuses, expected); err != nil {
		return fmt.Errorf("retirement: %w", err)
	}
	return h.boundary("cleanup-complete", time.Now())
}

func (h *harness) capacity(ctx context.Context, waves, count int) error {
	for range waves {
		sizes := make([]int, count)
		for i := range sizes {
			sizes[i] = 4096
		}
		expected, err := h.fill(ctx, sizes)
		if err != nil {
			return err
		}
		if err := h.drain(ctx, expected); err != nil {
			return err
		}
	}
	started := time.Now()
	entries := h.records([]int{256})
	result, err := h.queue.SubmitBatch(ctx, entries)
	boundaryErr := h.boundary("receipt-capacity-refusal", started)
	if result.Decision != durable.NotApplied || !errors.Is(err, durable.ErrCapacity) {
		return fmt.Errorf("receipt capacity returned %s, %w", result.Decision, err)
	}
	if boundaryErr != nil {
		return boundaryErr
	}
	if err := h.counts(0, 0, waves*count); err != nil {
		return err
	}
	refused := []expectedRecord{{id: entries[0].ID, size: len(entries[0].Payload)}}
	if err := h.check(ctx, refused, durable.Absent); err != nil {
		return err
	}
	return h.retire(ctx)
}

func (h *harness) churn(ctx context.Context, cycles int) error {
	for cycle := range cycles {
		for _, sizes := range [][]int{maximumSizes(), mixedSizes(), payloadLimitSizes()} {
			expected, err := h.fill(ctx, sizes)
			if err != nil {
				return err
			}
			if err := h.drain(ctx, expected); err != nil {
				return err
			}
			if err := h.retire(ctx); err != nil {
				return err
			}
		}
		if err := h.boundary(fmt.Sprintf("cycle-%d-complete", cycle+1), time.Now()); err != nil {
			return err
		}
	}
	return nil
}

func mixedSizes() []int {
	sizes := make([]int, 1024)
	for i := range sizes {
		sizes[i] = []int{256, 4096, 16 << 10}[i%3]
	}
	return sizes
}

func payloadLimitSizes() []int {
	sizes := make([]int, 1024)
	for i := range sizes {
		sizes[i] = 16 << 10
	}
	return sizes
}

func (h *harness) expectedStatus(id durable.ID, kind durable.StatusKind) durable.Status {
	floor, exists := h.retiredThrough[id.Producer]
	return durable.Status{
		ID: id, Kind: kind, HasRetirement: exists, RetiredThrough: floor,
	}
}

func (h *harness) frontierStatuses(kind durable.StatusKind) []durable.Status {
	expected := make([]durable.Status, h.producers)
	for i := range expected {
		id := durable.ID{Producer: producer(i), Sequence: h.sequences[i]}
		expected[i] = h.expectedStatus(id, kind)
	}
	return expected
}

func statusIDs(statuses []durable.Status) []durable.ID {
	ids := make([]durable.ID, len(statuses))
	for i, status := range statuses {
		ids[i] = status.ID
	}
	return ids
}

func validateStatuses(actual, expected []durable.Status) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("status length %d, want %d", len(actual), len(expected))
	}
	for i, status := range actual {
		if status != expected[i] {
			return fmt.Errorf("status %d: got %+v, want %+v", i, status, expected[i])
		}
	}
	return nil
}

func (h *harness) inspect(
	ctx context.Context,
	expected []durable.Status,
) (durable.Inspection, error) {
	ids := statusIDs(expected)
	started := time.Now()
	inspection, err := durable.Inspect(ctx, h.directory, ids)
	if err != nil {
		return durable.Inspection{}, err
	}
	if err := h.recorder.write(event{
		Kind: "inspection", Duration: time.Since(started), Inspection: &inspection,
	}); err != nil {
		return durable.Inspection{}, err
	}
	if err := h.recorder.sample(h.directory, "inspect"); err != nil {
		return durable.Inspection{}, err
	}
	if err := validateStatuses(inspection.Statuses, expected); err != nil {
		return durable.Inspection{}, fmt.Errorf("inspection: %w", err)
	}
	return inspection, nil
}

func (h *harness) lifecycle(ctx context.Context) error {
	before := h.queue.Snapshot().Store
	if err := h.close(ctx); err != nil {
		return err
	}
	expectedStatuses := h.frontierStatuses(durable.Retired)
	initial, err := h.inspect(ctx, expectedStatuses)
	if err != nil {
		return err
	}
	if initial.Store.UUID != before.UUID || initial.Store.Epoch != before.Epoch ||
		initial.Store.Policy != before.Policy || initial.Store.Durability != before.Durability ||
		initial.Store.Counts != before.Counts {
		return errors.New("close/inspect changed logical store")
	}
	for i := 0; i < 8; i++ {
		started := time.Now()
		result, err := durable.Maintain(ctx, h.directory)
		recordErr := h.recorder.write(event{
			Kind: "maintenance", Duration: time.Since(started), Maintenance: &result,
		})
		if err != nil {
			return errors.Join(err, recordErr)
		}
		if recordErr != nil {
			return recordErr
		}
		if err := h.boundary("maintain", started); err != nil {
			return err
		}
		after, err := h.inspect(ctx, expectedStatuses)
		if err != nil {
			return err
		}
		if after.Store.UUID != initial.Store.UUID || after.Store.Epoch != initial.Store.Epoch ||
			after.Store.Policy != initial.Store.Policy ||
			after.Store.Durability != initial.Store.Durability ||
			after.Store.Counts != initial.Store.Counts ||
			!slices.Equal(after.Statuses, initial.Statuses) {
			return errors.New("maintenance changed logical store")
		}
		if after.Store.FreePages >= initial.Store.FreePages {
			break
		}
		initial = after
	}
	if err := h.open(ctx); err != nil {
		return err
	}
	if err := validateReopened(h.queue.Snapshot().Store, initial.Store); err != nil {
		return err
	}
	statuses, err := h.queue.Lookup(ctx, statusIDs(expectedStatuses))
	if err != nil {
		return err
	}
	if err := validateStatuses(statuses, expectedStatuses); err != nil {
		return fmt.Errorf("reopen lookup: %w", err)
	}
	expected, err := h.fill(ctx, payloadLimitSizes())
	if err != nil {
		return err
	}
	if err := h.drain(ctx, expected); err != nil {
		return err
	}
	return h.retire(ctx)
}

func (h *harness) smallMain(ctx context.Context) error {
	started := time.Now()
	result, err := h.queue.SubmitBatch(ctx, h.records(maximumSizes()))
	boundaryErr := h.boundary("main-cap-refusal", started)
	if boundaryErr != nil {
		return boundaryErr
	}
	var native sqlite3.Error
	if result.Decision == durable.Applied || !errors.Is(err, durable.ErrStorage) ||
		errors.Is(err, durable.ErrCapacity) ||
		!errors.As(err, &native) || native.Code != sqlite3.ErrFull {
		return fmt.Errorf("main cap returned %s, %w", result.Decision, err)
	}
	if err := h.recorder.write(event{
		Kind: "expected-refusal", Detail: result.Decision.String() + ": " + err.Error(),
	}); err != nil {
		return err
	}
	joined := h.queue.Wait()
	if !errors.Is(joined, durable.ErrStorage) {
		return fmt.Errorf("fault join: %w", joined)
	}
	h.queue = nil
	inspection, err := h.inspect(ctx, h.frontierStatuses(durable.Absent))
	if err != nil {
		return err
	}
	if inspection.Store.Counts != (durable.StoreCounts{}) {
		return fmt.Errorf("failed batch left counts %+v", inspection.Store.Counts)
	}
	h.producers = 0
	h.sequences = [256]uint64{}
	if err := h.open(ctx); err != nil {
		return err
	}
	if err := validateReopened(h.queue.Snapshot().Store, inspection.Store); err != nil {
		return err
	}
	return h.close(ctx)
}

func validateReopened(actual, inspected durable.StoreInfo) error {
	if actual.UUID != inspected.UUID || actual.Policy != inspected.Policy ||
		actual.Durability != inspected.Durability || actual.Counts != inspected.Counts {
		return fmt.Errorf("reopen changed logical store: got %+v, inspected %+v", actual, inspected)
	}
	if actual.Epoch != inspected.Epoch+1 {
		return fmt.Errorf("reopen epoch %d, want %d", actual.Epoch, inspected.Epoch+1)
	}
	return nil
}
