package main

import (
	"context"
	"crypto/sha256"
	"math/bits"
	"path/filepath"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/durable"
)

func TestLatencyBuckets(t *testing.T) {
	var stats latency
	for _, duration := range []time.Duration{0, 1, 2, 3, 4, 7, 8} {
		stats.add(duration)
	}
	assert.Equal(t, uint64(7), stats.Count)
	assert.Equal(t, time.Duration(25), stats.Total)
	assert.Equal(t, time.Duration(8), stats.Max)
	assert.Equal(t, [4]uint64{2, 2, 2, 1}, [4]uint64(stats.Buckets[:4]))
}

func TestDeadlineAfterPartialCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		credits := make(chan struct{}, 2)
		credits <- struct{}{}
		stop := make(chan struct{})
		deadline := time.Now().Add(time.Second)
		assert.Equal(t, 1, reserveCredits(credits, 2, stop, deadline))
		assert.False(t, time.Now().Before(deadline))
	})
}

func TestWaveOracle(t *testing.T) {
	work := newWorkload(options{Payload: 256, Batch: 1})
	tests := []struct {
		name   string
		change func(*durable.Delivery)
	}{
		{name: "unknown producer", change: func(item *durable.Delivery) { item.ID.Producer[1] = 9 }},
		{name: "below base", change: func(item *durable.Delivery) { item.ID.Sequence = 0 }},
		{name: "outside prefix", change: func(item *durable.Delivery) { item.ID.Sequence = 2 }},
		{name: "wrong payload", change: func(item *durable.Delivery) { item.Payload = []byte("wrong") }},
		{name: "wrong codec", change: func(item *durable.Delivery) { item.Codec = "wrong" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := newWave([producerCount]uint64{})
			current.entered[0].Store(1)
			item := durable.Delivery{
				ID: durable.ID{Producer: producerID(0), Sequence: 1}, Codec: codec, Payload: work.payloads[0],
			}
			test.change(&item)
			_, err := current.validate(item, work.digests)
			require.Error(t, err)
		})
	}
	current := newWave([producerCount]uint64{})
	current.entered[0].Store(1)
	item := durable.Delivery{
		ID: durable.ID{Producer: producerID(0), Sequence: 1}, Codec: codec, Payload: work.payloads[0],
	}
	_, err := current.validate(item, work.digests)
	require.NoError(t, err)
	_, err = current.validate(item, work.digests)
	require.Error(t, err)
	current.accepted[0] = 1
	require.Error(t, current.checkComplete(), "missing Ack")
	current.acknowledged.Store(1)
	require.NoError(t, current.checkComplete())
	current.seen[0] = 0
	require.Error(t, current.checkComplete(), "missing ID")
	assert.Equal(t, sha256.Sum256(work.payloads[0]), work.digests[0])
}

func TestProductionSmoke(t *testing.T) {
	for _, policy := range []string{"FULL", "NORMAL"} {
		for _, batch := range []int{1, 256} {
			for _, payload := range []int{256, 4096} {
				opts := options{
					Directory: filepath.Join(t.TempDir(), "queue"), Policy: policy, Payload: payload,
					Batch: batch, Warmup: 10 * time.Millisecond, Measure: 20 * time.Millisecond, Revision: "smoke",
				}
				report, err := measure(opts)
				require.NoError(t, err, "policy %s batch %d", policy, batch)
				assert.True(t, report.Valid)
				assert.Positive(t, report.Measured.Accepted)
				assert.Equal(t, report.Measured.Accepted, report.Measured.Acknowledged)
				assert.Equal(t, durable.Closed, report.Final.State)
				assert.Zero(t, report.Final.Store.Counts.Receipts)
				assert.Equal(t, report.Before.Store.Epoch+1, report.Inspection.Store.Epoch)
			}
		}
	}
}

func TestExpiredWave(t *testing.T) {
	opts := options{Payload: 256, Batch: 256}
	config, err := queueConfig(opts)
	require.NoError(t, err)
	queue, err := durable.Open(context.Background(), t.TempDir(), config)
	require.NoError(t, err)
	work := newWorkload(opts)
	current, err := work.runWave(queue, time.Now().Add(-time.Second))
	require.NoError(t, err)
	assert.Zero(t, current.acknowledged.Load())
	require.NoError(t, queue.Close(context.Background()))
}

func TestInspectionOracle(t *testing.T) {
	expected := durable.StoreInfo{UUID: durable.UUID{1}, Epoch: 7}
	ids := make([]durable.ID, producerCount)
	statuses := make([]durable.Status, producerCount)
	for producer := range producerCount {
		ids[producer] = durable.ID{Producer: producerID(producer), Sequence: uint64(producer + 9)}
		statuses[producer] = durable.Status{
			ID: ids[producer], Kind: durable.Retired, HasRetirement: true,
			RetiredThrough: ids[producer].Sequence,
		}
	}
	valid := durable.Inspection{Store: expected, Statuses: statuses}
	valid.Store.Counts = durable.StoreCounts{Producers: producerCount, RetiredProducers: producerCount}
	require.NoError(t, checkInspection(valid, expected, ids))
	tests := []struct {
		name   string
		change func(*durable.Inspection)
	}{
		{name: "UUID", change: func(value *durable.Inspection) { value.Store.UUID[0]++ }},
		{name: "epoch", change: func(value *durable.Inspection) { value.Store.Epoch++ }},
		{name: "durability", change: func(value *durable.Inspection) { value.Store.Durability++ }},
		{name: "policy", change: func(value *durable.Inspection) { value.Store.Policy.MaxReceipts++ }},
		{name: "pending", change: func(value *durable.Inspection) { value.Store.Counts.PendingItems++ }},
		{name: "receipts", change: func(value *durable.Inspection) { value.Store.Counts.Receipts++ }},
		{name: "markers", change: func(value *durable.Inspection) {
			value.Store.Counts.CleanupMarkers++
		}},
		{name: "floor", change: func(value *durable.Inspection) { value.Statuses[0].RetiredThrough++ }},
		{name: "state", change: func(value *durable.Inspection) {
			value.Statuses[0].Kind = durable.Acknowledged
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			candidate.Statuses = append([]durable.Status(nil), valid.Statuses...)
			test.change(&candidate)
			require.Error(t, checkInspection(candidate, expected, ids))
		})
	}
}

func TestFullBatchCreditWaitAndStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		current := newWave([producerCount]uint64{})
		deadline := time.Now().Add(time.Hour)
		assert.Equal(t, creditWindow,
			reserveCredits(current.credits[0], creditWindow, current.stop, deadline))
		completed := make(chan int, 1)
		go func() { completed <- reserveCredits(current.credits[0], 1, current.stop, deadline) }()
		synctest.Wait()
		assert.Empty(t, completed, "credit wait must remain blocked before Ack returns credit")
		returnCredits(current.credits[0], 1)
		assert.Equal(t, 1, <-completed)
		close(current.stop)
		assert.Zero(t, reserveCredits(current.credits[0], creditWindow, current.stop, deadline))
	})
}

func TestMissingProgressStopsAtWatchdog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		current := newWave([producerCount]uint64{})
		current.accepted[0] = 1
		started := time.Now()
		err := current.waitDrain(time.Second)
		require.ErrorIs(t, err, errDrainTimeout)
		assert.Equal(t, time.Second, time.Since(started))
		assert.Zero(t, current.acknowledged.Load())
	})
}

func TestFailedWavePreservesAccounting(t *testing.T) {
	opts := options{Policy: "FULL", Payload: 256, Batch: 1}
	config, err := queueConfig(opts)
	require.NoError(t, err)
	queue, err := durable.Open(t.Context(), t.TempDir(), config)
	require.NoError(t, err)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for queue.Snapshot().Outcomes.Acks == 0 {
			select {
			case <-queue.Done():
				return
			default:
				runtime.Gosched()
			}
		}
		queue.StopAdmission()
	}()
	stats, err := newWorkload(opts).phase(queue, time.Hour)
	<-stopped
	require.Error(t, err)
	queue.Abort(err)
	_ = queue.Wait() // The intentional admission failure is already asserted above.
	assert.Positive(t, stats.Accepted)
	assert.Positive(t, stats.Acknowledged)
	assert.Positive(t, stats.Submit.Count)
	assert.Positive(t, stats.Ack.Count)
	assert.Equal(t, queue.Snapshot().Outcomes.Acks, stats.Acknowledged)
	require.NotNil(t, stats.FailedWave)
	var accepted, acked uint64
	for _, count := range stats.FailedWave.Accepted {
		accepted += count
	}
	for _, word := range stats.FailedWave.Acked {
		acked += uint64(bits.OnesCount64(word))
	}
	assert.Equal(t, stats.Accepted, accepted)
	assert.Equal(t, stats.Acknowledged, acked)
}
