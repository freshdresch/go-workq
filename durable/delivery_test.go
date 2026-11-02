package durable

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func receiveOne(t *testing.T, queue *Queue) Delivery {
	t.Helper()
	deliveries, err := queue.Receive(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	return deliveries[0]
}

func TestDeliveryRoundTrip(t *testing.T) {
	for _, durability := range []Durability{Synchronized, ProcessCrash} {
		t.Run(durability.String(), func(t *testing.T) {
			t.Parallel()
			config := testConfig(t)
			config.Durability = durability
			queue, directory := openTestQueue(t, config)
			entry := Entry{ID: ID{Sequence: 1}, Codec: "raw", Payload: []byte("payload")}
			result, err := queue.SubmitBatch(t.Context(), []Entry{entry})
			require.NoError(t, err)
			require.Equal(t, Applied, result.Decision)
			delivery := receiveOne(t, queue)
			assert.Equal(t, entry.ID, delivery.ID)
			assert.Equal(t, entry.Codec, delivery.Codec)
			assert.Equal(t, entry.Payload, delivery.Payload)
			delivery.Payload[0] = 'X'
			require.NoError(t, queue.Release(delivery.Token))
			assert.ErrorIs(t, queue.Release(delivery.Token), ErrStaleToken)
			retry := receiveOne(t, queue)
			assert.NotEqual(t, delivery.Token, retry.Token)
			assert.Equal(t, entry.Payload, retry.Payload)
			result, err = queue.AckBatch(t.Context(), []Token{retry.Token})
			require.NoError(t, err)
			assert.Equal(t, Applied, result.Decision)
			result, err = queue.AckBatch(t.Context(), []Token{retry.Token})
			require.NoError(t, err)
			assert.Equal(t, Applied, result.Decision)
			result, err = queue.AckBatch(t.Context(), []Token{delivery.Token})
			assert.ErrorIs(t, err, ErrStaleToken)
			assert.Equal(t, NotApplied, result.Decision)
			assert.ErrorIs(t, queue.Release(retry.Token), ErrStaleToken)
			queue.StopAdmission()
			deliveries, err := queue.Receive(t.Context(), 1)
			assert.Nil(t, deliveries)
			assert.ErrorIs(t, err, ErrDrained)
			require.NoError(t, queue.Close(t.Context()))
			reopened, err := Open(t.Context(), directory, config)
			require.NoError(t, err)
			defer func() { assert.NoError(t, reopened.Close(context.Background())) }()
			result, err = reopened.AckBatch(t.Context(), []Token{retry.Token})
			assert.ErrorIs(t, err, ErrStaleToken)
			assert.Equal(t, NotApplied, result.Decision)
			statuses, err := reopened.Lookup(t.Context(), []ID{entry.ID})
			require.NoError(t, err)
			assert.Equal(t, Acknowledged, statuses[0].Kind)
		})
	}
}

func TestAckAtomicValidation(t *testing.T) {
	t.Parallel()
	queue, _ := openTestQueue(t, testConfig(t))
	_, err := queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 1}}, {ID: ID{Sequence: 2}}})
	require.NoError(t, err)
	first, second := receiveOne(t, queue), receiveOne(t, queue)
	result, err := queue.AckBatch(t.Context(), []Token{first.Token, first.Token})
	assert.ErrorIs(t, err, ErrInvalidInput)
	assert.Equal(t, NotApplied, result.Decision)
	result, err = queue.AckBatch(t.Context(), []Token{first.Token, {}})
	assert.ErrorIs(t, err, ErrStaleToken)
	assert.Equal(t, NotApplied, result.Decision)
	statuses, err := queue.Lookup(t.Context(), []ID{first.ID, second.ID})
	require.NoError(t, err)
	assert.Equal(t, Pending, statuses[0].Kind)
	assert.Equal(t, Pending, statuses[1].Kind)
	result, err = queue.AckBatch(t.Context(), []Token{first.Token, second.Token})
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
}

func waitDeliveryState(t *testing.T, queue *Queue, predicate func(*Queue) bool) {
	t.Helper()
	for {
		queue.mu.Lock()
		matched := predicate(queue)
		changed := queue.changed
		queue.mu.Unlock()
		if matched {
			return
		}
		select {
		case <-changed:
		case <-t.Context().Done():
			t.Fatal("delivery state not reached")
		}
	}
}

func TestDeliveryWindowAndIndependentProgress(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	config.Runtime.Delivery.Items = 3
	config.Runtime.Delivery.ReceiveItems = 3
	config.Runtime.Ordinary.Commands = 1
	queue, _ := openTestQueue(t, config)
	entries := make([]Entry, 4)
	for index := range entries {
		entries[index].ID.Sequence = uint64(index)
	}
	_, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	var deliveries []Delivery
	for len(deliveries) < 3 {
		received, err := queue.Receive(t.Context(), 3-len(deliveries))
		require.NoError(t, err)
		require.NotEmpty(t, received)
		deliveries = append(deliveries, received...)
	}
	require.Len(t, deliveries, 3)
	assert.Equal(t, ID{Sequence: 0}, deliveries[0].ID)
	assert.Equal(t, ID{Sequence: 1}, deliveries[1].ID)
	assert.Equal(t, ID{Sequence: 2}, deliveries[2].ID)
	// A reserved but unhanded Ordinary command exhausts that class without an
	// earlier sequence. Progress must still execute and release window capacity.
	reserved, err := queue.reserve(t.Context(), &queue.ordinary, 256, false)
	require.NoError(t, err)
	result, err := queue.AckBatch(t.Context(), []Token{deliveries[0].Token})
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	next := receiveOne(t, queue)
	assert.Equal(t, ID{Sequence: 3}, next.ID)
	queue.mu.Lock()
	queue.clearLocked(&queue.ordinary, reserved)
	queue.mu.Unlock()
}

type acquisitionGate struct{ acquired, resume chan struct{} }

func (g *acquisitionGate) Acquired() {
	close(g.acquired)
	<-g.resume
}

func TestReceiveAcquisitionWinsCancellationAndStop(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			t.Parallel()
			queue, _ := openTestQueue(t, testConfig(t))
			_, err := queue.SubmitBatch(t.Context(), []Entry{{}})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gate := &acquisitionGate{acquired: make(chan struct{}), resume: make(chan struct{})}
			type result struct {
				deliveries []Delivery
				err        error
			}
			completed := make(chan result, 1)
			go func() {
				deliveries, err := queue.receiveObserved(ctx, 1, gate)
				completed <- result{deliveries: deliveries, err: err}
			}()
			<-gate.acquired
			if stop {
				queue.StopDelivery()
			} else {
				cancel()
			}
			close(gate.resume)
			received := <-completed
			require.NoError(t, received.err)
			require.Len(t, received.deliveries, 1)
			_, err = queue.AckBatch(t.Context(), []Token{received.deliveries[0].Token})
			require.NoError(t, err)
		})
	}
}

func TestStopAndCancellationBeforeAcquisition(t *testing.T) {
	t.Parallel()
	queue, _ := openTestQueue(t, testConfig(t))
	_, err := queue.SubmitBatch(t.Context(), []Entry{{}, {ID: ID{Sequence: 1}}})
	require.NoError(t, err)
	waitDeliveryState(t, queue, func(q *Queue) bool { return q.deliveryRetained == 512 })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	deliveries, err := queue.Receive(ctx, 1)
	assert.Nil(t, deliveries)
	assert.ErrorIs(t, err, context.Canceled)
	queue.StopDelivery()
	deliveries, err = queue.Receive(t.Context(), 1)
	assert.Nil(t, deliveries)
	assert.ErrorIs(t, err, ErrStopped)
	waitDeliveryState(t, queue, func(q *Queue) bool { return q.deliveryClosed })
	queue.mu.Lock()
	assert.Zero(t, queue.deliveryRetained)
	for _, slot := range queue.claims {
		assert.False(t, slot.used)
	}
	queue.mu.Unlock()
	statuses, err := queue.Lookup(t.Context(), []ID{{}, {Sequence: 1}})
	require.NoError(t, err)
	assert.Equal(t, Pending, statuses[0].Kind)
	assert.Equal(t, Pending, statuses[1].Kind)
}

func TestDeliveryScratchAndDescriptors(t *testing.T) {
	assert.Equal(t, uintptr(120), unsafe.Sizeof(Delivery{}))
	assert.LessOrEqual(t, sourceBufferCharge(16384), Bytes(16384))
	config := testConfig(t)
	minimum := deliveryScratchCharge(config.Policy.MaxPayload, config.Policy.MaxCodec)
	assert.Equal(t, Bytes(17664), minimum)
	config.Runtime.Delivery.ReadScratch = minimum - 1
	assert.ErrorIs(t, validateConfig(config), ErrInvalidInput)
	config.Runtime.Delivery.ReadScratch = minimum
	require.NoError(t, validateConfig(config))
	queue, _ := openTestQueue(t, config)
	payload := make([]byte, config.Policy.MaxPayload)
	codec := string(make([]byte, config.Policy.MaxCodec))
	_, err := queue.SubmitBatch(t.Context(), []Entry{{Payload: payload, Codec: codec}})
	require.NoError(t, err)
	delivery := receiveOne(t, queue)
	assert.Equal(t, payload, delivery.Payload)
	assert.Equal(t, codec, delivery.Codec)
}

func ExampleQueue_Receive() {
	directory, err := os.MkdirTemp("", "durable-example-")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := os.RemoveAll(directory); err != nil {
			panic(err)
		}
	}()
	policy := DurablePolicy{
		Profile: "sqlite-wal-v1", MaxPayload: 16384, MaxCodec: 64,
		MaxPendingItems: 4096, MaxPendingPayload: 16 << 20,
		MaxReceipts: 16384, MaxProducers: 256, DatabaseLimit: 1 << 30,
	}
	runtime, err := DefaultRuntime(policy)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	queue, err := Open(
		ctx,
		filepath.Join(directory, "queue"),
		Config{Policy: policy, Runtime: runtime},
	)
	if err != nil {
		panic(err)
	}
	if _, err := queue.SubmitBatch(ctx, []Entry{{Payload: []byte("work")}}); err != nil {
		panic(err)
	}
	deliveries, err := queue.Receive(ctx, 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(deliveries[0].Payload))
	result, err := queue.AckBatch(ctx, []Token{deliveries[0].Token})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Decision)
	if err := queue.Close(ctx); err != nil {
		panic(err)
	}
	// Output:
	// work
	// applied
}

type deliveryReadGate struct{ entered, resume chan struct{} }

func (g *deliveryReadGate) BeforeDeliveryRead() {
	close(g.entered)
	<-g.resume
}

func TestStopDeliveryDuringReadReclaimsWithoutCommandWake(t *testing.T) {
	t.Parallel()
	queue, _ := openTestQueue(t, testConfig(t))
	entered, release := make(chan struct{}), make(chan struct{})
	submitted := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(t.Context(), []Entry{{}},
			executionGate{entered: entered, release: release})
		submitted <- err
	}()
	<-entered
	gate := &deliveryReadGate{entered: make(chan struct{}), resume: make(chan struct{})}
	queue.mu.Lock()
	queue.deliveryObserver = gate
	queue.mu.Unlock()
	close(release)
	<-gate.entered
	// Consume the response before stopping so its reservation release cannot wake cleanup.
	require.NoError(t, <-submitted)
	queue.StopDelivery()
	close(gate.resume)
	waitDeliveryState(t, queue, func(q *Queue) bool { return q.deliveryClosed })
	queue.mu.Lock()
	assert.Zero(t, queue.deliveryRetained)
	for _, slot := range queue.claims {
		assert.False(t, slot.used)
	}
	queue.mu.Unlock()
	deliveries, err := queue.Receive(t.Context(), 1)
	assert.Nil(t, deliveries)
	assert.ErrorIs(t, err, ErrStopped)
	statuses, err := queue.Lookup(t.Context(), []ID{{}})
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.Equal(t, Pending, statuses[0].Kind)
}

func TestAckAndReleaseFollowHandoffOrder(t *testing.T) {
	for _, releaseFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(releaseFirst), func(t *testing.T) {
			t.Parallel()
			queue, _ := openTestQueue(t, testConfig(t))
			_, err := queue.SubmitBatch(t.Context(), []Entry{{}})
			require.NoError(t, err)
			delivery := receiveOne(t, queue)
			entered, resume := make(chan struct{}), make(chan struct{})
			barrier := make(chan error, 1)
			go func() {
				_, err := queue.submitBatchObserved(t.Context(), nil,
					executionGate{entered: entered, release: resume})
				barrier <- err
			}()
			<-entered
			ackDone, releaseDone := make(chan error, 1), make(chan error, 1)
			ack := func() {
				_, err := queue.AckBatch(t.Context(), []Token{delivery.Token})
				ackDone <- err
			}
			release := func() { releaseDone <- queue.Release(delivery.Token) }
			if releaseFirst {
				go release()
			} else {
				go ack()
			}
			waitDeliveryState(t, queue, func(q *Queue) bool {
				for index := range q.progress.slots {
					if q.progress.slots[index].handed {
						return true
					}
				}
				return false
			})
			if releaseFirst {
				go ack()
			} else {
				go release()
			}
			waitDeliveryState(t, queue, func(q *Queue) bool {
				count := 0
				for index := range q.progress.slots {
					if q.progress.slots[index].handed {
						count++
					}
				}
				return count == 2
			})
			close(resume)
			require.NoError(t, <-barrier)
			if releaseFirst {
				assert.NoError(t, <-releaseDone)
				assert.ErrorIs(t, <-ackDone, ErrStaleToken)
				retry := receiveOne(t, queue)
				assert.NotEqual(t, delivery.Token, retry.Token)
			} else {
				assert.NoError(t, <-ackDone)
				assert.ErrorIs(t, <-releaseDone, ErrStaleToken)
			}
		})
	}
}
