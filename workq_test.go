package workq

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring"
	"github.com/freshdresch/go-data-structures/ring/dpdk"
	"github.com/freshdresch/go-data-structures/ring/frontier"
)

// engineCase is one row of the design's engine selection table.
type engineCase struct {
	name       string
	options    []Option
	wantEngine engine[int]
}

func engineCases() []engineCase {
	return []engineCase{
		{
			name:       "SPSC",
			options:    []Option{WithSingleProducer(), WithSingleConsumer()},
			wantEngine: (*dpdk.SPSCQueue[int])(nil),
		},
		{
			name:       "MPSC",
			options:    []Option{WithSingleConsumer()},
			wantEngine: (*frontier.MPSCQueue[int])(nil),
		},
		{
			name:       "SPMC",
			options:    []Option{WithSingleProducer()},
			wantEngine: (*frontier.SPMCQueue[int])(nil),
		},
		{
			name:       "MPMC",
			options:    nil,
			wantEngine: (*frontier.MPMCQueue[int])(nil),
		},
	}
}

// setGOMAXPROCS sets GOMAXPROCS for the rest of the test. A test that calls it must not call
// t.Parallel, because GOMAXPROCS is process-wide.
func setGOMAXPROCS(t *testing.T, procs int) {
	t.Helper()
	previous := runtime.GOMAXPROCS(procs)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
}

// engineLimits returns the batch limits of the concrete queue behind eng.
func engineLimits(t *testing.T, eng engine[int]) (maxEnqueue int, maxDequeue int) {
	t.Helper()
	switch queue := eng.(type) {
	case *dpdk.SPSCQueue[int]:
		return queue.MaxEnqueueBatch(), queue.MaxDequeueBatch()
	case *frontier.MPSCQueue[int]:
		return queue.MaxEnqueueBatch(), queue.MaxDequeueBatch()
	case *frontier.SPMCQueue[int]:
		return queue.MaxEnqueueBatch(), queue.MaxDequeueBatch()
	case *frontier.MPMCQueue[int]:
		return queue.MaxEnqueueBatch(), queue.MaxDequeueBatch()
	default:
		require.FailNow(t, "unexpected engine type", "%T", eng)
		return 0, 0
	}
}

func requireIdleQueue(t *testing.T, queue *Queue[int]) {
	t.Helper()
	requireIdleSide(t, &queue.consumers)
	requireIdleSide(t, &queue.producers)
}

func TestNewSelectsEngine(t *testing.T) {
	limitCases := []struct {
		name                string
		options             []Option
		wantMaxPushBatch    int
		wantMaxReceiveBatch int
	}{
		{name: "default limits", wantMaxPushBatch: 32, wantMaxReceiveBatch: 32},
		{
			name:                "set limits",
			options:             []Option{WithMaxPushBatch(8), WithMaxReceiveBatch(4)},
			wantMaxPushBatch:    8,
			wantMaxReceiveBatch: 4,
		},
	}
	for _, engineCase := range engineCases() {
		for _, limitCase := range limitCases {
			t.Run(engineCase.name+"/"+limitCase.name, func(t *testing.T) {
				options := append(append([]Option{}, engineCase.options...), limitCase.options...)
				queue, err := New[int](64, options...)
				require.NoError(t, err)

				assert.IsType(t, engineCase.wantEngine, queue.engine)
				assert.Equal(t, 64, queue.Capacity())
				assert.Equal(t, limitCase.wantMaxPushBatch, queue.maxPushBatch)
				assert.Equal(t, limitCase.wantMaxReceiveBatch, queue.maxReceiveBatch)
				maxEnqueue, maxDequeue := engineLimits(t, queue.engine)
				assert.Equal(t, queue.maxPushBatch, maxEnqueue, "engine enqueue limit")
				assert.Equal(t, queue.maxReceiveBatch, maxDequeue, "engine dequeue limit")
				requireIdleQueue(t, queue)
			})
		}
	}
}

func TestNewDefaults(t *testing.T) {
	setGOMAXPROCS(t, 4)
	for _, capacity := range []int{1, 16, 32, 1024} {
		queue, err := New[int](capacity)
		require.NoError(t, err, "capacity %d", capacity)

		wantBatch := min(32, capacity)
		assert.Equal(t, wantBatch, queue.maxPushBatch, "push limit, capacity %d", capacity)
		assert.Equal(t, wantBatch, queue.maxReceiveBatch, "receive limit, capacity %d", capacity)
		assert.Equal(t, defaultConsumerWait(), queue.consumerWait, "capacity %d", capacity)
		assert.Equal(t, defaultProducerWait(), queue.producerWait, "capacity %d", capacity)
		requireIdleQueue(t, queue)
	}
}

func TestNewSingleProcDisablesSpins(t *testing.T) {
	setGOMAXPROCS(t, 1)
	queue, err := New[int](
		64,
		WithConsumerWait(WaitStrategy{Spins: 5, Yields: 6, NeverPark: true}),
		WithProducerWait(WaitStrategy{Spins: 7, Yields: 8}),
	)
	require.NoError(t, err)

	assert.Equal(t, WaitStrategy{Spins: 0, Yields: 6, NeverPark: true}, queue.consumerWait)
	assert.Equal(t, WaitStrategy{Spins: 0, Yields: 8}, queue.producerWait)
	requireIdleQueue(t, queue)
}

func TestNewErrors(t *testing.T) {
	cases := []struct {
		name     string
		capacity int
		options  []Option
		want     error
		wantText string
	}{
		{name: "zero capacity", capacity: 0, want: ring.ErrInvalidCount},
		{name: "capacity not a power of two", capacity: 48, want: ring.ErrInvalidCount},
		{name: "capacity above 2^30", capacity: 1<<30 + 1, want: ring.ErrInvalidCount},
		{
			name:     "push limit zero",
			capacity: 64,
			options:  []Option{WithMaxPushBatch(0)},
			want:     ring.ErrInvalidMaxBatch,
		},
		{
			name:     "push limit above capacity",
			capacity: 64,
			options:  []Option{WithMaxPushBatch(65)},
			want:     ring.ErrInvalidMaxBatch,
		},
		{
			name:     "receive limit zero",
			capacity: 64,
			options:  []Option{WithMaxReceiveBatch(0)},
			want:     ring.ErrInvalidMaxBatch,
		},
		{
			name:     "receive limit above capacity",
			capacity: 64,
			options:  []Option{WithMaxReceiveBatch(65)},
			want:     ring.ErrInvalidMaxBatch,
		},
		{
			name:     "negative consumer spins",
			capacity: 64,
			options:  []Option{WithConsumerWait(WaitStrategy{Spins: -1})},
			want:     ErrInvalidWaitStrategy,
			wantText: "consumer wait strategy has -1 spins",
		},
		{
			name:     "negative consumer yields",
			capacity: 64,
			options:  []Option{WithConsumerWait(WaitStrategy{Yields: -2})},
			want:     ErrInvalidWaitStrategy,
			wantText: "consumer wait strategy has -2 yields",
		},
		{
			name:     "negative producer spins",
			capacity: 64,
			options:  []Option{WithProducerWait(WaitStrategy{Spins: -3})},
			want:     ErrInvalidWaitStrategy,
			wantText: "producer wait strategy has -3 spins",
		},
		{
			name:     "negative producer yields",
			capacity: 64,
			options:  []Option{WithProducerWait(WaitStrategy{Yields: -4})},
			want:     ErrInvalidWaitStrategy,
			wantText: "producer wait strategy has -4 yields",
		},
		{
			name:     "consumer checked before producer, spins before yields",
			capacity: 64,
			options: []Option{
				WithConsumerWait(WaitStrategy{Spins: -5, Yields: -6}),
				WithProducerWait(WaitStrategy{Spins: -7}),
			},
			want:     ErrInvalidWaitStrategy,
			wantText: "consumer wait strategy has -5 spins",
		},
		{
			name:     "wait strategy checked before capacity",
			capacity: 48,
			options:  []Option{WithProducerWait(WaitStrategy{Spins: -1})},
			want:     ErrInvalidWaitStrategy,
		},
	}
	for _, engineCase := range engineCases() {
		for _, errorCase := range cases {
			t.Run(engineCase.name+"/"+errorCase.name, func(t *testing.T) {
				options := append(append([]Option{}, engineCase.options...), errorCase.options...)
				queue, err := New[int](errorCase.capacity, options...)

				assert.Nil(t, queue)
				require.ErrorIs(t, err, errorCase.want)
				assert.True(
					t,
					strings.HasPrefix(err.Error(), "new work queue: "),
					"error %q lacks the constructor prefix",
					err,
				)
				if errorCase.wantText != "" {
					assert.ErrorContains(t, err, errorCase.wantText)
				}
			})
		}
	}
}

func TestNewValidatesFinalValues(t *testing.T) {
	cases := []struct {
		name    string
		options []Option
	}{
		{
			name:    "push limit",
			options: []Option{WithMaxPushBatch(0), WithMaxPushBatch(8)},
		},
		{
			name: "consumer wait strategy",
			options: []Option{
				WithConsumerWait(WaitStrategy{Spins: -1}),
				WithConsumerWait(WaitStrategy{}),
			},
		},
	}
	for _, finalCase := range cases {
		t.Run(finalCase.name, func(t *testing.T) {
			queue, err := New[int](64, finalCase.options...)
			require.NoError(t, err)
			requireIdleQueue(t, queue)
		})
	}
}

func TestErrorIdentities(t *testing.T) {
	assert.True(t, errors.Is(ErrClosedDuringPush, ErrClosed))
	assert.Same(t, ring.ErrFull, ErrFull)
}
