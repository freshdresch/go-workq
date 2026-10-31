package runner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring"
	"github.com/freshdresch/go-data-structures/workq"
)

func validConfig() Config[int] {
	return Config[int]{
		Capacity: 8,
		Workers:  2,
		Handler:  func(context.Context, []int) []Result { return nil },
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		change func(Config[int]) Config[int]
	}{
		{"zero workers", func(config Config[int]) Config[int] {
			config.Workers = 0
			return config
		}},
		{"negative workers", func(config Config[int]) Config[int] {
			config.Workers = -1
			return config
		}},
		{"nil handler", func(config Config[int]) Config[int] {
			config.Handler = nil
			return config
		}},
		{"unknown mode", func(config Config[int]) Config[int] {
			config.FailureMode = FailureMode(255)
			return config
		}},
		{"negative error limit", func(config Config[int]) Config[int] {
			config.ErrorLimit = -1
			return config
		}},
		{"zero attempts", func(config Config[int]) Config[int] {
			config.Retry = &RetryPolicy{Delay: retryDelay}
			return config
		}},
		{"negative attempts", func(config Config[int]) Config[int] {
			config.Retry = &RetryPolicy{MaxAttempts: -1, Delay: retryDelay}
			return config
		}},
		{"nil delay", func(config Config[int]) Config[int] {
			config.Retry = &RetryPolicy{MaxAttempts: 1}
			return config
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolveConfig(test.change(validConfig()))
			require.Error(t, err)
		})
	}
}

func retryDelay(int, error) (time.Duration, bool) {
	return 0, true
}

func TestConfigOwnsPointerValues(t *testing.T) {
	producer := workq.WaitStrategy{Yields: 3}
	consumer := workq.WaitStrategy{Yields: 7}
	policy := RetryPolicy{MaxAttempts: 3, Delay: retryDelay}
	config := validConfig()
	config.FailureMode = Continue
	config.Queue.ProducerWait = &producer
	config.Queue.ConsumerWait = &consumer
	config.Retry = &policy
	resolved, err := resolveConfig(config)
	require.NoError(t, err)
	assert.Equal(t, 16, resolved.ErrorLimit)
	producer.Yields = 100
	consumer.Yields = 100
	policy.MaxAttempts = 100
	policy.Delay = nil
	assert.Equal(t, 3, resolved.Queue.ProducerWait.Yields)
	assert.Equal(t, 7, resolved.Queue.ConsumerWait.Yields)
	assert.Equal(t, 3, resolved.Retry.MaxAttempts)
	require.NotNil(t, resolved.Retry.Delay)
	_, retry := resolved.Retry.Delay(1, nil)
	assert.True(t, retry)
	config.ErrorLimit = 2
	resolved, err = resolveConfig(config)
	require.Error(t, err)
	config.Retry = nil
	resolved, err = resolveConfig(config)
	require.NoError(t, err)
	assert.Equal(t, 2, resolved.ErrorLimit)
	assert.Nil(t, resolved.Retry)
}

func TestQueueDefaultsAndOwnership(t *testing.T) {
	for _, workers := range []int{1, 3} {
		for _, capacity := range []int{8, 64} {
			config := validConfig()
			config.Workers = workers
			config.Capacity = capacity
			queue, err := workq.New[int](config.Capacity, queueOptions(config)...)
			require.NoError(t, err)
			snapshot := queue.Diagnostics().Config
			assert.Equal(t, min(32, capacity), snapshot.MaxPushBatch)
			assert.Equal(t, min(32, capacity), snapshot.MaxReceiveBatch)
			assert.Equal(t, 4, snapshot.ProducerWait.Yields)
			assert.Equal(t, 16, snapshot.ConsumerWait.Yields)
			if workers == 1 {
				assert.Equal(t, workq.EngineMPSC, snapshot.Engine)
			} else {
				assert.Equal(t, workq.EngineMPMC, snapshot.Engine)
			}
			queue.Close()
		}
	}
}

func TestQueueExplicitSettings(t *testing.T) {
	config := validConfig()
	config.Queue = QueueConfig{
		MaxPushBatch:    2,
		MaxReceiveBatch: 3,
		ProducerWait:    &workq.WaitStrategy{},
		ConsumerWait:    &workq.WaitStrategy{},
		Diagnostics:     workq.DiagnosticsConfig{Mode: workq.DiagnosticsCounters, Shards: 2},
	}
	resolved, err := resolveConfig(config)
	require.NoError(t, err)
	options := queueOptions(resolved)
	resolved.Queue.ProducerWait.Yields = 9
	queue, err := workq.New[int](config.Capacity, options...)
	require.NoError(t, err)
	defer queue.Close()
	snapshot := queue.Diagnostics().Config
	assert.Equal(t, 2, snapshot.MaxPushBatch)
	assert.Equal(t, 3, snapshot.MaxReceiveBatch)
	assert.Equal(t, workq.WaitStrategy{}, snapshot.ProducerWait)
	assert.Equal(t, workq.WaitStrategy{}, snapshot.ConsumerWait)
	assert.Equal(t, config.Queue.Diagnostics, snapshot.DiagnosticsConfig)
}

func TestDelegatedQueueValidation(t *testing.T) {
	cases := []struct {
		name     string
		capacity int
		queue    QueueConfig
		want     error
	}{
		{"capacity", 3, QueueConfig{}, ring.ErrInvalidCount},
		{"negative push", 8, QueueConfig{MaxPushBatch: -1}, ring.ErrInvalidMaxBatch},
		{"large push", 8, QueueConfig{MaxPushBatch: 9}, ring.ErrInvalidMaxBatch},
		{"negative receive", 8, QueueConfig{MaxReceiveBatch: -1}, ring.ErrInvalidMaxBatch},
		{"large receive", 8, QueueConfig{MaxReceiveBatch: 9}, ring.ErrInvalidMaxBatch},
		{
			"producer wait", 8,
			QueueConfig{ProducerWait: &workq.WaitStrategy{Spins: -1}},
			workq.ErrInvalidWaitStrategy,
		},
		{
			"consumer wait", 8,
			QueueConfig{ConsumerWait: &workq.WaitStrategy{Yields: -1}},
			workq.ErrInvalidWaitStrategy,
		},
		{
			"diagnostics", 8,
			QueueConfig{Diagnostics: workq.DiagnosticsConfig{Mode: workq.DiagnosticsMode(255)}},
			workq.ErrInvalidDiagnostics,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			config.Capacity = test.capacity
			config.Queue = test.queue
			resolved, err := resolveConfig(config)
			require.NoError(t, err)
			_, err = workq.New[int](resolved.Capacity, queueOptions(resolved)...)
			require.ErrorIs(t, err, test.want)
		})
	}
}
