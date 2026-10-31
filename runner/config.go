package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/freshdresch/go-data-structures/workq"
)

var (
	// ErrStopped reports rejection after admission closes, including an empty submission.
	ErrStopped = errors.New("runner: admission is stopped")
	// ErrAborted is the cause used when an explicit abort supplies no error.
	ErrAborted = errors.New("runner: aborted")
)

// FailureMode selects whether terminal handler failures cancel unrelated work.
type FailureMode uint8

const (
	// Abort cancels the run on a terminal handler failure. It is the default.
	Abort FailureMode = iota
	// Continue processes unrelated work, while the final outcome still reports failures.
	Continue
)

// Config specifies one runner. Shared handlers and callbacks must support concurrent calls.
type Config[T any] struct {
	// Capacity must be a power of two in [1, 2^30].
	Capacity int
	// Workers must be positive and remains fixed for the run.
	Workers int
	// Handler borrows its input until return. Nil results mean all positions succeeded.
	// Otherwise it must return one Result per position without retaining or changing input slots.
	// Result storage must remain stable until consumed. Cancellation is cooperative.
	Handler func(context.Context, []T) []Result
	// OnTerminal optionally reconciles and releases each accepted occurrence exactly once.
	// Calls can overlap. It must not join or block submitting to its own runner.
	OnTerminal func(T, ItemOutcome) error
	// FailureMode defaults to Abort. Unknown values are invalid.
	FailureMode FailureMode
	// Retry enables bounded retries when non-nil. Its values are copied at construction.
	Retry *RetryPolicy
	// ErrorLimit bounds retained operational failures by count. Zero means 16.
	ErrorLimit int
	// Queue selects batch limits, waits and diagnostics without exposing queue ownership.
	Queue QueueConfig
}

// QueueConfig contains the supported queue settings. Zero batch limits use workq defaults.
type QueueConfig struct {
	// MaxPushBatch bounds each publication chunk. A negative value is invalid.
	MaxPushBatch int
	// MaxReceiveBatch bounds each acquired batch. A negative value is invalid.
	MaxReceiveBatch int
	// ProducerWait is copied when supplied. Nil uses the workq default, unlike an explicit zero.
	ProducerWait *workq.WaitStrategy
	// ConsumerWait is copied when supplied. Nil uses the workq default, unlike an explicit zero.
	ConsumerWait *workq.WaitStrategy
	// Diagnostics uses workq's validation and defaults, including disabled diagnostics at zero.
	Diagnostics workq.DiagnosticsConfig
}

// RetryPolicy bounds handler attempts without publishing retry work back to the queue.
type RetryPolicy struct {
	// MaxAttempts must be positive and includes the original handler invocation.
	MaxAttempts int
	// Delay is called once per eligible ordinary failure with its attempt number and error.
	// It must return promptly, support concurrency and return a nonnegative duration even
	// when declining retry. It must not wait for terminal callbacks or runner completion.
	Delay func(attempt int, err error) (time.Duration, bool)
}

func resolveConfig[T any](config Config[T]) (Config[T], error) {
	if config.Workers <= 0 {
		return Config[T]{}, fmt.Errorf("runner: workers must be positive: %d", config.Workers)
	}
	if config.Handler == nil {
		return Config[T]{}, errors.New("runner: handler is nil")
	}
	if config.FailureMode != Abort && config.FailureMode != Continue {
		return Config[T]{}, fmt.Errorf("runner: invalid failure mode: %d", config.FailureMode)
	}
	if config.ErrorLimit < 0 {
		return Config[T]{}, fmt.Errorf("runner: negative error limit: %d", config.ErrorLimit)
	}
	if config.ErrorLimit == 0 {
		config.ErrorLimit = 16
	}
	if config.Retry != nil {
		policy := *config.Retry
		if policy.MaxAttempts <= 0 {
			return Config[T]{}, errors.New("runner: retry max attempts must be positive")
		}
		if policy.Delay == nil {
			return Config[T]{}, errors.New("runner: retry delay is nil")
		}
		config.Retry = &policy
	}
	if config.Queue.ProducerWait != nil {
		strategy := *config.Queue.ProducerWait
		config.Queue.ProducerWait = &strategy
	}
	if config.Queue.ConsumerWait != nil {
		strategy := *config.Queue.ConsumerWait
		config.Queue.ConsumerWait = &strategy
	}
	return config, nil
}

func queueOptions[T any](config Config[T]) []workq.Option {
	options := []workq.Option{workq.WithDiagnostics(config.Queue.Diagnostics)}
	if config.Workers == 1 {
		options = append(options, workq.WithSingleConsumer())
	}
	if config.Queue.MaxPushBatch != 0 {
		options = append(options, workq.WithMaxPushBatch(config.Queue.MaxPushBatch))
	}
	if config.Queue.MaxReceiveBatch != 0 {
		options = append(options, workq.WithMaxReceiveBatch(config.Queue.MaxReceiveBatch))
	}
	if config.Queue.ProducerWait != nil {
		options = append(options, workq.WithProducerWait(*config.Queue.ProducerWait))
	}
	if config.Queue.ConsumerWait != nil {
		options = append(options, workq.WithConsumerWait(*config.Queue.ConsumerWait))
	}
	return options
}
