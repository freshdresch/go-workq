package workq_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq"
)

func newLifecycleQueue(
	t *testing.T,
	capacity, receiveLimit int,
	diagnostics workq.DiagnosticsConfig,
) *workq.Queue[int] {
	t.Helper()
	queue, err := workq.New[int](
		capacity,
		workq.WithMaxPushBatch(1),
		workq.WithDiagnostics(diagnostics),
		workq.WithMaxReceiveBatch(receiveLimit),
		workq.WithConsumerWait(workq.WaitStrategy{}),
		workq.WithProducerWait(workq.WaitStrategy{}),
	)
	require.NoError(t, err)
	return queue
}

func requirePending(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("operation returned before its dependency")
	default:
	}
}

func TestLifecycleGracefulDrain(t *testing.T) {
	runLifecycleModes(t, runLifecycleGracefulDrain)
}

func runLifecycleGracefulDrain(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	for _, ending := range []string{"source stop", "finite exhaustion"} {
		t.Run(ending, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newLifecycleQueue(t, 2, 1, diagnostics)
				sourceCtx, stopSources := context.WithCancel(context.Background())
				abortCtx, cancelAbort := context.WithCancel(context.Background())
				outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
				entered, release := make(chan struct{}), make(chan struct{})
				admitted, produced, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
				releaseHandler := sync.OnceFunc(func() { close(release) })
				var producers, consumers, owners sync.WaitGroup
				defer func() {
					releaseHandler()
					stopSources()
					cancelAbort()
					producers.Wait()
					queue.Close()
					consumers.Wait()
					owners.Wait()
				}()
				var accepted, unaccepted, completed []int
				consumers.Go(func() {
					// consumeAndAbort owns failure recording, including secondary cancellation filtering.
					_ = consumeAndAbort(abortCtx, queue, "consumer", func(_ context.Context, batch []int) error {
						if batch[0] == 1 {
							close(entered)
							<-release
						}
						completed = append(completed, batch...)
						return nil
					}, outcome)
				})
				producers.Go(func() {
					defer close(produced)
					first := []int{1}
					count, err := queue.PushBatch(abortCtx, first)
					accepted = append(accepted, first[:count]...)
					unaccepted = append(unaccepted, first[count:]...)
					if failure := queueFailure(abortCtx, err); failure != nil {
						outcome.failAndAbort("producer", failure)
						return
					}
					<-entered
					items := []int{2, 3, 4}
					close(admitted)
					count, err = queue.PushBatch(abortCtx, items)
					accepted = append(accepted, items[:count]...)
					unaccepted = append(unaccepted, items[count:]...)
					if failure := queueFailure(abortCtx, err); failure != nil {
						outcome.failAndAbort("producer", failure)
						return
					}
					if ending == "source stop" {
						<-sourceCtx.Done()
					}
				})
				<-admitted
				synctest.Wait()
				requirePending(t, produced)
				graceCtx, cancelGrace := context.WithTimeout(context.Background(), time.Second)
				defer cancelGrace()
				if ending == "source stop" {
					stopSources()
				}
				owners.Go(func() {
					drainQueue(graceCtx, queue, &producers, &consumers, outcome, nil)
					close(drained)
				})
				synctest.Wait()
				requirePending(t, produced)
				requirePending(t, drained)
				require.NoError(t, abortCtx.Err())
				releaseHandler()
				owners.Wait()
				require.NoError(t, outcome.err())
				require.Equal(t, []int{1, 2, 3, 4}, accepted)
				require.Empty(t, unaccepted)
				require.Equal(t, accepted, completed)
			})
		})
	}
}

func TestLifecycleGraceDeadline(t *testing.T) {
	runLifecycleModes(t, runLifecycleGraceDeadline)
}

func runLifecycleGraceDeadline(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	for _, blocked := range []string{"producer", "consumer"} {
		t.Run(blocked, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newLifecycleQueue(t, 2, 1, diagnostics)
				sourceCtx, stopSources := context.WithCancel(context.Background())
				abortCtx, cancelAbort := context.WithCancel(context.Background())
				outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
				entered, produced, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var producers, consumers, owners sync.WaitGroup
				defer func() {
					stopSources()
					cancelAbort()
					producers.Wait()
					queue.Close()
					consumers.Wait()
					owners.Wait()
				}()
				var accepted, unaccepted []int
				consumers.Go(func() {
					// The outcome keeps independent failures and the grace reason.
					_ = consumeAndAbort(abortCtx, queue, "consumer", func(ctx context.Context, _ []int) error {
						close(entered)
						<-ctx.Done()
						return peerAbort{reason: ctx.Err()}
					}, outcome)
				})
				producers.Go(func() {
					defer close(produced)
					first := []int{1}
					count, err := queue.PushBatch(abortCtx, first)
					accepted = append(accepted, first[:count]...)
					unaccepted = append(unaccepted, first[count:]...)
					if failure := queueFailure(abortCtx, err); failure != nil {
						outcome.failAndAbort("producer", failure)
						return
					}
					<-entered
					if blocked == "consumer" {
						return
					}
					items := []int{2, 3, 4}
					count, err = queue.PushBatch(abortCtx, items)
					accepted = append(accepted, items[:count]...)
					unaccepted = append(unaccepted, items[count:]...)
					if failure := queueFailure(abortCtx, err); failure != nil {
						outcome.failAndAbort("producer", failure)
					}
				})
				<-entered
				synctest.Wait()
				if blocked == "producer" {
					requirePending(t, produced)
				} else {
					<-produced
				}
				graceCtx, cancelGrace := context.WithTimeout(context.Background(), time.Second)
				defer cancelGrace()
				stopSources()
				owners.Go(func() {
					drainQueue(graceCtx, queue, &producers, &consumers, outcome, nil)
					close(drained)
				})
				synctest.Wait()
				requirePending(t, drained)
				if blocked == "consumer" {
					require.ErrorIs(t, queue.Push(abortCtx, 99), workq.ErrClosed)
				}
				<-graceCtx.Done()
				owners.Wait()
				require.ErrorIs(t, outcome.err(), context.DeadlineExceeded)
				require.ErrorIs(t, abortCtx.Err(), context.Canceled)
				require.ErrorIs(t, sourceCtx.Err(), context.Canceled)
				require.Empty(t, outcome.failures)
				if blocked == "producer" {
					require.Equal(t, []int{1, 2, 3}, accepted)
					require.Equal(t, []int{4}, unaccepted)
				} else {
					require.Equal(t, []int{1}, accepted)
					require.Empty(t, unaccepted)
				}
			})
		})
	}
}

func TestLifecycleCompletedDrainStopsSupervision(t *testing.T) {
	runLifecycleModes(t, runLifecycleCompletedDrainStopsSupervision)
}

func runLifecycleCompletedDrainStopsSupervision(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	synctest.Test(t, func(t *testing.T) {
		queue := newLifecycleQueue(t, 2, 1, diagnostics)
		_, stopSources := context.WithCancel(context.Background())
		abortCtx, cancelAbort := context.WithCancel(context.Background())
		outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
		watching := make(chan context.Context, 1)
		release, drained := make(chan struct{}), make(chan struct{})
		releaseWatcher := sync.OnceFunc(func() { close(release) })
		var producers, consumers, owners sync.WaitGroup
		defer func() {
			releaseWatcher()
			stopSources()
			cancelAbort()
			producers.Wait()
			queue.Close()
			consumers.Wait()
			owners.Wait()
		}()
		var accepted, unaccepted, completed []int
		producers.Go(func() {
			items := []int{1, 2}
			count, err := queue.PushBatch(abortCtx, items)
			accepted, unaccepted = slices.Clone(items[:count]), slices.Clone(items[count:])
			if failure := queueFailure(abortCtx, err); failure != nil {
				outcome.failAndAbort("producer", failure)
			}
		})
		consumers.Go(func() {
			// The outcome owns failure handling.
			_ = consumeAndAbort(abortCtx, queue, "consumer", func(_ context.Context, batch []int) error {
				completed = append(completed, batch...)
				return nil
			}, outcome)
		})
		graceCtx, cancelGrace := context.WithTimeout(context.Background(), time.Second)
		defer cancelGrace()
		stopSources()
		owners.Go(func() {
			drainQueue(graceCtx, queue, &producers, &consumers, outcome, func(ctx context.Context) {
				watching <- ctx
				<-release
			})
			close(drained)
		})
		supervisionCtx := <-watching
		<-supervisionCtx.Done()
		synctest.Wait()
		require.ErrorIs(t, supervisionCtx.Err(), context.Canceled)
		require.NoError(t, graceCtx.Err())
		require.Equal(t, []int{1, 2}, accepted)
		require.Empty(t, unaccepted)
		require.Equal(t, accepted, completed)
		requirePending(t, drained)
		<-graceCtx.Done()
		require.ErrorIs(t, graceCtx.Err(), context.DeadlineExceeded)
		releaseWatcher()
		owners.Wait()
		require.NoError(t, outcome.err())
		require.NoError(t, outcome.abortReason)
		require.NoError(t, abortCtx.Err())
	})
}

func TestLifecycleWaitsForHandlerAfterAbort(t *testing.T) {
	runLifecycleModes(t, runLifecycleWaitsForHandlerAfterAbort)
}

func runLifecycleWaitsForHandlerAfterAbort(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	synctest.Test(t, func(t *testing.T) {
		queue := newLifecycleQueue(t, 2, 1, diagnostics)
		_, stopSources := context.WithCancel(context.Background())
		abortCtx, cancelAbort := context.WithCancel(context.Background())
		outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
		entered, release, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseHandler := sync.OnceFunc(func() { close(release) })
		var producers, consumers, owners sync.WaitGroup
		defer func() {
			releaseHandler()
			stopSources()
			cancelAbort()
			producers.Wait()
			queue.Close()
			consumers.Wait()
			owners.Wait()
		}()
		require.NoError(t, queue.Push(abortCtx, 1))
		var consumeErr error
		consumers.Go(func() {
			consumeErr = consumeAndAbort(abortCtx, queue, "consumer", func(context.Context, []int) error {
				close(entered)
				<-release
				return nil
			}, outcome)
		})
		<-entered
		graceCtx, cancelGrace := context.WithTimeout(context.Background(), time.Second)
		defer cancelGrace()
		stopSources()
		owners.Go(func() {
			drainQueue(graceCtx, queue, &producers, &consumers, outcome, nil)
			close(drained)
		})
		<-abortCtx.Done()
		synctest.Wait()
		requirePending(t, drained)
		releaseHandler()
		owners.Wait()
		require.ErrorIs(t, outcome.err(), context.DeadlineExceeded)
		require.Empty(t, outcome.failures)
		require.True(t, consumeErr == nil || errors.Is(consumeErr, context.Canceled))
	})
}

func TestLifecycleHandlerFailure(t *testing.T) {
	runLifecycleModes(t, runLifecycleHandlerFailure)
}

func runLifecycleHandlerFailure(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	synctest.Test(t, func(t *testing.T) {
		queue := newLifecycleQueue(t, 4, 3, diagnostics)
		sourceCtx, stopSources := context.WithCancel(context.Background())
		abortCtx, cancelAbort := context.WithCancel(context.Background())
		outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
		entered, release := make(chan struct{}), make(chan struct{})
		peerDone, sourceDone := make(chan struct{}), make(chan struct{})
		releaseHandler := sync.OnceFunc(func() { close(release) })
		var producers, consumers sync.WaitGroup
		defer func() {
			releaseHandler()
			stopSources()
			cancelAbort()
			producers.Wait()
			queue.Close()
			consumers.Wait()
		}()
		items := []int{1, 2, 3}
		count, err := queue.PushBatch(abortCtx, items)
		accepted, unaccepted := slices.Clone(items[:count]), slices.Clone(items[count:])
		require.NoError(t, err)
		require.Equal(t, items, accepted)
		require.Empty(t, unaccepted)
		sentinel := errors.New("item 2 failed")
		var progress batchProgress
		consumers.Go(func() {
			// The outcome owns failure handling.
			_ = consumeAndAbort(abortCtx, queue, "handler", func(ctx context.Context, batch []int) error {
				close(entered)
				<-release
				var err error
				progress, err = processBatch(ctx, batch, func(_ context.Context, item int) error {
					if item == 2 {
						return sentinel
					}
					return nil
				})
				return err
			}, outcome)
		})
		<-entered
		var peerItems []int
		consumers.Go(func() {
			defer close(peerDone)
			// The outcome owns failure handling.
			_ = consumeAndAbort(abortCtx, queue, "peer", func(_ context.Context, batch []int) error {
				peerItems = append(peerItems, batch...)
				return nil
			}, outcome)
		})
		producers.Go(func() {
			defer close(sourceDone)
			<-sourceCtx.Done()
		})
		synctest.Wait()
		requirePending(t, peerDone)
		requirePending(t, sourceDone)
		releaseHandler()
		drainQueue(context.Background(), queue, &producers, &consumers, outcome, nil)
		require.ErrorIs(t, outcome.err(), sentinel)
		require.Len(t, outcome.failures, 1)
		require.Equal(t, []int{1}, progress.completed)
		require.Equal(t, []int{2}, progress.failed)
		require.Equal(t, []int{3}, progress.unattempted)
		require.Empty(t, peerItems)
		count, err = queue.Receive(context.Background(), make([]int, 3))
		require.Zero(t, count)
		require.ErrorIs(t, err, workq.ErrClosed)
	})
}

func TestLifecycleIndependentFailures(t *testing.T) {
	runLifecycleModes(t, runLifecycleIndependentFailures)
}

func runLifecycleIndependentFailures(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	t.Run("independent request deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			queue := newLifecycleQueue(t, 2, 1, diagnostics)
			_, stopSources := context.WithCancel(context.Background())
			abortCtx, cancelAbort := context.WithCancel(context.Background())
			outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
			firstEntered, secondEntered := make(chan struct{}), make(chan struct{})
			firstRelease, secondRelease := make(chan struct{}), make(chan struct{})
			releaseFirst := sync.OnceFunc(func() { close(firstRelease) })
			releaseSecond := sync.OnceFunc(func() { close(secondRelease) })
			var producers, consumers sync.WaitGroup
			defer func() {
				releaseFirst()
				releaseSecond()
				stopSources()
				cancelAbort()
				producers.Wait()
				queue.Close()
				consumers.Wait()
			}()
			sentinel := errors.New("first operation failed")
			requestCtx, cancelRequest := context.WithTimeout(context.Background(), time.Second)
			defer cancelRequest()
			require.NoError(t, queue.Push(abortCtx, 1))
			consumers.Go(func() {
				// The outcome records this failure before it cancels the peer.
				_ = consumeAndAbort(abortCtx, queue, "first", func(context.Context, []int) error {
					close(firstEntered)
					<-firstRelease
					return sentinel
				}, outcome)
			})
			<-firstEntered
			require.NoError(t, queue.Push(abortCtx, 2))
			consumers.Go(func() {
				// This request has an independent deadline, even after shared abort.
				_ = consumeAndAbort(abortCtx, queue, "second", func(context.Context, []int) error {
					close(secondEntered)
					<-secondRelease
					<-requestCtx.Done()
					return requestCtx.Err()
				}, outcome)
			})
			<-secondEntered
			releaseFirst()
			<-abortCtx.Done()
			releaseSecond()
			drainQueue(context.Background(), queue, &producers, &consumers, outcome, nil)
			require.ErrorIs(t, outcome.err(), sentinel)
			require.ErrorIs(t, outcome.err(), context.DeadlineExceeded)
			require.NotErrorIs(t, outcome.err(), context.Canceled)
			require.Len(t, outcome.failures, 2)
			require.ErrorIs(t, outcome.failures[0], sentinel)
			require.ErrorIs(t, outcome.failures[1], context.DeadlineExceeded)
		})
	})
	for _, ending := range []string{"deadline first", "unmarked context error"} {
		t.Run(ending, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newLifecycleQueue(t, 2, 1, diagnostics)
				_, stopSources := context.WithCancel(context.Background())
				abortCtx, cancelAbort := context.WithCancel(context.Background())
				outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
				entered, release := make(chan struct{}), make(chan struct{})
				releaseHandler := sync.OnceFunc(func() { close(release) })
				var producers, consumers, owners sync.WaitGroup
				defer func() {
					releaseHandler()
					stopSources()
					cancelAbort()
					producers.Wait()
					queue.Close()
					consumers.Wait()
					owners.Wait()
				}()
				failure := errors.New("independent operation failed after grace")
				if ending == "unmarked context error" {
					failure = context.Canceled
				}
				require.NoError(t, queue.Push(abortCtx, 1))
				consumers.Go(func() {
					// An unmarked handler error remains independent regardless of its sentinel.
					_ = consumeAndAbort(abortCtx, queue, "handler", func(context.Context, []int) error {
						close(entered)
						<-release
						return failure
					}, outcome)
				})
				<-entered
				graceCtx, cancelGrace := context.WithTimeout(context.Background(), time.Second)
				defer cancelGrace()
				stopSources()
				owners.Go(func() {
					drainQueue(graceCtx, queue, &producers, &consumers, outcome, nil)
				})
				if ending == "deadline first" {
					<-abortCtx.Done()
				} else {
					outcome.requestAbort(errors.New("explicit abort before unmarked error"))
				}
				releaseHandler()
				owners.Wait()
				require.ErrorIs(t, outcome.err(), failure)
				require.NotErrorIs(t, outcome.err(), context.DeadlineExceeded)
				require.Len(t, outcome.failures, 1)
				if ending == "deadline first" {
					require.ErrorIs(t, outcome.abortReason, context.DeadlineExceeded)
				}
			})
		})
	}
}

func TestLifecycleSourceFailureAndExplicitAbort(t *testing.T) {
	runLifecycleModes(t, runLifecycleSourceFailureAndExplicitAbort)
}

func runLifecycleSourceFailureAndExplicitAbort(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	for _, ending := range []string{"source failure", "explicit abort"} {
		t.Run(ending, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newLifecycleQueue(t, 2, 1, diagnostics)
				sourceCtx, stopSources := context.WithCancel(context.Background())
				abortCtx, cancelAbort := context.WithCancel(context.Background())
				outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
				sourceResult := make(chan error, 1)
				produced, consumed := make(chan struct{}), make(chan struct{})
				var producers, consumers sync.WaitGroup
				defer func() {
					stopSources()
					cancelAbort()
					producers.Wait()
					queue.Close()
					consumers.Wait()
				}()
				cause := errors.New(ending)
				producers.Go(func() {
					defer close(produced)
					select {
					case err := <-sourceResult:
						if err != nil {
							outcome.failAndAbort("source", err)
						}
					case <-sourceCtx.Done():
						// Only this known source-read boundary treats requested source stop as expected.
					}
				})
				consumers.Go(func() {
					defer close(consumed)
					// The outcome owns failure handling.
					_ = consumeAndAbort(abortCtx, queue, "peer", func(context.Context, []int) error {
						return nil
					}, outcome)
				})
				synctest.Wait()
				requirePending(t, produced)
				requirePending(t, consumed)
				if ending == "source failure" {
					sourceResult <- cause
				} else {
					outcome.requestAbort(cause)
					outcome.requestAbort(errors.New("later abort must not replace first cause"))
				}
				drainQueue(context.Background(), queue, &producers, &consumers, outcome, nil)
				require.ErrorIs(t, outcome.err(), cause)
				require.NotErrorIs(t, outcome.err(), context.Canceled)
				if ending == "source failure" {
					require.Len(t, outcome.failures, 1)
				} else {
					require.Empty(t, outcome.failures)
				}
			})
		})
	}
}

func TestLifecyclePartialPushBatch(t *testing.T) {
	runLifecycleModes(t, runLifecyclePartialPushBatch)
}

func runLifecyclePartialPushBatch(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	for _, ending := range []string{"retry suffix", "abort"} {
		t.Run(ending, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newLifecycleQueue(t, 2, 1, diagnostics)
				_, stopSources := context.WithCancel(context.Background())
				abortCtx, cancelAbort := context.WithCancel(context.Background())
				outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
				var producers, consumers sync.WaitGroup
				defer func() {
					stopSources()
					cancelAbort()
					producers.Wait()
					queue.Close()
					consumers.Wait()
				}()
				canceledCtx, cancel := context.WithCancel(context.Background())
				cancel()
				items := []int{1, 2, 3}
				count, err := queue.PushBatch(canceledCtx, items)
				accepted, unaccepted := slices.Clone(items[:count]), slices.Clone(items[count:])
				require.Equal(t, 1, count)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, []int{1}, accepted)
				require.Equal(t, []int{2, 3}, unaccepted)
				if ending == "abort" {
					cause := errors.New("abort without retry")
					outcome.requestAbort(cause)
					drainQueue(context.Background(), queue, &producers, &consumers, outcome, nil)
					received := make([]int, 1)
					count, err = queue.Receive(context.Background(), received)
					require.NoError(t, err)
					require.Equal(t, 1, count)
					require.Equal(t, accepted, received[:count])
					count, err = queue.Receive(context.Background(), received)
					require.Zero(t, count)
					require.ErrorIs(t, err, workq.ErrClosed)
					require.Equal(t, []int{2, 3}, unaccepted)
					require.ErrorIs(t, outcome.err(), cause)
					return
				}
				var completed []int
				consumers.Go(func() {
					// The outcome owns failure handling.
					_ = consumeAndAbort(abortCtx, queue, "consumer", func(_ context.Context, batch []int) error {
						completed = append(completed, batch...)
						return nil
					}, outcome)
				})
				producers.Go(func() {
					count, err := queue.PushBatch(abortCtx, unaccepted)
					accepted = append(accepted, unaccepted[:count]...)
					unaccepted = slices.Clone(unaccepted[count:])
					if failure := queueFailure(abortCtx, err); failure != nil {
						outcome.failAndAbort("retry", failure)
					}
				})
				drainQueue(context.Background(), queue, &producers, &consumers, outcome, nil)
				require.NoError(t, outcome.err())
				require.Equal(t, items, accepted)
				require.Empty(t, unaccepted)
				require.Equal(t, items, completed)
			})
		})
	}
}

func TestLifecycleAdmissionGate(t *testing.T) {
	runLifecycleModes(t, runLifecycleAdmissionGate)
}

func runLifecycleAdmissionGate(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	for _, ending := range []string{"normal", "canceled partial"} {
		t.Run(ending, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := newLifecycleQueue(t, 4, 1, diagnostics)
				submitCtx, cancelSubmit := context.WithCancel(context.Background())
				gate := &admissionGate{}
				release, joined := make(chan struct{}), make(chan struct{})
				releaseCaller := sync.OnceFunc(func() { close(release) })
				var callers, owners sync.WaitGroup
				defer func() {
					releaseCaller()
					cancelSubmit()
					gate.stop()
					callers.Wait()
					gate.wait()
					queue.Close()
					owners.Wait()
				}()
				require.True(t, gate.admit())
				var accepted, unaccepted []int
				var pushErr error
				callers.Go(func() {
					defer gate.done()
					<-release
					items := []int{1, 2, 3}
					count, err := queue.PushBatch(submitCtx, items)
					accepted, unaccepted = slices.Clone(items[:count]), slices.Clone(items[count:])
					pushErr = err
				})
				gate.stop()
				owners.Go(func() {
					gate.wait()
					queue.Close()
					close(joined)
				})
				rejected := []int{}
				if gate.admit() {
					err := queue.Push(submitCtx, 99)
					gate.done()
					require.NoError(t, err)
				} else {
					rejected = append(rejected, 99)
				}
				require.Equal(t, []int{99}, rejected)
				synctest.Wait()
				requirePending(t, joined)
				if ending == "canceled partial" {
					cancelSubmit()
				}
				releaseCaller()
				callers.Wait()
				owners.Wait()
				if ending == "normal" {
					require.NoError(t, pushErr)
					require.Equal(t, []int{1, 2, 3}, accepted)
					require.Empty(t, unaccepted)
				} else {
					require.ErrorIs(t, pushErr, context.Canceled)
					require.Equal(t, []int{1}, accepted)
					require.Equal(t, []int{2, 3}, unaccepted)
				}
				var completed []int
				err := queue.Consume(context.Background(), func(_ context.Context, batch []int) error {
					completed = append(completed, batch...)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, accepted, completed)
			})
		})
	}
}

func TestLifecyclePipelineFailure(t *testing.T) {
	runLifecycleModes(t, runLifecyclePipelineFailure)
}

func runLifecyclePipelineFailure(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	synctest.Test(t, func(t *testing.T) {
		stage1 := newLifecycleQueue(t, 2, 2, diagnostics)
		stage2 := newLifecycleQueue(t, 1, 1, diagnostics)
		_, stopSources := context.WithCancel(context.Background())
		abortCtx, cancelAbort := context.WithCancel(context.Background())
		outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
		entered, release := make(chan struct{}), make(chan struct{})
		forwarding, forwarded := make(chan struct{}), make(chan struct{})
		joined := make(chan struct{})
		releaseHandler := sync.OnceFunc(func() { close(release) })
		var producers, stage1Consumers, stage2Consumers, owners sync.WaitGroup
		defer func() {
			releaseHandler()
			stopSources()
			cancelAbort()
			closePipeline(stage1, stage2, &producers, &stage1Consumers, &stage2Consumers)
			owners.Wait()
		}()
		require.NoError(t, stage2.Push(abortCtx, 0))
		sentinel := errors.New("downstream operation failed")
		var stage1Err, stage2Err error
		stage2Consumers.Go(func() {
			stage2Err = consumeAndAbort(abortCtx, stage2, "stage 2", func(
				_ context.Context,
				_ []int,
			) error {
				close(entered)
				<-release
				return sentinel
			}, outcome)
		})
		<-entered
		items := []int{2, 3}
		count, err := stage1.PushBatch(abortCtx, items)
		require.NoError(t, err)
		require.Equal(t, len(items), count)
		var progress forwardProgress
		var acquired []int
		stage1Consumers.Go(func() {
			defer close(forwarded)
			stage1Err = consumeAndAbort(abortCtx, stage1, "stage 1", func(
				ctx context.Context,
				batch []int,
			) error {
				acquired = slices.Clone(batch)
				close(forwarding)
				var err error
				progress, err = forwardDoubled(ctx, stage2, batch)
				return err
			}, outcome)
		})
		owners.Go(func() {
			closePipeline(stage1, stage2, &producers, &stage1Consumers, &stage2Consumers)
			close(joined)
		})
		<-forwarding
		synctest.Wait()
		requirePending(t, forwarded)
		requirePending(t, joined)
		require.NoError(t, abortCtx.Err())
		releaseHandler()
		owners.Wait()
		require.ErrorIs(t, outcome.err(), sentinel)
		require.Len(t, outcome.failures, 1)
		require.EqualError(t, outcome.failures[0], "stage 2: downstream operation failed")
		require.ErrorIs(t, stage2Err, sentinel)
		require.ErrorIs(t, stage1Err, context.Canceled)
		require.NotErrorIs(t, stage1Err, workq.ErrClosed)
		require.NotErrorIs(t, stage2Err, workq.ErrClosed)
		require.Equal(t, []int{2, 3}, acquired)
		require.Equal(t, []int{2, 3}, items)
		require.Equal(t, []int{4}, progress.forwarded)
		require.Equal(t, []int{6}, progress.unforwarded)
		remaining := make([]int, 1)
		count, err = stage2.Receive(context.Background(), remaining)
		require.NoError(t, err)
		require.Equal(t, []int{4}, remaining[:count])
		_, err = stage2.Receive(context.Background(), remaining)
		require.ErrorIs(t, err, workq.ErrClosed)
		_, err = stage1.Receive(context.Background(), remaining)
		require.ErrorIs(t, err, workq.ErrClosed)
	})
}

func TestLifecycleSelfFeedingFailure(t *testing.T) {
	runLifecycleModes(t, runLifecycleSelfFeedingFailure)
}

func runLifecycleSelfFeedingFailure(t *testing.T, diagnostics workq.DiagnosticsConfig) {
	synctest.Test(t, func(t *testing.T) {
		queue := newLifecycleQueue(t, 1, 1, diagnostics)
		_, stopSources := context.WithCancel(context.Background())
		abortCtx, cancelAbort := context.WithCancel(context.Background())
		outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
		rootEntered, peerEntered := make(chan struct{}), make(chan struct{})
		release := make(chan struct{})
		seeded, joined := make(chan struct{}), make(chan struct{})
		releaseRoot := sync.OnceFunc(func() { close(release) })
		var seeders, consumers, owners sync.WaitGroup
		defer func() {
			releaseRoot()
			stopSources()
			cancelAbort()
			joinSelfFeeding(queue, &seeders, &consumers)
			owners.Wait()
		}()
		work := &outstandingWork{queue: queue}
		work.add(1)
		sentinel := errors.New("local node failed")
		var localVisits []int
		var rootErr, peerErr error
		work.add(1)
		require.NoError(t, queue.Push(abortCtx, 1))
		consumers.Go(func() {
			rootErr = consumeAndAbort(abortCtx, queue, "tree consumer", func(
				ctx context.Context,
				batch []int,
			) error {
				return visitTree(ctx, queue, work, batch, func(_ context.Context, node int) error {
					localVisits = append(localVisits, node)
					if node == 1 {
						close(rootEntered)
						<-release
						return nil
					}
					return sentinel
				})
			}, outcome)
		})
		<-rootEntered
		work.add(1)
		require.NoError(t, queue.Push(abortCtx, 10))
		consumers.Go(func() {
			peerErr = consumeAndAbort(abortCtx, queue, "peer", func(
				ctx context.Context,
				batch []int,
			) error {
				return visitTree(ctx, queue, work, batch, func(ctx context.Context, _ int) error {
					close(peerEntered)
					<-ctx.Done()
					return peerAbort{reason: ctx.Err()}
				})
			}, outcome)
		})
		<-peerEntered
		var accepted, unaccepted []int
		var seedErr error
		work.add(1)
		seeders.Go(func() {
			defer close(seeded)
			defer work.finish()
			items := []int{20, 21}
			work.add(len(items))
			count, err := queue.PushBatch(abortCtx, items)
			seedErr = err
			accepted, unaccepted = slices.Clone(items[:count]), slices.Clone(items[count:])
			for range unaccepted {
				work.finish()
			}
			if failure := queueFailure(abortCtx, err); failure != nil {
				outcome.failAndAbort("seeder", failure)
			}
		})
		work.finish()
		owners.Go(func() {
			joinSelfFeeding(queue, &seeders, &consumers)
			close(joined)
		})
		synctest.Wait()
		requirePending(t, seeded)
		requirePending(t, joined)
		require.NoError(t, abortCtx.Err())
		releaseRoot()
		owners.Wait()
		require.ErrorIs(t, outcome.err(), sentinel)
		require.Len(t, outcome.failures, 1)
		require.ErrorIs(t, rootErr, sentinel)
		require.ErrorIs(t, peerErr, context.Canceled)
		require.ErrorIs(t, seedErr, context.Canceled)
		require.NotErrorIs(t, seedErr, workq.ErrClosed)
		require.Equal(t, []int{1, 3}, localVisits)
		require.Positive(t, work.count.Load())
		require.Equal(t, []int{20}, accepted)
		require.Equal(t, []int{21}, unaccepted)
		remaining := make([]int, 1)
		count, err := queue.Receive(context.Background(), remaining)
		require.NoError(t, err)
		require.Equal(t, []int{20}, remaining[:count])
		_, err = queue.Receive(context.Background(), remaining)
		require.ErrorIs(t, err, workq.ErrClosed)
	})
}

func runLifecycleModes(t *testing.T, run func(*testing.T, workq.DiagnosticsConfig)) {
	t.Helper()
	for _, scenario := range []struct {
		name   string
		config workq.DiagnosticsConfig
	}{
		{name: "disabled"},
		{name: "counters", config: workq.DiagnosticsConfig{
			Mode: workq.DiagnosticsCounters, Shards: 1, CapacitySampleEvery: 1,
		}},
		{name: "timing", config: workq.DiagnosticsConfig{
			Mode: workq.DiagnosticsTiming, Shards: 1,
			CapacitySampleEvery: 1, TimingSampleEvery: 1,
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) { run(t, scenario.config) })
	}
}
