package workq

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsEndpointConcurrentIntegrity(t *testing.T) {
	for _, shards := range []int{1, 4} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			queue, err := New[int](16, WithDiagnostics(DiagnosticsConfig{
				Mode: DiagnosticsTiming, Shards: shards,
				TimingSampleEvery: 1, CapacitySampleEvery: 1,
			}))
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			shared := queue.NewDiagnosticsEndpoint()
			copied := shared
			endpoints := []DiagnosticsEndpoint[int]{shared, copied, shared}
			for range shards + 1 {
				endpoints = append(endpoints, queue.NewDiagnosticsEndpoint())
			}
			pushes := []func(context.Context, int) error{queue.Push}
			receives := []func(context.Context, []int) (int, error){queue.Receive}
			for _, endpoint := range endpoints {
				pushes = append(pushes, endpoint.Push)
				receives = append(receives, endpoint.Receive)
			}
			const perProducer = 64
			total := len(pushes) * perProducer
			items := make(chan int, total)
			failures := make(chan error, len(pushes)+len(receives))
			var producers, consumers, collectors sync.WaitGroup
			stopCollectors := make(chan struct{})
			collectorStarted := make(chan struct{}, 2)
			for range 2 {
				collectors.Go(func() {
					queue.Diagnostics()
					collectorStarted <- struct{}{}
					for {
						select {
						case <-stopCollectors:
							return
						default:
							queue.Diagnostics()
							runtime.Gosched()
						}
					}
				})
			}
			for range 2 {
				<-collectorStarted
			}
			for _, receive := range receives {
				consumers.Go(func() {
					buffer := make([]int, 3)
					for {
						count, err := receive(ctx, buffer)
						for _, item := range buffer[:count] {
							select {
							case items <- item:
							default:
								failures <- fmt.Errorf("received more than %d items", total)
								cancel()
								return
							}
						}
						if errors.Is(err, ErrClosed) {
							return
						}
						if err != nil {
							failures <- err
							return
						}
					}
				})
			}
			for index, push := range pushes {
				producers.Go(func() {
					for offset := range perProducer {
						if err := push(ctx, index*perProducer+offset); err != nil {
							failures <- err
							return
						}
					}
				})
			}
			producers.Wait()
			queue.Close()
			consumers.Wait()
			close(stopCollectors)
			collectors.Wait()
			close(failures)
			for err := range failures {
				assert.NoError(t, err)
			}
			close(items)
			seen := make([]bool, total)
			for item := range items {
				require.GreaterOrEqual(t, item, 0)
				require.Less(t, item, total)
				assert.False(t, seen[item], "duplicate item %d", item)
				seen[item] = true
			}
			for item, found := range seen {
				assert.True(t, found, "missing item %d", item)
			}
			snapshot := queue.Diagnostics()
			assertDiagnosticsConserved(t, snapshot, uint64(total), uint64(total))
			for _, side := range []DiagnosticsSideSnapshot{
				snapshot.Producer.DiagnosticsSideSnapshot, snapshot.Consumer.DiagnosticsSideSnapshot,
			} {
				assert.Equal(t, side.CapacityEligibleAttempts, side.CapacityCompletedProbes)
			}
			assertIdleQueue(t, queue)
		})
	}
}

func TestDiagnosticsEndpointOverlappingRecording(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, fake := newTimingDiagnosticsQueue(t, 1)
		endpoint := queue.NewDiagnosticsEndpoint()
		origin := queue.NewDiagnosticsEndpoint()
		fake.pushRaw(sequence(1, 8)...)
		probeEntered := make(chan struct{})
		resumeProbe := make(chan struct{})
		fake.setHooks(func(call engineCall) {
			if call.kind == callPeek && call.transferKind == callEnqueueBurst &&
				fake.callCount(callEnqueue) == 3 {
				close(probeEntered)
				<-resumeProbe
			}
		}, nil)
		result := make(chan error, 1)
		go func() { result <- endpoint.Push(context.Background(), 9) }()
		synctest.Wait()
		requireParked(t, &queue.producers, 1)
		count, err := origin.TryReceive(make([]int, 1))
		require.NoError(t, err)
		require.Equal(t, 1, count)
		<-probeEntered
		before := queue.Diagnostics().Producer
		assert.Equal(t, uint64(3), before.CapacitySelectedProbes)
		assert.Equal(t, uint64(2), before.CapacityCompletedProbes)
		assert.Zero(t, before.Timing.WaitSamples)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, endpoint.Push(ctx, 10), context.Canceled)
		during := queue.Diagnostics().Producer
		assert.Equal(t, uint64(1), during.Timing.WaitSamples)
		assert.Equal(t, uint64(1), during.WaitEnds[WaitCanceled])
		assert.Zero(t, during.WaitEnds[WaitProgress])
		assert.Equal(t, uint64(4), during.CapacityCompletedProbes)
		assert.Equal(t, uint64(1), during.Attempts[PhaseInitial]-before.Attempts[PhaseInitial])
		assert.Equal(t, uint64(1), during.Attempts[PhaseExtra]-before.Attempts[PhaseExtra])
		close(resumeProbe)
		require.NoError(t, <-result)
		snapshot := queue.Diagnostics()
		assert.Equal(t, uint64(2), snapshot.Producer.Timing.WaitSamples)
		assert.Equal(t, uint64(1), snapshot.Producer.WakeRetryEnds[RetryProgress])
		assert.Equal(t, uint64(1), snapshot.Producer.WaitEnds[WaitProgress])
		assert.Equal(t, uint64(5), snapshot.Producer.CapacityCompletedProbes)
		assert.Equal(t, uint64(1), snapshot.Producer.TokensReceivedForRetry)
		assertDiagnosticsConserved(t, snapshot, 1, 1)
		assertIdleQueue(t, queue)
	})
}

func TestDiagnosticsEndpointConsumeHandlerPush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, _ := newTimingDiagnosticsQueue(t, 1)
		endpoint := queue.NewDiagnosticsEndpoint()
		var handled []int
		result := make(chan error, 1)
		go func() {
			result <- endpoint.Consume(context.Background(), func(_ context.Context, batch []int) error {
				handled = append(handled, batch...)
				if batch[0] == 1 {
					return endpoint.TryPush(2)
				}
				queue.Close()
				return nil
			})
		}()
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)
		require.NoError(t, endpoint.TryPush(1))
		require.NoError(t, <-result)
		assert.Equal(t, []int{1, 2}, handled)
		snapshot := queue.Diagnostics()
		assertDiagnosticsConserved(t, snapshot, 2, 2)
		assert.Equal(t, uint64(1), snapshot.Consumer.WaitEnds[WaitProgress])
		assert.Equal(t, uint64(2), snapshot.Producer.CapacityCompletedProbes)
		assert.Equal(
			t, snapshot.Consumer.CapacityEligibleAttempts, snapshot.Consumer.CapacityCompletedProbes,
		)
		for _, counters := range []*diagnosticsSideCounters{
			&queue.diagnostics.counters[1].producer.diagnosticsSideCounters,
			&queue.diagnostics.counters[1].consumer.diagnosticsSideCounters,
		} {
			var unused DiagnosticsSideSnapshot
			unused.initializeHistogramMinima()
			unused.addCounters(counters, nil)
			unused.normalizeEmptyHistograms()
			assert.Zero(t, unused)
		}
		assertIdleQueue(t, queue)
	})
}
