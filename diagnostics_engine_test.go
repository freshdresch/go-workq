package workq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type diagnosticsModeCase struct {
	name   string
	config DiagnosticsConfig
}

func diagnosticsModeCases() []diagnosticsModeCase {
	return []diagnosticsModeCase{
		{name: "disabled"},
		{name: "counters", config: DiagnosticsConfig{
			Mode: DiagnosticsCounters, Shards: 1, CapacitySampleEvery: 1,
		}},
		{name: "timing", config: DiagnosticsConfig{
			Mode: DiagnosticsTiming, Shards: 1, CapacitySampleEvery: 1, TimingSampleEvery: 1,
		}},
	}
}

func TestDiagnosticsEngineObservations(t *testing.T) {
	for _, bound := range []bool{false, true} {
		for _, engineCase := range engineCases() {
			for _, mode := range diagnosticsModeCases()[1:] {
				for _, transfer := range []struct {
					name      string
					requested int
					moved     int
					remaining int
					source    CapacitySource
				}{
					{"full", 2, 2, 3, TransferCachedAllowed},
					{"short", 8, 5, 0, TransferFresh},
				} {
					name := engineCase.name + "/" + mode.name + "/" + transfer.name
					if bound {
						name += "/endpoint"
					}
					t.Run(name, func(t *testing.T) {
						options := append([]Option{}, engineCase.options...)
						options = append(options, WithDiagnostics(mode.config))
						consumerQueue, consumerEngine := newTestEngine(t, options)
						primeConsumerCache(t, consumerEngine)
						dst := make([]int, transfer.requested)
						receive := consumerQueue.TryReceive
						if bound {
							receive = consumerQueue.NewDiagnosticsEndpoint().TryReceive
						}
						moved, err := receive(dst)
						require.NoError(t, err)
						require.Equal(t, transfer.moved, moved)
						assert.Equal(t, sequence(1, moved), dst[:moved])
						consumer := consumerQueue.Diagnostics().Consumer
						transferRemaining := 0
						if engineCase.name == "MPSC" && transfer.source == TransferCachedAllowed {
							// Even an internally fresh full burst keeps the conservative source.
							transferRemaining = 3
						}
						assertEngineObservations(
							t,
							consumer.AvailableItems,
							transfer.source,
							transferRemaining,
							transfer.remaining,
						)
						assert.Zero(t, consumer.AttemptsWithoutRemainingCount)
						assertIdleQueue(t, consumerQueue)

						producerQueue, producerEngine := newTestEngine(t, options)
						primeProducerCache(t, producerEngine)
						push := producerQueue.TryPushBatch
						if bound {
							push = producerQueue.NewDiagnosticsEndpoint().TryPushBatch
						}
						moved, err = push(sequence(100, transfer.requested))
						if transfer.source == TransferFresh {
							require.ErrorIs(t, err, ErrFull)
						} else {
							require.NoError(t, err)
						}
						require.Equal(t, transfer.moved, moved)
						producer := producerQueue.Diagnostics().Producer
						assertEngineObservations(
							t,
							producer.ReusableSlots,
							transfer.source,
							0,
							transfer.remaining,
						)
						assert.Zero(t, producer.AttemptsWithoutRemainingCount)
						dequeueAll(t, producerEngine, 6, engineTestCapacity-5)
						dequeueAll(t, producerEngine, 100, moved)
						assertIdleQueue(t, producerQueue)
					})
				}
			}
		}
	}

}

func assertEngineObservations(
	t *testing.T,
	observations [CapacitySourceCount]CapacityHistogram,
	source CapacitySource,
	transferRemaining, freshRemaining int,
) {
	t.Helper()
	assert.Equal(t, uint64(1), observations[source].Count)
	if transferRemaining == 0 {
		assert.Equal(t, uint64(1), observations[source].ZeroCount)
	} else {
		assert.Zero(t, observations[source].ZeroCount)
	}
	assert.Equal(t, uint64(transferRemaining), observations[source].Sum)
	assert.Equal(t, uint64(1), observations[DiagnosticProbe].Count)
	assert.Equal(t, uint64(freshRemaining), observations[DiagnosticProbe].Sum)
	for other := range CapacitySourceCount {
		if other != source && other != DiagnosticProbe {
			assert.Zero(t, observations[other], "unexpected source %v", other)
		}
	}
}

func TestDiagnosticsHoleCountsAreIndependent(t *testing.T) {
	for _, hole := range []string{"publication", "release"} {
		for _, mode := range diagnosticsModeCases()[1:] {
			t.Run(hole+"/"+mode.name, func(t *testing.T) {
				fake := newScriptedEngine[int](8)
				queue := newTestQueue(t, fake, WithDiagnostics(mode.config))
				if hole == "publication" {
					fake.holdVisibility()
					fake.pushRaw(sequence(1, 8)...)
				} else {
					// Model the unreleased consumer reservation from the frontier fixtures.
					// The consumer sees no claimable work and the producer cannot reuse room.
					fake.setHooks(func(call engineCall) {
						if call.kind == callEnqueueBurst || call.transferKind == callEnqueueBurst {
							fake.setForceEmpty()
						}
					}, nil)
				}
				moved, err := queue.TryReceive(make([]int, 2))
				require.NoError(t, err)
				require.Zero(t, moved)
				moved, err = queue.TryPushBatch([]int{9, 10})
				require.ErrorIs(t, err, ErrFull)
				require.Zero(t, moved)
				snapshot := queue.Diagnostics()
				assertEngineObservations(t, snapshot.Consumer.AvailableItems, TransferFresh, 0, 0)
				assertEngineObservations(t, snapshot.Producer.ReusableSlots, TransferFresh, 0, 0)

				if hole == "publication" {
					fake.releaseVisibility()
					dst := make([]int, 8)
					moved, err = queue.TryReceive(dst)
					require.NoError(t, err)
					require.Equal(t, 8, moved)
					assert.Equal(t, sequence(1, 8), dst)
				} else {
					fake.setHooks(nil, nil)
				}
				moved, err = queue.TryPushBatch([]int{9, 10})
				require.NoError(t, err)
				require.Equal(t, 2, moved)
				snapshot = queue.Diagnostics()
				assert.Equal(t, uint64(6), snapshot.Producer.ReusableSlots[TransferCachedAllowed].Sum)
				assert.Equal(t, uint64(6), snapshot.Producer.ReusableSlots[DiagnosticProbe].Sum)
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestDiagnosticsEndpointEngineParity(t *testing.T) {
	for _, engineCase := range engineCases() {
		for _, mode := range diagnosticsModeCases() {
			t.Run(engineCase.name+"/"+mode.name, func(t *testing.T) {
				var snapshots [2]DiagnosticsSnapshot
				for index, bound := range []bool{false, true} {
					options := append([]Option{}, engineCase.options...)
					options = append(options, WithDiagnostics(mode.config), WithMaxReceiveBatch(2))
					queue, err := New[int](8, options...)
					require.NoError(t, err)
					push, pushBatch := queue.Push, queue.PushBatch
					tryPush, tryPushBatch := queue.TryPush, queue.TryPushBatch
					receive, tryReceive, consume := queue.Receive, queue.TryReceive, queue.Consume
					if bound {
						endpoint := queue.NewDiagnosticsEndpoint()
						push, pushBatch = endpoint.Push, endpoint.PushBatch
						tryPush, tryPushBatch = endpoint.TryPush, endpoint.TryPushBatch
						receive, tryReceive, consume = endpoint.Receive, endpoint.TryReceive, endpoint.Consume
					}
					ctx := context.Background()
					require.NoError(t, push(ctx, 1))
					require.NoError(t, tryPush(2))
					moved, err := pushBatch(ctx, []int{3, 4})
					require.NoError(t, err)
					require.Equal(t, 2, moved)
					moved, err = tryPushBatch([]int{5, 6})
					require.NoError(t, err)
					require.Equal(t, 2, moved)
					dst := make([]int, 1)
					moved, err = receive(ctx, dst)
					require.NoError(t, err)
					require.Equal(t, 1, moved)
					assert.Equal(t, []int{1}, dst)
					moved, err = tryReceive(dst)
					require.NoError(t, err)
					require.Equal(t, 1, moved)
					assert.Equal(t, []int{2}, dst)
					queue.Close()
					var handled []int
					require.NoError(t, consume(ctx, func(_ context.Context, batch []int) error {
						handled = append(handled, batch...)
						return nil
					}))
					assert.Equal(t, []int{3, 4, 5, 6}, handled)
					assertIdleQueue(t, queue)
					snapshots[index] = queue.Diagnostics()
					if mode.config.Mode == DiagnosticsDisabled {
						assert.Zero(t, snapshots[index].Producer)
						assert.Zero(t, snapshots[index].Consumer)
						continue
					}
					assert.Equal(t, uint64(6), snapshots[index].Producer.MovedSize.Sum)
					assert.Equal(t, uint64(6), snapshots[index].Consumer.MovedSize.Sum)
					assertDiagnosticsConserved(t, snapshots[index], 6, 6)
					// Wall-clock durations vary, but sample starts and completions must agree.
					snapshots[index].Producer.Timing.WaitDuration = [WaitOutcomeCount]DurationHistogram{}
					snapshots[index].Producer.Timing.ParkSelectDuration = [ParkSelectCaseCount]DurationHistogram{}
					snapshots[index].Consumer.Timing.WaitDuration = [WaitOutcomeCount]DurationHistogram{}
					snapshots[index].Consumer.Timing.ParkSelectDuration = [ParkSelectCaseCount]DurationHistogram{}
				}
				assert.Equal(t, snapshots[0], snapshots[1])
			})
		}
	}
}
