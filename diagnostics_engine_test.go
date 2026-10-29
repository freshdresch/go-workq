package workq

import (
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
				t.Run(engineCase.name+"/"+mode.name+"/"+transfer.name, func(t *testing.T) {
					options := append([]Option{}, engineCase.options...)
					options = append(options, WithDiagnostics(mode.config))
					consumerQueue, consumerEngine := newTestEngine(t, options)
					primeConsumerCache(t, consumerEngine)
					dst := make([]int, transfer.requested)
					moved, err := consumerQueue.TryReceive(dst)
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
					moved, err = producerQueue.TryPushBatch(sequence(100, transfer.requested))
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
