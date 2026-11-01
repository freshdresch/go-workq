package workq

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring"
)

func TestDiagnosticsConfigModesAndShards(t *testing.T) {
	for _, mode := range []DiagnosticsMode{DiagnosticsCounters, DiagnosticsTiming} {
		for _, shards := range []int{0, 1, 16, 256, 512} {
			t.Run(
				fmt.Sprintf("mode=%d/shards=%d", mode, shards),
				func(t *testing.T) {
					cfg := DiagnosticsConfig{Mode: mode, Shards: shards, CapacitySampleEvery: 7}
					queue, err := New[int](2, WithDiagnostics(cfg))
					require.NoError(t, err)
					require.NotNil(t, queue.diagnostics)
					if shards == 0 {
						cfg.Shards = 16
					}
					if mode == DiagnosticsTiming {
						cfg.TimingSampleEvery = 64
						require.Len(t, queue.diagnostics.Timing, cfg.Shards)
					} else {
						assert.Nil(t, queue.diagnostics.Timing)
					}
					require.Len(t, queue.diagnostics.Counters, cfg.Shards)
					assert.Equal(t, cfg, queue.Diagnostics().Config.DiagnosticsConfig)
					assert.Equal(t, cfg, queue.diagnostics.Config)
				},
			)
		}
	}
}

func TestDiagnosticsConfigInvalid(t *testing.T) {
	cases := []struct {
		name string
		cfg  DiagnosticsConfig
	}{
		{"unknown mode", DiagnosticsConfig{Mode: 255}},
		{"disabled shards", DiagnosticsConfig{Shards: 1}},
		{"disabled timing", DiagnosticsConfig{TimingSampleEvery: 1}},
		{"disabled capacity", DiagnosticsConfig{CapacitySampleEvery: 1}},
		{"counter timing", DiagnosticsConfig{Mode: DiagnosticsCounters, TimingSampleEvery: 1}},
	}
	for _, mode := range []DiagnosticsMode{DiagnosticsCounters, DiagnosticsTiming} {
		for _, shards := range []int{-1, 3, 15, 255, 257, int(^uint(0) >> 1)} {
			cases = append(
				cases,
				struct {
					name string
					cfg  DiagnosticsConfig
				}{fmt.Sprintf("mode=%d/shards=%d", mode, shards),
					DiagnosticsConfig{Mode: mode, Shards: shards}},
			)
		}
	}
	for _, testCase := range cases {
		t.Run(
			testCase.name,
			func(t *testing.T) {
				queue, err := New[int](64, WithDiagnostics(testCase.cfg))
				assert.Nil(t, queue)
				require.ErrorIs(t, err, ErrInvalidDiagnostics)
				assert.NotSame(t, ErrInvalidDiagnostics, err)
				assert.ErrorContains(t, err, "new work queue:")
			},
		)
	}
}

func TestDiagnosticsConfigFinalValues(t *testing.T) {
	valid := DiagnosticsConfig{
		Mode:                DiagnosticsTiming,
		Shards:              1,
		TimingSampleEvery:   ^uint32(0),
		CapacitySampleEvery: ^uint32(0),
	}
	queue, err := New[int](
		64,
		WithDiagnostics(DiagnosticsConfig{Mode: 255}),
		WithDiagnostics(valid),
	)
	require.NoError(t, err)
	assert.Equal(t, valid, queue.Diagnostics().Config.DiagnosticsConfig)
	queue, err = New[int](64, WithDiagnostics(valid), WithDiagnostics(DiagnosticsConfig{}))
	require.NoError(t, err)
	assert.Nil(t, queue.diagnostics)
	_, err = New[int](64, WithDiagnostics(valid), WithDiagnostics(DiagnosticsConfig{Shards: -1}))
	require.ErrorIs(t, err, ErrInvalidDiagnostics)
	for _, rate := range []uint32{1, 17, ^uint32(0)} {
		valid.TimingSampleEvery = rate
		queue, err = New[int](64, WithDiagnostics(valid))
		require.NoError(t, err)
		assert.Equal(t, rate, queue.Diagnostics().Config.TimingSampleEvery)
	}
}

func TestDiagnosticsConfigPrecedence(t *testing.T) {
	invalid := WithDiagnostics(DiagnosticsConfig{Mode: 255})
	cases := []struct {
		name     string
		capacity int
		options  []Option
		want     error
	}{
		{"consumer wait", 3,
			[]Option{invalid, WithConsumerWait(WaitStrategy{Spins: -1}), WithMaxPushBatch(0)},
			ErrInvalidWaitStrategy},
		{"producer wait", 3,
			[]Option{invalid, WithProducerWait(WaitStrategy{Yields: -1}), WithMaxReceiveBatch(0)},
			ErrInvalidWaitStrategy},
		{"diagnostics before capacity", 3,
			[]Option{invalid, WithMaxPushBatch(0)}, ErrInvalidDiagnostics},
		{"capacity before batch", 3,
			[]Option{WithDiagnostics(DiagnosticsConfig{Mode: DiagnosticsCounters}), WithMaxPushBatch(0)},
			ring.ErrInvalidCount},
		{"valid diagnostics invalid batch", 64,
			[]Option{WithDiagnostics(DiagnosticsConfig{Mode: DiagnosticsTiming}), WithMaxPushBatch(0)},
			ring.ErrInvalidMaxBatch},
	}
	for _, engineCase := range engineCases() {
		for _, testCase := range cases {
			t.Run(
				engineCase.name+"/"+testCase.name,
				func(t *testing.T) {
					options := append(append([]Option{}, engineCase.options...), testCase.options...)
					queue, err := New[int](testCase.capacity, options...)
					assert.Nil(t, queue)
					require.ErrorIs(t, err, testCase.want)
				},
			)
		}
	}
}

func TestDiagnosticsDisabledStatic(t *testing.T) {
	setGOMAXPROCS(t, 1)
	kinds := []EngineKind{EngineSPSC, EngineMPSC, EngineSPMC, EngineMPMC}
	for i, engineCase := range engineCases() {
		t.Run(
			engineCase.name,
			func(t *testing.T) {
				options := append(
					append([]Option{}, engineCase.options...),
					WithMaxPushBatch(4),
					WithMaxReceiveBatch(2),
					WithConsumerWait(WaitStrategy{Spins: 9, Yields: 5}),
					WithProducerWait(WaitStrategy{Spins: 7, Yields: 3, NeverPark: true}),
				)
				queue, err := New[int](64, options...)
				require.NoError(t, err)
				assert.Nil(t, queue.diagnostics)
				want := DiagnosticsSnapshot{
					SchemaVersion: 1,
					Config: DiagnosticsStaticConfig{
						Engine:          kinds[i],
						Capacity:        64,
						MaxPushBatch:    4,
						MaxReceiveBatch: 2,
						ConsumerWait:    WaitStrategy{Yields: 5},
						ProducerWait:    WaitStrategy{Yields: 3, NeverPark: true},
						ConsumerWakeCap: 1,
					},
				}
				assert.Equal(t, want, queue.Diagnostics())
				queue.Close()
				assert.Equal(t, want, queue.Diagnostics())
				// A nil engine proves collection needs only its concrete type, not its methods.
				queue.engine = nil
				want.Config.Engine = EngineUnknown
				assert.Equal(t, want, queue.Diagnostics())
			},
		)
	}
}
