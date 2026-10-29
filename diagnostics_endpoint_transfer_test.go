package workq

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type endpointProducerCall func([]int) (int, error)

type endpointProducerCase struct {
	name     string
	batch    bool
	blocking bool
	bind     func(DiagnosticsEndpoint[int], context.Context) endpointProducerCall
}

func endpointProducerCases() []endpointProducerCase {
	return []endpointProducerCase{
		{"Push", false, true, func(
			endpoint DiagnosticsEndpoint[int],
			ctx context.Context,
		) endpointProducerCall {
			push := endpoint.Push
			return func(items []int) (int, error) {
				err := push(ctx, items[0])
				if err != nil && err != ErrClosedDuringPush {
					return 0, err
				}
				return 1, err
			}
		}},
		{"TryPush", false, false, func(
			endpoint DiagnosticsEndpoint[int],
			_ context.Context,
		) endpointProducerCall {
			push := endpoint.TryPush
			return func(items []int) (int, error) {
				err := push(items[0])
				if err != nil && err != ErrClosedDuringPush {
					return 0, err
				}
				return 1, err
			}
		}},
		{"PushBatch", true, true, func(
			endpoint DiagnosticsEndpoint[int],
			ctx context.Context,
		) endpointProducerCall {
			push := endpoint.PushBatch
			return func(items []int) (int, error) { return push(ctx, items) }
		}},
		{"TryPushBatch", true, false, func(
			endpoint DiagnosticsEndpoint[int],
			_ context.Context,
		) endpointProducerCall {
			return endpoint.TryPushBatch
		}},
	}
}

func TestDiagnosticsEndpointProducerBinding(t *testing.T) {
	for _, method := range endpointProducerCases() {
		for _, mode := range []DiagnosticsMode{
			DiagnosticsDisabled, DiagnosticsCounters, DiagnosticsTiming,
		} {
			t.Run(fmt.Sprintf("%s/mode=%d", method.name, mode), func(t *testing.T) {
				fake := newScriptedEngine[int](8)
				config := DiagnosticsConfig{Mode: mode}
				if mode != DiagnosticsDisabled {
					config.Shards = 4
				}
				queue := newTestQueue(t, fake, WithDiagnostics(config))
				first := queue.NewDiagnosticsEndpoint()
				second := queue.NewDiagnosticsEndpoint()
				copied := first
				for _, endpoint := range []DiagnosticsEndpoint[int]{first, second, copied} {
					push := method.bind(endpoint, context.Background())
					for range 3 {
						moved, err := push([]int{7})
						require.NoError(t, err)
						require.Equal(t, 1, moved)
						assert.Equal(t, []int{7}, fake.takeRaw(1))
					}
				}
				if mode == DiagnosticsDisabled {
					assert.Nil(t, queue.diagnostics)
					assert.Zero(t, queue.Diagnostics().Producer)
					return
				}
				for index, want := range []uint64{6, 3, 0, 0} {
					counters := &queue.diagnostics.counters[index].producer
					assert.Equal(t, want, counters.attempts[PhaseInitial].Load())
					assert.Equal(t, want, counters.requestedSize.count.Load())
					assert.Equal(t, want, counters.movedSize.sum.Load())
				}
				assert.Equal(t, uint64(2), queue.diagnostics.creationTicket.Load())
				assert.Equal(t, uint64(9), queue.Diagnostics().Producer.MovedSize.Sum)
				kind := callEnqueue
				if method.batch {
					kind = callEnqueueBurst
				}
				assert.Equal(t, 9, fake.callCount(kind))
				assert.Len(t, fake.recordedCalls(), 9)
			})
		}
	}
}

func TestDiagnosticsEndpointProducerBoundaries(t *testing.T) {
	for _, method := range endpointProducerCases() {
		for _, scenario := range []string{
			"success", "partial", "empty", "closed", "canceled ready", "canceled full",
			"close during attempt", "close before next chunk", "cancel before next chunk",
		} {
			if !method.batch && scenario == "empty" {
				continue
			}
			betweenChunks := scenario == "close before next chunk" ||
				scenario == "cancel before next chunk"
			if betweenChunks && (!method.batch || !method.blocking) {
				continue
			}
			t.Run(method.name+"/"+scenario, func(t *testing.T) {
				queue, fake := newProducerDiagnosticsQueue(t, 0, WithMaxPushBatch(3))
				queue.NewDiagnosticsEndpoint()
				endpoint := queue.NewDiagnosticsEndpoint()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				items := []int{1}
				requested := []int{1}
				accepted := []int{1}
				if method.batch {
					items = sequence(1, 5)
					requested = []int{3, 2}
					accepted = []int{3, 2}
				}
				wantMoved := len(items)
				var wantErr error
				switch scenario {
				case "partial":
					fake.setMaxMove(1)
					if method.batch {
						requested = []int{3, 2, 1, 2, 1}
						accepted = []int{1, 1, 1, 1, 1}
						if !method.blocking {
							wantMoved, wantErr = 1, ErrFull
							requested, accepted = []int{3}, []int{1}
						}
					}
				case "empty":
					items, requested, accepted = nil, nil, nil
					wantMoved = 0
				case "closed":
					cancel()
					queue.Close()
					wantMoved, wantErr = 0, ErrClosed
					requested, accepted = nil, nil
				case "canceled ready":
					cancel()
					if method.batch && method.blocking {
						wantMoved, wantErr = 3, context.Canceled
						requested, accepted = []int{3}, []int{3}
					}
				case "canceled full":
					cancel()
					fake.pushRaw(sequence(10, 8)...)
					wantMoved, wantErr = 0, ErrFull
					if method.blocking {
						wantErr = context.Canceled
					}
					requested, accepted = requested[:1], []int{0}
				case "close during attempt":
					fake.setHooks(nil, func(engineCall) {
						cancel()
						queue.Close()
					})
					requested, accepted = requested[:1], accepted[:1]
					wantMoved, wantErr = accepted[0], ErrClosedDuringPush
				case "close before next chunk", "cancel before next chunk":
					queue.hooks.beforeChunkCheck = func() {
						cancel()
						if scenario == "close before next chunk" {
							queue.Close()
						}
					}
					wantMoved, wantErr = 3, context.Canceled
					if scenario == "close before next chunk" {
						wantErr = ErrClosedDuringPush
					}
					requested, accepted = []int{3}, []int{3}
				}
				moved, err := method.bind(endpoint, ctx)(items)
				require.Equal(t, wantErr, err)
				require.Equal(t, wantMoved, moved)
				if moved > 0 {
					assert.Equal(t, items[:moved], fake.takeRaw(moved))
				}
				snapshot := queue.Diagnostics().Producer
				assert.Equal(t, uint64(len(requested)), snapshot.Attempts[PhaseInitial])
				assert.Equal(t, uint64(moved), snapshot.MovedItems[PhaseInitial])
				var requestedSum, zero, partial, full uint64
				var requestedBuckets, movedBuckets [65]uint64
				for index, size := range requested {
					requestedSum += uint64(size)
					requestedBuckets[size]++
					movedBuckets[accepted[index]]++
					switch accepted[index] {
					case 0:
						zero++
					case size:
						full++
					default:
						partial++
					}
				}
				assert.Equal(t, requestedSum, snapshot.RequestedItems[PhaseInitial])
				assert.Equal(t, zero, snapshot.ZeroProgress[PhaseInitial])
				assert.Equal(t, partial, snapshot.PartialProgress[PhaseInitial])
				assert.Equal(t, full, snapshot.FullProgress[PhaseInitial])
				for size := range 4 {
					assert.Equal(t, requestedBuckets[size], snapshot.RequestedSize.Buckets[size])
					assert.Equal(t, movedBuckets[size], snapshot.MovedSize.Buckets[size])
				}
				assert.Equal(t, uint64(len(requested)), snapshot.RequestedSize.Count)
				assert.Equal(t, uint64(len(accepted)), snapshot.MovedSize.Count)
				assert.Zero(t, queue.diagnostics.counters[0].producer.attempts[PhaseInitial].Load())
				assert.Equal(
					t,
					uint64(len(requested)),
					queue.diagnostics.counters[1].producer.attempts[PhaseInitial].Load(),
				)
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestDiagnosticsEndpointReceiveBinding(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		for _, size := range []int{0, 1, 5} {
			for _, mode := range diagnosticsModeCases() {
				name := fmt.Sprintf("blocking=%t/size=%d/%s", blocking, size, mode.name)
				t.Run(name, func(t *testing.T) {
					config := mode.config
					if config.Mode != DiagnosticsDisabled {
						config.Shards = 4
					}
					fake := newScriptedEngine[int](8)
					queue := newTestQueue(
						t,
						fake,
						WithMaxReceiveBatch(3),
						WithDiagnostics(config),
					)
					first := queue.NewDiagnosticsEndpoint()
					second := queue.NewDiagnosticsEndpoint()
					copied := first
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					requested := min(size, 3)
					for _, endpoint := range []DiagnosticsEndpoint[int]{first, second, copied} {
						receive := endpoint.TryReceive
						if blocking {
							receive = func(dst []int) (int, error) {
								return endpoint.Receive(ctx, dst)
							}
						}
						for range 2 {
							fake.pushRaw(sequence(1, requested)...)
							dst := make([]int, size)
							moved, err := receive(dst)
							require.NoError(t, err)
							require.Equal(t, requested, moved)
							assert.Equal(t, sequence(1, requested), dst[:moved])
						}
					}
					if config.Mode == DiagnosticsDisabled {
						assert.Zero(t, queue.Diagnostics().Consumer)
					} else {
						for index, attempts := range []uint64{4, 2, 0, 0} {
							if size == 0 {
								attempts = 0
							}
							counters := &queue.diagnostics.counters[index].consumer
							assert.Equal(t, attempts, counters.attempts[PhaseInitial].Load())
							assert.Equal(t, attempts, counters.requestedSize.count.Load())
							assert.Equal(t, attempts*uint64(requested), counters.movedSize.sum.Load())
							assert.Equal(t, attempts, counters.capacityCompletedProbes.Load())
						}
					}
					kind := callDequeueBurst
					if size == 1 {
						kind = callDequeue
					}
					wantCalls := 6
					if size == 0 {
						wantCalls = 0
					}
					assert.Equal(t, wantCalls, fake.callCount(kind))
					assertIdleQueue(t, queue)
				})
			}
		}
	}
}

func TestDiagnosticsEndpointConsumerBoundaries(t *testing.T) {
	for _, method := range []string{"Receive", "TryReceive", "Consume"} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed=%t", method, closed), func(t *testing.T) {
				queue, fake := newConsumerDiagnosticsQueue(t, 1, WithMaxReceiveBatch(2))
				fake.pushRaw(1, 2, 3)
				if closed {
					queue.Close()
				}
				before := queue.Diagnostics()
				endpoint := queue.NewDiagnosticsEndpoint()
				assert.Equal(t, before, queue.Diagnostics())
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if method == "Consume" {
					err := endpoint.Consume(ctx, func(context.Context, []int) error {
						t.Fatal("canceled Consume called handler")
						return nil
					})
					require.Same(t, context.Canceled, err)
					assert.Equal(t, before, queue.Diagnostics())
					assert.Equal(t, []int{1, 2, 3}, fake.takeRaw(3))
					return
				}
				receive := endpoint.TryReceive
				if method == "Receive" {
					receive = func(dst []int) (int, error) {
						return endpoint.Receive(ctx, dst)
					}
				}
				moved, err := receive(nil)
				require.NoError(t, err)
				require.Zero(t, moved)
				assert.Equal(t, before, queue.Diagnostics())
				dst := make([]int, 4)
				for _, want := range [][]int{{1, 2}, {3}} {
					moved, err = receive(dst)
					require.NoError(t, err)
					require.Equal(t, len(want), moved)
					assert.Equal(t, want, dst[:moved])
				}
				moved, err = receive(dst)
				require.Zero(t, moved)
				switch {
				case closed:
					require.Same(t, ErrClosed, err)
				case method == "Receive":
					require.Same(t, context.Canceled, err)
				default:
					require.NoError(t, err)
				}
				assertIdleQueue(t, queue)
			})
		}
	}
}

func TestDiagnosticsEndpointConsumeBinding(t *testing.T) {
	for _, mode := range diagnosticsModeCases() {
		for _, outcome := range []string{"complete", "error", "panic"} {
			t.Run(mode.name+"/"+outcome, func(t *testing.T) {
				config := mode.config
				if config.Mode != DiagnosticsDisabled {
					config.Shards = 4
				}
				fake := newScriptedEngine[int](8)
				queue := newTestQueue(t, fake, WithMaxReceiveBatch(2), WithDiagnostics(config))
				first := queue.NewDiagnosticsEndpoint()
				second := queue.NewDiagnosticsEndpoint()
				copied := first
				for _, endpoint := range []DiagnosticsEndpoint[int]{first, second, copied} {
					for range 2 {
						fake.pushRaw(1, 2, 3, 4)
						ctx, cancel := context.WithCancel(context.Background())
						var received []int
						var buffer *int
						handler := func(actual context.Context, batch []int) error {
							assert.Same(t, ctx, actual)
							if buffer == nil {
								buffer = &batch[0]
							}
							assert.Same(t, buffer, &batch[0])
							received = append(received, batch...)
							assertIdleQueue(t, queue)
							switch outcome {
							case "error":
								return errHandler
							case "panic":
								panic("endpoint handler panic")
							}
							if len(received) == 4 {
								cancel()
							}
							return nil
						}
						if outcome == "panic" {
							assert.PanicsWithValue(t, "endpoint handler panic", func() {
								_ = endpoint.Consume(ctx, handler)
							})
						} else {
							err := endpoint.Consume(ctx, handler)
							if outcome == "error" {
								require.ErrorIs(t, err, errHandler)
								assert.EqualError(t, err, "workq: handler: "+errHandler.Error())
							} else {
								require.Same(t, context.Canceled, err)
							}
						}
						cancel()
						assert.Equal(t, sequence(1, len(received)), received)
						if outcome != "complete" {
							assert.Equal(t, []int{3, 4}, fake.takeRaw(2))
						}
						fake.mu.Lock()
						assert.Empty(t, fake.visible)
						fake.mu.Unlock()
						assertIdleQueue(t, queue)
					}
				}
				if config.Mode == DiagnosticsDisabled {
					assert.Zero(t, queue.Diagnostics().Consumer)
					return
				}
				for index, attempts := range []uint64{4, 2, 0, 0} {
					if outcome == "complete" {
						attempts *= 2
					}
					counters := &queue.diagnostics.counters[index].consumer
					assert.Equal(t, attempts, counters.attempts[PhaseInitial].Load())
					assert.Equal(t, attempts, counters.requestedSize.count.Load())
					assert.Equal(t, 2*attempts, counters.movedSize.sum.Load())
					assert.Equal(t, attempts, counters.capacityCompletedProbes.Load())
				}
			})
		}
	}
}

func TestDiagnosticsEndpointConsumeClearsBuffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, err := New[*int](8, WithMaxReceiveBatch(2), WithDiagnostics(DiagnosticsConfig{
			Mode: DiagnosticsTiming, Shards: 2, CapacitySampleEvery: 1, TimingSampleEvery: 1,
		}))
		require.NoError(t, err)
		payload := 7
		require.NoError(t, queue.TryPush(&payload))
		endpoint := queue.NewDiagnosticsEndpoint()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		batches := make(chan []*int, 1)
		results := make(chan error, 1)
		go func() {
			results <- endpoint.Consume(ctx, func(_ context.Context, batch []*int) error {
				batches <- batch
				return nil
			})
		}()
		batch := <-batches
		synctest.Wait()
		requireParked(t, &queue.consumers, 1)
		assert.Equal(t, []*int{nil, nil}, batch[:cap(batch)])
		cancel()
		require.Same(t, context.Canceled, <-results)
		assertIdleSide(t, &queue.consumers)
		assertIdleSide(t, &queue.producers)
	})
}
