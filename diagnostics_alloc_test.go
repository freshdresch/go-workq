package workq

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"unsafe"
)

func TestDiagnosticsAllocTransfers(t *testing.T) {
	for _, engineCase := range engineCases() {
		for _, mode := range diagnosticsModeCases() {
			for _, size := range []int{1, 8} {
				t.Run(fmt.Sprintf("%s/%s/%d", engineCase.name, mode.name, size), func(t *testing.T) {
					options := append([]Option{}, engineCase.options...)
					options = append(options, WithDiagnostics(mode.config))
					queue, _ := newTestEngine(t, options)
					items := make([]int, size)
					dst := make([]int, size)
					ctx := context.Background()
					for _, blocking := range []bool{false, true} {
						run := func() {
							var err error
							if size == 1 {
								if blocking {
									err = queue.Push(ctx, 7)
								} else {
									err = queue.TryPush(7)
								}
							} else if blocking {
								_, err = queue.PushBatch(ctx, items)
							} else {
								_, err = queue.TryPushBatch(items)
							}
							if err != nil {
								t.Fatal(err)
							}
							var moved int
							if blocking {
								moved, err = queue.Receive(ctx, dst)
							} else {
								moved, err = queue.TryReceive(dst)
							}
							if moved != size || err != nil {
								t.Fatalf("receive = %d, %v", moved, err)
							}
						}
						assertDiagnosticsAllocs(t, 0, run)
					}
					assertDiagnosticsAllocs(t, 0, func() {
						snapshot := queue.Diagnostics()
						if snapshot.Config.Capacity != engineTestCapacity {
							t.Fatal("snapshot lost capacity")
						}
					})
				})
			}
		}
	}
}

func assertDiagnosticsAllocs(t *testing.T, want float64, run func()) {
	t.Helper()
	// Warm stacks and pools before AllocsPerRun performs its own additional warmup.
	run()
	if got := testing.AllocsPerRun(100, run); got != want {
		t.Fatalf("allocations per run = %g, want %g", got, want)
	}
}

func TestDiagnosticsAllocWaits(t *testing.T) {
	if raceEnabled {
		// The race runtime deliberately drops sync.Pool entries. Those misses are not diagnostics.
		t.Skip("warmed sync.Pool allocation assertion requires a non-race build")
	}
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, engineCase := range engineCases() {
		for _, mode := range diagnosticsModeCases() {
			for _, producer := range []bool{false, true} {
				t.Run(
					fmt.Sprintf("%s/%s/producer=%v", engineCase.name, mode.name, producer),
					func(t *testing.T) {
						options := append([]Option{}, engineCase.options...)
						options = append(
							options,
							WithDiagnostics(mode.config),
							WithConsumerWait(WaitStrategy{}),
							WithProducerWait(WaitStrategy{}),
						)
						queue, _ := newTestEngine(t, options)
						dst := make([]int, 1)
						ctx := context.Background()
						if producer {
							for range engineTestCapacity {
								if err := queue.TryPush(7); err != nil {
									t.Fatal(err)
								}
							}
						}
						start := make(chan struct{})
						done := make(chan error)
						joined := make(chan struct{})
						go func() {
							defer close(joined)
							for range start {
								if producer {
									done <- queue.Push(ctx, 7)
								} else {
									_, err := queue.Receive(ctx, dst)
									done <- err
								}
							}
						}()
						defer func() {
							close(start)
							<-joined
						}()
						run := func() {
							start <- struct{}{}
							side := &queue.consumers
							if producer {
								side = &queue.producers
							}
							for side.load().parked() == 0 {
								runtime.Gosched()
							}
							var err error
							if producer {
								_, err = queue.TryReceive(dst)
							} else {
								err = queue.TryPush(7)
							}
							if err != nil {
								t.Fatal(err)
							}
							if err := <-done; err != nil {
								t.Fatal(err)
							}
						}
						assertDiagnosticsAllocs(t, 0, run)
					},
				)
			}
		}
	}
}

func TestDiagnosticsAllocConsume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, engineCase := range engineCases() {
		for _, mode := range diagnosticsModeCases() {
			t.Run(engineCase.name+"/"+mode.name, func(t *testing.T) {
				options := append([]Option{}, engineCase.options...)
				options = append(options, WithDiagnostics(mode.config))
				queue, _ := newTestEngine(t, options)
				// Consume owns one buffer, one waiter, and the waiter's token channel per call.
				assertDiagnosticsAllocs(t, 3, func() {
					err := queue.Consume(ctx, nil)
					if err != context.Canceled {
						t.Fatalf("Consume = %v, want canceled", err)
					}
				})
			})
		}
	}
}

func TestDiagnosticsAllocConsumeAttempts(t *testing.T) {
	if raceEnabled {
		// fmt.Errorf uses sync.Pool, whose race-mode random drops add unrelated allocations.
		t.Skip("handler error allocation assertion requires a non-race build")
	}
	stop := errors.New("handler stopped")
	ctx := context.Background()
	for _, engineCase := range engineCases() {
		for _, batches := range []int{1, 16} {
			for _, mode := range diagnosticsModeCases() {
				t.Run(fmt.Sprintf("%s/%d/%s", engineCase.name, batches, mode.name), func(t *testing.T) {
					options := append([]Option{}, engineCase.options...)
					options = append(options, WithDiagnostics(mode.config))
					queue, _ := newTestEngine(t, options)
					handled := 0
					handler := func(_ context.Context, batch []int) error {
						handled++
						if len(batch) != 1 {
							t.Fatal("unexpected batch size")
						}
						if handled == batches {
							return stop
						}
						return queue.TryPush(7)
					}
					run := func() {
						handled = 0
						if err := queue.TryPush(7); err != nil {
							t.Fatal(err)
						}
						if err := queue.Consume(ctx, handler); !errors.Is(err, stop) {
							t.Fatalf("Consume = %v, want handler error", err)
						}
					}
					run()
					allocations := testing.AllocsPerRun(100, run)
					// Three owned allocations plus fmt.Errorf's string and wrapped error are fixed per call.
					if allocations != 5 {
						t.Fatalf("Consume allocations = %g, want 5", allocations)
					}
					t.Logf("Consume allocations including buffer, waiter, and handler error = %g", allocations)
				})
			}
		}
	}
}

func TestDiagnosticsAllocStorage(t *testing.T) {
	for _, mode := range []DiagnosticsMode{DiagnosticsCounters, DiagnosticsTiming} {
		for _, shards := range []int{1, 16, 256} {
			config := DiagnosticsConfig{Mode: mode, Shards: shards}
			queue, _ := newTestEngine(t, []Option{WithDiagnostics(config)})
			storage := queue.diagnostics
			want := unsafe.Sizeof(*storage) +
				uintptr(cap(storage.counters))*unsafe.Sizeof(storage.counters[0])
			if mode == DiagnosticsTiming {
				want += uintptr(cap(storage.timing)) * unsafe.Sizeof(storage.timing[0])
			} else if len(storage.timing) != 0 {
				t.Fatal("counter mode allocated timing shards")
			}
			got, ok := diagnosticsAllocationSize(storage.config)
			if !ok || got != want || len(storage.counters) != shards {
				t.Fatalf("storage = %d, %v, want %d with %d shards", got, ok, want, shards)
			}
			t.Logf("mode=%v shards=%d requested bytes=%d", mode, shards, got)
		}
	}
}
