// Shared test support extracted from go-data-structures. See ORIGIN.md.
package benchmark

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// ringCapacity is the number of slots every implementation is built with, and the number of
// elements each holds.
const ringCapacity = 1024

// multiWorkers is the number of goroutines on each multi side of a mode.
const multiWorkers = 4

// seqBits is how many low bits of a packed int64 element hold the sequence number. The producer
// ID is in the bits above them.
const seqBits = 56

// cacheLine is the Apple silicon cache line size, which also covers a 64-byte line and its
// prefetched neighbor on x86.
const cacheLine = 128

// sink receives each batch a consumer dequeues. The batch is only valid during the call, because
// the consumer reuses its buffer.
type sink[T any] interface {
	record(batch []T)
}

// recordSink keeps a copy of every element it receives, so tests can check what was dequeued.
type recordSink[T any] struct {
	items []T
}

func (r *recordSink[T]) record(batch []T) {
	r.items = append(r.items, batch...)
}

// item is what the ptr element type points to.
type item struct {
	value int64
}

// packInt64 combines a producer ID and a sequence number into one element.
func packInt64(producer int, seq int64) int64 {
	return int64(producer)<<seqBits | seq
}

// unpackInt64 returns the producer ID and sequence number packInt64 combined into v.
func unpackInt64(v int64) (producer int, seq int64) {
	return int(v >> seqBits), v & (1<<seqBits - 1)
}

func fillInt64(buf []int64, producer int, nextSeq int64) {
	for k := range buf {
		buf[k] = packInt64(producer, nextSeq+int64(k))
	}
}

// producerShare returns how many of total elements producer sends when they are split across
// producers as evenly as possible. The first total % producers producers send one more.
func producerShare(total, producers, producer int) int {
	share := total / producers
	if producer < total%producers {
		share++
	}
	return share
}

// benchTimer is the part of *testing.B the runs use, so tests can run them without a benchmark.
type benchTimer interface {
	ResetTimer()
	StopTimer()
}

// runTimed runs work as the timed region of a run. It collects garbage first, so no collection
// left over from setup runs inside the timed region.
func runTimed(timer benchTimer, work func()) {
	runtime.GC()
	timer.ResetTimer()
	work()
	timer.StopTimer()
}

// newBuffers isolates occupied worker buffers with at least 128 bytes of exterior guards on
// each side. Separate small allocations alone can share a cache line with neighboring objects:
// at batch 1, every producer buffer and the consumer's destination packed onto one 64-byte line,
// which made single-element results depend on allocation placement.
func newBuffers[T any](count, size int) [][]T {
	const benchmarkBufferGuardBytes = 128
	buffers := make([][]T, count)
	for i := range buffers {
		guard := 0
		elementSize := unsafe.Sizeof(*new(T))
		if size > 0 && elementSize > 0 {
			guard = int(1 + (benchmarkBufferGuardBytes-1)/elementSize)
		}
		backing := make([]T, guard+size+guard)
		buffers[i] = backing[guard : guard+size : guard+size]
	}
	return buffers
}

// benchMode is how many producers and consumers a run has.
type benchMode struct {
	name      string
	producers int
	consumers int
	// relaxed is whether the mode's multi sides use relaxed tail sync.
	relaxed bool
}

// The named modes below use multiWorkers goroutines on each multi side.
var (
	// spscMode has one producer and one consumer.
	spscMode = benchMode{name: "SPSC", producers: 1, consumers: 1}
	// mpscMode has many producers and one consumer.
	mpscMode = benchMode{name: "MPSC", producers: multiWorkers, consumers: 1}
	// spmcMode has one producer and many consumers.
	spmcMode = benchMode{name: "SPMC", producers: 1, consumers: multiWorkers}
	// mpmcMode has many producers and many consumers.
	mpmcMode = benchMode{name: "MPMC", producers: multiWorkers, consumers: multiWorkers}
)

// benchModes lists every mode, in the order the throughput benchmarks run them.
var benchModes = []benchMode{spscMode, mpscMode, spmcMode, mpmcMode}

// noopTimer lets tests call the runs without a benchmark timer.
type noopTimer struct{}

func (noopTimer) ResetTimer() {}

func (noopTimer) StopTimer() {}

// accountingError checks what the consumers recorded when producers sent total elements, and
// returns an error describing the first problem it finds. Every (producer, sequence) pair must
// be received exactly once, and within each consumer each producer's sequence numbers must
// strictly increase.
func accountingError(sinks []*recordSink[int64], producers, total int) error {
	seen := make([][]bool, producers)
	for p := range seen {
		seen[p] = make([]bool, producerShare(total, producers, p))
	}
	received := 0
	for c, recorded := range sinks {
		lastSeq := make([]int64, producers)
		for p := range lastSeq {
			lastSeq[p] = -1
		}
		for _, v := range recorded.items {
			producer, seq := unpackInt64(v)
			if producer < 0 || producer >= producers || seq >= int64(len(seen[producer])) {
				return fmt.Errorf(
					"consumer %d received producer %d sequence %d, never sent",
					c,
					producer,
					seq,
				)
			}
			if seen[producer][seq] {
				return fmt.Errorf(
					"consumer %d received producer %d sequence %d again",
					c,
					producer,
					seq,
				)
			}
			if seq <= lastSeq[producer] {
				return fmt.Errorf(
					"consumer %d received producer %d sequence %d after %d",
					c, producer, seq, lastSeq[producer],
				)
			}
			seen[producer][seq] = true
			lastSeq[producer] = seq
			received++
		}
	}
	if received != total {
		return fmt.Errorf("received %d elements, want %d", received, total)
	}
	return nil
}

// accountingDeadline bounds one accounting run, so a stop-order regression fails fast with the
// case named instead of waiting for the global test timeout.
const accountingDeadline = 30 * time.Second

// runWithin calls run in its own goroutine and returns its result. When run does not return
// within accountingDeadline, it fails the test with a message that names stuck. When run returns
// an error, it fails the test with that error.
func runWithin[R any](t *testing.T, stuck string, run func() (R, error)) R {
	t.Helper()
	type outcome struct {
		result R
		err    error
	}
	outcomes := make(chan outcome, 1)
	go func() {
		result, err := run()
		outcomes <- outcome{result: result, err: err}
	}()
	var done outcome
	select {
	case done = <-outcomes:
	case <-time.After(accountingDeadline):
		// The test is failing anyway, so the stuck goroutine is abandoned, not joined.
		t.Fatalf("%s: run did not finish within %v", stuck, accountingDeadline)
	}
	if done.err != nil {
		t.Fatalf("run failed: %v", done.err)
	}
	return done.result
}

// runState holds the abort flag and the first error for one benchmark or accounting run. Every
// producer and consumer checks the flag, so an adapter error ends the run instead of hanging it.
// The flag has a cache line to itself, so those checks never contend with other run state.
type runState struct {
	_       [cacheLine]byte
	abort   atomic.Bool
	_       [cacheLine - unsafe.Sizeof(atomic.Bool{})]byte
	err     atomic.Pointer[error]
	onAbort func()
}

// newRunState returns a run that calls onAbort, when it is not nil, the first time it fails.
func newRunState(onAbort func()) *runState {
	return &runState{onAbort: onAbort}
}

// fail records err and aborts the run, unless the run has already failed. Only the first call
// stores its error and runs the abort hook, so later calls do nothing.
func (s *runState) fail(err error) {
	if !s.err.CompareAndSwap(nil, &err) {
		return
	}
	s.abort.Store(true)
	if s.onAbort != nil {
		s.onAbort()
	}
}

// aborted reports whether the run has failed.
func (s *runState) aborted() bool {
	return s.abort.Load()
}

// firstError returns the error the run failed with, or nil if it has not failed.
func (s *runState) firstError() error {
	stored := s.err.Load()
	if stored == nil {
		return nil
	}
	return *stored
}

func TestRunFail(t *testing.T) {
	errA := errors.New("first failure")
	errB := errors.New("second failure")
	hookCalls := 0
	state := newRunState(func() { hookCalls++ })

	state.fail(errA)
	state.fail(errB)

	if !state.aborted() {
		t.Errorf("aborted() = false after fail, want true")
	}
	if !errors.Is(state.firstError(), errA) {
		t.Errorf("firstError() = %v, want %v", state.firstError(), errA)
	}
	if hookCalls != 1 {
		t.Errorf("abort hook ran %d times, want 1", hookCalls)
	}
}

func TestRunLayout(t *testing.T) {
	var state runState
	abortOffset := unsafe.Offsetof(state.abort)
	errOffset := unsafe.Offsetof(state.err)
	if abortOffset < cacheLine {
		t.Errorf("abort is at offset %d, want at least %d", abortOffset, cacheLine)
	}
	if errOffset-abortOffset < cacheLine {
		t.Errorf("err is %d bytes after abort, want at least %d", errOffset-abortOffset, cacheLine)
	}
}
