// Polling adapter support extracted from go-data-structures. See ORIGIN.md.
package benchmark

import (
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
	"github.com/freshdresch/go-data-structures/ring/frontier"
)

// queue is the shape the harness drives. Each method moves as many elements as it can right
// now, without blocking, and returns how many it moved.
type queue[T any] interface {
	enqueue(items []T) int
	dequeue(dst []T) int
}

// consume dequeues from source into dst until the producers are done and source is empty, or
// until the run aborts, passes each dequeued batch to recorder, and returns how many elements it
// received. dst must not be empty. It yields after every attempt that moved nothing.
//
// Important: it loads producersDone before each dequeue attempt, and returns only when the flag
// was already set before an attempt that moved nothing. The reverse order, dequeue and then load
// the flag, can return while a producer's last elements are still in flight. The dequeue finds the
// queue empty, then the producer enqueues its last elements and finishes, and then the flag load
// sees the flag set.
func consume[T any, S sink[T]](
	source queue[T],
	state *runState,
	producersDone *atomic.Bool,
	dst []T,
	recorder S,
) int {
	received := 0
	for {
		done := producersDone.Load()
		if state.aborted() {
			return received
		}
		moved := source.dequeue(dst)
		if moved > 0 {
			recorder.record(dst[:moved])
			received += moved
			continue
		}
		if done {
			return received
		}
		runtime.Gosched()
	}
}

// enqueueAll enqueues every element of items on target, retrying the unsent remainder and
// yielding whenever an attempt moves nothing. A full queue is normal backpressure, so it retries.
// It returns early when the run aborts, because an aborted run's queue may never drain.
func enqueueAll[T any](target queue[T], state *runState, items []T) {
	for unsent := items; len(unsent) > 0; {
		if state.aborted() {
			return
		}
		moved := target.enqueue(unsent)
		if moved == 0 {
			runtime.Gosched()
		}
		unsent = unsent[moved:]
	}
}

type spscBurstQueue[T any] struct {
	ring *dpdk.SPSCQueue[T]
}

func (q *spscBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *spscBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type mpscBurstQueue[T any] struct {
	ring *dpdk.MPSCQueue[T]
}

func (q *mpscBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *mpscBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type spmcBurstQueue[T any] struct {
	ring *dpdk.SPMCQueue[T]
}

func (q *spmcBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *spmcBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type mpmcBurstQueue[T any] struct {
	ring *dpdk.MPMCQueue[T]
}

func (q *mpmcBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *mpmcBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

// newModeRing chooses and wraps a concrete public queue before timing starts.
// Each adapter calls its concrete queue directly so ring rows have one dispatch boundary,
// like the third-party adapters. Concurrent sides default to ordered publication.
func newModeRing[T any](
	mode benchMode,
	state *runState,
	single bool,
	extra ...dpdk.Option,
) (queue[T], error) {
	var options []dpdk.Option
	if mode.relaxed {
		if mode.producers > 1 {
			options = append(options, dpdk.WithRelaxedMultiProdEnqueue())
		}
		if mode.consumers > 1 {
			options = append(options, dpdk.WithRelaxedMultiConsDequeue())
		}
	}
	options = append(options, extra...)
	switch {
	case mode.producers > 1 && mode.consumers > 1:
		created, err := dpdk.NewMPMCQueue[T](ringCapacity, options...)
		if err != nil {
			return nil, err
		}
		if single {
			return &mpmcSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &mpmcBurstQueue[T]{ring: created}, nil
	case mode.producers > 1:
		created, err := dpdk.NewMPSCQueue[T](ringCapacity, options...)
		if err != nil {
			return nil, err
		}
		if single {
			return &mpscSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &mpscBurstQueue[T]{ring: created}, nil
	case mode.consumers > 1:
		created, err := dpdk.NewSPMCQueue[T](ringCapacity, options...)
		if err != nil {
			return nil, err
		}
		if single {
			return &spmcSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &spmcBurstQueue[T]{ring: created}, nil
	default:
		created, err := dpdk.NewSPSCQueue[T](ringCapacity, options...)
		if err != nil {
			return nil, err
		}
		if single {
			return &spscSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &spscBurstQueue[T]{ring: created}, nil
	}
}

// newModeFrontier builds the ring/frontier queue for mode before timing starts, wrapped like
// newModeRing's adapters. It supports only the modes in frontierModes.
func newModeFrontier[T any](mode benchMode, state *runState, single bool) (queue[T], error) {
	switch {
	case mode.producers > 1 && mode.consumers == 1:
		created, err := frontier.NewMPSCQueue[T](ringCapacity)
		if err != nil {
			return nil, err
		}
		if single {
			return &frontierMPSCSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &frontierMPSCBurstQueue[T]{ring: created}, nil
	case mode.producers == 1 && mode.consumers > 1:
		created, err := frontier.NewSPMCQueue[T](ringCapacity)
		if err != nil {
			return nil, err
		}
		if single {
			return &frontierSPMCSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &frontierSPMCBurstQueue[T]{ring: created}, nil
	case mode.producers > 1 && mode.consumers > 1:
		created, err := frontier.NewMPMCQueue[T](ringCapacity)
		if err != nil {
			return nil, err
		}
		if single {
			return &frontierMPMCSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &frontierMPMCBurstQueue[T]{ring: created}, nil
	default:
		return nil, fmt.Errorf("frontier has no queue for mode %s", mode.name)
	}
}

type frontierSPMCBurstQueue[T any] struct {
	ring *frontier.SPMCQueue[T]
}

func (q *frontierSPMCBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *frontierSPMCBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type frontierSPMCSingleQueue[T any] struct {
	ringSingleQueue
	ring *frontier.SPMCQueue[T]
}

func (q *frontierSPMCSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), frontier.ErrFull)
}

func (q *frontierSPMCSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, frontier.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

type frontierMPMCBurstQueue[T any] struct {
	ring *frontier.MPMCQueue[T]
}

func (q *frontierMPMCBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *frontierMPMCBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type frontierMPMCSingleQueue[T any] struct {
	ringSingleQueue
	ring *frontier.MPMCQueue[T]
}

func (q *frontierMPMCSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), frontier.ErrFull)
}

func (q *frontierMPMCSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, frontier.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

type frontierMPSCBurstQueue[T any] struct {
	ring *frontier.MPSCQueue[T]
}

func (q *frontierMPSCBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *frontierMPSCBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type frontierMPSCSingleQueue[T any] struct {
	ringSingleQueue
	ring *frontier.MPSCQueue[T]
}

func (q *frontierMPSCSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), frontier.ErrFull)
}

func (q *frontierMPSCSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, frontier.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

// ringSingleQueue shares run error handling without adding queue-method dispatch.
type ringSingleQueue struct {
	state *runState
}

type spscSingleQueue[T any] struct {
	ringSingleQueue
	ring *dpdk.SPSCQueue[T]
}

func (q *spscSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), dpdk.ErrFull)
}

func (q *spscSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, dpdk.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

type mpscSingleQueue[T any] struct {
	ringSingleQueue
	ring *dpdk.MPSCQueue[T]
}

func (q *mpscSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), dpdk.ErrFull)
}

func (q *mpscSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, dpdk.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

type spmcSingleQueue[T any] struct {
	ringSingleQueue
	ring *dpdk.SPMCQueue[T]
}

func (q *spmcSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), dpdk.ErrFull)
}

func (q *spmcSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, dpdk.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

type mpmcSingleQueue[T any] struct {
	ringSingleQueue
	ring *dpdk.MPMCQueue[T]
}

func (q *mpmcSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), dpdk.ErrFull)
}

func (q *mpmcSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, dpdk.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

// movedOrFail returns 1 when err is nil, and 0 when err is the expected error. Any other error
// fails the run, and it returns 0. It compares with == because the ring returns ErrFull and
// ErrEmpty unwrapped.
func (q *ringSingleQueue) movedOrFail(err, expected error) int {
	if err == nil {
		return 1
	}
	if err == expected {
		return 0
	}
	q.state.fail(err)
	return 0
}
