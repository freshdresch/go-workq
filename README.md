# workq

workq is an in-process work queue. Any number of producers add items, and each item goes to
exactly one of any number of consumers.

## Example

This program adds 100 numbers to a queue in one `PushBatch` call. Two consumers each double the
numbers they receive and add them to a total. The queue holds 16 items, so `PushBatch` waits
whenever the queue is full.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/freshdresch/go-data-structures/workq"
)

func main() {
	queue, err := workq.New[int](16)
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var total atomic.Int64
	handler := func(_ context.Context, batch []int) error {
		for i := range batch {
			total.Add(int64(batch[i] * 2))
		}
		return nil
	}

	const consumerCount = 2
	consumeErrs := make([]error, consumerCount)
	var consumers sync.WaitGroup
	for i := range consumerCount {
		consumers.Go(func() {
			consumeErrs[i] = queue.Consume(ctx, handler)
			if consumeErrs[i] != nil {
				cancel()
			}
		})
	}

	items := make([]int, 100)
	for i := range items {
		items[i] = i + 1
	}
	_, pushErr := queue.PushBatch(ctx, items)
	if pushErr != nil {
		cancel()
	}

	// Close after every producer call has returned. The consumers then drain the queue.
	queue.Close()
	consumers.Wait()

	errs := append([]error{pushErr}, consumeErrs...)
	err = errors.Join(errs...)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("total:", total.Load())
}
```

Output:

```text
total: 10100
```

## Choosing sides

`New` picks the engine from the sides that the caller declares. The engine is the ring queue
under the package.

The engines come from [`ring/frontier`](../ring/README.md), whose concurrent callers on one side
finish in any order without waiting for one another, and
[`ring/dpdk`](../ring/dpdk/README.md), modeled on DPDK's `rte_ring`. SPSC means a single producer
and a single consumer, and MPMC means multiple producers and multiple consumers. MPSC and SPMC mix
the two.

| Declared | Engine |
|---|---|
| `WithSingleProducer` and `WithSingleConsumer` | `dpdk.SPSCQueue` |
| `WithSingleConsumer` only | `frontier.MPSCQueue` |
| `WithSingleProducer` only | `frontier.SPMCQueue` |
| Neither | `frontier.MPMCQueue` |

A declared single side is a promise the library cannot check. Breaking it corrupts items, as
with the ring queues. The default is therefore MPMC, the only engine that is safe for any
caller. `Receive`, `TryReceive`, and `Consume` all belong to the consumer side, so a single
consumer must serialize them against each other. The same rule applies to the producer calls,
so a single producer must serialize all of them.

Measured costs from the [ring comparison reports](../benchmark/README.md), in ns per `int64`
element:

| Topology | Single items | Batch 32 |
|---|---:|---:|
| SPSC, `dpdk.SPSCQueue` | about 11 | 1.4 |
| MPMC, 4 producers and 4 consumers, `frontier.MPMCQueue` | 24.4 | 2.01 |
| MPSC, 4 producers and 1 consumer, `frontier.MPSCQueue` | 31.4 | 1.9 |

Single items use the faster of `Enqueue` and a one-element burst in the comparison reports:
burst for MPMC, and `Enqueue` for MPSC.

`WithMaxReceiveBatch` bounds how many items one `Receive` or handler call gets, and
`WithMaxPushBatch` bounds each chunk a push hands to the engine. Both must be in [1, capacity].
Each default is the smaller of 32 and the capacity, a provisional value until the benchmarks
compare 16, 32, 64, and 128.

## Waiting

A waiting caller goes through three phases. The side's `WaitStrategy` sets the counts for the
first two phases.

1. **Spin.** It polls up to `Spins` times without registering anywhere. It checks `ctx` with a
   non-blocking `select` on `ctx.Done()` before the first poll and every 64 polls after that.
2. **Yield.** It registers so the other side knows it is checking, and it polls up to `Yields`
   times. It calls
   `runtime.Gosched` between polls, not before the first one, so a consumer woken from a park
   polls at once. It checks `ctx` after each poll. A caller woken from a park always gets at
   least one poll in this phase, even when `Yields` is 0.
3. **Park.** Unless `NeverPark` is set, it parks. With `NeverPark`, it keeps yielding and
   checking `ctx`, and it stays registered for the whole wait. The other side then
   never takes a lock to wake it.

```go
type WaitStrategy struct {
    Spins     int  // empty polls before the first yield
    Yields    int  // polls with runtime.Gosched between them, after the spins
    NeverPark bool // keep yielding after the yields instead of parking
}
```

When consumers are parked, publication and registered-consumer cleanup can wake a group sized
for the visible backlog and receive batch limit. The group targets at most the smaller of
capacity and the construction-time `GOMAXPROCS`. A declared single consumer retains one-at-a-time
wakes. These targets count registered pollers, not handlers, and promise neither handler order
nor activation latency. Successful acquisition adds no explicit handoff yield.

Wake groups make consumers runnable, but the Go scheduler decides when they execute. When
other goroutines occupy the available CPUs, some consumers can start too late to participate
in a burst. More participating consumers do not necessarily mean faster burst completion.

The queue does not explicitly yield after acquiring a batch to give another consumer a turn.
This avoids suspending that consumer with a batch already acquired. Under contention, the
tradeoff can be slower burst completion and more CPU progress for competing goroutines than
with an explicit handoff yield. For latency-sensitive workloads, measure burst completion
with the CPU contention expected in deployment. Wake groups do not reserve CPU capacity or
guarantee prompt handler execution.

`New` rejects a negative `Spins` or `Yields` with an error that wraps `ErrInvalidWaitStrategy`.
Zero is valid for both. The zero value, `WaitStrategy{}`, parks at once after the first empty
poll. It never busy-waits, so a caller who passes it by mistake gets slow wakes, not a spinning
CPU.

`New` reads `runtime.GOMAXPROCS(0)` once. With `GOMAXPROCS` at 1, spinning cannot help, so the
queue treats `Spins` as 0, as `sync.Mutex` does. Reading it on every wait is not an option,
because `runtime.GOMAXPROCS` takes the global scheduler lock even when it only reads. A program
that changes `GOMAXPROCS` after `New` keeps the old choice. Since Go 1.25 the runtime also
changes `GOMAXPROCS` on its own when a container's CPU limit changes, so the choice can go stale
without the program doing anything. That affects spinning and the consumer wake cap, never
correctness.

Each side has its own strategy, set with `WithConsumerWait` and `WithProducerWait`. Consumers
wait when the queue is empty, which is the steady state when producers are the bottleneck.
Producers wait when the queue is full, which is the steady state when consumers are the
bottleneck. In either case, the waiting side's spinning takes CPU time from the side that sets
the rate, so the waiting side should usually park soon. The exception is a consumer that must
react quickly to sparse items, which may want to spin longer.

The proposed defaults, to be checked by the latency measurement, are:

| Side | Spins | Yields | NeverPark |
|---|---:|---:|---|
| Consumer | 64 | 16 | false |
| Producer | 16 | 4 | false |

Producers park sooner because a full queue usually stays full for a while, since consumers drain
it at their own rate. An empty queue in a latency-sensitive service is more often a short gap.

## Shutdown

The [lifecycle recipes](lifecycle.md) separate graceful source stop from immediate abort.
Graceful stop requests an end to admission while consumers drain already admitted work.
An operational failure or explicit abort cancels the shared worker context. Both paths join
every owned goroutine.

`Close` follows the rule for closing a Go channel. Call `Close` only after every producer call
has returned. The consumers then receive every pushed item before they see the queue closed.

Unlike a Go channel, `Close` does not panic. Every producer call made after `Close` returns
`ErrClosed` and pushes nothing. A second `Close` does nothing.

A producer call that races with `Close` breaks the rule. It does not panic. When the library
detects the race, the call returns `ErrClosedDuringPush`, and its items may not be delivered. A
nil error means delivery only when no producer call overlaps `Close` and the consumers keep
receiving until the queue is closed and drained. If every `Consume` returns early, through a
handler error or `ctx`, nothing drains the queue. A producer that waits after every consumer has
returned, without a `Close`, waits until its `ctx` is done. Pass a `ctx` that the owner cancels
when the consumers stop. Running producers and consumers in one `errgroup` does this.

### Pipelines

A pipeline chains queues. A stage's handler calls `PushBatch` on the next stage's queue with the
`ctx` it receives, so the stage's handlers are the next stage's producers. Close the first queue
after its outside producers return. Wait until every `Consume` on that queue has returned, for
example with a `sync.WaitGroup`. Only then close the next queue. Closing every queue at once
breaks the rule for every queue after the first. [`Example_pipeline`](example_test.go) shows
this order. The [pipeline recipe](lifecycle.md#pipelines) covers failure attribution and partial
forwarding.

### Self-feeding queues

Crawlers and recursive tasks have handlers that push new items into their own queue. Two rules
apply.

1. **Never block or drop in the handler.** If every consumer's handler blocks in `Push` on the
   same full queue, nothing drains it. Dropping an item on `ErrFull` loses work. Retrying
   `TryPush` in a loop deadlocks, as blocking does. Keeping the item for a later call strands
   it, because the handler may never be called again. Instead, the handler offers new items with
   `TryPush` or `TryPushBatch`. Items that do not fit go on a stack local to the handler call.
   The handler processes that stack until it is empty before it returns. It offers new items
   found along the way in the same way.
2. **Close from the last handler, not from outside.** The handlers are producers, so the channel
   rule forbids calling `Close` while any handler might still push. Keep an atomic count of
   outstanding work:
   - Each item counts from just before its push until a handler finishes it. Items on a
     handler's local stack still count.
   - The owner, meaning the code that sets up the queue, holds one count of its own from the
     start.
   - Each outside producer, such as a seeder, holds one count for its whole life. The owner adds
     that count before it starts the producer. The producer removes it only after its last push
     call returns.
   - The owner releases its own count only after it has started every outside producer.
   - Whoever brings the count to zero calls `Close`. That can be a handler that finishes the last
     item, an outside producer giving up its count, or the owner.

At zero, no outside producer can push again, and no item is queued, in a handler, or on a local
stack. So no push is in flight, and the channel rule holds. The owner's count, and adding each
producer's count before it starts, prevent the mistake that `sync.WaitGroup` warns about.
Without them, a handler could finish the first seeder's last item and call `Close` before a
second seeder had counted itself.

A handler or seeder failure records its cause and cancels the shared worker context. Join all
seeders and consumers, then close for cleanup. Do not decrement failed or abandoned items to
force the count to zero. The [self-feeding recipe](lifecycle.md#self-feeding-queues) demonstrates
abort with a positive outstanding count.

When a self-feeding queue is a pipeline stage, the handlers of the stage before it are outside
producers. The owner holds one count on that stage's behalf. It releases that count after every
`Consume` on the upstream queue has returned.

A handler that pushes into its own queue makes the consumers producers. A queue built with
`WithSingleProducer` then has more than one producer, which breaks the declaration. Such a
queue needs the default MPMC engine, or MPSC when it has one consumer.
[`Example_selfFeeding`](example_test.go) shows the pattern.

## When to use something else

A Disruptor-based library is the better choice in two cases. The first is when every handler must
see every event. In the LMAX Disruptor, every handler sees every event, and handlers can be gated
on each other to form a dependency graph. This library does not offer either one. The second is
when the workload publishes one event at a time and single-event latency matters most. In
unpublished smoke runs against `smarty/go-disruptor` v0.5.0, the Disruptor was about 10 ns faster
per single event, probably because it claims slots with an atomic add. This library aims at
batched workloads and does not try to close that gap.

For single items without the Disruptor's model, a plain MPMC queue is also faster, such as the
one in xsync, a Go concurrency library. In the [MPMC comparison report](../benchmark/ring/mpmc.md),
xsync moved a single `int64` in 18.0 ns. `frontier.MPMCQueue` took 24.4 ns with one-element bursts
and 30.8 ns with `Enqueue` and `Dequeue`, before this library adds its own cost.
