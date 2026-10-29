// Package workq is an in-process work queue. Any number of producers add items, and any number
// of consumers acquire them. Each acquired item goes to one consumer. Both sides work in
// batches. A caller that finds no items or no room spins briefly and then, by default, parks.
//
// The queue is built on the fixed-capacity ring queues in this module, which never wait. In the
// ring/frontier queues, concurrent callers on one side finish in any order, and the other side
// sees a caller's work once every earlier caller on that side has finished. The ring/dpdk queues
// follow DPDK's rte_ring. This package adds blocking calls, parking, shutdown, and backpressure.
//
// # Ownership and completion
//
// A successful push reports acceptance. Acquisition removes work from the queue before
// application processing succeeds. Completion belongs to the application. Handler failure does
// not automatically redeliver acquired items. The queue guarantees neither exactly-once
// processing nor durable delivery. [Queue.PushBatch] and [Queue.TryPushBatch] report the accepted
// prefix even on error, subject to the delivery exception for races with [Queue.Close]. Never
// replay that prefix solely because submission returned an error.
//
// FIFO describes queue acquisition order. Concurrent API calls can return in a different order,
// and consumers can start or finish processing in a different order. Concurrent producers have
// no guaranteed wall-clock order, and a whole batch is not admitted atomically.
//
// Elements are shallow value copies. A producer can reuse its input slice's storage after its
// call returns, but pointers, maps, and slice fields still reference shared payloads. Consumers
// can access accepted work before the producer returns. Submission does not establish a safe
// time to mutate referenced payloads. The application defines ownership and synchronization.
// [Queue.Receive] buffers belong to callers. [Queue.Consume] lends its batch only for the handler
// call. Copying that batch preserves values, not independent payload objects.
//
// [Queue.Close] and closed-and-drained observations do not wait for handlers or certify
// successful processing. A peer may still hold acquired work. Join all owned consumers and
// reconcile their outcomes and accepted work. Joining alone does not prove success. Work started
// asynchronously by handlers needs its own owner and completion boundary. See workq/lifecycle.md
// in the repository for shutdown, outcome attribution, and batch reconciliation recipes.
//
// # Diagnostics
//
// [WithDiagnostics] enables cumulative internal counters or sampled wait timing at construction.
// [Queue.Diagnostics] returns a detached snapshot. Collect each delta series sequentially from
// the same queue lifetime and schema. Live fields need not describe one instant. Disabled
// snapshots report static metadata only, including zero WaitState counts and closed flags.
//
// Diagnostics measure attempts, transfers, waits, wakes, and capacity observations. They do not
// measure application completion or exact queue depth. Close does not freeze recording. Join
// all queue callers before collecting final values and reconcile handler outcomes separately.
// Retain a [DiagnosticsEndpoint] from [Queue.NewDiagnosticsEndpoint] to reuse one recording
// shard across operations. The constructor example shows producer and consumer endpoints with
// queue-owned closure and snapshots. See workq/diagnostics.md in the repository for endpoint
// sharing, tuning, sampling, memory, and collection rules.
//
// # Engine selection
//
// New picks the ring queue, called the engine, from the sides the caller declares.
//
//   - WithSingleProducer and WithSingleConsumer together give dpdk.SPSCQueue.
//   - WithSingleConsumer alone gives frontier.MPSCQueue.
//   - WithSingleProducer alone gives frontier.SPMCQueue.
//   - Neither gives frontier.MPMCQueue, which is safe for any caller.
//
// A declared single side is a promise the library does not check. Breaking it corrupts items.
// A single consumer must serialize Receive, TryReceive, and Consume against each other, and a
// single producer must serialize all producer calls. The default is MPMC for that reason.
//
// # When to use something else
//
// Each item goes to one consumer. If every handler must see every event, or handlers must run
// in a dependency graph, use a library based on the LMAX Disruptor. A Disruptor is also the
// better choice when the workload publishes one event at a time and single-event latency matters
// most. This package is built for batched workloads. For single items without the Disruptor's
// model, a plain MPMC queue, such as the one in xsync, is faster.
//
// # Waiting
//
// A consumer waits when the queue is empty, and a producer waits when it is full. A waiting
// caller goes through three phases, with counts from its side's WaitStrategy.
//
//  1. Spin. It polls up to Spins times without registering anywhere.
//  2. Yield. It registers as a poller and polls up to Yields more times, with runtime.Gosched
//     between polls.
//  3. Park. It parks until the other side wakes it. With NeverPark set, it keeps yielding
//     instead.
//
// WithConsumerWait and WithProducerWait set a WaitStrategy for each side, because the two sides
// wait for different reasons. The waiting side's spinning takes CPU time from the side that
// sets the rate, so the defaults park soon. Producers park sooner than consumers, because a full
// queue usually stays full for a while. A consumer that must react quickly to sparse items may
// want to spin longer.
//
// Consumer wakes target the visible backlog in receive batches, capped by capacity and the
// construction-time GOMAXPROCS. A declared single consumer uses one-at-a-time wakes. This cap
// counts registered pollers, not handlers. Successful acquisition adds no explicit handoff
// yield. Handler order and activation latency remain unspecified.
//
// # Shutdown
//
// Close follows the rule for closing a Go channel. Call it only after every producer call has
// returned. Consumers then receive every pushed item before they see the queue closed. A
// producer call that races Close returns ErrClosedDuringPush, and its items may not be
// delivered.
//
// A pipeline chains queues. A stage's handler calls PushBatch on the next stage's queue with the
// ctx it receives, so the stage's handlers are the next stage's producers. Close the first queue
// after its outside producers return. Wait until every Consume on it has returned, for example
// with a sync.WaitGroup, and only then close the next queue. Closing every queue at once breaks
// the rule for every queue after the first. Example_pipeline shows this order.
//
// # Queues that feed themselves
//
// Crawlers and recursive tasks have handlers that push new items into their own queue. Two rules
// apply.
//
// First, never block or drop in the handler. If every consumer's handler blocks in Push on the
// same full queue, nothing drains it. Dropping an item on ErrFull loses work. Retrying TryPush
// in a loop deadlocks like blocking, and keeping the item for a later call strands it, because
// the handler may never be called again. Instead, process overflow inline. The handler offers
// new items with TryPush or TryPushBatch and puts the items that do not fit on a stack local to
// its call. It processes that stack until it is empty before it returns, and offers new items
// found along the way in the same way.
//
// Second, close from whoever finishes the last work, not from outside. The handlers are
// producers, so the channel rule forbids calling Close while any handler might still push. Keep
// an atomic count of outstanding work.
//
//   - Each item counts from just before its push until a handler finishes it. Items on a
//     handler's local stack still count.
//   - The owner, meaning the code that sets up the queue, holds one count of its own from the
//     start.
//   - Each outside producer, such as a seeder, holds one count for its whole life. The owner
//     adds that count before it starts the producer, and the producer removes it after its last
//     push call returns.
//   - The owner releases its own count only after it has started every outside producer.
//   - Whoever brings the count to zero calls Close.
//
// At zero, no outside producer can push again, and no item is queued, in a handler, or on a
// local stack, so no push can be in flight. The owner's count, and adding each producer's count
// before the producer starts, stop a handler from closing the queue after the first seeder's
// work is done but before a second seeder has counted itself. A handler that fails must still
// finish the counts of the items it holds, or the caller must cancel ctx. Otherwise the count
// never reaches zero, and the other consumers wait until ctx is done. Example_selfFeeding shows
// the pattern.
//
// When a self-feeding queue is a pipeline stage, the handlers of the stage before it are outside
// producers. The owner holds one count on that stage's behalf and releases it after every
// Consume on the upstream queue has returned. A handler that pushes into its own queue makes the
// consumers producers, so such a queue must not use WithSingleProducer.
package workq
