# Wakeup protocol

`workq` lets a caller wait for items or free slots while keeping track of who will check for
progress before everyone goes to sleep.

This document is for maintainers and contributors changing waiting, wake selection, or an engine.
For queue use, see the [README](README.md). The explanation follows the current source.

## How waiting works

The ring queue, called the **engine**, stores items and reports available items or free slots. A
**publication** records a completed enqueue. A **release** records that consumed slots can be
reused. After either operation, the caller checks whether the opposite side needs waking. A wake
tells a caller to try again. It does not reserve an item or a slot.

Each direction has a `waitSide` in [waitside.go](waitside.go). The consumer side tracks
callers waiting for items. The producer side tracks callers waiting for room. Each side has counts
of **pollers** and **parked** callers, plus a mutex-protected list of **waiters**. A waiter
contains list links and a channel that holds one **wake token**. The queue's separate `done`
channel broadcasts closure to both sides.

Here, a poller is a caller registered to keep checking, or to arrange another check when it
leaves. It need not be running. A waker registers a selected caller before sending its token. A
parked caller has joined the wait list, but can still be making its last attempt before it blocks.
These names describe protocol state, not the Go scheduler's state.

The ordinary consumer path is below. Exit paths for cancellation and closure are described later.
`NeverPark` stays in the registered polling state instead of joining the list.

```text
receive / spinForItems
  | no items after unregistered attempts
  v
register -> yieldForItems                 <-------------------------------+
  | no items, parking enabled                                            |
  v                                                                      |
enlist: pollers -1, parked +1                                             |
  |                                                                      |
  v                                                                      |
extra dequeue attempt                                                    |
  | no items                                                             |
  v                                                                      |
wait on token / ctx.Done() / done                                         |
  | token received                                                       |
  +----------------------------------------------------------------------+

producer publishes items -> wakeConsumersAfterPublication
  -> wakeOne or wakeGroup, when needed
  -> transfer parked to pollers, unlink selected waiters, send tokens

ordinary successful registered receive -> check for waiting producers
  -> release consumer registration, then check for remaining items
  -> return batch (Consume calls its handler after this cleanup)
```

Follow one consumer through [receive.go](receive.go), [wait.go](wait.go), and
[consumer_wake.go](consumer_wake.go):

1. `receive` tries the engine immediately. `spinForItems` can try again without registering.
   These attempts avoid wait bookkeeping when an item is available. Success checks for waiting
   producers because removing items releases room.
2. `receiveWait` calls `register` before the yield phase. The count now tells producers that a
   consumer will check again. If that consumer stops, its cleanup must check whether another
   consumer needs a wake. Otherwise a producer could rely on a caller that has already left.
3. If the yield phase finds nothing, `parkForItems` calls `enlist` under the side mutex. This
   moves the caller from the poller count to the parked count and adds its waiter to the list.
   If the side is closed, enlist changes nothing and the caller cleans up its registration.
4. After joining the list, `parkForItems` tries the engine once more. Suppose an item arrived
   after the previous empty attempt but before enlist. The producer could have seen no parked
   caller to wake. This extra attempt finds that item instead of sleeping past it.
5. If the extra attempt finds nothing, the caller waits for its token, cancellation, or `done`.
   A producer's wake check can select it. `wakeOneLocked` or `wakeGroup` first transfers its
   count back to pollers, then unlinks it and sends one token. Another producer can therefore
   see that someone will retry even before the recipient runs.
6. After receiving the token, `yieldForItems` permits at least one registered iteration, even
   when configured yields are zero. An ordinary successful attempt checks for waiting producers
   and then calls `leaveConsumerPolling`. That cleanup drops its registration before checking
   whether remaining items need another consumer. Checking first would leave a gap in which a
   producer could rely on the departing registration. If the registered consumer observes closure,
   `leavePollingAndDrain` first runs that cleanup, then calls `drainCheck` to attempt a dequeue.
   If the drain moves items, it checks for waiting producers. Both paths return after cleanup.

Three details matter when changing this path:

- A token grants another attempt, not ownership of work. A competing consumer can take the item
  first. The recipient must retry or clean up the registration that the waker gave it.
- Cancellation can race with selection. `leaveListWith` checks membership under the mutex. A
  selected caller consumes its token and releases its registration even if cancellation won the
  caller's `select`.
- A zero-length engine call moves nothing, but it can refresh internal state. It is still an
  engine operation with concurrency restrictions, as the engine assumptions below explain.

The producer path follows the same register, enlist, extra-attempt, and cleanup sequence in
`pushWait`. Its selection policy differs: it wakes one producer when no producer is registered,
and unsuccessful woken producers rejoin the oldest end during that wait. The policy does not
reserve capacity or guarantee fairness.

## Consumer wake groups

Waking a group lets several consumers try available batches.
[consumer_wake.go](consumer_wake.go) estimates how many batches are ready.
The target uses a cap chosen by `newQueue` in [queue.go](queue.go). Let `B=maxReceiveBatch` and let
`C` be `max(1,min(GOMAXPROCS at construction,capacity))`, with `C=1` for a declared single
consumer. For fresh available count `A`, `T(0)=0` and `T(A)=min(C,1+(A-1)/B)` for positive `A`.
For example, 17 available items with a receive batch limit of 8 and a cap of 4 give a target of 3
pollers. Existing pollers count toward that target. The formula uses the configured batch limit,
even if callers supply smaller receive buffers.

`wakeConsumersAfterPublication` retains its first post-publication closed observation. It skips
the availability check when nobody is parked or the sampled poller count is already at the cap.
For `C=1`, it uses the zero-poller one-wake primitive without a publication peek. Otherwise its
publication target is `max(1,T(A))`. That floor preserves the wake obligation even when visibility
is zero or another consumer removed the sampled work.

Consumer cleanup first drops accounting and then samples availability. It uses `T(A)` without the
publication floor. It can replenish while pollers remain. Successful unregistered receives and
spin successes do not acquire a new consumer cleanup obligation. A successful extra attempt by a
still-listed caller removes itself before replenishment. A still-listed cancellation or close exit
removes only itself.

Under the mutex, `wakeGroup(t)` reads the poller count `P` and parked count `W`. It chooses
`k=min(W,t-P)` when `P<t` and `W>0`. A compare-and-swap (CAS) commits the transfer only if the
state word still matches the value read. CAS retries retain `t` and recompute `k`. One successful
CAS transfers the whole group before the first token is sent. Early recipients may receive their
tokens and leave polling before later recipients get theirs. The invocation still completes its
original `k` sends and never refills the group.

The cap bounds wake-driven transfers toward a target at their transfer instant. It does not bound
independent registration, handlers, runnable consumers, or work in flight. Small receive buffers
and expensive handlers do not change the target. Stale samples can cause unnecessary wakes or less
parallelism than a newer sample would suggest.

## Producer retry priority

The producer side keeps the zero-poller one-wake policy in [waitside.go](waitside.go).
`pushWait` and `parkForRoom` in [wait.go](wait.go) own the retry priority.
A `pushWait` invocation initially parks at newest. Once it has received a token, unsuccessful
retries join oldest until that invocation
makes any progress or returns an error. Partial progress ends that wait episode. A later
`pushWait`, even in the same `PushBatch`, starts at newest. Oldest insertion restores priority
ahead of still-parked callers, not an original FIFO rank among retries. Unregistered producers can
take room first. No capacity reservation, anti-barging guarantee, bounded wait, or starvation
freedom follows from this policy.

## Cancellation and close

Cancellation and a successful extra attempt both need to leave the list. If cleanup finds the
caller listed while holding the mutex, it removes itself without receiving a token. If a waker
selected it first, cleanup waits for the side mutex, then receives the token and releases the
acquired poller registration. Pool return occurs only after this cleanup. A `select` choice alone
does not tell the caller which accounting state it owns.

`Close` sets the consumer closed bit and closes `done` under the consumer mutex. It releases that
mutex before setting the producer closed bit under the producer mutex. Repeated Close calls do not
close `done` twice. The current implementation still visits the producer side on a repeated call.
Enlist either precedes closure on its side or sees that side closed and retains its polling
obligation for cleanup. During the producer publication gap, a producer can enlist before its side
bit is set, but `done` is already closed.

Close does not clear lists or revoke tokens. Wakes can occur after a closed bit is set. A group
before Close finishes its sends under the mutex, and a later group can still select waiters that
have not cleaned up. All paths retain the same token lifecycle.

An empty receive attempt before observing closure is insufficient to establish drained state. The
drain attempt follows the load that observed closure. The delivery guarantee requires all producer
calls to finish before Close and consumers to continue draining. Racing producer reservations can
invalidate delivery, including otherwise successful pushes.

Error precedence depends on what the caller observes. A canceled waiting producer checks the
consumer closed bit after cleanup through `producerContextError`, including the gap before the
producer bit is set. Closure published after that observation need not override the context error.
Consumer wait exits check closure at their existing boundaries. `Consume` also has its distinct
context check before a new receive. Do not turn the public shorthand “closed wins” into a claim of
one atomic decision spanning cancellation, closure, cleanup, and return.

## State and invariants

The `sideWord` atomic word stores pollers in bits 0–30, parked callers in bits 32–62, and closed
in bit 63. Each side also owns a mutex, doubly linked list, and waiter pool. The introductory
walkthrough defines what the counts mean. They do not count handlers or scheduler activity.

The stable invariant names refer to the following boundaries.

- I1: At mutex boundaries, `parked` equals the waiter-list length.
- I2: `pollers` equals registrations acquired and not yet released or transferred to parked.
- I3: Each selected waiter receives exactly one token and consumes it before reuse.

For I1, every parked-count change and matching list mutation occur in the same side critical
section. Counts and links can temporarily disagree inside that section. Reading an atomic count
without the mutex does not authorize inspecting list pointers or `listed`.

The transition table below lists every acquisition, transfer, and release required by I2.
Receiving a token does not register again because selection already did that.

For I3, selection, unlinking, token send, and token receive are separate steps. A still-listed
waiter has an empty token channel. Selection transfers accounting before sending. Each waiter is
unlinked before its one send. There can be a short interval after unlinking and before the send,
so “unlisted” does not by itself mean “a token is currently buffered.” Cancellation or a
successful extra attempt uses the mutex to determine whether selection already happened.

Waiter reuse requires an unlisted waiter, nil links, and an empty channel. A pooled waiter owns no
producer retry priority. That priority belongs to a `pushWait` invocation, not its waiter.
`Consume` owns a waiter across its receives, while the other blocking paths borrow one for a park.
Cleanup establishes this state before reuse. The pool does not reset it. Each count can represent
at most `2^31 - 1`. The current `register` path does not check overflow.
There is no defined overflow policy.

## Transitions

In this table, `P` is the poller count and `W` is the parked count.

| Procedure | Accounting at success | Obligation |
| --- | --- | --- |
| `register` | `P += 1` | Refuse new registration if closed. |
| `enlist` | `P -= 1`, `W += 1` | Link under the mutex, then make the extra attempt. |
| `wakeOneLocked` | `P += 1`, `W -= 1` | Recheck zero pollers, unlink, send one token. |
| `wakeGroup(t)` | `P += k`, `W -= k` | Transfer once, then unlink and send exactly `k` tokens. |
| Token receive | No count change | Continue with the waker's registration. |
| `leavePollingWith` | `P -= 1` | Sample cleanup visibility after releasing registration. |
| Listed `leaveListWith` | `W -= 1` | Unlink under the mutex, with no token to consume. |
| Selected `leaveListWith` | Later `P -= 1` | Receive the token, then leave polling normally. |
| `setClosedLocked` | Set closed | Preserve counts and serialize against enlist. |

If enlist finds its side closed, it leaves the caller registered and unlisted, so the caller still
owes leave-polling cleanup. CAS failure reloads counts and rechecks eligibility. It does not
silently abandon a wake obligation.

An ordinary successful attempt checks the opposite side before its own cleanup. If a registered
consumer observes closure, `leavePollingAndDrain` first releases its registration and runs consumer
cleanup. Then `drainCheck` attempts a dequeue and checks for waiting producers if it moves items.
There is no deliberate handoff yield after acquiring a batch. Ordinary wait yields, preemption,
and engine retries can still delay return. `NeverPark` retains registration until cleanup. Other
parked consumers can still be selected by group wakes while that registration remains.

## Engine assumptions

The engine stores items independently of the wait lists. Its observations connect queue progress
to wake decisions. A1 and A4 name the required freshness guarantees. Their numbering is retained
for existing source references. A2 described a historical performance assumption and is not needed
for wake correctness.

[engine.go](engine.go) selects the [DPDK SPSC engine](../ring/dpdk/spsc.go) when both
sides are declared single. Otherwise it selects a frontier engine:
[MPSC](../ring/frontier/mpsc_base.go), [SPMC](../ring/frontier/spmc.go), or
[MPMC](../ring/frontier/mpmc.go). The letters name single or multiple producers and consumers.
Callers must serialize all operations on a declared single side, including failed and zero-length
calls.

A **reservation** claims positions before copying finishes. A **frontier** is the end of a
contiguous sequence of completed reservations or released slots. **Discovery** reads the engine
records to extend that known sequence. A completed reservation beyond an unfinished one can
therefore exist without being available to the opposite side.

The [batch-count contract](../ring/frontier/options.go) and [engine
tests](engine_test.go) document and exercise the relevant engine behavior.

A1 concerns an attempt that returns fewer items or slots than requested, including zero. It must
refresh the opposite side's progress sufficiently to observe the completion or release records
that precede its relevant atomic observations, subject to reservations by competitors. A full
successful burst may use cached progress. Do not reuse its returned count as a fresh cleanup
observation.

A4 concerns zero-length `DequeueBurst(nil)` and `EnqueueBurst(nil)`. They move nothing and refresh
the available-item or free-slot observation. A fresh observation can become stale as soon as a
concurrent operation proceeds. It neither reserves work nor promises that a later attempt
succeeds. Freshness is not an atomic snapshot of all concurrent activity.

A completed enqueue can remain hidden behind an earlier unfinished reservation. Similarly,
released slots can remain hidden behind an earlier unfinished release. The operation that
completes the missing frontier progress must perform its own opposite-side wake check. A prior
successful enqueue or wake does not discharge that obligation.

For multi-consumer engines, publication's zero-length dequeue discovery is safe concurrently with
consumers and may advance shared frontier state. A declared single consumer uses cap one and
avoids producer-side dequeue peeks. Single-side caller serialization remains an engine
requirement. Those engines have caller-owned caches. Adding concurrent peeks would violate their
serialization requirement even though a peek moves no items.

There is no single boundary for all of an API call's effects:

- The state CAS commits a registration, parked transfer, release, or closed-bit update.
- The side mutex joins parked accounting with list changes and selection with token sends.
- Engine reservation, completion, and frontier visibility are separate events. The wake CAS and
  token send do not enqueue, dequeue, or reserve any item or slot.
- Batch calls can complete several chunks. Their wake observations and error observations are
  per attempt or chunk where the source places them.
- The consumer closed-bit publication, closing `done`, and producer closed-bit publication occur
  in that order but are separate events.

Go atomics are sequentially consistent and provide the ordering used to compare state observations
with engine publication and release records. That fact does not establish A1/A4 by itself or make
ordinary list fields safe without the mutex. Channel synchronization and mutex synchronization
complete the ownership argument. Neither side mutex may be held while taking the other. Engine
calls and discovery peeks stay outside both side critical sections because frontier discovery can
retry or yield.

## Why a wake is not lost

The ordering argument depends on the engine assumptions above and on participants continuing their
required transitions. It is not a proof of scheduler liveness. The safety claim is conservation of
counts, list membership, and tokens. For progress, the protocol keeps someone responsible for
checking persistent visible work when eligible callers remain parked. That check can belong to a
registered caller, a caller's cleanup, or the operation that made progress. This claim depends on
those participants continuing their required transitions.

The cases below explain why a required check remains after each race:

1. Publication after an empty attempt but before enlist can see no parked waiter. The extra
   attempt after enlist closes that window. A caller can have registered before publication, so
   “every later parker registered after publication” is not a valid shortcut.
2. Publication with registered pollers can rely on a later attempt or exit. Parking transfers
   responsibility to its extra attempt. Leaving polling transfers it through the fresh peek
   after the decrement. An earlier peek could miss a publication that counted this poller.
3. Publication with zero pollers and parked callers enters the wake path. The locked recheck
   finds another responsible registration, no remaining parked caller, or transfers at least
   one registration. The consumer publication floor preserves this case when discovery says
   zero. Group fanout beyond one is a parallelism policy, not the missing-wake argument.
4. Completion before a cleanup peek is covered by that fresh peek. Completion after it is
   covered by the completing operation's later wake check. This is also why every progress
   attempt checks the opposite side rather than trusting an earlier operation's wake.
5. A selected caller that cancels or finishes its extra attempt must consume its token and
   leave polling. Its cleanup carries the obligation onward. A still-listed canceled caller
   never acquired that registration and removes only itself.
6. A full burst can use cached progress and miss a particular later visible item while taking
   earlier FIFO items. Finite capacity bounds those earlier items. This addresses stale full
   bursts, but does not prove termination of arbitrary chains of canceled callers or scheduler
   delays. Eventual service also needs consumers that continue receiving and engine operations
   that complete. Producer progress does not imply service to each waiting producer.

The same ordering applies when a consumer releases room and producers wait for it, with the
one-producer wake policy described above.

## Progress limits

Progress can stop at an unfinished engine reservation, a stalled protocol participant, a lack of
consumers, or scheduler delay. A correct handoff of responsibility cannot make those participants
run or finish.

## Evidence and maintenance

The implementation is in [waitside.go](waitside.go), [wait.go](wait.go),
[consumer_wake.go](consumer_wake.go), [queue.go](queue.go), [push.go](push.go), and
[receive.go](receive.go). The tests below support particular transitions and races.
Each group checks the named behavior, rather than proving its whole section:

- Counts, token transfer, and clean waiter reuse under I1–I3 ([waitside_test.go](waitside_test.go),
  [wait_test.go](wait_test.go)): `checkWaitSide`, `TestWaitSideWakeOneRegistersWoken`,
  `TestWaitSideWaiterReuse`, and `TestPushWaitPooledWaiterLeftClean`.
- Extra attempts and cleanup peeks covering registration and ordering windows
  ([wait_test.go](wait_test.go),
  [waitside_test.go](waitside_test.go)): `TestReceiveWaitItemDuringLastPoll`,
  `TestReceiveWaitItemDuringExtraAttempt`, `TestWaitSidePeekAfterDecrement`,
  `TestWaitSidePeekRacesCompletion`, and `TestWaitSideStalledProducer`.
- Selected callers consuming tokens despite cancellation or an extra-attempt success
  ([waitside_test.go](waitside_test.go),
  [wait_test.go](wait_test.go), [regression_test.go](regression_test.go)):
  `TestWaitSideCancelRacingWaker`, `TestReceiveWaitRemovedCallerHandsOff`, and
  `TestParkedProducerCancellationAfterTokenTransfer`.
- Whole-group accounting, fixed sends across retries, and sampled targets
  ([consumer_wake_test.go](consumer_wake_test.go)):
  `TestConsumerWakeGroupAccounting`, `TestConsumerWakeGroupRetryAndEarlyExit`,
  `TestConsumerPublicationSnapshotRaces`, `TestConsumerCleanupReplenishesWithPollers`, and
  `TestConsumerPublicationPathsWakeGroups`.
- Skipped publication peeks, a fixed construction cap, and no successful handoff yield
  ([consumer_wake_test.go](consumer_wake_test.go),
  [consumer_handoff_test.go](consumer_handoff_test.go)):
  `TestConsumerPublicationSkipsPeek`, `TestConsumerWakeCapStaysAtConstructionValue`, and
  `TestConsumerSuccessWithoutHandoffYield`.
- Oldest insertion after a failed wake and reset after partial progress
  ([producer_priority_test.go](producer_priority_test.go)):
  `TestProducerFailedWakeRestoresOldest`, `TestProducerPartialProgressResetsPriority`, and
  `TestWaitSideMultipleOldestInsertions`.
- Cleanup during close, the producer publication gap, and a fresh close-drain attempt
  ([consumer_wake_test.go](consumer_wake_test.go),
  [regression_test.go](regression_test.go), [wait_test.go](wait_test.go),
  [receive_test.go](receive_test.go)): `TestConsumerGroupCloseAndCancellationCleanup`,
  `TestProducerClosePublicationGap`, `TestReceiveWaitDrainsItemPushedDuringClose`, and
  `TestTryReceiveDrainsItemPushedDuringClose`.
- Fresh zero-length and short bursts, plus retries after an empty dequeue
  ([engine_test.go](engine_test.go)):
  `TestEngineZeroLengthBurstIsFresh`, `TestEngineShortBurstMovesEverything`, and
  `TestEngineDequeueAfterFailedDequeue` across the selected engines.
  `TestWaitSideProtocolStress` in [protocol_stress_test.go](protocol_stress_test.go)
  adds concurrent frontier-model evidence.

For example, `TestReceiveWaitItemDuringLastPoll` inserts an item while a registered consumer's
last poll reports empty. The extra attempt then receives it. `TestWaitSidePeekAfterDecrement`
forces publication during cleanup and checks that the remaining waiter gets a token.
`TestConsumerWakeGroupRetryAndEarlyExit` invalidates the first CAS and lets early recipients
leave. It checks that the successful transfer fixes the number of sends, with no refill.
`TestProducerPartialProgressResetsPriority` checks that pushing one item ends retry priority, even
when the same batch call must wait again.

A protocol change must update the relevant invariant, transition, ordering case, and test
reference in the same change. An engine change must reconsider A1/A4 and zero-length-call
concurrency. A scheduling policy change must state whether it affects correctness obligations or
only parallelism and retry order. Keep invariant identifiers and section anchors stable.
