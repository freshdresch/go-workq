# Overload and backpressure recipes

Choose what the application does when producers offer work faster than consumers finish it.
The queue supplies waiting and non-waiting calls. The application owns rejection, discard, and
submission budgets. These recipes use the existing API without adding policy to the queue.

## Choose a policy

| Policy | Queue operation | Application responsibility |
|---|---|---|
| Wait for room | `Push` / `PushBatch` | Slow the source and keep consumers available |
| Reject when full | `TryPush` / `TryPushBatch` | Return unaccepted work to its owner |
| Discard when full | The same `Try` operations | Count discards and release owned resources |
| Limit waiting | `Push` / `PushBatch` with a deadline | Account for accepted and unaccepted work |

Rejection returns responsibility to a caller that can handle failure. Discarding ends that
responsibility under an explicit lossy policy, such as optional observations. A full queue does
not justify dropping work that the application promised to complete.

For backpressure to reach the source, wait synchronously before reading or admitting more work.
Starting a goroutine for every blocked push moves the backlog into goroutines and their payloads.
Bound producer counts and buffers independently. Use the work abort context for indefinite waits
so consumer failure can release producers. The [lifecycle recipes](lifecycle.md) explain ownership,
abort, joining, close, and drain.

Waiting has no fairness guarantee. A particular producer can wait without bound under sustained
load. A `Try` call avoids waiting for room but has no wall-clock latency guarantee. See the
[waiting section](README.md#waiting) for wait strategies and scheduler limits.

Queue capacity bounds queue storage, not total unfinished work. Producer buffers, acquired
batches, local overflow stacks, and asynchronous handlers hold work outside that storage.
Submitted work was offered in a producer call. Accepted work is reported by the returned result.
Acquired work was removed by a consumer. Completed work reached the application's success
boundary. Consumers can complete work before its push returns.

Use the [observability recipes](observability.md) for outcome counts and measurement boundaries.
Queue capacity and live counter subtraction do not provide admission control for total work.

## Reject an unaccepted suffix

[`Example_overloadReject`](example_overload_test.go) offers `[1, 2, 3]` once to an empty queue
with capacity two. No consumer runs during submission, so `TryPushBatch` accepts `[1, 2]` and
returns `ErrFull`. The caller retains `[3]` as rejected work. The example then closes and drains
the queue and reports exactly `[1, 2]` as completed.

Record the returned count before classifying the error. Only `items[n:]` remains unaccepted.
The accepted prefix can already be in a handler when the producer gets an error. Copy a saved
partition if it must outlive reuse of the producer's slice. The
[batch reconciliation recipe](lifecycle.md#batch-reconciliation) explains suffix resubmission
and close-race limitations. Never resubmit the accepted prefix with the suffix.

Only `ErrFull` selects the overload rejection policy. `ErrClosed` and `ErrClosedDuringPush` are
lifecycle failures, not overload discards. The latter wraps `ErrClosed` and means delivery can be
uncertain. Preserve the count and expose the failure. Close only after all producer calls return.

Under a deliberate discard policy, count the unaccepted IDs as discarded and release any
producer-owned resources. The example uses integer IDs. It does not demonstrate payload cleanup
or durable acknowledgment, and it does not retry rejected work.

## Give one submission a wait budget

[`Example_overloadWaitBudget`](example_overload_test.go) fills a capacity-two queue with `[1, 2]`.
It submits `3` with a short `context.WithTimeout` derived from the work abort context. The queue
stays full until the call returns `context.DeadlineExceeded`. The caller records `3` as
unaccepted, then closes and drains `[1, 2]` with the still-live work context.

The child timeout does not cancel the parent workload or undo accepted work. Always release the
child context with its cancel function. A failure that ends consumers still cancels the parent
work context, as the lifecycle recipe requires. A request-scoped budget must not become the
context for all consumers.

The example holds consumers back only to make the deadline outcome deterministic. Normal
applications keep consumers running while producers wait. Do not copy that staging into an
indefinite wait policy. The one-millisecond duration illustrates a timer, not a recommended
service setting. Choose the budget from the application's requirements.

Run both examples and their focused tests from the repository root:

```sh
go test ./workq -run 'TestOverload|Example_overload' -count=1
```

The output contains IDs and outcomes. It makes no elapsed-time promise.

## Understand deadline boundaries

A submission deadline bounds willingness to wait cooperatively. It does not guarantee return
by a wall-clock instant, cancel accepted work, or establish a strict publication cutoff.
Scheduling and queue operations can delay observation of cancellation.

`Push` checks its context only when it would wait. With room available, an already canceled or
expired context can still produce a successful push. `PushBatch` also checks between chunks
after the first. An already done context can still allow its first chunk. A retry within a
partly accepted chunk does not add a context check before the retry. Use returned counts and
errors to decide acceptance, rather than the context state.

`Receive` can acquire available items with a canceled context. `Consume` checks cancellation
before receiving, but cancellation can race that check. Handlers receive the consumer's context,
not the submission context. A submission deadline does not attach an expiry time to the payload.

If an item must not begin an operation after its deadline, carry an explicit item deadline and
check it at the application's operation boundary. A `ctx.Err()` precheck can avoid an already
known cancellation, but cancellation can still race a push. Checks alone cannot promise that an
external side effect finishes before a deadline.

Strict admission cutoff, queue publication cutoff, processing eligibility, and completion by a
deadline are different contracts. The [admission gate](lifecycle.md#a-strict-admission-cutoff)
orders application admission against a stop decision. Previously admitted calls can publish
afterward. These recipes add no strict-deadline mechanism.

## Compose with handlers and pipelines

Do not block or repeatedly retry publication into the same queue from all its handlers. If the
queue fills, no consumer remains to drain it. A deadline can end those waits, but it does not
define how required work will finish. Use the
[self-feeding recipe](lifecycle.md#self-feeding-queues) when every generated item must be processed.
Its local overflow stack has separate memory requirements. Do not copy a lossy ingress policy
into that recipe and abandon required children.

Apply acceptance independently at each pipeline stage. An accepted downstream prefix must not
be resubmitted with its unaccepted suffix. The [pipeline recipe](lifecycle.md#pipelines) owns
forwarding failures and shutdown order.
