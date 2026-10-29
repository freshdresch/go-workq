# Lifecycle and failure handling

These recipes show how an application owns admission, queue operations, handlers, and shutdown.
Use them with the contracts in [push.go](push.go), [receive.go](receive.go), and
[queue.go](queue.go). The executable examples use default MPMC queues and integer payloads.

The [observability recipes](observability.md) add counts and latency measurements at these
application boundaries without changing lifecycle ownership.

## Stop sources, drain, or abort

Use two independent contexts. `sourceCtx` requests that sources stop admitting new work.
`abortCtx` controls queue operations and handlers. Create each from `context.Background()`.
A signal that requests graceful shutdown cancels `sourceCtx`. It must not also cancel `abortCtx`.

A producer checks source cancellation before acquiring another batch. Blocking source reads
must cooperate with cancellation or return when an owned resource closes. Requested cancellation
at that source-read boundary is expected. An independent source failure triggers abort.

After admission, a producer finishes its finite batch using `PushBatch(abortCtx, batch)`, even
if source stop was requested. It records both the accepted prefix and the unaccepted suffix.
Source stop is cooperative: an admission can overlap the request. Joining producers establishes
when all publication has stopped. A strict admission cutoff needs the gate below.

[Example_gracefulShutdown](example_lifecycle_test.go) shows a fixed set of registered producers.
Its owner follows this sequence:

1. Start the grace deadline and request source stop.
2. Keep consumers running while joining producers.
3. After every producer call returns, close the queue.
4. Join consumers and the deadline watcher.
5. Return the recorded outcome.

Finite source exhaustion follows the same join, close, and drain path without a shutdown error.
On an operational failure or explicit abort, record the reason, stop sources, and cancel
`abortCtx` immediately. Then follow the same join and close sequence. Do not close under a
producer to release its wait.

Accepted, acquired, and completed describe different points. A push accepts an item into the
queue. A receive acquires it and removes it from the queue. The application completes its work.
These recipes interpret a nil handler result as completed application work. A handler that
starts asynchronous work must join it or explicitly transfer ownership before returning.
The queue cannot verify an external side effect from a nil result.

A successful drain requires completed accepted work and all owned goroutines joined. A single
nil `Consume` result cannot establish this, especially during an abort race. Abort can leave
accepted items queued, acquired items unfinished, and unaccepted items with the producer.

## A strict admission cutoff

For dynamic submitters, register each admitted call under the same mutex that closes admission.
This is the private `admissionGate` used by the [lifecycle tests](lifecycle_test.go):

```go
type admissionGate struct {
	mutex   sync.Mutex
	stopped bool
	calls   sync.WaitGroup
}

func (gate *admissionGate) admit() bool {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	if gate.stopped {
		return false
	}
	gate.calls.Add(1)
	return true
}

func (gate *admissionGate) stop() {
	gate.mutex.Lock()
	gate.stopped = true
	gate.mutex.Unlock()
}

func (gate *admissionGate) done() { gate.calls.Done() }

// wait is called only after stop, so no new call can race with the join.
func (gate *admissionGate) wait() { gate.calls.Wait() }
```

A submitter calls `admit`. If it returns false, report application admission rejection without
calling the queue. If it returns true, defer `done`, then call `PushBatch` outside the mutex.
Record its accepted and unaccepted partitions before classifying the error.

The owner calls `stop`, then `wait`, then closes the queue and joins consumers. Never hold the
mutex across a queue operation or a join. Admission acknowledges responsibility for the call,
not queue acceptance. A call admitted before the cutoff can publish afterward. All such calls
must return before `Close`. Rejection by this gate is not `workq.ErrClosed`.

## Deadline supervision and cooperation

Start `context.WithTimeout` immediately before requesting source stop. The grace period must
cover both producer joins and consumer drain. A producer parked on a full queue needs coverage
before consumers can finish.

The private `drainQueue` in [example_lifecycle_test.go](example_lifecycle_test.go) derives a
cancelable supervision context from the grace context. Its watcher requests abort only when
that child's error is `context.DeadlineExceeded`. After both worker joins, `drainQueue` cancels
the child and joins the watcher. The caller also cancels the parent grace context to release
its timer.

Canceling the child after successful drain records `context.Canceled`, even if the watcher
first runs after the parent's deadline. The watcher checks its own context, so that later
expiration cannot turn completed drain into an abort. If expiration wins the cancellation
race, the child retains `context.DeadlineExceeded`. Genuine overlap can resolve either way.
`TestLifecycleCompletedDrainStopsSupervision` exercises delayed watcher observation in the
[lifecycle tests](lifecycle_test.go).

The deadline bounds when the owner requests abort. It does not bound when the owner returns.
Sources, handlers, engine operations, and resource cleanup must return before their joins
finish. Context cancellation cannot kill a goroutine. The owner continues joining after expiry,
including any watcher it started. A hard process deadline belongs to an external supervisor.
`TestLifecycleWaitsForHandlerAfterAbort` demonstrates this limit with a held handler that the
test explicitly releases. It never leaves that handler behind.

Cancellation also permits fast-path progress. `Push` checks cancellation when it would wait.
`PushBatch` additionally checks between chunks after the first. `Receive` can acquire available
items under a canceled context. `Consume` checks before each receive, but cancellation can
occur between that check and a handler call. Handlers must cooperate too.

## Failure attribution and outcomes

The private `recipeOutcome` records attributed operational failures under a mutex before
broadcasting cancellation. It also retains the first explicit abort or grace-expiration reason.
After all joins, `err()` returns `errors.Join` of operational failures, if any. Otherwise it
returns the abort reason, or nil if neither exists.

The first recorded operational failure is primary. This means mutex insertion order, not
wall-clock causality. Concurrent independent failures have no stable order, and all remain
matchable with `errors.Is`. An independent handler failure remains visible even if grace
expired first. Queue calls return context errors, which alone do not identify the originating
stage or preserve an application's abort cause.

`queueFailure` classifies errors only at known queue-operation boundaries. Shared context
cancellation there is secondary. It checks `ErrClosedDuringPush` before `ErrClosed`, because
the former wraps the latter. Both remain failures for an admitted producer.

`consumeAndAbort` wraps the handler locally. It records an independent handler failure before
returning it to `Consume`, then avoids recording `Consume`'s wrapped copy a second time.
A handler uses the private `peerAbort` wrapper only after actually observing the shared abort
context, or after classifying a downstream queue cancellation as secondary. An independent
request timeout is still a failure, even if it matches `context.DeadlineExceeded`. Preserve
an error when its origin is uncertain. Do not suppress arbitrary handler errors merely because
they match a context sentinel.

In the table, abort includes recording the reason, stopping sources, and canceling workers.
Operational failures take precedence over an explicit abort cause or deadline.

| Event | Owner action | Outcome after joins |
|---|---|---|
| Exhaustion or source stop | Join, close, drain | Nil if handlers succeed |
| Source or handler failure | Record failure and abort | All failures, primary first |
| Grace expiry | Abort, keep joining | Deadline unless failures exist |
| Explicit abort | Abort, keep joining | Cause unless failures exist |
| Shared peer cancellation | Join peer | Existing outcome |
| Receive sees closed and drained | End receive loop | Normal end |
| Consume returns nil during abort | Join consumer | Existing outcome |
| Admitted push returns ErrClosed | Record failure and abort | Failure, nothing accepted |
| Push returns ErrClosedDuringPush | Record violation and abort | Failure, delivery uncertain |

A closed-and-drained receive does not establish that other handlers finished. A nil consumer
result during abort does not clear the owner's recorded outcome.

These private functions encode the selected example policy. Owners still construct contexts,
register workers, and perform joins. They do not provide a public lifecycle manager.

## Batch reconciliation

For `PushBatch(ctx, items)` returning `(n, err)`, save `items[:n]` as accepted and `items[n:]`
as unaccepted, even when `err` is non-nil. Copy any saved slices. Under the no-close-race
contract, consumers can complete the accepted prefix before `PushBatch` returns. Never replay
that prefix solely because the call returned an error.

[Example_partialPushBatch](example_lifecycle_test.go) uses capacity 2, push limit 1, and an
already canceled context. Its first call accepts one item, then returns `context.Canceled`.
With consumers running and a live context, it submits only the suffix. Such a retry requires
that the application still wants the work and consumers remain available. During abort, these
recipes retain the unaccepted suffix for reconciliation and do not retry it. `TryPushBatch`
returning `ErrFull` uses the same prefix rule. A close race does not permit a blanket retry of
accepted or uncertain work.

[Example_handlerFailure](example_lifecycle_test.go) demonstrates fail-fast processing of an
acquired batch. Its private `processBatch` records copied completed, failed, and unattempted
IDs. The failed item can have an uncertain external side effect. Items after it were acquired
but not attempted. None returns to the queue automatically. An abort check before an item
leaves that item and the remaining suffix unattempted. These acquired IDs differ from a
producer's unaccepted suffix.

An application can instead handle recoverable per-item errors and continue, or finish a batch
before returning an aggregate failure. Those are different completion policies. Replaying a
failed external operation needs application-specific idempotency or reconciliation. Blindly
requeueing an acquired suffix from a handler can deadlock when every handler blocks on the
same full queue.

Queue elements are copied values. Producers can reuse their input slices after a push returns.
Handlers must not retain the received batch slice after returning. Copying a slice of pointers
does not copy the pointed-to payloads. The application must define payload ownership and avoid
concurrent mutation. Acceptance transfers responsibility according to that application policy.

`Close` releases queue waiters but does not join goroutines or release files, buffers, and other
resources held by payloads. Abort cleanup must account for queued items, acquired unfinished
items, and producer-owned unaccepted items. The examples' integer IDs describe work, not a
resource cleanup implementation or durable completion record.

## Pipelines

[Example_pipeline](example_test.go) shares `abortCtx` across both stages and its outside producer.
Only outside sources observe graceful source stop. A stage 1 handler is a stage 2 producer.
The private `closePipeline` therefore joins outside producers, closes stage 1, joins stage 1
consumers, closes stage 2, and joins stage 2 consumers. Use that order on success and abort.
Do not close every queue together while upstream handlers can still publish.

The private `forwardDoubled` allocates new doubled values and returns copied forwarded and
unforwarded partitions. Its handler saves that report before returning an error. The forwarded
prefix belongs to stage 2. The unforwarded suffix is already acquired upstream work. Replaying
the entire upstream batch could duplicate downstream effects. Forwarding is not an atomic
transfer between queues.

`TestLifecyclePipelineFailure` in [lifecycle_test.go](lifecycle_test.go) holds a failing stage 2
handler while stage 1 forwards `[4]` and parks on `[6]`. The downstream failure is recorded
before cancellation releases the upstream push. Shared cancellation adds no duplicate failure.
The test joins both stages through the same closure function and reconciles the prefix and suffix.

## Self-feeding queues

[Example_selfFeeding](example_test.go) counts each item before publication, including children
that overflow onto a handler's local stack. The private `visitTree` offers children once with
`TryPushBatch`, then processes its unaccepted suffix locally on `ErrFull`. It never blocks or
loops retrying publication into its own full queue. It checks abort between local items and
finishes an item's count only after its processing and publication step succeeds.

The owner starts with a lifetime token. Each seeder receives its own token before it starts
and retains it until its final queue call returns. The owner releases its token after registering
all seeders. A seeder rolls back counts only for provably unaccepted seeds. On success, zero
means there are no queued, acquired, or local items and no producer can publish again. Whoever
reaches zero closes the queue.

On failure, record the cause and cancel peers and seeders. Do not finish failed or abandoned
items merely to force zero. The private `joinSelfFeeding` joins seeders and consumers, then
closes for cleanup even when the count remains positive. That close is safe because no handler
or seeder can publish again. A repeated close is harmless if normal completion won a race.
Local stacks and queued payloads still need application cleanup when they own resources.

`TestLifecycleSelfFeedingFailure` in [lifecycle_test.go](lifecycle_test.go) holds roots 1 and 10
while a seeder fills the queue with 20 and parks on 21. Root 1 overflows children to its local
stack and fails on node 3. Root 10 observes abort. All workers join with outstanding work still
positive. The test does not decrement abandoned work to simulate completion or resume traversal.

For a self-feeding pipeline stage, hold an upstream producer token until every upstream
`Consume` returns. Upstream handlers can publish until then. Failure still uses shared abort
and joins. Publishing handlers invalidate a single-producer declaration, so these examples use
MPMC. Queue capacity does not bound memory held in local stacks, acquired batches, or producers.
