# Observability recipes

These recipes measure application progress and latency through the existing workq API.
The application owns all counters, timestamps, and collection. Direct queue use adds no
observability work. Start with the [lifecycle recipes](lifecycle.md) for ownership and shutdown.
Metrics report progress. They do not decide when work may stop.

## Choose the measurements

The executable examples and their private helpers live in
[example_observability_test.go](example_observability_test.go).

| Recipe | Use it for | Added work |
|---|---|---|
| `Example_observabilityCounts` | Final outcomes and reconciliation | Worker-local count updates |
| `Example_observabilitySampledTiming` | Admission and call timing | Clocks, envelopes, buckets |
| Full timing | Every acquired item's admission delay | The sampled recipe with probability `1` |
| `Example_observabilityLive` | Approximate live progress | Atomics and a collector |

Counts cover every event even when latency is sampled. The counts-only recipe needs no clock,
atomics, or collector. It merges worker-local totals after joining workers. Timing can also use
worker-local histograms when reporting only after joins. The live example measures handler-call
latency and keeps independent accepted, acquired, and completed totals.

Run the examples and focused tests from the repository root:

```sh
go test ./workq -run 'TestObservability|Example_observability' -count=1
```

The examples print stable counts. Actual durations, rates, and bucket distributions depend on
scheduling and are omitted from executable output.

## Name the boundary

Admission means the application accepts responsibility for work. Queue acceptance is the count
reported by a producer call. Acquisition removes work from the queue. Completion is the
application's declared success boundary. Each can happen at a different time.

| Measurement | Boundaries |
|---|---|
| Admission-to-observed-acquisition delay | Admission to consumer observation |
| Submission-to-observed-acquisition delay | Before submission to consumer observation |
| Producer call latency | Immediately before and after a producer call |
| Handler call latency | Immediately around the application's handler work |
| Completion rate | Change in completed items divided by elapsed collection time |

The sampled example defines admission as the finite source's handoff of each item. An existing
admission gate can timestamp its successful decision while holding its mutex. A stamp immediately
before `Push` measures submission delay unless the application defines admission there. One
admission timestamp per batch is valid only when the batch is admitted as a unit.

Observe immediately after `Receive` or `TryReceive`, or at entry to a `Consume` handler wrapper.
Acquisition already happened, and wake bookkeeping or preemption can delay that observation.
A shared observation for one received batch does not measure each item's processing start.

Admission delay includes producer delay, queue residence, and delay before observation.
Producer call latency includes copies, engine work, wake bookkeeping, waiting, and scheduling.
Handler latency includes downstream waits and scheduling. A long call does not prove that the
queue was full or that the goroutine was parked. `TryPush` can also encounter contention and
preemption. These measurements cannot recover exact queue residence or blocked time.

Do not stamp after `Push` returns. A consumer can finish before that return, and frontier
reservations can delay visibility after copying. Subtracting producer call latency from
admission delay does not isolate residence and can produce a negative result.

## Preserve timestamp and payload ownership

The sampled recipe uses a value envelope with a payload, sampled flag, and immutable `time.Time`.
Choose sampling and initialize the timestamp before the first queue call. Use `time.Time`
subtraction and `time.Duration` to retain in-process monotonic timing. Do not serialize timestamps
or convert them to Unix nanoseconds before subtraction.

Published timestamp state must remain immutable. Consumers read copied envelope fields after
acquisition, so timestamp atomics are unnecessary. Producer buffers can be reused after the
call returns. Pointer payloads still need an owner who controls mutation and release.
A consumer must not retain the `Consume` batch slice after its handler returns. Copy scalar
measurements, or copy envelopes if the application transfers ownership of work.

On retry, preserve an unaccepted item's original admission timestamp while the application
continues responsibility for it. Measure every producer call separately, including unsuccessful
calls. Never restamp an accepted prefix. A new admission attempt follows the application's
chosen policy. The recipes keep no per-item timestamp map or raw sample history.

## Count returned outcomes

Record calls, offered items, accepted items, and unaccepted items separately. For batch calls,
record `n` accepted and `len(items)-n` unaccepted before classifying the error. Both `PushBatch`
and `TryPushBatch` can accept a prefix and return an error.

For a single-item call, nil means one accepted. `ErrClosedDuringPush` means one accepted with
uncertain delivery. Other documented errors mean zero accepted. Classify `ErrClosedDuringPush`
before `ErrClosed` because it wraps `ErrClosed`. The examples use bounded outcome categories:
success, full, canceled, deadline, closed, close race, and other.

Empty batches count as calls with zero offered items. Rejection by an application admission gate
has no queue call and needs a separate application count. Retries add new offered-item attempts,
so these totals do not count unique business jobs. Call latency is per call. Dividing a batch
call's duration by its item count does not produce item latency.

Cancellation alone does not determine acceptance or acquisition. `Push` checks cancellation when
it must wait. `PushBatch` also checks between chunks after the first. `Receive` can acquire
available items under a canceled context. `Consume` checks before receiving, but cancellation
can race that check. Count returned results and actual processing outcomes.

## Own completion and reconcile after joins

The per-item helper records a completed prefix, one failed item, and an unattempted suffix when
processing fails. Failed means an operation returned failure. Its external side effect can still
be uncertain. Unattempted means acquired work abandoned before an attempt. A batch handler whose
error cannot identify item outcomes records a failed handler call and unresolved items.
Do not infer that every item failed, or that none completed, from a non-nil batch result.

A nil handler result means completion only if its contract says every item finished successfully.
An asynchronous handoff needs a separate completion owner, or the handler must join that work.
Handler return alone does not prove durability. The recipes do not recover panics for metrics.
If an application already recovers, it can report affected work as unresolved. Process failure
can prevent a final report.

Keep consumers running while producers finish. Join producers before closing the queue, then
join consumers. After accounting finishes, and with no close race:

- Accepted minus acquired counts accepted items left unacquired.
- Acquired partitions into completed, failed, unattempted, and unresolved items.
- A successful drain requires every accepted item completed and no lifecycle failure.

A nil `Consume` return alone does not prove drain. After abort, preserve the distinction between
producer-owned unaccepted work, queued work, and acquired unfinished work. Any close race
invalidates delivery inference for the whole run, including producer calls that returned nil.
The final report must expose that violation. Instrumentation must not change close, cancel,
retry, or error policy.

[Pipeline counts](lifecycle.md#pipelines) are stage-local. A downstream accepted prefix does not
automatically complete the upstream business operation. Work processed on a
[self-feeding local overflow stack](lifecycle.md#self-feeding-queues) is neither queue acceptance
nor queue acquisition. Count it separately. Keep the lifecycle recipe's tokens responsible for
termination independently of metrics.

## Collect live progress

The live example allocates three fixed worker slots: one producer and two consumers. Every
field read by its collector is atomic. Scrapes are race-free but approximate across fields and
workers. The handler histogram uses the same four disjoint buckets as the timing recipe.
The histogram sum and bucket counts can also describe different instants.

Accepted totals become known only when producer calls return. Acquired and completed totals can
advance first. Even a coherent scrape cannot remove that event ordering. Do not publish
`accepted - acquired` as exact live depth or `accepted - completed` as reliable outstanding work.
Do not clamp negative differences. Publish the independent totals.

The handler wrapper adds an active call and its acquired batch size at entry, then removes both
on exit, including an error return. `handler_held_items` counts items held by active calls. It
can include already completed items in those batches. It is not unfinished accepted work.
The compact recipe publishes item dispositions when the handler exits. An application that
needs finer progress can publish each item's disposition as it finishes.

The collector computes completed-item deltas over actual elapsed time, rather than assuming
each ticker notification represents one full interval. Its single latest snapshot belongs to
the collector until join. Export from the collector, outside queue and handler paths. The
example retains no stream of samples and adds no exporter.

The collector has a context independent of work abort. Its owner joins workers, requests final
collection, waits for the final-collection acknowledgment, then cancels and joins the collector.
Thus an abort still receives a final report. After workers join, final totals stop changing and
can be reconciled. No collector reads worker-local error fields before those workers join.

An application that needs coherent local snapshots can use a per-worker mutex around short
metric updates and snapshots. Never hold it across a queue call or handler. This adds contention
and does not turn cross-worker snapshots into one instant. The recipes do not implement that
alternative or use metrics as lifecycle synchronization.

## Bound cost and interpret samples

With `W` fixed worker slots and `H` fixed histogram buckets, metric storage is `O(W * H)`.
Retire slots after join before reuse. Bound labels to queue or stage, operation, and outcome.
Do not label by item ID or error text. Histograms retain counts and sums, not payloads or raw
samples. The examples need no telemetry event channel.

Timestamp storage travels with envelopes in queues and caller-owned batches. Queue capacity
alone does not bound producer buffers, active handler batches, asynchronous work, or local
overflow stacks. Bound those separately. If a later exporter adds a bounded telemetry channel,
overflow must drop telemetry and count drops rather than block work. Drops can bias distributions.

Costs include count updates, per-item failure attribution, random sampling, clock reads,
envelope copies, histogram updates, and atomic traffic or mutex contention for live collection.
Sharing an observation endpoint across a received batch saves clock reads. Sharing an admission
timestamp requires a real batch admission event. These recipes promise no numeric overhead.
Exact internal timing and performance experiments require separate work.

The sampled example uses independent per-item selection with probability `0.5`, chosen before
latency observation. Its fixed seed makes the example reproducible. Setting the probability to
`1` enables full timing. Sampling complete admission batches is a different policy and produces
clustered samples. Periodic selection can correlate with workload cycles. Report the policy,
sample count, offered and accepted counts, and call outcomes with the histogram. A few samples
do not establish a population tail.

Admission-delay histograms are conditional on acquisition. Rejected items have no acquisition
sample. Accepted items still queued after abort, or still in flight during a live scrape, are
missing and can include the slowest work. Report unreconciled counts beside the histogram.
Do not record zero or a timeout duration for a missing observation. Drain and join finite runs
before final distribution reporting. Continuous windows naturally omit work not yet acquired.
The bounded recipe keeps no per-item ledger to estimate those missing tails.
