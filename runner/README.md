# workq/runner

`runner` owns a bounded `workq` queue, a fixed worker group, submission admission, optional retries,
and terminal cleanup. `New` starts a single-use runner after validating its configuration.
The owner must call `Shutdown`, `StopAdmission` followed by `Wait`, or `Abort` followed by `Wait`.
Construct a new runner for another run.

See [executable examples](example_test.go) for a finite job, service shutdown, external abort,
native partial results, retries, resource release, and two-stage supervision.

## Submit and reconcile

`SubmitBatch(ctx, items)` returns the exact accepted prefix length, even on error. The runner owns
`items[:n]`, and the caller owns `items[n:]`. Release or retry only that suffix. Never replay the
accepted prefix because the submission returned an error. Acceptance does not establish success.
Accepted items can reach handlers and terminal callbacks before submission returns.

The runner shallow-copies values into its queue. After submission returns, callers may reuse slice
storage but must not concurrently mutate shared accepted payloads. A handler borrows its input
slice and must not retain or modify its slots. Join asynchronous processing before claiming success,
or define an application ownership transfer that counts as success.

The submission context controls publication only. Canceling it after acceptance does not cancel
handlers. A pre-canceled nonempty call accepts nothing after checking the admission gate.
Cancellation racing publication can still accept a prefix through the queue's fast path.
After admission closes, all new calls return zero and `ErrStopped`, even with a canceled context or
an empty batch. An admitted empty batch succeeds. Admitted calls can keep publishing during graceful
drain until their own cancellation or runner abort.

`OnTerminal` runs once per accepted occurrence in all terminal states. Duplicate payload values are
separate occurrences. When it owns resource release, handlers must not release those resources too.
Callback calls can overlap on workers. Queued abort cleanup invokes callbacks after workers exit.
A callback error records cleanup failure and aborts even under `Continue`. Other callbacks still
run.
The application must retain information needed to repair failed external cleanup.

## Results and retries

A nil handler response means every position succeeded. A non-nil response must have one valid
`Result` per input position. `ResultSucceeded` requires no error. `ResultFailed` requires an error.
`ResultUnattempted` requires no error and runner abort. For an attempted operation interrupted by
runner cancellation, call `PeerCanceled(ctx)` only after observing the supplied handler context
cancel. Ordinary context errors remain failures. Invalid responses abort, establish no successes,
and are never retried.

`Items` continues after ordinary item failures in the same batch. Failure policy applies when the
whole batch returns. Use `Queue.MaxReceiveBatch: 1` for an item-sized failure boundary.
`WholeBatch` maps an error to failure for every position. It cannot infer which external writes
committed. Prefer native per-position results for partial progress. Failed and abandoned items can
have external effects. Neither terminal callbacks nor `Wait` provide durable acknowledgment.

Retries are disabled by default. A non-nil `RetryPolicy` requires positive `MaxAttempts`, including
the original attempt, and a callable `Delay`. Delay must return promptly and support concurrent
calls.
It is called once per eligible ordinary failure, even after abort, but never after attempt
exhaustion.
It must return a nonnegative duration even when declining retry. A negative delay aborts in either
failure mode. Retrying ambiguous effects requires idempotency or application reconciliation.

Retries stay in worker-held storage. A worker finishes its held subset before acquiring fresh work.
Long delays can occupy every worker and block submission. Abort cancels retry waits. Intermediate
failures that later succeed add attempts but do not make the final result fail. Terminal failures
remain failures even if abort races them. Retry candidates become abandoned on abort and preserve
their latest ordinary error. Unattempted work has zero attempts unless earlier attempts occurred.

## Stop, abort and join

Stop service sources first, then call `Shutdown` with a fresh grace context derived from
`context.Background()`. Separately join source goroutines that the application started.
`StopAdmission` closes admission synchronously and starts drain. `Wait` joins but does not stop an
open runner. `Shutdown` stops admission and joins. Grace cancellation, including explicit
cancellation, requests global abort. A cancellation observed by any concurrent `Shutdown` caller
can abort the shared run. It is not a private timeout on waiting.

`Abort(cause)` closes admission, cancels the runner context, and closes `Aborting` before returning.
It does not join. Nil cause means `ErrAborted`. The first committed cause wins. `AbortCause` can
observe that cause before publication finishes. Observing `Aborting` closed guarantees publication
finished. `Done` and `Wait` require every admitted submission, worker and terminal callback to
finish.
A late abort after completion changes nothing. Completion racing grace cancellation can resolve
either way. After completion wins, later grace expiration cannot alter the outcome.

Deadlines are cooperative. Shutdown can exceed its grace period while application functions remain
blocked. Handlers, terminal callbacks and retry policies must support concurrent calls. They must
not join their own runner or block submitting to it. The runner does not recover application panics.
Normal completion guarantees require application functions to return without `runtime.Goexit`.

## Compose stages

Create downstream stages first. Forward with the upstream handler context. Reconcile every returned
forwarding prefix immediately. Downstream acceptance means ownership transfer only when the
application defines that transfer as upstream success. It does not prove downstream processing
success. Retry only unaccepted forwarding suffixes. Retrying a whole upstream operation can
duplicate
already accepted downstream work. The pipeline example disables upstream retries for this reason.

Keep an application-owned supervisor active while shutting down stages in upstream-to-downstream
order. Watch each stage's `Aborting` and `Done`, disable handled channels, and propagate an abort
cause to all dependent stages before joining any stage. If `Done` arrives first, inspect
`AbortCause`
and propagate it when non-nil. A gracefully completed stage leaves `Aborting` open forever.
Join the supervisor too. The example preserves stage attribution across forwarding failures.

`Continue` still returns a final failure, but provides no early operational-failure notification.
A supervisor can inspect `Wait` after `Done` for those completed failures. Use `Abort` when
dependent
stages need prompt cancellation. Early publication lets a peer release resources needed by a blocked
terminal callback. It does not resolve arbitrary dependency cycles or noncooperative functions.

Self-feeding tasks need outstanding-work accounting and inline overflow processing. Use the existing
[workq self-feeding recipe](../lifecycle.md#self-feeding-queues). Blocking recursive submission can
deadlock every worker. The runner does not infer quiescence or support recursive completion rules.

## Reports and memory

Read final `Summary` and `Diagnostics` after `Wait`. Accepted occurrences then reconcile as
`Accepted == Succeeded + Failed + Abandoned`. Live counts can represent different instants and
need not reconcile. Each `Summary` call costs O(workers + retained failures): it reads the worker
counters and copies the retained failure records.
Returned failure slices are detached, but application error objects remain shared and immutable.
`Wait` returns an aggregate of retained operational failures, otherwise the abort cause, otherwise
nil. `errors.Is` matches retained causes. `ErrOmittedFailures` reports truncation. `AbortCause`
remains
in `Summary` even when operational failures take precedence in `Wait`.

`ErrorLimit` defaults to 16 and bounds retained failures by count. It does not bound bytes because
an error can retain arbitrary objects. Omitted failures are counted. Queue diagnostics describe
transfers, waits and wakes, not terminal outcomes. Abort cleanup receives count as transfers too.

For queue capacity C, W workers and effective receive limit B, the queue and worker payload arrays
contain C + W*B slots. Retry subsets compact in place without a second payload batch. After workers
exit, abort cleanup uses another B-slot payload buffer. Workers hold O(W*B) outcome and retry
metadata, and cleanup has B outcomes. Loop variables, callback arguments and in-place swaps add
O(W) transient payload value copies.
`Submit` also creates a one-element submission slice per call.
`Items` and failing `WholeBatch` allocate at most B result entries per invocation. Native response
storage belongs to the handler and must remain stable until the runner consumes it.

Accounting uses one atomic Accepted counter and four atomic outcome counters per worker, plus
four for queued cleanup. Outcome counters track Succeeded, Failed, Abandoned and Attempts.

P concurrent admitted submissions add O(P) bookkeeping. Callers whose context has a nil `Done`
channel use the runner context directly and avoid the cancellation bridge and its registrations.
Callers with a non-nil `Done` channel retain the bridge. Before submission returns, the runner stops
its caller and abort registrations and joins any callbacks that started. Queue capacity does not
bound P. Caller input slices, referenced payload memory, application reconciliation storage and
handler-created resources are outside the payload-slot bound. Bound source concurrency
and input batch sizes when total memory must be bounded. Completion clears runner references to
terminal payloads and joins cancellation registrations, workers and cleanup.
