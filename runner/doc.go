// Package runner owns a bounded workq queue, workers, retries and terminal cleanup for one run.
// New starts a single-use runner. Its owner must eventually call Shutdown, or Abort then Wait.
// A runner cannot restart and must not be copied. Concurrent submission and lifecycle calls
// are supported.
//
// # Acceptance and ownership
//
// SubmitBatch transfers exactly the returned prefix, including on error. Never replay that
// prefix merely because submission failed. Accepted values are shallow copies and may reach a
// handler or terminal callback before submission returns. The caller retains the suffix and may
// reuse its slice storage after return. It must not concurrently mutate shared accepted payloads.
// Submission cancellation does not cancel accepted work. A pre-canceled nonempty submission
// accepts nothing, but cancellation racing publication can still leave an accepted prefix.
// Admission closure takes precedence over caller cancellation, including for empty batches.
//
// A handler borrows its batch until return and must not retain or modify its slots. Nil results
// establish success for every position. Otherwise return one valid Result per position. Failed
// or abandoned work may already have external effects. The runner provides neither durable
// acknowledgment nor deduplication. Applications must reconcile uncertain effects before retrying.
//
// Items checks cancellation between calls but applies failure policy only when the entire batch
// returns. Set Queue.MaxReceiveBatch to one if that boundary must be one item. WholeBatch cannot
// infer partial effects. Prefer native results for partially successful bulk operations.
//
// OnTerminal reconciles and releases each accepted occurrence once, including duplicate values.
// It owns resource release in every terminal state when configured. Handlers must not release the
// same resources independently. Handlers, terminal callbacks and retry policies must support
// concurrent calls. They must not join their own runner or block submitting to it. Callback
// failure aborts even under Continue, and other terminal callbacks still run.
//
// # Cancellation and completion
//
// StopAdmission starts graceful drain. Shutdown also joins, and cancellation of its grace context
// requests global abort before it continues joining. Use a fresh grace context after stopping
// service sources. A grace deadline cannot terminate a noncooperative handler or callback.
// Application functions must return normally. The runner does not recover panics, and completion
// guarantees exclude runtime.Goexit in handlers, retry policies and terminal callbacks.
//
// Abort publishes runner cancellation and closes Aborting without joining. Its first cause is
// preserved. Done closes only after submissions, workers and cleanup finish. A graceful run,
// including Continue with failures, leaves Aborting open. Supervisors must watch both channels,
// propagate abort to dependent runners before joining, and remain active during ordered shutdown.
// Continue provides no early operational-failure notification. See the executable examples and
// README for application-owned supervision and forwarding reconciliation.
//
// # Accounting and storage
//
// Read Summary and Diagnostics after Wait for final snapshots. At completion Accepted equals
// Succeeded + Failed + Abandoned. Live counts need not reconcile. Diagnostics count queue
// transfers, including abort cleanup, rather than processing success. Wait returns retained
// operational failures, or the abort cause when there are none. ErrorLimit bounds error count,
// not object bytes.
//
// For capacity C, W workers and receive limit B, queued and worker-held payload storage has
// C + W*B slots. Abort cleanup adds a B-slot buffer after workers exit. Retry compaction reuses
// worker storage without another payload batch. Workers also hold O(W*B) outcome and retry
// metadata. Loop variables, callback arguments and swaps add O(W) transient payload value copies.
// Submit creates a one-element submission slice per call. Adapters allocate result slices of at
// most B entries per invocation. Concurrent admitted submissions add O(P) registrations.
// Caller slices, payload-referenced memory, callback storage and handler resources are excluded.
// Applications needing a total memory bound must also bound source concurrency and input batches.
package runner
