# Durable work queue

`workq/durable` stores bounded work in SQLite WAL across application restarts. Applications own
encoding, source IDs, workers, retry policy, and external effects. The queue has no exactly-once
external-effect guarantee. A destination needs idempotency keyed by the source ID or participation
in an application transaction.

The implementation uses the `sqlite-wal-v1` profile with a 64 MiB operational WAL envelope.
The [profile manifest](PROFILE.md) records finite production resource measurements and initial
deployment provision. Release qualification, production comparison, and final review remain pending.

## Build and run

Use the module's Go toolchain, cgo, and a C compiler. Go and Bazel use pinned
`github.com/mattn/go-sqlite3` v1.14.52 with bundled SQLite 3.53.4. The support scope is Linux amd64
on local ext4, with a private queue directory, working locks, and storage that honors sync.
Network filesystems, Windows-mounted WSL paths, and unqualified overlay/FUSE filesystems are
outside that scope. Physical power-loss qualification has not run.

From the repository root, choose a new private directory on local ext4:

```sh
go run ./workq/examples/durable -directory /path/on/ext4/demo-queue
bazelisk run //workq/examples/durable -- -directory /path/on/ext4/demo-queue
```

The [runnable example](../examples/durable/main.go) submits three fixed source records, prints them,
and acknowledges successful writes. Repeating it uses identical IDs and bytes, so acknowledged
records do not print again. A crash between printing and Ack can repeat output. Printing is a
demonstration effect, not a durable destination receipt.

## Configure explicitly

Choose durable limits for the application, then request runtime defaults:

```go
policy := durable.DurablePolicy{
    Profile: "sqlite-wal-v1", MaxPayload: 16 << 10, MaxCodec: 64,
    MaxPendingItems: 4096, MaxPendingPayload: 16 << 20,
    MaxReceipts: 16384, MaxProducers: 256, DatabaseLimit: 1 << 30,
}
runtime, err := durable.DefaultRuntime(policy)
if err != nil {
    return err
}
queue, err := durable.Open(ctx, directory, durable.Config{
    Policy: policy, Durability: durable.Synchronized, Runtime: runtime,
})
```

These numbers are logical ceilings. The [profile manifest](PROFILE.md) gives separate initial
process-memory and disk provision recommendations. Zero `Config` is invalid.
Runtime zero fields do not request defaults. Runtime tuning may change on reopen
within supported relations. Persisted logical policy and durability must match exactly.

`Synchronized` is the zero/default durability and uses FULL. Applied mutations survive application
and system crashes under the storage assumptions. `ProcessCrash` explicitly selects NORMAL.
Recent Applied submissions, Acks, and retirement floors can disappear after OS crash or power
loss. There is no bounded loss window. NORMAL retains atomic batches and application-crash
recovery. Initialization and token epochs use FULL in both modes.

The main database cap excludes WAL and filesystem overhead. The checkpoint trigger permits one
group's overshoot and is not a hard WAL cap. An observed WAL length above 64 MiB irreversibly
stops new delivery and unstarted mutations for the session. A crossing commit remains Applied,
and healthy Lookup and Release remain available. SQLite cache and mmap settings are not total-memory
limits. Caller inputs, worker allocations, retained delivery bytes, native SQLite allocations,
and runtime overhead require separate provision. There is no perpetual Ack reserve or hard RSS
promise.

Concurrent callers need an application limit. Waiting calls can retain old queue-created change
notification channels, plus caller stacks, contexts, and runtime wait state. The queue roots one
current notification generation, but paused callers can keep distinct closed generations live.
These live caller-lifetime objects are outside the fixed queue-retained budgets. A channel measured
112 bytes on Go 1.27.3 linux/amd64. That measurement is build-specific. Queue counters do not bound
this overhead, caller-retained deliveries, backend allocations, GC garbage, or total process memory.

Caller-created input, configuration, and error graphs can remain live through queue references.
Their backing storage is outside queue-created allocation budgets. For example, Abort retains the
full supplied cause so Wait and errors.Is/errors.As preserve its identity and details. Applications
must bound the size and lifetime of those objects. The capped Snapshot cause detail still counts.
Error-reporting graphs and formatting have the separate boundary below. Exclusion from the ledger
does not mean referenced memory is free.

Working-storage budgets do not bound application Error/Is/As methods, backend diagnostics, or
error-reporting graphs and formatting.
Formatted wrappers can retain a queue-created copy of a diagnostic message as well as its original
graph. Fault and Abort reporting can therefore exceed the working-storage allowances. Applications
need process headroom for that cost. Snapshot copies at most 256 detail bytes, but preparing that
copy can allocate more. This preserves full returned causes and errors.Is/errors.As behavior.

Open, Inspect, Maintain, and ChangeDurability limit directory input, absolute form, and resolved
form to 4095 bytes each. Longer paths return ErrInvalidInput. Relative and symlink aliases remain
supported. Filesystem normalization and resolution need separate startup/backend memory provision,
including temporary allocations made by queue-invoked library calls. This phase precedes logical
validation and active pools. Symlink traversal scratch can exceed the input limit. Retained
canonical paths and DSNs remain charged to fixed storage.

## Submit and reconcile

Recover each ID from a durable source position, or persist it before submission. Every retry
must use the same producer, sequence, codec, and payload. A new ID creates a new occurrence.
Keep caller-owned input unchanged during the call. The queue reserves and copies admitted input.
After return, callers may reuse it even when the decision is Unknown.

| Result | Meaning |
| --- | --- |
| Applied, nil | The complete effect holds under the selected policy. Submit does not mean processed. |
| NotApplied, error | This invocation changed nothing. An earlier invocation can still exist. |
| Unknown, error | The command can still execute or may have committed. Preserve IDs and reconcile. |

Healthy ordered `Lookup` settles earlier handoffs. It returns current state, not an invocation
history. If the session faults, stop and join workers, close it, then use offline `Inspect`.
An error never means Absent. Successful Inspect validates and synchronizes the recovered snapshot,
including surviving WAL. It returns zero Inspection on any error. It cannot restore lost NORMAL
transactions. Keep the store quiescent across multiple inspection calls.

A failed Ack does not authorize repeating the external effect. Reconcile the ID or retry the same
current token in a healthy session. After reopen, receive a fresh token and consult the
destination's durable idempotency record before repeating an effect.

## Own attempts and shutdown

Receive returns detached bytes and a fenced token. A nonempty acquisition returns nil error even
if cancellation races afterward. Only the owner may use the attempt. There are no visibility
leases or automatic retry timers. Stop processing before Release permits another attempt.
Poison work remains pending until the application establishes success or changes its policy.

The example's feeder holds at most four deliveries per Receive. Runner SubmitBatch transfers
only its returned accepted prefix, including on error. The feeder releases only the unaccepted
suffix after it stops using those deliveries. Accepted payloads remain with the runner until its
terminal callbacks finish. Only established handler success is acknowledged. Failed or abandoned
attempts remain pending for recovery.

For graceful drain, stop sources and durable admission, process until ErrDrained, stop delivery,
and join the feeder and runner. For interruption, stop delivery, cancel the feeder, abort workers,
and join both. Keep durable Ack service alive until all runner callbacks finish, then call Close.
An Ack failure signals the supervisor and returns from OnTerminal. A callback must never join its
own runner. Runner success counters do not prove durable completion.

Close needs neither pending zero nor all tokens returned. It cuts off handoffs, settles earlier
commands, and joins resources. A deadline requests Abort and still waits for the join. Kernel I/O
or an uncooperative application worker can delay shutdown. Done/Wait establish queue resource join
and lock release. Join old workers before reopening because queue tokens cannot fence external
effects in a still-running worker.

Healthy graceful closure performs an explicit checked FULL/TRUNCATE barrier. Closure reports
barrier errors and errors actually returned by the driver and owned descriptors. SQLite suppresses
some native destructor errors, which the API cannot report. The pinned driver can also hide a
concurrent independent SQL failure behind context cancellation. Cancellation alone does not prove
storage health. Separately observed errors remain visible. These limits do not weaken a prior
FULL commit or successful Inspect barrier. Close does not upgrade NORMAL's public success policy.

## Retirement and offline operations

Acknowledged receipts continue to consume receipt capacity. Coordinate `RetireThrough` with source
retries. A floor retires holes as well as completed IDs and refuses to cross pending work. Bounded
cleanup later removes eligible receipts. Producer records consume capacity for the store lifetime.
Retirement does not immediately reclaim physical file space or make capacity available.

When Stopping closes, stop sources and workers and preserve known external outcomes by ID.
After joining and closing, address the reported storage condition. Offline calls reject a live
owner with ErrInUse and retain the lock until their resources close.

`Maintain(ctx, directory)` checkpoints and performs bounded incremental vacuum. It preserves
logical records, policy, and epoch. It can return completed physical progress with an error.
Check HasStore and HasSizes before using associated result fields. It does not remove live work,
change logical limits, clear external reader pins, or guarantee the next Ack succeeds.

For an offline MaintenanceError, check `WALValid` before reading WALBytes and `Checkpoint.Valid`
before using returned checkpoint progress. Duration describes the attempt even without progress.
Offline refusal supplies available checkpoint/WAL facts, not main-file or page/free measurements.
Sampled live Snapshot and successful Inspection provide their documented physical observations.
Unavailable values are not measured zeros.

`ChangeDurability(ctx, directory, expected, next)` synchronizes recovered state, checks the
expected policy, then commits the change with FULL. Inspect resolves Unknown. Opening with a
different policy never changes it. Upgrading protects surviving state, not NORMAL operations
already lost. Downgrading affects future ordinary writes.

Preserve `queue.sqlite3`, its surviving WAL, and the permanent LOCK. Never delete WAL or unlink
LOCK as recovery. Do not copy only the main database while live. Maintain needs writable access
and storage headroom. If unchanged maintenance cannot address the pressure, diagnose the capacity
or filesystem condition rather than assuming another call guarantees progress.
