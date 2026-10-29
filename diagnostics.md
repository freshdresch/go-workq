# Queue diagnostics

Use diagnostics to tune capacity, batch limits, waiting, and wakes. They count the queue's
internal attempts and transitions. Keep application completion and latency measurements beside
them. Acquisition removes an item before processing succeeds, and a handler failure does not
undo an acquisition. The [observability recipes](observability.md) cover application outcomes.

## Start with a tuning question

Start with `DiagnosticsCounters` and no extra probes. Compare requested and moved batch sizes,
zero-progress attempts, and waits before choosing a more expensive measurement.

| Question | Evidence to compare |
| --- | --- |
| Do producers lack usable room? | Producer zero progress, waits, fresh reusable-slot samples |
| Do consumers lack visible work? | Consumer zero progress, waits, fresh available-item samples |
| Does polling produce progress? | Attempts and progress by phase, phase entries, actual yields |
| Do wakes produce progress? | Retry outcomes and attempts, token receipts, re-enlistments |
| Is group fanout useful? | Consumer group targets and sizes, aggregate retry outcomes |
| Do batch limits fit the work? | Requested and moved size histograms, strict partial attempts |
| How long do waits last? | Selected wait durations, outcomes, starts, and completed samples |

High producer waits, fresh reusable-slot samples near zero, and available-item samples near
capacity support testing more burst capacity, faster consumers, or different receive batches.
More capacity cannot fix a sustained service-rate deficit. Compare application completion rate
and latency to distinguish a burst from that deficit.

Low reusable slots and low available items do not establish that the queue is both full and
empty. Reservations, publication or release gaps, overlapping observations, and workload changes
can produce that pattern. Smaller batches are a hypothesis to test when long copies delay
progress. These metrics cannot identify a stalled goroutine or count reservation gaps.

## Configure recording at construction

Pass `WithDiagnostics(DiagnosticsConfig{...})` to `New`. Options apply in argument order, and
`New` validates their final values. Invalid diagnostics wrap `ErrInvalidDiagnostics`.
Configuration is immutable. There is no live enable, disable, or reset operation.

| Mode | Recording |
| --- | --- |
| `DiagnosticsDisabled` | Static configuration only. This is the default. |
| `DiagnosticsCounters` | Every completed event, with optional capacity probes and no clocks |
| `DiagnosticsTiming` | Counters plus sampled wait and park-select durations |

Enabled `Shards: 0` resolves to 16. Other enabled values must be positive powers of two.
There is no 256-shard cap. The shard count and aggregate diagnostic storage must fit the
platform's `int`. Validation checks the descriptor and both backing arrays before allocation.
This is a representability check, not a memory budget. A valid configuration can still exceed
available memory or runtime allocation limits. The library does not recover allocation failures.
Queue capacity has a separate limit and does not limit shard count.

A shard is a counter storage block, not a worker identity. `Shards` is the total per queue,
not a count per endpoint or a caller limit. Sixteen is a storage default, not a performance
recommendation.

In timing mode, `TimingSampleEvery: 0` resolves to 64. A positive value `N` selects each wait
episode with probability `1/N`. One selects every episode. Counter mode requires this field
to be zero.

`CapacitySampleEvery: 0` adds no probes. A positive `N` independently selects each completed
attempt with probability `1/N` for a fresh probe on that attempt's side. Both enabled modes
support this setting. It never samples or suppresses the ordinary event counters. Disabled
mode requires every auxiliary field to be zero.

[ExampleWithDiagnostics](example_diagnostics_test.go) shows timing with sampled capacity
probes and prints the resolved defaults. It does not recommend those rates for a workload.

## Retain an endpoint for repeated operations

Create `producer := queue.NewDiagnosticsEndpoint()` once where the application owns a producer.
Reuse it for later `Push`, `PushBatch`, `TryPush`, or `TryPushBatch` calls. A consumer can retain
its own endpoint for `Receive`, `TryReceive`, or `Consume`. Retain reusable receive buffers too.
[ExampleQueue_NewDiagnosticsEndpoint](example_diagnostics_test.go) shows both endpoints and
keeps closure and aggregate collection on the queue.

Each enabled endpoint has an immutable assignment to one whole shard, including both sides
and timing storage. Copies keep that assignment. A separate constructor call obtains the next
assignment. Construction cycles through the queue's shards in atomic ticket order. The first
`Shards` creations receive distinct assignments, but this is not an active-endpoint uniqueness
guarantee. With four shards, retain the first endpoint, discard the next three, and create a
fifth. The first and fifth now share shard zero. Creating an endpoint for every submission
defeats the intended reuse. The queue neither tracks abandoned endpoints nor balances active ones.

Copies, concurrent calls, excess endpoints, and ordinary queue calls can share shards safely.
All updates remain atomic and each invocation has separate recording state. Sharing can reduce
the benefit of stable assignment, but it does not drop events. Ordinary calls still select from
all shards. One shard means every caller shares it. Snapshots remain aggregate per queue, with
no per-endpoint identity, fairness, or processing-success measurements.

Endpoint methods follow the corresponding queue methods' ownership, errors, batching, waiting,
and cleanup contracts. `WithSingleProducer` and `WithSingleConsumer` are queue-wide serialization
promises across ordinary and endpoint calls together. Endpoints add no serialization and do not
change engine selection. `Consume` runs the handler on the caller's goroutine. Ordinary `Consume`
already selects one shard for its whole invocation. An endpoint reuses its assignment across
invocations, rather than removing a draw from every internal receive.

There is no endpoint close, release, flush, registration, or background activity. The application
owns goroutines and cancellation. Stop and join producers before `queue.Close()`, then join
consumers and reconcile their outcomes. Collect final metrics with `queue.Diagnostics()` after
all calls and cleanup finish. `Capacity`, `Close`, and `Diagnostics` belong to the queue.

An endpoint keeps its queue and queue-owned storage reachable. The queue keeps no references
to endpoints. Discarding one needs no cleanup and does not reset counters. Closure leaves
endpoints usable under the existing closed-queue contracts, including draining accepted work.
Construction is valid for disabled and closed queues and records no metrics. A disabled endpoint
does not enable recording or allocate diagnostic storage. Construct endpoints through the queue.
The zero value is unusable, and calling its transfer methods is a programming error.

Endpoint recording retains no contexts, handlers, or buffers after a call returns. Accepted
payloads and `Consume` buffers keep their existing ownership rules.

## Collect a sequential series

`queue.Diagnostics()` returns a detached cumulative value with fixed arrays. It retains no
queue, payload, waiter, context, or caller buffer. Retaining or editing it cannot change the
queue. Collection is safe concurrently with operations and other collectors. It performs no
engine calls and starts no background work.

`SchemaVersion` identifies array order and bucket interpretation. Version 1 uses the phase,
outcome, source, and bucket constants in [diagnostics.go](diagnostics.go). Do not interpret an
unknown enum value as zero. Assign application queue names outside this fixed schema.

`Config` reports the resolved diagnostics settings, engine kind, capacity, batch limits,
effective wait strategies, and construction-time consumer wake cap. `Producer` and `Consumer`
contain cumulative events and histograms. In enabled modes, `WaitState` independently reads each
side's registered pollers, listed parked callers, and closed flag. These are protocol
registrations, not scheduler or operating-system state.

Disabled snapshots report only the schema and static configuration. Every metric remains zero.
Even with parked callers or after `Close`, `WaitState` counts and closed flags remain zero.
Do not use disabled `WaitState.Closed` to determine whether the queue is closed.

Finish one snapshot call before starting the next call in that collector's series. Independent
collectors can run concurrently, but must keep separate delta series. Never combine overlapping
snapshots by completion time. A collector can read 10 and pause while another reads 11 and
returns. The first can then return 10 later, without any counter wrap.

Individual fields are atomic, but a snapshot does not describe one instant across all fields
or shards. A moved sum can appear before its attempt count. A token receipt can appear before
its send count. Do not use these live discrepancies as control signals or queue depth.

[ExampleQueue_Diagnostics](example_diagnostics_test.go) takes a snapshot, submits bounded work,
closes after its producer returns, joins its consumer, and takes the next snapshot. It prints
queue acquisitions separately from the application's processing result.

Counters, buckets, and sums use unsigned modulo-`2^64` arithmetic. Subtract only sequential,
non-overlapping snapshots from the same queue lifetime and schema. Every counter or sum delta
must cover fewer than `2^64` increments or sum units. Nanosecond sums under concurrency can wrap
sooner than event counts. Choose collection intervals for the maximum aggregate rate. A decrease
does not prove a reset.

Subtract corresponding histogram buckets, counts, and sums as `uint64`. Never subtract `Min`
or `Max`, which are lifetime extrema. Derive interval quantiles from interval buckets, with
bucket resolution and sample coverage stated. Rates need the collector's actual elapsed time.
The library supplies no collection clock.

`Close` neither resets nor freezes recording. Cleanup and already active calls can still update
metrics after closure. Final values require joining all queue callers. Join asynchronous work
started by handlers separately, and reconcile application outcomes. Joining queue callers does
not prove processing success. Follow the [lifecycle recipes](lifecycle.md).

Run the executable examples from the repository root:

```sh
go test ./workq -count=1 \
  -run 'ExampleWithDiagnostics|ExampleQueue_(Diagnostics|NewDiagnosticsEndpoint)'
```

## Attempts, phases, and waits

An engine attempt is one nonempty enqueue or dequeue invocation that returns to workq.
A probe is a zero-length burst that moves nothing. Probes are never attempts. An engine call
that has not returned is absent from attempt totals. Rejections before an engine call and empty
public inputs are absent too. Engine reservation retries are internal to a single attempt.

For a clamped request of `r > 0` and a result of `m` items, `ZeroProgress` means `m == 0`,
`PartialProgress` means `0 < m < r`, and `FullProgress` means `m == r`. These partition
`Attempts` for each phase. `RequestedItems` includes repeated requests, so it does not count
unique jobs. `MovedItems` counts actual transfers. `RequestedSize` and `MovedSize` each contain
one observation per attempt. Zero moved sizes are recorded too.

A logical batch call can return a partial result after several full attempts. Conversely,
several partial attempts can finish a logical call. Application return accounting and engine
attempt accounting answer different questions. An enqueue remains recorded if a later wake
check returns `ErrClosedDuringPush`. A dequeue remains recorded if its handler fails or panics.

The six phases have fixed array indexes:

| Phase | Attempt location |
| --- | --- |
| `PhaseInitial` | Ordinary chunk attempts and partial-chunk retries outside a wait |
| `PhaseSpin` | Unregistered spinning |
| `PhaseYield` | Bounded registered polling |
| `PhaseNeverPark` | Unbounded registered polling |
| `PhaseExtra` | The extra attempt after successful park enlistment |
| `PhaseDrain` | A dequeue after the prescribed closure observation |

A wait episode starts inside `pushWait` or `receiveWait` after a zero-progress attempt. Any
positive transfer ends that episode after cleanup, even if the producer's logical batch remains
unfinished. A later failure starts another episode.

`WaitStarts` counts entries. `WaitEnds` uses `WaitProgress`, `WaitClosed`, `WaitCanceled`, or
`WaitDeadline` from the returned result. Positive progress takes precedence even in a close
race. `WaitEndsWithCloseRace` separately counts `ErrClosedDuringPush`, with or without progress.
Handler work is outside the episode.

`SpinEntries`, `YieldEntries`, and `NeverParkEntries` require an actual attempt in their phase.
`Registrations` counts successful poller registrations. `YieldCalls[phase]` counts actual workq
`runtime.Gosched` calls, not configured budgets. One bounded yield attempt can require no yield,
even for the forced post-token attempt when the configured budget is zero. Engine-internal
yields and CAS retries are absent. These counters measure neither CPU time nor context switches.

`ParkEnlistments` counts successful transfers from polling to the waiter list. The extra attempt
can succeed before any select occurs. `ParkEnlistClosed` counts enlistment refused by closure.
`ParkSelectStarts` and `ParkSelectEnds` count reached selects and their chosen `ParkToken`,
`ParkContext`, or `ParkDone` cases. Enlistment does not prove that the goroutine slept.

## Wakes and retry aggregation

`WakeCalls` counts calls by `WakeOne` or `WakeGroup` even when no recipient is eligible.
`WakeTransfers` counts successful accounting transfers. `WakeRecipients` counts their committed
recipient totals, and `TokensSent` counts successful token sends. Failed CAS retries add no
transfer. Consumer-only `GroupTarget` records one requested target per group call.
`GroupActualSize` records the committed size, or zero if the group transferred nobody.

A normal token receipt increments `TokensReceivedForRetry` and begins a wake retry. Cleanup
receipts increment `TokensDrainedByCleanup` instead. Extra-attempt success can lead to a cleanup
drain without a retry. Closure broadcasts through `done` and does not manufacture tokens.

`WakeRetryAttempts` and `WakeRetryZeroProgress` cover actual attempts after a normal token until
progress, successful re-enlistment, or terminal exit. `WakeRetryEnds` uses `RetryProgress`,
`RetryReparked`, `RetryClosed`, `RetryCanceled`, or `RetryDeadline`. A terminal retry can have
zero attempts. A drain attempt can finish it with progress. Refused enlistment is not reparked.

`ProducerOldestReenlistments` counts successful oldest insertions while producer priority is
active. That priority survives failed wake retries until any progress ends the wait episode.
Partial progress resets it for the next episode.

A wake grants a retry opportunity, not ownership of items or room. High repark fractions support
investigating competition, stale observations, or fanout. They do not identify a cause. Outcomes
are aggregates, with no link to a particular group. Progress after a wake does not prove that
wake was necessary. Shards provide no per-worker fairness attribution.

## Capacity observations and freshness

Producer `ReusableSlots` measures room available for reservation at an enqueue observation.
Consumer `AvailableItems` measures claimable work in the contiguous visible prefix at a dequeue
observation. These are separate distributions in item units, bounded by configured capacity.
Do not subtract one from capacity to infer the other or call either one physical queue depth.

Reservations can hold room before publication or after acquisition. Publication gaps can hide
completed items, and release gaps can delay reuse. Handler work has already left the queue.
The four engines expose different discovery behavior. See the
[engine assumptions](wakeup.md#engine-assumptions) for the shared observation contract.

Each source has its own histogram:

| Source | Recorded observation |
| --- | --- |
| `TransferFresh` | A short burst with zero or partial progress |
| `TransferCachedAllowed` | A full burst, whose remaining count can use cached progress |
| `PublicationProbe` | The required consumer probe after publication |
| `CleanupProbe` | A required consumer or producer probe during wait cleanup |
| `DiagnosticProbe` | An optional sampled probe on the attempt's own side |

A burst's remaining count reflects its admission or discovery observation after its own
transfer, not a new observation at method return. Full bursts can be fresh internally, but
the interface does not certify that fact. Short bursts and zero-length probes refresh progress.
Even fresh observations can become stale before the caller returns.

Keep source distributions separate. Short bursts overrepresent pressure and empty states.
Publication and cleanup probes overrepresent waiters and handoffs. Full-burst counts can
understate room or work because of cached progress.

Singleton engine methods return no remaining count. `AttemptsWithoutRemainingCount` reports
that coverage gap. Missing counts are not zero observations. Optional probes can cover singleton
traffic without replacing singleton methods. `CapacityEligibleAttempts`, `CapacitySelectedProbes`,
and `CapacityCompletedProbes`
report probe opportunities, selections, and completed observations separately.

Each selected probe runs after that attempt's required wake checks and cleanup, before its
caller continues. It preserves the already determined result and can run after closure or
cancellation. It is an engine operation that can scan, retry, yield, and change discovery
caches or shared frontiers. Cancellation does not interrupt it. Sampling bounds frequency,
not the latency of one probe. Probes never drive wake decisions or probe the opposite side.

Histograms are event-weighted. Frequent retries contribute more observations than quiet periods.
They do not measure wall-time fractions full or empty. Idle periods and stalled engine calls
produce no samples. Report attempt coverage and probe counts alongside capacity histograms.
Counter mode reads no clocks, so it cannot report a source's age in seconds. Unchanged counts
between collections do not turn old observations into a fresh gauge.

Extra discovery can change the pressure being investigated and improve later cache hits.
Compare tuning conclusions with and without probes, alongside application outcomes.

## Timing and histogram interpretation

Timing selects once per wait episode before its first clock read. A selected `WaitDuration`
runs from episode entry through normal cleanup, immediately before return. It excludes the
initial failed attempt and includes subsequent attempts, diagnostics, polling, parks, preemption,
and scheduler delay. Durations are grouped by the actual wait outcome.

Within a selected episode, every reached park select gets a `ParkSelectDuration`. Its interval
starts immediately before selection and ends when the chosen branch resumes, before cleanup.
It excludes the extra attempt. A ready branch can produce a near-zero duration. This is not
OS-blocked time, goroutine suspension time, or the entire park helper's duration.

`Timing.WaitStarts` and `Timing.ParkSelectStarts` count selected starts. `WaitSamples` and
`ParkSelectSamples` count completed durations. Unfinished waits contribute starts but no zero
duration. A live histogram can omit its longest active waits. Duration deltas include waits
that finished in the interval even if they started earlier.

Episode sampling selects clusters of park intervals. Those intervals share the episode's
selection probability and are correlated. Report this policy and sample counts. Do not claim
independent park samples or a stable tail from sparse data. Ordinary counters remain complete.
Timing uses monotonic `time.Time` subtraction and reports nanoseconds. A selected episode adds
two clock reads plus two per reached select, with histogram updates.

Size histograms have 58 buckets: exact sizes 0 through 32, then inclusive ranges `[33,64]`,
`[65,128]`, and doubled upper bounds through `2^30`. Duration histograms have 64 buckets: zero,
one nanosecond, then `[2,3]`, `[4,7]`, and doubled ranges through the largest `time.Duration`.
Counts are events, never item-weighted. `Sum` uses items or nanoseconds as appropriate.
`Min` and `Max` are lifetime observations and are meaningful only when the histogram is nonempty.
Empty histograms report zero extrema. Capacity `ZeroCount` repeats bucket zero, and `AtCapacity`
counts observations equal to configured capacity.

## Memory and recording cost

Diagnostics storage is bounded by shard count and fixed metric fields. No raw samples,
per-worker registry, or per-item metadata accumulate. Enabled ordinary calls choose a random
shard, and `Consume` retains one across its loop. Endpoint calls reuse their fixed assignment.
Concurrent calls can collide. More shards increase memory and snapshot work and can reduce
counter contention. They do not remove it.

For the current amd64 layout, requested type storage in bytes is:

- Disabled diagnostics: zero allocated diagnostic storage.
- Counter mode: `80 + Shards * 9728`.
- Timing mode: `80 + Shards * (9728 + 7936)`.

The 80 bytes describe the diagnostics descriptor, including the endpoint creation ticket.
Each shard includes both sides. At 16 shards,
counters occupy 152 KiB and timing adds 124 KiB, plus the descriptor. The queue itself occupies
816 bytes on amd64 in every mode. Engine storage, ring payloads, `Consume` buffers, waiter pools,
and allocator rounding are separate. These are implementation layout figures, not API size
promises. [Layout tests](diagnostics_layout_test.go) verify them.

An endpoint value occupies 16 bytes on the current amd64 layout. Constructing a value requires
no separate library heap allocation. Application storage and goroutine captures follow ordinary
Go escape rules. Keeping more endpoints costs caller-owned values, not more metric arrays.
Churn does not grow queue-owned storage. Both storage and snapshot reduction grow linearly with
`Shards`, while the detached snapshot has fixed size.

Enabled recording adds atomic updates, histogram work, and cache contention. Ordinary calls
also select a random shard. Endpoint construction adds one atomic ticket update, while endpoint
transfers use the fixed assignment. Timing and capacity sampling retain their independent draws.
Snapshots read many cache lines and compete with updates. Timing and probes add their own
costs. Warmed-path allocation tests establish no extra per-attempt diagnostic allocation, not
zero construction cost or the absence of ordinary waiter-pool allocations.

Disabled recording performs no metric writes, ticket updates, random draws, extra probes,
or clock reads. Ordinary disabled calls do no endpoint selection.
It still incurs checks, helper calls, larger stack frames, and recording-state initialization.
The queue layout also grows. A nil diagnostics pointer does not imply zero overhead. Runtime
performance acceptance requires workload comparisons with the uninstrumented baseline.

## Final reconciliation

After every queue caller joins, and without metric arithmetic wrap, the following identities
help check recording:

- Each phase's attempts equal zero-progress plus partial-progress plus full-progress counts.
- Transfer histogram counts equal attempts, and moved sums equal actual transfers.
- Wait starts equal wait ends, and park-select starts equal selected-case ends.
- Normal token receipts equal wake-retry ends.
- Wake recipients equal sent tokens, which equal normal receipts plus cleanup drains.
- Successful group transfers equal nonzero actual-group-size observations.
- Selected timing starts equal completed samples after all eligible calls finish.

These identities do not prove liveness or successful processing. Live snapshots need not
reconcile. A close race still invalidates delivery inference under the lifecycle contract.
Internal enqueue totals can lag concurrent dequeues, so their live difference is not queue
depth. Keep completion and handler outcomes in application-owned accounting.
