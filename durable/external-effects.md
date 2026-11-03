# Applying external effects with a durable queue

`workq/durable` can deliver an operation again after its destination already committed it.
Make that retry safe by giving the destination a stable operation key and an atomic way to
record both the effect and its receipt. A matching retry returns the original result.
Acknowledge the queue only after establishing that destination success.

This gives at most one committed effect per key when every destination writer follows the
protocol and storage preserves its records. Eventual completion also needs retained intent,
retries, available capacity, and a destination that eventually resolves the operation.
Only with both properties can an application call the operation exactly once. Handlers can
execute repeatedly, and destination commit and queue acknowledgment remain separate transactions.

Two examples show the same inventory adjustment across different destination boundaries:

- [Transactional destination](../examples/transactional/README.md) commits the inventory change
  and receipt in one SQLite transaction.
- [HTTP idempotency destination](../examples/idempotency/README.md) runs a loopback service that
  persists the same transaction before replying. The client validates a matching receipt.

Both start with quantity 10 and apply +3, -1, and +2. After completion, persistent quantity is 14
and the adjustment count is 3. Replay preserves those totals. Their READMEs give Go and Bazel
commands, restart instructions, and a deliberate stop between destination commit and Ack.

The [printing example](../examples/durable/main.go) remains useful for runner supervision.
Its output can repeat after a crash between printing and Ack. Printing has no durable receipt
and is outside the effect guarantee described here.

## Assign identity when the source records intent

An effect key identifies a logical business operation, across every delivery attempt and restart.
Include the destination contract or tenant, source stream incarnation, durable operation ID,
and effect name. Several effects from one source occurrence need separate effect names.

The examples derive the operation ID from `durable.ID`. Their
[request mapping](../examples/transactional/source.go) uses the delivery's original codec and bytes:

```go
request := inventory.Request{
	Key: inventory.Key{
		Namespace: sourceNamespace,
		Producer:  delivery.ID.Producer,
		Sequence:  delivery.ID.Sequence,
		Effect:    "inventory.adjust",
	},
	Codec:   delivery.Codec,
	Payload: delivery.Payload,
}
```

Here `sourceNamespace` is persisted source configuration, and `delivery` is a `durable.Delivery`.
The `inventory` package is internal example code, not a supported destination adapter.
Applications implement their own domain transaction with the same guarantees.

The [key encoding](../examples/internal/inventory/key.go) is versioned and length-delimited.
It preserves all 16 producer bytes and the full unsigned 64-bit sequence. The database stores
sequence as eight big-endian bytes, so values above SQLite's signed integer range remain distinct.
Do not concatenate arbitrary components with an ambiguous separator.

Preserve the key version and namespace across retries, software upgrades, source replay, and
queue recreation. A genuinely new source incarnation needs a new namespace if it can reuse IDs.
Renaming old work makes it a new effect and defeats deduplication. This applies to restored
backups too.
Queue directories, UUIDs, delivery tokens, and process IDs must not determine the effect key.
Tokens fence queue operations for an attempt. They cannot fence an old destination request.

If several queue IDs can represent the same logical effect, carry a separate business operation
ID in the payload. Conversely, two intentional identical purchases need distinct operation IDs.
A payload hash alone cannot distinguish them.

Persist the codec and exact request bytes with the intent, then compare them on every duplicate.
Different bytes under the same key are a conflict, even if they decode to the same business value.
If semantic equality is required, canonicalize once before persisting the intent. Never regenerate
timestamps or other changing fields on retry. The examples retain bounded exact bytes, so they
need no hash-collision assumption.

## Commit the receipt with the business change

The [inventory transaction](../examples/internal/inventory/apply.go) owns both the receipt and
inventory tables in a destination database outside the queue directory. Every writer must use
this transaction protocol:

1. Begin a transaction with the destination's required isolation and uniqueness enforcement.
2. Read the receipt for the complete key. If it exists, compare its codec and exact request bytes.
3. If the request matches, return the saved result after successful transaction completion.
   If it differs, return a conflict without changing the receipt or inventory.
4. For a new key, validate the business request and capacity, then insert a provisional receipt.
5. Apply the inventory change and fill the receipt with its original result in that transaction.
6. Commit both records before reporting success.

A failed business update rolls back the provisional receipt too. No committed incomplete receipt
counts as success. The example checks affected-row counts, the item, nonnegative quantity, and
signed overflow. A duplicate returns the original resulting quantity, even after later adjustments
change the current inventory.

The SQLite destination uses WAL, FULL synchronization, an immediate write transaction, and a
250 ms busy timeout. It verifies these settings on its owned connection. The write transaction
serializes receipt inspection with competing writers. Other databases need their own isolation
and uniqueness recipe. Copying the SQL without those guarantees is insufficient.

The worker calls `Store.Apply` before `Queue.AckBatch`. This fragment shows one attempt after
receiving a delivery. It uses the request above, an open `store` and `queue`, and owner-provided
`ctx` and `ackParent` contexts inside a function returning `error`:

```go
if _, err := store.Apply(ctx, request); err != nil {
	return fmt.Errorf("apply adjustment, leave queue work pending: %w", err)
}
ackCtx, cancel := context.WithTimeout(ackParent, 5*time.Second)
defer cancel()
commit, err := queue.AckBatch(ackCtx, []durable.Token{delivery.Token})
if err != nil {
	return fmt.Errorf("destination succeeded, Ack %s: %w", commit.Decision, err)
}
return nil
```

The [complete worker](../examples/transactional/worker.go) adds bounded retries and Ack
reconciliation. The owner keeps a separate bounded Ack lifetime available after worker
cancellation so a known success can still be recorded. A returned error leaves responsibility
with that owner. It never authorizes an unguarded second inventory update.

An uncertain destination commit requires authoritative recovery with the same key and bytes.
In this example, a commit or cleanup error marks the Store failed before releasing its connection.
Waiting and later operations cannot reuse it. The owner stops, joins its workers, closes the Store,
and opens a fresh Store on the next command invocation. A new immediate transaction can then
recover a matching receipt or perform the still-owed atomic operation. A failed lookup proves
neither success nor absence.

A busy begin on a healthy Store can retry the whole same-key operation. The worker checks
`Store.Failed()` before retrying and permits at most three attempts. Identity conflicts, business
rejections, capacity refusal, cancellation, and exhausted retries stop with unfinished work pending.
The examples do not automatically acknowledge rejected operations or choose a dead-letter policy.

Request deadlines do not bound native cleanup time. The owner retains operation ownership until
Commit or Rollback returns, even after cancellation. It must not start a replacement Store while
an old native operation still owns its resources.

## Require durable idempotency from an HTTP destination

The [HTTP client](../examples/idempotency/client.go) sends `POST /adjustments` with the canonical
key in `Idempotency-Key`, the codec in `X-Adjustment-Codec`, and the original payload body.
The [server](../examples/idempotency/server.go) uses the same atomic inventory transaction.
`GET /adjustments/{key}` reads a persisted receipt in an authoritative transaction.

A successful reply includes the key, codec, exact original request, adjustment ID, and original
result. The client checks this binding and the result shape before permitting Ack. A 2xx status
alone does not establish success. A POST can return either a newly committed or a saved receipt.
The remote example reports `post_successes` and `lookup_recoveries`, not a count of new commits
inferred from POST replies. The persistent adjustment count measures committed business effects.

If a reply is lost, malformed, mismatched, canceled, or uncertain, keep the queue entry pending.
Query the same key or retry the identical POST within the destination's contract. A matching
successful receipt permits Ack. A rejection or conflict stops for application or operator action.
A 503 or transport error remains unresolved.

Even an authoritative absent response cannot prevent an older POST from arriving afterward.
Both requests must use the same key and atomic destination protocol. An eventually consistent
read, expired key, or timeout never permits a new key or a non-idempotent fallback. An expired
or unresolvable key requires authoritative reconciliation before further automatic progress.

A header named `Idempotency-Key` supplies no guarantee by itself. A participating destination must
persist the request binding and result, serialize concurrent duplicates, and retain receipts
through the application's replay horizon. Production authentication must also scope keys to the
correct tenant. The example uses loopback HTTP without production authentication or TLS.

Writing a local receipt before an arbitrary network call can suppress an effect that never happens.
Writing it afterward can permit duplicates. Holding that local transaction open during the call
does not close the gap. If a service calls another destination, that next destination must also
supply the required idempotency or atomicity contract. A separate local result journal, if needed,
is another recoverable operation.

## Recover queue state without guessing destination state

Keep the logical key, queue ID, destination outcome, and queue decision separate in diagnostics.
Do not log request payloads to explain recovery. These actions assume immutable source records
and that all previous workers acknowledged only established destination success:

| Observation | Next action |
| --- | --- |
| Destination success, Ack `Applied` | Finish this attempt and retain the destination receipt. |
| Destination success, Ack `NotApplied` | Resolve the refusal before another valid Ack. |
| Success, Ack `Unknown` | Use healthy ordered Lookup or retry the exact current token. |
| Destination unresolved | Resolve or retry the original key at the destination. Do not Ack yet. |
| Queue `Pending` after reopen | Receive a current token, resolve or apply the same key, then Ack. |
| Queue `Acknowledged` | Do not apply the effect again. |
| Queue `Absent` | Resubmit original bytes if retained source intent says the operation is owed. |
| Queue `Retired` | Respect the replay barrier. A retired hole does not prove an effect happened. |
| Lookup or inspection error | Preserve uncertainty and stop dependent progress. |

`NotApplied` describes only that queue invocation. An earlier invocation can still exist.
`Unknown` can mean a command is still executing. Healthy ordered `Queue.Lookup` settles earlier
queue handoffs, but it does not query the destination. If the queue is no longer healthy, stop and
join workers, close it, and use successful offline `durable.Inspect` before recovery. Keep the
store quiescent across inspection calls. Inspect cannot restore lost NORMAL transactions.

Never fabricate a token after a stale-token refusal. Release and reopen invalidate old attempts.
Receive a current delivery, recover its saved destination result, and acknowledge that token.
Stop using an attempt before Release, and join old workers before reopening. Cancellation of an
HTTP request does not prove that its server stopped. Destination deduplication must cover delayed
requests after local workers finish too.

## Preserve per-item outcomes in batches and runners

A batch groups transport work. It does not create one business transaction or one identity.
For independently committed items A, B, and C, if A succeeds, B is unresolved, and C is rejected,
only A is eligible for Ack. Resolve B by its original key. Leave C pending for an explicit
application decision. Replaying the source batch preserves every original key.

`AckBatch` is atomic within the queue. A stale member can refuse the whole Ack batch even though
several destination effects already succeeded. Recover their receipts and use current tokens.
The [batch fixtures](../examples/transactional/batch_test.go) exercise both histories. If an
application needs an atomic business batch, its destination must commit all mutations and receipts
together, and an uncertain commit requires reconciling every member.

The [runner adapter fixture](../examples/transactional/runner_test.go) initializes every result
as failed before attempting effects. It sets `runner.ResultSucceeded` only after established
destination success. An unknown outcome must never become a zero-value success. Terminal failure
leaves durable work pending, so the supervisor must notice it instead of waiting forever for
`ErrDrained`.

This handler body uses the example's `effectRequest` mapping and returns one explicit result per
`durable.Delivery`. The fixture also models a lost success observation to test the unresolved path:

```go
results := make([]runner.Result, len(deliveries))
for index := range results {
	results[index] = runner.Result{State: runner.ResultFailed, Err: inventory.ErrUnresolved}
}
for index, delivery := range deliveries {
	_, err := store.Apply(ctx, effectRequest(delivery))
	if err != nil {
		results[index] = runner.Result{State: runner.ResultFailed, Err: err}
		continue
	}
	results[index] = runner.Result{State: runner.ResultSucceeded}
}
return results
```

Use the [existing durable example](../examples/durable/main.go) for the supervision lifecycle.
`runner.SubmitBatch` transfers its accepted prefix even when it returns an error. Release only the
unaccepted suffix that nobody is processing. Keep Ack service alive until workers and terminal
callbacks finish. A terminal callback signals the supervisor and returns. It cannot join its own
runner. On interruption, stop delivery, cancel and join the feeder and workers, finish callbacks,
and then close the queue and destination. Runner success counters do not prove Ack committed.

## Keep source intent in an outbox

A destination receipt cannot recover an intent lost before queue submission. If a source business
change and its submission are separate writes, a crash between them can leave no work to deliver.
Commit the source change and an outbox row in one source database transaction.

For a concrete recipe, give each row its original key, queue producer and sequence, codec, payload,
and a `handed_off` marker. Store the full unsigned sequence as eight big-endian bytes. The
[focused outbox fixture](../examples/transactional/outbox_test.go) uses a separate FULL SQLite
source database, `source_changes` and `outbox` tables, and the public queue API. It is a test of
handoff behavior, not a dispatcher library.

Use `durable.Synchronized` and corresponding synchronized source storage for this handoff recipe:

1. Assign the stable identity and serialize the immutable request before committing source intent.
2. In one source transaction, write the business change and its complete outbox row.
3. On each bounded dispatcher pass, select undispatched rows, for example at most four rows
   ordered by sequence with `handed_off = 0`. Build `durable.Entry` values from their stored bytes.
4. Call `Queue.SubmitBatch`. Establish acceptance for each row before advancing its marker.
5. In a separate source transaction, set `handed_off = 1` for those established accepted rows.

A crash before source commit leaves neither change nor intent. A crash after source commit leaves
an undispatched row. A crash after queue acceptance but before the marker causes an identical
resubmission. Concurrent dispatchers may also resubmit identical rows. They must never change
the original ID, key, codec, or bytes.

For an `Unknown` submission, use healthy ordered Lookup of the original IDs. `Pending` or
`Acknowledged` establishes acceptance for the known immutable entry. `Absent` permits identical
resubmission. A lookup error does not advance a marker. If the queue stopped, join and close it,
then reconcile through successful offline Inspect before restarting the dispatcher.

Do not mark a `Retired` row accepted from that status alone. Retirement includes holes that were
never submitted. The fixture refuses Retired because it has no completion-frontier history.
An application may advance only when its durable history independently establishes the row's
completion or intentional exclusion. Otherwise stop for reconciliation.

Do not use the largest observed sequence as a scan cursor. Sequence 12 can commit before 11,
and a cursor advanced to 12 would skip the later row forever. Repeatedly scan undispatched rows
or maintain explicit gap state. A handoff frontier records accepted intent. A completion frontier
records established business and queue completion, plus durably excluded holes. Only a contiguous,
proved completion frontier can inform retirement and source replay policy.

If the queue uses `ProcessCrash` (NORMAL), a handoff marker cannot end source recovery obligations
for power-loss protection. Retain the original intent until destination completion is reconciled.
The recovery process must scan even `handed_off = 1` rows whose destination completion is unproved.
For each such row, query the original destination key. A matching successful receipt establishes
the effect. Otherwise preserve uncertainty or retry through destination idempotency. If queue work
is absent and the effect remains owed, rebuild it with the original key, ID, codec, and bytes.
Retaining rows that no recovery process scans does not supply this guarantee.

This outbox does not enforce effect ordering. Distinct keys can commit out of order. If operations
depend on order, enforce the sequence at the destination or serialize the relevant stream.

## Retain receipts through replay and restore

Destination receipts must outlive every path that can replay the original operation. Include queue
redelivery, source backups, outbox backlog, delayed network requests, manual recovery, and recreated
queues. The examples retain receipts indefinitely and refuse new keys at 64 receipts. Existing
keys still resolve at capacity. A bounded queue does not imply a bounded destination history.

Queue `RetireThrough` and cleanup affect only queue records. Neither proves that old network
requests stopped or that a new queue will reject old source work. Successful Ack, queue cleanup,
and age alone do not justify destination receipt expiration. Unbounded replay has no proved
finite receipt TTL.

A production cleanup policy needs a durable, monotone barrier honored by every replay path,
no unresolved work below it, and protection from delayed requests. A destination-specific floor
checked atomically before applying old keys is one possible design. Its identity ordering and
restore policy must preserve that barrier. The examples implement no such floor or receipt cleanup.

Keep destination business state and receipts at one consistent recovery point. Preserve the
SQLite database and surviving WAL together. Restoring only one side breaks the transaction
premise. Even a consistent older destination snapshot can lose effects the queue already
acknowledged. Coordinate source, queue, and destination recovery so those original identities can
be reconciled or replayed. Never rename restored operations to force them through.

## Understand the durability and deployment limits

`Synchronized` uses FULL. Successful queue mutations survive application and system crashes under
the queue's documented storage assumptions. `ProcessCrash` uses NORMAL, which permits recent
submissions, Acks, and retirement floors to roll back after OS crash or power loss. The possible
loss has no bounded time window.

A lost Ack can redeliver a committed effect, which a retained destination receipt absorbs.
A lost sole submission leaves no recoverable intent, which destination idempotency cannot repair.
A lost retirement floor can admit old work again, so the floor cannot justify deleting destination
receipts. A FULL queue Ack also cannot repair destination success reported before its own storage
was durable.

The examples require Linux amd64, cgo, private directories on local ext4, and storage that honors
synchronization. Keep the destination outside the queue directory. Untrusted paths, concurrent
symlink replacement, and production HTTP security are outside their ownership model.
Payloads are bounded at 1,024 bytes, codecs at 64 bytes, and HTTP responses at 4 KiB. The HTTP
service bounds active requests and live connections. Its deadlines request cancellation, but
owners still join handlers and native storage work before resource closure.

The [queue profile](PROFILE.md) describes finite software qualification. Process-kill tests and
modeled lost-response or NORMAL histories establish application behavior for those histories.
They do not establish physical power-loss qualification or qualify another deployment filesystem.
There is no cross-destination atomicity, automatic compensation, or universal exactly-once HTTP
or email guarantee in this pattern.
