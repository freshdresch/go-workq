# Transactional inventory destination

This command applies three inventory adjustments through `durable`. Each destination
transaction commits the inventory change and its receipt together. A matching retry returns the
saved result. The worker acknowledges its queue delivery only after destination success.

The guarantee requires all destination writers to follow this protocol, durable original source
identities and bytes, retained receipts, and storage that honors synchronization. These conditions
provide at most one committed effect per key. Eventual completion also requires retries, available
capacity, and a destination that eventually responds. Queue acknowledgment is a separate transaction.

## Run and repeat

Use Linux amd64, a cgo-enabled Go toolchain compatible with `go.mod`, and private directories on
local ext4. The queue enforces its existing storage profile. Run commands from the repository root.
Choose a new operator-owned parent directory. Keep both databases and any surviving WAL files for
restart. The destination file must be outside the queue directory. Existing parent directories must
resolve successfully. Equal and nested paths, including resolved symlink aliases, are rejected.

1. Create the private parent directory on local ext4.

   ```sh
   mkdir -m 700 "$HOME/workq-transactional-demo"
   ```

2. Run the command.

   ```sh
   go run ./examples/transactional \
     -directory "$HOME/workq-transactional-demo/queue" \
     -destination "$HOME/workq-transactional-demo/inventory.sqlite"
   ```

3. Repeat the same command with the same paths.

The first run reports `attempts=3 commits=3 duplicates=0 acknowledged=3`, followed by
`quantity=14 adjustments=3`. The completed repeat reports zero invocation counters and the same
persistent quantity and adjustment count. Each completed delivery logs its effect key, producer,
sequence, destination success, duplicate observation, and queue acknowledgment. Payloads are not
logged. Output errors propagate, but output is outside the transaction and can be lost or repeated.

Bazel provides the same command. Use a distinct new private parent directory for a separate demo.

```sh
bazelisk run //examples/transactional:transactional -- \
  -directory "$HOME/workq-transactional-demo/queue" \
  -destination "$HOME/workq-transactional-demo/inventory.sqlite"
```

## Stop between destination commit and Ack

Use another new private parent directory on local ext4 for this demonstration.

1. Create the parent directory.

   ```sh
   mkdir -m 700 "$HOME/workq-transactional-interruption"
   ```

2. Request the deliberate interruption.

   ```sh
   go run ./examples/transactional \
     -directory "$HOME/workq-transactional-interruption/queue" \
     -destination "$HOME/workq-transactional-interruption/inventory.sqlite" \
     -stop-after-commit
   ```

3. Run the command again without `-stop-after-commit`, with the same paths.

The interrupted command returns an error after the first known destination success, before Ack.
On fresh paths it reports quantity 13 and one adjustment, with all three entries still pending.
This is an orderly demonstration interruption, not a process-crash test. Restart obtains a current
queue token, resolves the saved result, and acknowledges that token. It then finishes the remaining
adjustments. The restart reports `attempts=3 commits=2 duplicates=1 acknowledged=3` and persistent
quantity 14 with three adjustments. Another completed run does no work.

## Identity and recovery

`source.go` is this demo's checked-in durable source intent. Its namespace is
`workq-transactional-demo-v1`, its 16-byte producer is `workq-tx-demo-v1`,
and its sequences are 1, 2, and 3. The exact JSON requests adjust `widget` by +3, -1, and +2.
The effect is `inventory.adjust` and the codec is `inventory-adjustment-v1`. The initial quantity
is 10. A key contains namespace, producer, sequence, and effect. Queue tokens and queue UUIDs are
not part of it.

Preserve the original namespace and exact bytes when replaying records, even through a recreated
queue. Changing the source records or resetting sequence numbers requires a new source incarnation
and namespace. Renaming old work defeats deduplication. Different bytes under an existing key
produce a conflict and leave work pending.

Unknown submission decisions require healthy ordered queue Lookup before delivery starts. Pending
or Acknowledged establishes acceptance for these immutable records. Absent permits identical
resubmission. Retired or a lookup error requires reconciliation. Unknown Ack decisions use ordered
Lookup and retry only the exact current token when Pending. A stale token stops the command.
Submission, apply, and Ack operations have at most three attempts. On exhaustion, preserve both
paths and restart after correcting the cause. Reopening an existing queue first performs offline
Inspect. A stopped queue is closed before any later inspection and reopen.

Each apply attempt requests a five-second deadline. A known success can finish its Ack under a
fresh five-second context even after worker cancellation. Cancellation prevents another effect
attempt. Native SQLite Commit or Rollback can outlast those deadlines. The owner waits for native
work and cleanup before closing resources. An uncertain commit or rollback poisons the destination and stops this owner.
A busy begin on a healthy destination retries the whole same-key operation within the attempt limit.
Only a subsequent command invocation opens a fresh Store and resolves the original request.
Rejected requests and capacity failures do not cause automatic Ack.

If cancellation is already visible when `inventory.Open` starts with valid input, it returns
before creating the destination. A healthy retry can initialize that fresh path. Cancellation
that races after the initial check, initialization failure, or process death can leave the
destination and any surviving WAL. `Open` never deletes or reinitializes an existing incomplete
destination. Same-path restart requires a complete valid schema. An error alone does not prove
that no business effect exists.

Stop and preserve a failed destination and its WAL. Use a fresh destination path only when
authoritative evidence establishes failed first initialization and no business effect ever ran
there. File size or missing tables alone is not such evidence. The demo owner starts no business
work before successful initialization, but operators must also exclude prior use and other writers.
Otherwise, preserve the database and WAL and investigate or restore receipts and business state
at one consistent recovery point. Do not erase a destination to make a restart error disappear.
Preserve original source identities and bytes. A new namespace is not a recovery workaround.
Existing complete destinations and normal commit-before-Ack recovery use their original paths.

## Retention and limits

The queue uses `Synchronized` with profile `sqlite-wal-v1`, a 64 MiB database limit, 16 pending items,
16,384 pending payload bytes, 64 receipts, four producers, 1,024-byte payloads, and 64-byte codecs.
The destination uses WAL and FULL synchronization, a 250 ms busy timeout, and 64 permanent receipts.
It refuses new keys at capacity while still resolving existing keys. Queue retirement and cleanup
do not remove destination receipts and do not establish a safe destination expiration time.

Keep the destination's receipts and business state at one consistent recovery point. Restoring an
older destination can lose effects that the queue already acknowledged. Disaster recovery needs
an application replay policy beyond this example. Untrusted paths and another writer replacing
symlinks are outside this private-directory ownership model.

The persistent quantity, adjustment count, and complete receipt rows are the destination oracle.
Invocation counters record observations only. The existing queue qualification covers finite
software tests on Linux amd64/local ext4. This example does not claim physical power-loss
qualification or extend that qualification to other filesystems.
