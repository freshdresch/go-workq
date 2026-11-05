# Durable storage formats and qualification

Historical v1 software qualification completed on November 3, 2026, for the tested Linux amd64 build
on WSL2 local ext4. All 20 resource cases and five reruns with the installed 64 MiB envelope passed.
The production FULL/NORMAL comparison completed all 80 runs, and each passed its validators.
The final independent library review reported no findings. Physical power-loss qualification
was not performed. These results do not establish universal resource bounds, arbitrary corruption
containment, or exactly-once external effects.

## Format, policy, and runtime limits

`sqlite-wal-v1` retains its original schema and durable ceilings. `sqlite-wal-v2` removes literal
payload and codec ceilings from the schema. Both preserve IDs, digests, tokens, and accounting.
Open requires an exact policy match. Offline Resize changes policy and can migrate v1 to v2.
There is no automatic migration or downgrade. See the [usage guide](README.md) for shrink and
Unknown reconciliation rules.

V2 requires positive payload, pending item/byte, receipt, producer, and main-file limits.
MaxCodec may be zero. Pending bytes must cover one maximum payload, and receipts must cover
pending items. Counts fit uint32. Checked `MaxPayload + MaxCodec + 128` must fit the pinned
1,000,000,000-byte SQLite row limit and the connection's effective length limit. The main cap
is a positive multiple of 4096, with at most 4,294,967,294 pages. These structural limits do not
promise that configured logical capacity fits the main cap or the machine's memory.

Runtime counts and byte budgets are caller-selected, subject to checked arithmetic, allocation
representability, and minimum relationships. DefaultRuntime preserves default counts and raises
byte budgets for larger records. SQLite cache rounds down to KiB, with a positive signed-32-bit
KiB setting. BusyTimeout rounds down to signed-32-bit milliseconds. Mmap requires exact readback.
The pinned Linux backend refuses requests above 2,147,418,112 bytes instead of silently clamping.

The default WALRefusal is 67,108,864 bytes (64 MiB), including when its field is explicitly zero.
Require `0 < CheckpointTrigger < WALRefusal <= MaxInt64`. The default trigger is 16 MiB.
Cached and current observations prevent a later low sample from bypassing refusal. An observation
strictly above the threshold irreversibly stops new delivery and unstarted mutations for that
session. Equality is allowed. A crossing commit remains Applied. Healthy Lookup and Release
remain available. The threshold reserves no disk and is not a hard file-size or allocated-block
cap. It does not apply to offline migration. There is no permanent Ack reserve.

## Parameterized working-memory ledger

Let `A(n) = 2 * roundUp(n, 8)` be the supported-build allocator allowance, with `A(0) = 0`.
Let O and P be Ordinary.Commands and Progress.Commands, D be Delivery.Items, G be Group.Commands,
and R be D rounded up to a power of two. Sizes below are `unsafe.Sizeof` on the qualified build.
The configuration-dependent array allowance is:

```
A(O * sizeof(command)) + A(P * sizeof(command))
+ A(D * sizeof(claim))
+ A(R * sizeof(int)) + 2 * A(R * sizeof(uint64))
+ A(G * sizeof(groupedCommand))
```

Add Ordinary.Retained, Progress.Retained, Delivery.Retained, and Delivery.ReadScratch for admitted
work and live delivery storage. Reservations precede input copying and survive abandoned calls
until the owner releases them. Receive descriptor allowances are included in delivery accounting.
Add fixed queue/store/owner headers, paths, channels, timers, and the 256-by-24-byte cleanup key
buffer with allocator rounding. Larger cleanup quotas reuse that buffer in bounded chunks.
`layout_test.go` checks the arrays and fixed objects. Its historical 1 MiB fixed-storage and
48 KiB group assertions apply only to the default configuration, not arbitrary runtime counts.

Startup and offline validation run separately from live pools. They transfer codec and payload
through fixed 32 KiB chunks, independent of backlog and maximum record size. One driver source
and one Scan copy can coexist, so allow two chunks plus fixed record, hash, metadata, and schema
scratch. Migration transfers at most 256 keys at a time and copies payload rows inside SQLite.
The validation traversal can take time proportional to history.

This ledger describes queue-owned algorithmic working storage. It excludes caller input and
retained delivery storage, waiting caller lifetimes, diagnostic graphs/formatting, path-resolution
temporaries, GC-retained garbage, and native SQLite/pager/integrity/schema allocations. See the
[ownership boundaries](README.md) for those exclusions. Cache and mmap are separate backend
exposure. Neither this ledger nor acceptance of a representable configuration bounds total RSS.

## Finite expanded-policy check

`TestLargeLimitsRealAPI` uses v2 with 10,000 pending items, 64 MiB pending bytes, 20,000 receipts,
512 producers, 64 KiB payloads, 256-byte codecs, and a 2 GiB main cap. With default runtime counts,
it submits 9000 entries over 300 producers and observes 18,462,721 pending payload bytes.
One payload has 32,769 bytes and codecs have 65 bytes. Close, Inspect, reopen, and bounded
Receive/Ack calls verify identities and digests. A second 9000-entry wave establishes 18,000
retained acknowledged receipts before retirement and cleanup. This finite correctness check
extends behavioral coverage. It does not measure RSS, disk peaks, throughput, or latency.

## Historical installed configuration

The campaign below used v1 with 4096 pending items, 16 MiB pending payload, 16,384 receipts,
256 lifetime producers, 16 KiB payloads, 64-byte codecs, and a 1 GiB main cap. Submit and group
encoded limits were 2 MiB. Cache accepted 8 through 64 MiB and mmap 0 through 256 MiB in that
implementation. The installed WAL threshold was fixed at 64 MiB, with a 4 through 16 MiB
checkpoint trigger. Those historical runtime ceilings no longer restrict either storage format.
The numbers and provenance below remain evidence only for those tested settings.

## Calibration and provision

The production resource campaign ran 20 cases across cache/mmap pairs 8/0, 8/256, 64/0, and
64/256 MiB. All cases passed their logical validators. It exercised capacity, maximum groups,
lifecycle reclamation, small main-page caps, and NORMAL maximum groups. The maximum encoded group
was 2,097,152 bytes. The largest sampled WAL was 20,946,112 bytes, which exceeded the maximum
checkpoint trigger by 4,168,896 bytes. The largest positive command-boundary WAL change was
4,812,160 bytes. That change is not an exact transaction-growth measurement because checkpoints
and cleanup can intervene.

The historical installed envelope was about 3.20 times the largest sampled WAL and leaves
46,162,752 bytes above that observation. It was 48 MiB above the largest then-supported
checkpoint trigger. Those margins come from finite calibration and an explicit operating
decision, not an all-history growth proof.

The sampled process RSS maximum was 102,961,152 bytes, and the Go HeapAlloc maximum was
42,657,544 bytes. These measurements include the harness and can miss peaks. Native heap
attribution was unavailable. Repeated churn showed reuse at measured cycle boundaries. The four
lifecycle variants reclaimed the main file from 17,629,184 to 53,248 bytes in two bounded Maintain
calls while preserving their logical checks.

For one queue at those historical tested ceilings, the initial deployment recommendation was 512
MiB process memory plus application-owned work and diagnostic/path-resolution allowance, and
1.25 GiB available local disk. These are starting provisions, not universal RSS or disk
guarantees. The tested cache and mmap maxima alone permitted up to 320 MiB of pager/mapping
exposure. Monitor the actual workload and keep headroom for WAL overshoot, shared memory,
filesystem metadata, and maintenance.

Evidence is recorded in the session's `round10a-run` directory and `wal-envelope-decision.md` under
`/home/adam/.chatgpt/scratchpad/go-data-structures/sqlite-durable-autopilot-20261102`.
Round 10b retains fresh build information, source hashes, test logs, and installed-envelope reruns
in that session's `round10b` directory. Final software checks and the independent library review
are recorded in `final-checks/ASSESSMENT.md` and `final-independent-review/REPORT.md`.
These artifacts are local development evidence, not a portable release attestation.

The production comparison used commit `7e3ed78`, with ten FULL/NORMAL pairs in each of four
workload cells. Its run manifest is `round10c-run/manifest.json` in the same session.
The source SHA-256 was
`de7e8377de18d6753f9ea825b8b6617199421e9bc55ff539dcfc3126b47d7f36`.
The executable SHA-256 was
`234f9a196174a19d2e4fad7e561278fad25c89a4462f22543ee312d472a3d449`.
See [the benchmark report](../benchmark/workq/durable.md) for measurements and interpretation.

## Dependency, build, and deployment scope

Both module graphs pin `github.com/mattn/go-sqlite3` v1.14.52 with bundled SQLite 3.53.4.
The header source ID is
`2026-07-24 19:02:57 bf7c7f30031888f4e796e429ab3978879485813aaca6f641c7b33e4e09459bcc`.
Do not substitute a system SQLite library or enable the `libsqlite3` build tag.
The module language minimum remains Go 1.25.5. The measured resource executable used Go 1.27.3,
linux/amd64 with GOAMD64=v1, cgo enabled, and gcc 14.2.0. The Go build used the driver defaults,
without optional SQLite feature tags. Default cgo CFLAGS and LDFLAGS were `-O2 -g`.
The driver adds `-std=gnu99`, `-DSQLITE_ENABLE_RTREE`, `-DSQLITE_THREADSAFE=1`,
`-DHAVE_USLEEP=1`, `-DSQLITE_ENABLE_FTS3`, `-DSQLITE_ENABLE_FTS3_PARENTHESIS`,
`-DSQLITE_TRACE_SIZE_LIMIT=15`, `-DSQLITE_OMIT_DEPRECATED`,
`-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1`, `-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT`, and
`-Wno-deprecated-declarations`.
The queue explicitly sets and reads back synchronous policy rather than relying on that default.

Bazelisk uses the repository's Bazel 9.3.1 pin, rules_go 0.63.0, and Gazelle 0.52.2. Its dependency
graph also uses the bundled pinned driver. Build/test success does not alone qualify another
compiler, toolchain, allocator, optional feature set, or effective mmap limit. Recheck the memory
ledger and resource cases for a changed supported build. Final Go and Bazel dependency checks
confirmed that normal builds exclude the fault shim. The Go vulnerability scan reported no
findings, but it does not audit the SQLite C amalgamation. Native AddressSanitizer checks passed.
The final checks could not run clang-format, clang-tidy, or buildifier because those tools were
unavailable.

The supported deployment scope is Linux amd64 on qualified local ext4, with a private queue
directory, working advisory locks and file/directory synchronization, and storage that honors
sync. Measurements used WSL2 ext4. They do not qualify physical power-loss behavior. Network
filesystems, Windows-mounted WSL paths, and unqualified overlay/FUSE deployments are excluded.
The Synchronized policy uses FULL. ProcessCrash uses NORMAL and can lose recent successful
operations after an OS crash or power loss. Preserve the database and WAL together.
