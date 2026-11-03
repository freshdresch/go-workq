# sqlite-wal-v1 profile manifest

Finite software qualification completed on November 3, 2026, for the tested Linux amd64 build
on WSL2 local ext4. All 20 resource cases and five reruns with the installed 64 MiB envelope passed.
The production FULL/NORMAL comparison completed all 80 runs, and each passed its validators.
The final independent library review reported no findings. Physical power-loss qualification
was not performed. These results do not establish universal resource bounds, arbitrary corruption
containment, or exactly-once external effects.

## Installed operating limits

The private profile in `config.go` fixes the WAL refusal envelope at 67,108,864 bytes (64 MiB).
Every supported runtime setting uses that value. There is no calibration bypass or caller override.
The checkpoint trigger accepts 4 through 16 MiB and defaults to 16 MiB. Before a write, the owner
checks cached and current WAL observations before checkpointing. It also observes after each group.
A WAL observation strictly greater than 64 MiB enters irreversible MaintenanceRequired. Equality
is allowed. New delivery and unstarted mutations stop, including empty and no-op mutations.
An already committed operation retains Applied with nil error. Healthy Lookup and Release remain
available. A later lower observation does not reactivate the session.

The envelope is an operational refusal threshold, not a hard file-size or allocated-block cap.
The crossing transaction can overshoot it. It reserves no disk blocks and guarantees no permanent
Ack reserve. The main database has a separate maximum of 1 GiB, enforced through max_page_count.
Logical maxima remain 4096 pending items, 16 MiB pending payload, 16,384 receipts, 256 lifetime
producers, 16 KiB payloads, and 64-byte codecs. A public Submit and a group each permit at most
2 MiB encoded work. Cache accepts 8 through 64 MiB, and mmap accepts 0 through 256 MiB.

## Calibration and provision

The production resource campaign ran 20 cases across cache/mmap pairs 8/0, 8/256, 64/0, and
64/256 MiB. All cases passed their logical validators. It exercised capacity, maximum groups,
lifecycle reclamation, small main-page caps, and NORMAL maximum groups. The maximum encoded group
was 2,097,152 bytes. The largest sampled WAL was 20,946,112 bytes, which exceeded the maximum
checkpoint trigger by 4,168,896 bytes. The largest positive command-boundary WAL change was
4,812,160 bytes. That change is not an exact transaction-growth measurement because checkpoints
and cleanup can intervene.

The installed envelope is about 3.20 times the largest sampled WAL and leaves 46,162,752 bytes
above that observation. It is 48 MiB above the largest supported checkpoint trigger. Those margins
come from finite calibration and an explicit operating decision, not an all-history growth proof.

The sampled process RSS maximum was 102,961,152 bytes, and the Go HeapAlloc maximum was
42,657,544 bytes. These measurements include the harness and can miss peaks. Native heap
attribution was unavailable. Repeated churn showed reuse at measured cycle boundaries. The four
lifecycle variants reclaimed the main file from 17,629,184 to 53,248 bytes in two bounded Maintain
calls while preserving their logical checks.

For one queue at the tested ceilings, the initial deployment recommendation is 512 MiB process
memory plus application-owned work and diagnostic/path-resolution allowance, and 1.25 GiB available
local disk. These are starting provisions, not universal RSS or disk guarantees. Cache and mmap
maxima alone permit up to 320 MiB of pager/mapping exposure. Monitor the actual workload and keep
headroom for WAL overshoot, shared memory, filesystem metadata, and maintenance.

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
See [the benchmark report](../../benchmark/workq/durable.md) for measurements and interpretation.

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
