# Production durable comparison

This command implements the November 3 FULL/NORMAL comparison protocol. The 80-run campaign
must wait for resource calibration, installation and retesting of a finite WAL envelope,
independent harness review, and parent approval.

Each process creates its supplied directory and refuses an existing directory. Four producers
have separate windows of 256 outstanding items. A producer alone reserves its window, so a
partial batch reservation cannot deadlock with another producer. Credits return only after
successful Ack. Each producer checks the deadline again after waiting for credits. No Submit or
Ack context is canceled. Waves hold at most 8192 IDs, with a 2048-record prefix per producer.
A failed call or oracle aborts the queue, joins workers, preserves the fixture, and marks the
result invalid. Joined-wave counters are merged before any failure return or retirement call.
Failed results retain one bounded wave of base sequences, entered and accepted prefixes,
validated received and acknowledged bitsets, the last received IDs, and the last call decisions.
The entered suffix beyond each accepted prefix identifies an unconfirmed SubmitBatch. Its last
call decision distinguishes Unknown from NotApplied. There are no retries or capacity exceptions.
A 30-second watchdog fails stalled drain or cleanup.
This is a harness failure threshold, not a queue completion-time guarantee. Entered Submit/Ack
calls still join without context cancellation. The campaign runner also limits each measured
subprocess to 120 seconds, with headroom beyond the internal watchdog and samples that took a
few seconds. If the outer timeout expires, the runner kills and waits for the process, marks the
sample and campaign failed, and retains partial output and databases as rejected evidence.

Warmup drains, validates, retires, and waits for cleanup on the same open queue. Measurement
starts afterward. The measured interval includes wave bookkeeping, all drains and retirement,
cleanup, final Close/checkpoint, sampler join, final Snapshot, final file/RSS observation, and
boundary counter collection. Reopen validation is timed separately. Queue boundary Snapshots
provide aggregate commits and groups, not an exact distribution of group sizes.

Submit/Ack latency describes batch calls. Histogram bucket zero includes 0 and 1 ns. Bucket k
contains [2^k, 2^(k+1)) ns. Batch-size arrays index the actual item count. Throughput uses exact
acknowledged records divided by the whole measured interval. CPU is process user plus system.
Go allocation deltas exclude native SQLite allocation. Sampler observations every 20 ms and at
boundaries can miss peaks. Sampling, hashing each delivered payload, channels, credit-wait timers,
and goroutines,
per-wave bookkeeping, and API result allocation add overhead included in the measured totals.
Immutable payloads, entry arrays, and Ack token buffers are allocated before warmup.

From the benchmark directory, build with the approved environment:

```sh
export PATH=/usr/local/go/bin:/home/adam/.go/bin:/usr/bin:/bin:$PATH
export TMPDIR=/home/adam/.chatgpt/scratchpad/go-data-structures/sqlite-durable-autopilot-20261102/tmp
go test ./cmd/durable-comparison -count=1
go test -race ./cmd/durable-comparison -count=1
golangci-lint run ./cmd/durable-comparison/...
python3 -B -m unittest discover -s cmd/durable-comparison -p 'test_*.py'
```

After campaign approval, invoke the runner with a new output directory. It preserves all results,
failures, databases, command lines, source hashes, binary hash, and environment. The runner
builds once in the new output directory. It records the exact Go environment, compiler versions,
build command and Go binary metadata. This is a fixed comparison environment, not a portable
compiler support policy. Builds require GCC and G++ 14.2.0 at `/usr/bin/gcc` and `/usr/bin/g++`.
The compiler flags are `-O2 -g`, with empty `CGO_CPPFLAGS`. Custom CGO flags and compiler,
include, library, or tool-search environment overrides fail before the build. Unset the named
variable and rerun. The runner fixes tool PATH to `/usr/local/go/bin:/usr/bin:/bin` and passes
the same captured, explicit environment to build and sample processes. That environment keeps
allowlisted runtime settings, including `GOMAXPROCS`, `GOGC`, `GOMEMLIMIT`, and `GODEBUG`.
The manifest records this enforced environment.
System compiler headers and libraries belong to that fixed host toolchain, whose executable
hashes and versions are recorded. They are not enumerated by the Go source manifest. The runner
hashes the selected source files of every dependency,
including standard library and cgo inputs, module files, and runner sources before and after the
build. It checks those inputs and the executable before and after every invocation. The result's
revision is the source manifest SHA-256. Git HEAD is separate descriptive metadata.

The runner requires an ext4 output mount whose reported source starts with `/dev/`. It records
the mount's `MAJ:MIN` and the corresponding sysfs path, device metadata, and direct backing-device
links. These are observations of the host's visible storage stack, not proof of physical locality.
The runner also records CPU models, affinity, and visible cgroup-v2 ancestor limits. Unreadable
host fields explicitly report unavailable. It validates
required result fields, scheduled options, frozen runtime configuration, counters, histograms,
actual batches, derived metrics, final counts, and reopen identity and retirement frontiers.
Both resource summaries require positive maximum and final RSS observations. Main and WAL file
sizes may be zero.
Queue commit latency counts must match successful commits. Queue Ack latency counts must match
completed AckBatch calls, while queue Ack outcomes count items. Latency aggregates must obey
duration bounds and remain monotonic. Final snapshots require stopped admission and delivery,
zero owned occupancy, and a successful shutdown checkpoint with zero WAL bytes. It
alternates policy order within each ten-pair cell and stops on the first failure. Review evidence
before removing successful databases or the binary. The runner never deletes evidence.

```sh
python3 cmd/durable-comparison/campaign.py \
  --output /path/to/new/campaign-directory \
  --approved-campaign
```

The standalone Go command remains available for bounded smoke checks. Its `-revision` is a
caller-supplied label. Only a runner-owned campaign build binds that label to a source manifest.
The checked-in JSON fixture is one short real smoke result used only to test the acceptance gate.
It is not a campaign measurement. If production defaults change, review and update the frozen
configuration in `validation.py` before authorizing a campaign.
