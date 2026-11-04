# Durable queue production measurements

Resource calibration completed on November 3, 2026. All 20 cases passed their correctness checks
with no retries or omitted runs. The 64 MiB WAL threshold is installed and passed five fresh
resource cases and independent review. All 80 production comparison runs passed, with ten paired
repetitions per cell and no retries. Final software checks and independent library review also
passed. Physical power-loss qualification was not performed.

## Resource protocol

Each case ran once in a fresh process through the production durable API on local ext4. The matrix
covered 8 and 64 MiB cache settings, mmap disabled and 256 MiB, and both durability policies for
maximum-group churn. Other cases used Synchronized. Logical limits were 4096 pending items, 16 MiB
pending payload, 16,384 receipts, 256 producers and a 1 GiB main database cap. Group delay was zero
for these serial resource cases.

Capacity filled and drained four waves without retirement, verified receipt-cap refusal, then
retired all frontiers. Churn exercised exact 2 MiB encoded batches, mixed payloads and the 16 MiB
payload limit over eight cycles on one connection. Lifecycle added Close, Inspect, bounded Maintain
and reopen. The small-main case exercised real SQLite page-cap refusal and recovered an empty store.

The harness validated IDs, payloads, acknowledgments, retirement metadata, complete counts, store
identity and reopened epochs. It sampled resources every 20 ms and at command boundaries. Sampling
can miss peaks. RSS includes the harness, JSON encoding, input payloads and correctness bookkeeping.
Go heap counters exclude SQLite native allocations. Native heap attribution was unavailable. No
forced GC occurred.

## Observations

| Case | Cache / mmap (MiB) | Policy | Peak WAL (bytes) | Peak RSS (bytes) | Peak Go HeapAlloc (bytes) |
| --- | ---: | --- | ---: | ---: | ---: |
| capacity | 8 / 0 | synchronized | 19,800,752 | 62,406,656 | 39,635,896 |
| maximum-group | 8 / 0 | synchronized | 19,244,552 | 64,598,016 | 41,769,992 |
| lifecycle | 8 / 0 | synchronized | 19,158,032 | 68,440,064 | 41,380,576 |
| small-main | 8 / 0 | synchronized | 45,352 | 17,055,744 | 6,728,104 |
| maximum-group | 8 / 0 | process-crash | 19,236,312 | 64,782,336 | 41,872,400 |
| capacity | 8 / 256 | synchronized | 20,291,032 | 83,292,160 | 38,922,400 |
| maximum-group | 8 / 256 | synchronized | 19,158,032 | 82,563,072 | 41,809,176 |
| lifecycle | 8 / 256 | synchronized | 19,158,032 | 86,466,560 | 41,855,488 |
| small-main | 8 / 256 | synchronized | 45,352 | 18,583,552 | 8,164,168 |
| maximum-group | 8 / 256 | process-crash | 19,248,672 | 82,857,984 | 41,660,832 |
| capacity | 64 / 0 | synchronized | 20,946,112 | 78,725,120 | 39,147,024 |
| maximum-group | 64 / 0 | synchronized | 19,302,232 | 75,751,424 | 42,657,544 |
| lifecycle | 64 / 0 | synchronized | 19,158,032 | 81,108,992 | 42,166,360 |
| small-main | 64 / 0 | synchronized | 45,352 | 16,896,000 | 6,694,120 |
| maximum-group | 64 / 0 | process-crash | 18,696,592 | 75,018,240 | 42,013,392 |
| capacity | 64 / 256 | synchronized | 19,627,712 | 99,950,592 | 38,882,352 |
| maximum-group | 64 / 256 | synchronized | 19,158,032 | 92,549,120 | 41,926,784 |
| lifecycle | 64 / 256 | synchronized | 19,158,032 | 102,961,152 | 42,113,088 |
| small-main | 64 / 256 | synchronized | 45,352 | 16,842,752 | 6,725,392 |
| maximum-group | 64 / 256 | process-crash | 19,063,272 | 93,700,096 | 41,830,376 |

The largest sampled WAL was 20,946,112 bytes, 4,168,896 bytes above the 16 MiB trigger. Its
allocated size was 20,951,040 bytes. The largest positive change between boundary samples was
4,812,160 bytes. This change is not exact transaction growth because cleanup and checkpoints can
intervene. Every maximum-group run observed a 2,097,152-byte encoded group.

The largest main database length and allocation were 22,446,080 bytes. Churn reused a
17,629,184-byte main file without a per-cycle size increase. In each lifecycle case, maintenance
reduced the main file from 17,629,184 to 831,488 to 53,248 bytes. Free pages fell from 4286 to 190
to zero. A third bounded call confirmed no further reduction. Logical state checks passed after
maintenance and reopen.

Peak process RSS was 102,961,152 bytes. Peak Go HeapAlloc was 42,657,544 bytes, HeapInuse was
43,655,168 bytes, and HeapSys was 53,837,824 bytes. Completed cycle-boundary samples showed reuse
rather than monotonic retained growth over eight cycles. These observations do not establish a
universal memory bound.

## Operational provision

The selected WAL refusal threshold is 64 MiB (67,108,864 bytes), about 3.20 times the largest
observed WAL. It allocates no storage in advance. Five fresh cases tested the installed threshold at
cache/mmap extremes, including NORMAL maximum-group churn. All passed once. Their largest sampled
WAL was 20,356,952 bytes and their largest positive boundary change was 4,812,160 bytes. A crossing
transaction can overshoot this threshold before observation stops subsequent writes. The checkpoint
trigger, refusal threshold and available disk are separate quantities.

For one queue at the tested ceilings, start with 512 MiB of process memory plus application-owned
work and exceptional diagnostic/path-resolution allowance, and 1.25 GiB available local disk at the
1 GiB main cap. Monitor the actual workload and filesystem. These are starting provisions with
headroom, not hard RSS or filesystem guarantees, and they provide no perpetual acknowledgment
reserve. The maximum cache and mmap settings alone can expose up to 320 MiB.

## Provenance and limits

The harness built against Go 1.27.3 on Linux amd64 with cgo, github.com/mattn/go-sqlite3 v1.14.52
and bundled SQLite 3.53.4. The production source hashes stayed unchanged during all 20 cases. Git
HEAD advanced from fc0115e to 5060cba because a fault-test-only commit completed during calibration.

Raw JSONL, stderr, exact commands, per-run hashes and environment are retained locally in the
sqlite-durable-autopilot-20261102/round10a-run evidence directory. The files are intentionally
uncommitted. The harness source is in ../cmd/durable-resource.

These results qualify only the finite software workloads described here. They do not test physical
power loss, arbitrary filesystem behavior, a universal WAL bound, or a hard process-memory limit.
Earlier adapter benchmarks used different completion rules and are not directly comparable to this
production workload. The production comparison below uses its own reviewed completion and
measurement protocol.

## Production FULL/NORMAL comparison

The comparison ran on November 3, 2026, from 08:06:13 through 08:10:14 UTC. All 80 fresh processes
passed their correctness and provenance checks. Each of four cells had ten paired FULL/NORMAL
repetitions, with alternating policy order. No run failed, was omitted, or was retried.

Four producers submitted to one consumer with 256 outstanding credits each, for a total cap of 1024.
The runtime used the default 1 ms group delay. ReceiveItems equaled the selected batch limit. Each
process warmed up for 100 ms, then admitted measured work for one second. Timing included all
subsequent acknowledgment, retirement, cleanup, final Close/checkpoint and sampler join.
Consequently, elapsed time could exceed one second substantially. The workload performed no
application work beyond validating IDs and payload digests.

Payload and entry buffers were prepared before warmup. The measurements still include harness
bookkeeping, timers, goroutines, payload hashing and resource sampling. Go allocation counters
exclude native SQLite allocations. A separate reopen check verified identity, epoch, policy, empty
counts and retirement frontiers. The harness source is
[durable-comparison](../cmd/durable-comparison/README.md).

### Throughput

Values are medians across ten processes. Parentheses show the median percentage change across the
ten paired NORMAL/FULL ratios, which need not equal the ratio of the two displayed medians.
Intervals are percentile bootstrap 95% intervals for that paired median, using 10,000 resamples with
seed 20261103. Ten pairs provide limited precision, and intervals do not account for other machines
or workloads.

| Payload (bytes) | Batch limit | FULL (records/s) | NORMAL (records/s) | Paired change interval |
| ---: | ---: | ---: | ---: | --- |
| 256 | 1 | 270.0 | 478.6 (+76.5%) | +53.1% to +82.4% |
| 256 | 256 | 6,549.6 | 7,481.5 (+19.5%) | +9.4% to +25.5% |
| 4096 | 1 | 263.5 | 439.0 (+69.2%) | +63.4% to +76.4% |
| 4096 | 256 | 5,073.2 | 5,072.1 (-0.1%) | -4.2% to +5.6% |

NORMAL improved throughput in both batch-1 cells and the 256-byte batch-256 cell. The 4096-byte
batch-256 cell showed no clear throughput difference. Larger batches substantially increased
throughput under both policies, while weakening durability was unnecessary to obtain that batching
benefit. These are measured workload differences, not a profile of the underlying cause.

### CPU and allocation cost

| Payload / batch | FULL CPU (µs/record) | NORMAL CPU (µs/record) | FULL Go allocation (bytes/record) | NORMAL Go allocation (bytes/record) |
| --- | ---: | ---: | ---: | ---: |
| 256 / 1 | 1,254.1 | 925.1 (-26.1%) | 56,897 | 45,890 (-19.81%) |
| 256 / 256 | 146.4 | 142.6 (-2.8%) | 10,080 | 10,079 (-0.01%) |
| 4096 / 1 | 1,382.6 | 1,016.3 (-25.1%) | 71,810 | 58,445 (-18.27%) |
| 4096 / 256 | 195.2 | 190.9 (-1.9%) | 21,620 | 21,623 (+0.01%) |

CPU is process user plus system time per acknowledged record. Batch-1 allocation totals were much
larger than the batched totals, and differed by policy. They include the complete timed process
workload, so these results do not identify a per-call queue allocation cost or establish its cause.

### Batch-call latency and drain time

Latency values below are medians of each process's mean call latency, not tail percentiles or
per-record latency. Each call includes waiting for its result.

| Payload / batch | FULL Submit (ms/call) | NORMAL Submit (ms/call) | FULL Ack (ms/call) | NORMAL Ack (ms/call) | FULL elapsed (s) | NORMAL elapsed (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 / 1 | 3.929 | 2.511 (-35.7%) | 3.666 | 2.027 (-43.7%) | 3.773 | 3.021 (-19.0%) |
| 256 / 256 | 28.153 | 24.953 (-10.7%) | 17.604 | 16.046 (-11.5%) | 1.209 | 1.096 (-11.7%) |
| 4096 / 1 | 4.288 | 2.773 (-33.9%) | 3.755 | 2.213 (-41.0%) | 3.604 | 3.120 (-14.0%) |
| 4096 / 256 | 37.630 | 33.010 (-12.0%) | 22.673 | 23.712 (+4.9%) | 1.226 | 1.184 (-3.1%) |

In the batch-256 cells, the median of the per-process mean Receive/Ack batch sizes was 128. A limit
of 256 does not mean every acquisition returns 256 records. Raw results retain the full batch-size
and call-latency histograms. The large batch-1 drain cost is included in the throughput denominator,
as the protocol requires.

### Every throughput sample

Each row is one paired repetition, in records/s. The policy that ran first alternated by repetition.

| Payload / batch | Repetition | FULL | NORMAL |
| --- | ---: | ---: | ---: |
| 256 / 1 | 1 | 269.2 | 468.7 |
| 256 / 1 | 2 | 252.4 | 445.2 |
| 256 / 1 | 3 | 268.1 | 488.0 |
| 256 / 1 | 4 | 270.1 | 496.8 |
| 256 / 1 | 5 | 265.4 | 509.2 |
| 256 / 1 | 6 | 274.6 | 473.2 |
| 256 / 1 | 7 | 270.0 | 356.4 |
| 256 / 1 | 8 | 273.4 | 248.7 |
| 256 / 1 | 9 | 274.2 | 483.9 |
| 256 / 1 | 10 | 272.7 | 493.3 |
| 256 / 256 | 1 | 6,546.7 | 7,881.2 |
| 256 / 256 | 2 | 6,605.2 | 7,228.3 |
| 256 / 256 | 3 | 4,408.3 | 7,072.1 |
| 256 / 256 | 4 | 6,552.5 | 7,128.9 |
| 256 / 256 | 5 | 6,587.3 | 7,855.4 |
| 256 / 256 | 6 | 6,026.7 | 7,868.6 |
| 256 / 256 | 7 | 6,570.7 | 7,907.0 |
| 256 / 256 | 8 | 6,489.7 | 7,269.7 |
| 256 / 256 | 9 | 6,425.4 | 7,693.3 |
| 256 / 256 | 10 | 6,588.2 | 7,126.8 |
| 4096 / 1 | 1 | 253.7 | 417.9 |
| 4096 / 1 | 2 | 258.3 | 444.9 |
| 4096 / 1 | 3 | 245.4 | 442.7 |
| 4096 / 1 | 4 | 264.9 | 432.7 |
| 4096 / 1 | 5 | 266.1 | 452.9 |
| 4096 / 1 | 6 | 266.6 | 435.2 |
| 4096 / 1 | 7 | 265.9 | 419.2 |
| 4096 / 1 | 8 | 262.2 | 457.7 |
| 4096 / 1 | 9 | 233.8 | 434.8 |
| 4096 / 1 | 10 | 266.2 | 447.7 |
| 4096 / 256 | 1 | 5,071.9 | 5,882.6 |
| 4096 / 256 | 2 | 5,037.4 | 5,317.7 |
| 4096 / 256 | 3 | 5,052.0 | 5,219.0 |
| 4096 / 256 | 4 | 4,774.6 | 4,618.5 |
| 4096 / 256 | 5 | 4,872.4 | 5,158.5 |
| 4096 / 256 | 6 | 5,157.4 | 4,893.6 |
| 4096 / 256 | 7 | 5,153.6 | 4,985.7 |
| 4096 / 256 | 8 | 5,143.7 | 4,852.9 |
| 4096 / 256 | 9 | 5,074.4 | 4,970.4 |
| 4096 / 256 | 10 | 5,089.5 | 5,182.5 |

### Comparison provenance and limits

The campaign used an AMD Ryzen 7 5800X, affinity CPUs 0 through 15, and GOMAXPROCS 16. GOMAXPROCS,
GOGC, GOMEMLIMIT and GODEBUG had no explicit environment overrides. The output mount was local WSL2
ext4 on /dev/sdd. The manifest records visible cgroup limits, mount options, compiler versions and
enforced build settings. The compiler environment was restricted to the reviewed defaults. System
headers and libraries remained host inputs rather than individually hashed source inputs.

The measured HEAD was `7e3ed78d0843910fca657fc8239bd802821b3d3a`. The selected-source manifest
revision was `sha256:de7e8377de18d6753f9ea825b8b6617199421e9bc55ff539dcfc3126b47d7f36`. The
executable SHA-256 was `234f9a196174a19d2e4fad7e561278fad25c89a4462f22543ee312d472a3d449`. The
runner checked source and binary hashes before and after every sample.

Across the 80 runs, sampled RSS reached 30,257,152 bytes, WAL length reached 18,144,512 bytes, and
main database length reached 6,139,904 bytes. These were below the earlier resource campaign
observations. WAL stayed below the installed threshold. Sampling can miss peaks. The raw results,
paired calculations, all CPU/allocation samples, intervals and build provenance remain local in the
session's `round10c-run` directory. The 80 successful fixture databases and executable were removed
after evidence review. All 171 retained evidence files kept identical before/after hashes.

Keep FULL as the default for acknowledged operations that must survive system crashes under the
documented storage assumptions. NORMAL remains an explicit choice when recent successful operations
may be lost after an OS crash or power loss. Its measured speed advantage varied with batching and
payload size. This comparison did not test a physical power cut, guarantee performance on another
host, or compare directly against the earlier adapter workload.
