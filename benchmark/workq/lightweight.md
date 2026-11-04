# Lightweight record batching results

All 660 processes completed successfully in 51 minutes 12 seconds: 330 clean timing runs and
330 separate diagnostic runs. Every cell retained ten paired process repetitions. There were
no retries or exclusions. All process checks passed, all stderr files were empty, and the
reviewed manifest matched before and after execution.

Workq batching produced a substantial advantage over individual-record channels for minimal
and approximately 100 ns processing. At approximately 1 us, that advantage disappeared.
Channels carrying immutable prebuilt slices were faster and used less CPU than workq at
batch 32. This is a best-case alternative with different ownership and copying requirements,
not a like-for-like record transport comparison. It hands off descriptors without copying
records at the queue boundary. The individual-record channel is the closer transport baseline.

## Workload and measurement

Two producers and two consumers transferred immutable 32-byte records through a 1024-record
buffer. GOMAXPROCS was 4, with affinity to four distinct cores (0,2,4,6). Go was 1.27.3.
The unchanged library source was commit 021f5c05050e1ca6749cabdaed90cf2f050645c6.
Each producer repeatedly traversed its half of a 65,536-record, 2 MiB corpus. The corpus was
constructed before measurement. This is a cache-friendly dispatch workload, not a model of
unbounded streaming data, mutable ownership transfer, parsing, or memory allocation.

All consumers classified records and accumulated private checksums. Additional dependent
arithmetic used 0, 82, or 824 iterations. The isolated arithmetic calibration measured about
0.76 ns, 96.2 ns, and 1038.7 ns respectively. These labels exclude classification, checksum
bookkeeping, and transport. Arithmetic results contributed to checked outputs.

Workq used default MPMC/wait settings and PushBatch/Receive limits of 1, 8, or 32. Batch one
used a length-one PushBatch, not the separate Push API. Individual-record channels sent each
record separately, then blocked for the first receive and drained up to the selected size.
The channel-of-slices control buffered 1024/batch immutable slice descriptors. Consumers
processed each slice as one batch. All transports had equal buffered-record capacity, with
additional in-flight batches outside that bound. The slice control retained the same corpus.

Sustained record counts were fixed across backends and batch sizes within each cost level:
1,012,400,128 minimal, 60,751,872 at 100 ns, and 7,340,032 at 1 us. Bursts contained 8192
records, with 198, 193, or 161 bursts per process respectively and 20 ms idle after each burst.
CPU measurements for bursts include idle. Service throughput excludes idle.

Workers persisted across warmup and measurement. Completion used two serial markers after
producers finished. Service time includes dispatch and that completion barrier. Workers waited
on harness job channels between bursts, so burst results do not isolate native queue idle
wakeup. An empty reference epoch had median 3.68 us, but that value was not subtracted and is
not a loaded-barrier estimate. GC remained enabled, with one forced collection after warmup
and before measurement. No measured timing run had a garbage collection.

Each process was the independent repetition unit. Tables show medians across ten processes.
Paired changes are medians of within-block ratios. Intervals are percentile bootstrap 95%
intervals from 10,000 whole-block resamples, seed 2026102903. They are exploratory and not
adjusted for multiple comparisons. A zero-crossing interval does not establish equivalence.
Ratios of displayed medians can differ from paired ratios.

## Sustained throughput and CPU

M = million completed records per second. CPU is total process CPU nanoseconds per record.

| Work | Batch | Workq M/s | Channel M/s | Slices M/s | Workq ns | Channel ns | Slices ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| minimal | 1 | 27.716 | 22.825 | n/a | 130.03 | 89.72 | n/a |
| minimal | 8 | 113.966 | 22.760 | 149.633 | 33.05 | 89.86 | 13.34 |
| minimal | 32 | 236.551 | 23.431 | 337.592 | 15.42 | 87.01 | 6.04 |
| 100ns | 1 | 7.699 | 8.245 | n/a | 334.67 | 275.58 | n/a |
| 100ns | 8 | 17.387 | 10.083 | 17.200 | 142.95 | 225.71 | 126.58 |
| 100ns | 32 | 18.612 | 11.674 | 19.454 | 120.34 | 194.10 | 112.08 |
| 1us | 1 | 1.742 | 1.682 | n/a | 1325.96 | 1312.14 | n/a |
| 1us | 8 | 1.836 | 1.803 | 1.862 | 1242.98 | 1239.52 | 1180.37 |
| 1us | 32 | 1.815 | 1.828 | 1.881 | 1244.61 | 1211.34 | 1161.59 |

Paired workq changes at batch 32, relative to individual-record channels:

| Work | Throughput change [95% interval] | CPU/record change [95% interval] |
| --- | --- | --- |
| minimal | +915.34% [+894.79%, +925.57%] | -82.15% [-82.31%, -82.01%] |
| 100ns | +58.54% [+56.31%, +60.83%] | -37.88% [-38.72%, -37.23%] |
| 1us | -0.78% [-1.68%, +0.03%] | +2.91% [+2.06%, +3.71%] |

The minimal-work paired throughput ratio was about 10.15x, with 82% less CPU per record.
At 100 ns, workq delivered 59% more throughput with 38% less CPU. Both clearly exceeded the
proposed 10% practical-benefit threshold. At 1 us, throughput changed by about -0.8% with an
interval crossing zero, while workq used about 2.9% more CPU. The crossover lies somewhere
between the tested 100 ns and 1 us levels on this host. These three points cannot locate it
more precisely or establish a portable threshold.

Within workq, increasing minimal-work batches from 1 to 32 increased throughput about 8.58x
and reduced CPU per record 88%. The corresponding record-channel receive-batch change improved
minimal-work throughput only about 3%. This supports batching as a major source of the win.
It does not isolate an individual internal mechanism such as wake groups or atomic operations.

Batch one was not a general efficiency win. At minimal work, workq delivered 22% more throughput
but used 44% more CPU than record channels. At 100 ns, it delivered 7% less throughput and used
22% more CPU. The useful claim here is about batch-oriented use, not universal superiority.

## Immutable-slice channel control: a best-case alternative

Paired workq changes at batch 32 relative to channels carrying whole immutable slices:

| Work | Throughput change [95% interval] | CPU/record change [95% interval] |
| --- | --- | --- |
| minimal | -30.12% [-30.56%, -28.35%] | +155.09% [+151.98%, +159.29%] |
| 100ns | -4.00% [-5.22%, -3.87%] | +7.10% [+6.85%, +8.27%] |
| 1us | -3.21% [-4.18%, -2.89%] | +7.06% [+6.67%, +7.90%] |

The slice channel reached about 338 million records/s for minimal work, versus 237 million
for workq. Workq throughput was about 30% lower and CPU/record about 2.55x the slice channel.
At 100 ns and 1 us, throughput differences narrowed to about 4% and 3%, while workq still used
about 7% more CPU. The application can achieve very efficient dispatch if it already owns
complete immutable batches and can hand off descriptors. This control is not equivalent to
copying individual values into a queue or dynamically repartitioning received batches.

The slice control eliminates work that workq performs. Workq copies records into its queue and
then into caller-owned receive buffers. The slice channel passes a descriptor that refers to
existing storage. One complete producer batch goes to one consumer. Workq lets consumers acquire
batches independently of producer batch boundaries.

The control relies on prebuilt batches whose backing storage stays immutable and valid until
consumers finish. This experiment retains that storage for the entire process. It does not
measure the cost of constructing fresh batches, coordinating mutable buffer ownership, or
recycling buffers. Applications that need those operations would have additional costs, whose
magnitude this experiment did not establish.

The control therefore shows what an application can achieve when it can avoid record copying
and already owns suitable batches. It does not establish that workq performs equivalent work
less efficiently. Workq's substantial batching advantage over individual-record channels remains
the relevant result for the closer comparison.

## Finite bursts

Median completion time for 8192 records, batch 32, excluding configured idle:

| Work | Workq us | Record channel us | Slice channel us |
| --- | ---: | ---: | ---: |
| minimal | 68.63 | 401.53 | 39.61 |
| 100ns | 550.44 | 813.74 | 522.25 |
| 1us | 4624.26 | 4646.42 | 4528.49 |

Against individual-record channels, paired median burst completion was about 83% shorter for
minimal work and 32% shorter at 100 ns. The 1 us difference was below 1% and its median-burst
interval included zero. Burst CPU/record fell about 74% and 32% for minimal and 100 ns work.
At 1 us the CPU interval included zero. Whole-slice channels completed the bursts fastest.
These are closed-loop finite-burst results with harness dispatch and completion costs, not
open-loop latency guarantees or a new test of idle consumer wake latency.

## Actual batching and allocations

Separate diagnostics used ten repetitions per cell with counters removed from clean builds.
They processed 65,536 sustained records or one 8192-record burst. Means below are per-process
application receive sizes, summarized by their median across repetitions.

| Profile | Work | Batch | Workq | Record channel | Slice channel |
| --- | --- | ---: | ---: | ---: | ---: |
| sustained | minimal | 8 | 8.00 | 7.97 | 8.00 |
| sustained | minimal | 32 | 32.00 | 31.39 | 32.00 |
| sustained | 100ns | 8 | 8.00 | 7.99 | 8.00 |
| sustained | 100ns | 32 | 32.00 | 31.84 | 32.00 |
| sustained | 1us | 8 | 8.00 | 8.00 | 8.00 |
| sustained | 1us | 32 | 32.00 | 32.00 | 32.00 |
| burst | minimal | 32 | 32.00 | 31.03 | 32.00 |
| burst | 100ns | 32 | 32.00 | 31.63 | 32.00 |
| burst | 1us | 32 | 32.00 | 32.00 | 32.00 |

Logical submissions reached the configured size. Workq and slice channels received full
batches in these diagnostic medians. Record channels also drained nearly full batches, so
their lower throughput was not explained by receiving mostly single records. They still paid
for individual channel operations. Public workq submission sizes do not reveal internal
partial enqueue chunks. Short diagnostics do not establish the exact batching distribution
of the much longer uninstrumented runs.

No backend incurred an allocation per record. Small nonzero runtime/library allocations remained.
The sustained per-cell median workq rate ranged from about 0.00000031 to 0.0000405 allocations
per record. This is not a zero-allocation guarantee. There were zero measured garbage collections
across all 330 clean timing processes. Detailed process metrics are retained in processes.csv.

## Verification, decisions, and recommendation

Astra prepared the harness. Independent review found an avoidable 32-byte producer-side copy
in the record-channel adapter. With user approval, Astra switched to indexed sends, rebuilt
and refroze the harness. Independent limited review confirmed that both channel binaries pass
the corpus address directly, without the extra copy. Tests, race checks, vet, and staticcheck
passed. Six unrelated existing lint findings were reproduced against the archived original
and left unchanged. No production library code changed.

Parent decisions kept batch counters in separate short diagnostics, accepted an explicitly
documented dispatch/barrier-inclusive burst metric, retained common fixed counts despite
slower single-item runs, and preserved old evidence when rebuilding the channel adapter.
These choices prevented counters and adaptive per-backend sizing from entering the clean runs.

Keep the library unchanged. The strongest demonstrated use is dispatching cheap, already-parsed
work in batches when callers want to enqueue records through a queue API rather than manage
whole immutable slice handoffs. Event routing and lightweight classification are plausible
applications of this pattern, not separately measured production workloads. When the caller
already owns immutable batches, a channel of slices is a strong alternative. At about 1 us per
record here, choose based on API, ownership, and lifecycle needs rather than expected speed.

This campaign establishes a useful region, not a universal crossover or proof for every queue
capacity, producer count, processor, payload, or arrival pattern. No further tuning or experiment
was performed automatically.

## Artifacts

Reviewed manifest: `8a15d9178b3adf9ec5da5c5620bd42bff596cfa0b3a7432bf1ceda927d7e6d75`.
The raw outputs, harness, commands, schedule, calibration, environment, provenance, verification,
and completion records remain in the local experiment directory:
`benchmark/.local/workq-lightweight-20261029/`. That directory is ignored by Git.
The prior harness is preserved in the sibling
`workq-lightweight-20261029-prior-51bb15444b9a.tar.gz` archive.

From the local experiment directory, run `python3 analyze.py` to reproduce process summaries
and 225 paired comparisons in JSON/CSV. These reproduction artifacts are local evidence, not
files included with this report. Analysis and this report are outside the frozen measurement
manifest.
