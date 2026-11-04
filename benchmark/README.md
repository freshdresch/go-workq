# Work queue benchmarks

This separate Go module compares workq with channels and the external ring engines.
The [origin record](ORIGIN.md) identifies the frozen source and extracted test support.
The [lightweight report](workq/lightweight.md) and [durable report](workq/durable.md)
are unchanged historical results from go-data-structures, not qualification of this repository.

The [durable comparison](cmd/durable-comparison/README.md) and
[resource harness](cmd/durable-resource/README.md) use the production durable API.
Their Python scripts use only the standard library. Run campaigns only after separate approval.

## Work queue benchmarks

`BenchmarkWorkq` compares the [`workq`](../workq/README.md) queue with a buffered channel, a
channel of batches, and the raw engine with polling. `BenchmarkWorkqTuning` runs `workq` with
one setting changed from its defaults at a time, to settle the receive batch limit, the push
batch limit, and the wait strategy of each side. Both run with `go test` in this module.

From the repository root, enter the benchmark module:

```bash
cd benchmark
go test -run '^$' -bench '^BenchmarkWorkq$' -benchtime 200ms -count 1 ./
go test -run '^$' -bench '^BenchmarkWorkqTuning$' -benchtime 200ms -count 1 ./
```

- A full run of `BenchmarkWorkq` took about 94s at `-benchtime 200ms -count 1` on a 16-CPU
  machine, and `BenchmarkWorkqTuning` is of a similar size. With `-count` above about 6, add
  `-timeout 0` or a larger timeout, because `go test`'s default is 10 minutes.
- Run on an idle machine.
- The harness's timing tests (`TestWorkqPacedLatency`, `TestWorkqPacedSleep`, and
  `TestWorkqBurst`) also assume an idle machine and can fail on a loaded one.
- Rows always run in the same order within each case, and `-count` repeats each case back to
  back. Each experiment proposal defines how it orders or interleaves rows.

Sub-benchmarks are named `load=<load>/mode=<mode>/batch=<n>/work=<d>/impl=<row>`, and the
`paced` load adds `pace=<d>` before `batch`. `work` is the handler's busy work per item, and
`batch` is how many items a producer pushes per call.

### Rows

| Row | Batches | What it measures |
|---|---|---|
| `workq` | 1, 32, 128 | `PushBatch` and `Consume` on `workq.New`, with the defaults |
| `chan` | 1, 32, 128 | A `chan int64` of 1024 slots, one send and one receive per item |
| `chan-batch` | 32, 128 | A channel of slices, with buffers recycled through a free list |
| `engine` | 1, 32, 128 | The ring engine `workq` picks, with polling producers and consumers |

Every row holds 1024 items. At batch 1, the `workq` row pushes a one-element burst with
`PushBatch`, not `Push`. `chan-batch` runs only at batches 32 and 128, because at batch 1 it
makes four channel operations per item against two for `chan`, so it would show only its
pooling overhead. A `chan-batch` producer blocked on a full channel holds one filled buffer, so
up to one batch per producer is in flight beyond the 1024.

Batch 128 is above `workq`'s push limit of 32, so `workq` splits each push into chunks, and
batch 128 is where the `push*` rows differ. The `engine` row splits its pushes into 32-item
pieces and dequeues into 32-item buffers, to match `workq`'s engine calls. It does not follow
the `recv*` and `push*` rows.

The tuning rows each change one setting:

| Rows | Setting | Batches | Loads |
|---|---|---|---|
| `default` | none | 1, 32, 128 | all |
| `recv16`, `recv64`, `recv128` | receive limit | 1, 32 | `saturated`, `wide-consumers` |
| `push16`, `push64`, `push128` | push limit | 128 | `saturated`, `wide-producers`, `push-wait` |
| `cwait-park`, `cwait-long`, `cwait-never` | consumer wait | 1, 32 | see below |
| `pwait-park`, `pwait-long`, `pwait-never` | producer wait | 1, 32 | see below |

The `recv*` rows run in `saturated` only in SPMC and MPMC, and the `push*` rows only in MPSC
and MPMC. The `push*` rows also run in `push-wait`. The `cwait-*` rows run in `paced`, `burst`,
and, at `work=0` only, `saturated` and `wide-consumers`, because with handler work there the
consumers are the bottleneck and almost never wait. The `pwait-*` rows run in `saturated`,
`wide-producers`, and `push-wait`.

`default` also stands for a receive limit and a push limit of 32. `park` parks after the first
empty poll, `long` spins and yields four times as long as the default, and `never` spins and
yields with the default counts but never parks.

### Loads

| Load | Modes | Producer | Batches | Handler work |
|---|---|---|---|---|
| `saturated` | SPSC, MPSC, SPMC, MPMC | as fast as it can | 1, 32, 128 | 0, 1us, 50us |
| `wide-consumers` | SPMC16 | as fast as it can | 1, 32, 128 | 0, 1us, 50us |
| `wide-producers` | MPSC16 | as fast as it can | 1, 32, 128 | 0, 1us, 50us |
| `push-wait` | MPSC, MPMC, MPSC16 | as fast as it can, timing each push | 1, 32, 128 | 1us, 50us |
| `paced` | SPSC, SPMC, SPMC16 | one item per pace | 1 | 0, 1us, 50us |
| `burst` | SPMC | 10,000 items at once, after a pause | 32 | 1us |

`paced` runs at `pace=10us` and `pace=100us` in SPSC, SPMC, and SPMC16, and at `pace=2ms` in
SPSC and SPMC. It leaves out 50us work at `pace=10us` and in SPMC16. `SPMC16` and `MPSC16` run
17 goroutines, more than the CPUs of a 16-CPU runner, on purpose.

Each load is meant to reach one regime. The harness does not measure which regime a run was in.

- *Queue often empty, consumers waiting*: `paced`, and `saturated` at `work=0` in SPSC and
  SPMC. With several producers, `work=0` does not reliably keep the queue empty.
- *More workers than load*: `paced` in SPMC16, a steady trickle with most of the 16 consumers
  parked.
- *Queue full, consumers the bottleneck*: `saturated`, `wide-*`, and `push-wait` at `work=1us`
  and `work=50us`. `wide-consumers` with work measures 16 busy consumers, not spare workers.
- *Parked consumers hit by a burst*: `burst` at `work=1us`, where the consumers are the
  bottleneck.

### Metrics

One op is one item handled, except in `burst`, where one op is one burst and `ns/op` includes
the pause before it. Every sub-benchmark reports allocations and these metrics:

- `cpu-ns/op` is the process's user plus system CPU time across the timed region, per op. It
  shows spinning and wasted wakeups that `ns/op` hides. It is reported only on Unix.
- In `paced`, `ns/op` is the pace, and the latency metrics are the result. `p50-ns`, `p99-ns`,
  and `max-ns` are the time from an item's push to its handler, across every consumer.
  `late-p99-ns` is the 99th percentile of how late the producer pushed each item against its
  schedule. Latency runs from the push, so the producer's lateness does not enter it.
- In `burst`, `first-start-ns` is the mean time from a burst's start to the first item any
  consumer handled. `fourth-start-ns` is the mean time to the fourth consumer's first item,
  and `fourth-start-max-ns` is its maximum. `missed-share` is the share of bursts in which some
  consumer handled no item. `fourth-start-ns` leaves out missed bursts, so read it together
  with `missed-share`.
- In `push-wait`, `max-push-ns` is the longest single `push` call of any producer. It is a
  single maximum, so GC pauses and preemption dominate it. It times a whole call, which at
  batch 128 covers several chunks of the push limit, not one wait.
- `max-consumer-share` is the largest share of the items that one consumer handled, reported
  only with several consumers. An even split is 1 divided by the consumer count, and a larger
  value means one consumer took more than its share, which is the risk a larger receive limit
  carries.
- In `paced`, `ns/op` is the pace plus the 1ms lead divided by the op count, which matters only
  at small counts such as `-benchtime 100x`, and in `burst` and at `pace=2ms`, `B/op` and
  `allocs/op` spread each run's fixed setup over few ops.

### Read the measurements

At `pace=10us` and `pace=100us`, the paced producer yields in a loop with `runtime.Gosched`
until each item is due. On the development machine:

- A sleeping producer would be uneven. A 20µs `time.Sleep` took about 1.08ms when the other
  goroutines were parked and about 34µs when one was yielding, so it would send bursts in some
  rows and paced items in others.
- A producer that spins without yielding traps the consumer it wakes. The send puts that
  consumer in the producer's P's `runnext` slot. With `chan int64` in SPSC, p50 latency was
  about 89µs at `pace=100us` and 47µs at `pace=10us`, against about 0.6µs and 0.25µs with the
  yield. With the yield, the woken consumer may run on the producer's P.
- Locking the producer to its OS thread raised p50 latency to about 26µs, because each yield
  hands off the P.

The yield loop costs about 1.3 CPUs in every row, including the runtime threads it wakes, so
`cpu-ns/op` at those paces is roughly 1.3 times `ns/op` plus the consumers' CPU. The loop also
keeps idle Ps spinning, so the scheduler is warm when an item arrives, and latency at those
paces is a lower bound on the cost of a park. A parked `workq` consumer measured p50 latency
of about 1.3µs with the yielding producer and about 1.85µs with a sleeping one. At `pace=2ms`
the producer sleeps between items, so it costs little CPU and measures the cold case.
`pace=2ms` collects few samples at a time-based `-benchtime`, so its `p99-ns` needs a count
such as `-benchtime 2000x`. Limit that count to the `pace=2ms` cases, because 2000x across
every sub-benchmark would make each burst case take about 9s:

```bash
go test -run '^$' -bench 'BenchmarkWorkq/load=paced/.*/pace=2ms' -benchtime 2000x -count 1 ./
```

The burst producer does not yield while it pushes. A consumer that a push wakes can wait in
the producer's P's `runnext` slot until another P steals it or the producer blocks on a full
queue. The start delays therefore include how long each row's producer takes to fill the
queue, and `first-start-ns` shows that delay apart from the chain of wakes. The `engine` row
and `cwait-never` never park, so for them the pause only lets the queue empty.

Design benchmark 5 compares `workq` with producers that only poll. The `pwait-never` row, which
spins and yields but never parks, and the `engine` row, whose producers poll the ring, stand in
for it.

### Not covered

- Light load with several producers, and light load at batch 32. A paced load in MPSC or MPMC
  would need one yielding producer per goroutine, which warms the scheduler further and
  distorts the wake it measures. The consumer's wait path is the same whatever the producer
  count, and `paced` in SPSC and SPMC measures it.
- `Enqueue` against a one-element burst, and `Dequeue` against a one-element `DequeueBurst`, in
  `workq`'s own path. Both need library changes. `BenchmarkCompare` covers the engine side.
- `smarty/go-disruptor`. It exists only on the unmerged `disruptor-comparison` branch, and
  adding it here would duplicate that work.
- The push limit's cost to consumers: the time one producer's unfinished chunk holds back items
  that later producers reserved. It needs latency with several producers, which is deferred.
- Wake order, oldest first against newest first. It needs a library change.
- An interface call against a type switch. It needs a library change.
- Wakes per item, and producer wakes per consumer batch. They need counters inside `workq`.
- The cost of the spin phase to the other side, per side. It needs counters inside `workq`. The
  wait tuning rows show it only in aggregate, through `ns/op` and `cpu-ns/op`.
