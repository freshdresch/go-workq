# Durable queue resource harness

This Linux command runs one bounded case in one process through the production API.
It writes JSON lines to stdout and retains the fixture for evidence review. It does not
run the performance comparison or qualify a release profile.

Build from the benchmark module:

```sh
go build -o /path/to/scratch/durable-resource ./cmd/durable-resource
```

After protocol approval, run each case in a fresh process with a new directory under
private task scratch. For example:

```sh
/path/to/scratch/durable-resource -case capacity -cache-mib 8 -mmap-mib 0 \
  -directory /path/to/scratch/capacity-8-0 > /path/to/scratch/capacity-8-0.jsonl
```

Supported cases are `capacity`, `maximum-group`, `lifecycle`, and `small-main`.
Cache choices are 8 and 64 MiB. Mmap choices are 0 and 256 MiB. The default policy is
`synchronized`. Only `maximum-group` also accepts `-durability process-crash`.
The case deadline is ten minutes. Storage joins still follow the library contract.
Every failed fixture must be preserved. Remove successful fixtures only after validating
and reviewing the evidence. Never retry a failed case silently.

Capacity uses four waves of 4096 records and checks receipt refusal at 16384.
Maximum-group uses eight cycles, each containing an exact 2 MiB encoded batch,
a mixed-size fill, and a 16 MiB payload fill. Each fill is drained, acknowledged,
and retired. Cleanup has a thirty-second deadline. Lifecycle runs one such cycle,
closes, inspects, and calls Maintain at most eight times. It stops maintenance when
free pages stop falling, then reopens and repeats the payload fill and drain.
Small-main uses a 256 KiB main cap and requires native SQLITE_FULL for the exact
2 MiB batch, then checks empty recovered state and successful reopen.

Samples include process RSS, Go heap, queue mapping residency when available, and
separate file lengths and allocated bytes. Sampling runs every 20 ms and at command
boundaries. Snapshot retains queue charges, group, checkpoint, and page observations.
`wal_boundary_delta_bytes` compares consecutive boundary file samples. Cleanup and
idle checkpoints may intervene. Neither that delta nor cached LastGroup is an exact
transaction write-volume counter. Sequence/time and validity fields remain in Snapshot.
Sampling can miss transient peaks. Native heap attribution is unavailable.
The harness input payloads, validation oracle, JSON encoding, and sampling allocations
contribute to process memory. No forced GC occurs.

Successful Open establishes settings through the library's exact readback checks in
`store.configure` and `selectDurability`. The harness does not query live pragmas through
a second connection. Offline operations use their fixed 64 MiB cache and 256 MiB mmap.
JSON records use the library's current Snapshot/Config field names for nested values.
Keep the binary/source revision with the evidence when interpreting those records.
