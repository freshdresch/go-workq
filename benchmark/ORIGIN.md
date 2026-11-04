# Benchmark source origin

The benchmark assets were copied from `github.com/freshdresch/go-data-structures`
at original revision `0af75a87ce83467f2b92090dba80c8b764ad93bc`.
Their earlier history remains in that repository. The repository's Apache-2.0
[license](../LICENSE) applies to the copied source.

The six `workq*.go` files, both durable commands, Python scripts, fixture, race selectors,
and reports retain their source behavior. Imports now use `github.com/freshdresch/go-workq`.
The files in `workq/` are unchanged historical reports. Their original revisions, commands,
paths, and measurements describe the old repository and have not been requalified here.

Shared support came from these original paths at the same revision:

| Original path | Extracted support |
| --- | --- |
| `benchmark/harness_test.go` | `harness_test.go`, excluding unused pointer pool, pointer fill, fill function type, and element-size reporting |
| `benchmark/buffers_test.go` | Buffer allocation checks, unchanged |
| `benchmark/polling_test.go` | `engine_support_test.go`, containing the queue interface, consume/enqueue loops, ring constructors, and their adapters |
| `benchmark/compare_test.go` | Topology and adapter outcome tests, abort sentinel/count, and error/goroutine helpers in `engine_support_checks_test.go` |
| `benchmark/impls_test.go` | `newTestRing` in `engine_support_checks_test.go` |

The extracted adapter bodies keep their original branches. They call public ring packages
from `go-data-structures v0.0.0-20261103091550-0af75a87ce83`. No ring implementation was copied.
The nested module replaces only `go-workq` with its parent directory. The external ring
module resolves by its published version without a local replacement.

New durable campaigns hash both modules, their selected dependency source files and module
files, and the Python harness. Their manifests also record the selected external ring
module path, version, and module checksums. Existing campaign approval requirements still apply.
