# go-workq

Go queues and worker supervision for bounded in-memory work and persistent SQLite delivery.
Choose the package that owns the behavior your application needs.

| Package | Use it for |
| --- | --- |
| [`workq`](workq/README.md) | A bounded in-memory queue with blocking waits and diagnostics |
| [`runner`](runner/README.md) | Owned workers, retries, processing summaries, and shutdown |
| [`durable`](durable/README.md) | Persistent delivery, acknowledgments, recovery, and queue maintenance |
| [`workq/wait`](workq/wait) | Public wait strategy values for queue and runner configuration |
| [`workq/diagnostics`](workq/diagnostics) | Public diagnostic settings and snapshot types |

Imports use `github.com/freshdresch/go-workq/workq`,
`github.com/freshdresch/go-workq/runner`, or `github.com/freshdresch/go-workq/durable`.
The repository root contains documentation and build configuration and has no Go package.
Runner callers can configure defaults with only the runner import. Explicit queue settings can
use workq's aliases or its public wait and diagnostics packages.

The ring engines remain in `github.com/freshdresch/go-data-structures`. The module pins
`v0.0.0-20261103091550-0af75a87ce83` and preserves shared error identities.
Existing applications must update their old workq imports. The new package paths have different
Go type identities, so old and new queue types are not interchangeable.

## Requirements and builds

The module requires Go 1.25.5 or later. Queue and runner builds support `CGO_ENABLED=0`.
Durable operation requires Linux amd64, cgo, and a C compiler. Read the
[durability profile](durable/PROFILE.md) for filesystem, storage, and resource requirements.
That profile and the benchmark reports describe their recorded historical builds. Migration
checks do not renew those qualification claims.

From the repository root, ordinary Go builds need no Bazel installation.

```sh
CGO_ENABLED=0 go build ./workq/... ./runner
CGO_ENABLED=1 go build ./durable/... ./examples/...
```

## Examples and guides

The [queue guide](workq/README.md) covers admission and waiting. The
[runner guide](runner/README.md) covers processing ownership and shutdown.
For persistent work, start with the [durable guide](durable/README.md) and its
[runnable example](examples/durable/main.go).

```sh
go run ./examples/durable -directory /path/on/ext4/demo-queue
```

Use a new directory on a filesystem that meets the durability profile.
The [lifecycle](workq/lifecycle.md), [overload](workq/overload.md), and
[observability](workq/observability.md) guides link to their examples under `examples/`.

## Local verification

Run the bounded migration checks through [scripts/verify-migration.sh](scripts/verify-migration.sh).
Set `WORKQ_VERIFY_SCRATCH` to an absolute persistent scratch directory outside the checkout.
The script keeps temporary files and caches there. It requires Bash and GNU timeout.
Each mode stops on its first failure and has a 15-minute overall limit.

```sh
export WORKQ_VERIFY_SCRATCH=/path/to/persistent/workq-verification
bash scripts/verify-migration.sh go
bash scripts/verify-migration.sh bazel
bash scripts/verify-migration.sh lint
```

| Mode | Coverage |
| --- | --- |
| `go` | Root and benchmark compile/link, exact queue/runner/durable/example smoke tests, non-cgo queue/runner, one Python fixture check |
| `bazel` | Root libraries/examples and ordinary/fault test builds, then four exact smoke tests |
| `lint` | Formatting checks and lint for root packages and the separate benchmark module |

Use Bazelisk for optional Bazel builds. The repository retains Bazel 9.3.1, rules_go 0.63.0,
and Gazelle 0.52.2. `//:libraries` and `//:examples` collect public targets. Benchmark code is
excluded from Bazel and has its own [Go module and guide](benchmark/README.md).

The script limits Go concurrency to two processes and one build package at a time. Bazel uses
two jobs and one local test job. Builds have 300-second limits. Selected tests have 60-second
internal and 120-second external limits. Lint has a 120-second limit per command.

Migration acceptance also requires source/hash accounting, copied historical v1 fixture
checks, and separate external consumers for default runner, both explicit configuration styles,
direct queue use, and durable persistence. These checks use migration evidence outside the
repository and are not part of the reusable script. Run verification commands sequentially and
keep their combined active execution within 15 minutes during migration.

Full suites, broad race runs, crash/fault stress matrices, resource qualification, fuzzing, and
performance campaigns remain separate release qualification. No hosted CI was migrated or run.
