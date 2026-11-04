#!/usr/bin/env bash
set -euo pipefail

: "${WORKQ_VERIFY_SCRATCH:?Set an absolute persistent scratch directory outside the checkout}"
case "$WORKQ_VERIFY_SCRATCH" in
    /*) ;;
    *) echo 'WORKQ_VERIFY_SCRATCH must be absolute' >&2; exit 2 ;;
esac
case "$WORKQ_VERIFY_SCRATCH/" in
    /tmp/*) echo 'Use persistent scratch storage, not /tmp' >&2; exit 2 ;;
esac

cd "$(dirname "${BASH_SOURCE[0]}")/.."
repository=$(pwd -P)
mkdir -p "$WORKQ_VERIFY_SCRATCH"
scratch=$(cd "$WORKQ_VERIFY_SCRATCH" && pwd -P)
case "$scratch/" in
    /tmp/*) echo 'Resolved scratch must use persistent storage, not /tmp' >&2; exit 2 ;;
esac
case "$scratch/" in
    "$repository/"*) echo 'Scratch must be outside the checkout' >&2; exit 2 ;;
esac
export GOMAXPROCS=2 GOWORK=off GOFLAGS=-p=1 GOMEMLIMIT=1GiB
export TMPDIR="$scratch/tmp" GOCACHE="$scratch/go-cache" GOMODCACHE="$scratch/go-mod"
export BAZELISK_HOME="$scratch/bazelisk" XDG_CACHE_HOME="$scratch/cache"
export GOLANGCI_LINT_CACHE="$scratch/lint-cache"
mkdir -p "$TMPDIR" "$GOCACHE" "$GOMODCACHE" "$BAZELISK_HOME" "$XDG_CACHE_HOME"

run() {
    local limit=$1
    shift
    local remaining=$((900 - SECONDS))
    if ((remaining <= 0)); then
        echo 'Verification reached the 15-minute limit' >&2
        exit 124
    fi
    if ((limit > remaining)); then
        limit=$remaining
    fi
    printf 'Running:'
    printf ' %q' "$@"
    printf '\n'
    timeout --kill-after=5s "${limit}s" "$@"
}

go_test() {
    run 120 go test -p=1 -parallel=1 -count=1 -timeout=60s "$@"
}

queue_filter='^(TestNewSelectsEngine|TestNewDefaults|TestErrorIdentities'
queue_filter+='|TestDiagnosticsPackageCompatibility|TestTryPushFull|TestCloseDrainsRemainingItems'
queue_filter+='|TestReceiveWaitClosedBeatsContext|ExampleQueue)$'
runner_filter='^(TestConfigValidation|TestQueueDefaultsAndOwnership|TestQueueExplicitSettings'
runner_filter+='|TestFinalDiagnosticsAfterSuccessfulDrain|ExampleRunner_Shutdown)$'
durable_filter='^(TestGoldenEncodings|TestSchemaUnsignedOrderingAndReopen)$'

bazel_command=(
    bazelisk
    --batch
    "--output_user_root=$scratch/bazel-output"
    --host_jvm_args=-Xmx1024m
    "--host_jvm_args=-Djava.io.tmpdir=$TMPDIR"
)
bazel_options=(
    --jobs=2 --local_test_jobs=1 --local_resources=cpu=2 --local_resources=memory=2048
    "--repository_cache=$scratch/bazel-repository-cache"
    "--disk_cache=$scratch/bazel-disk-cache"
    "--action_env=TMPDIR=$TMPDIR" --action_env=GOMAXPROCS=2
    --test_env=GOMAXPROCS=2 --test_timeout=60
)

case "${1:-}" in
    go)
        run 300 go test -p=1 -parallel=1 -count=1 -timeout=30s -run='^$' ./...
        run 300 go test -p=1 -parallel=1 -count=1 -timeout=30s \
            -tags=durable_faulttest -run='^$' ./...
        go_test ./workq -run="$queue_filter"
        go_test ./runner -run="$runner_filter"
        go_test ./workq/internal/diagnostics \
            -run='^(TestDiagnosticsStorageInitialization|TestDiagnosticsSnapshotCounterReduction)$'
        go_test ./durable -run="$durable_filter"
        go_test -tags=durable_faulttest ./durable -run='^TestFaultProductionDirectorySyncError$'
        go_test ./examples/durable -run='^TestRunGracefulAndIdenticalSourceRetry$'
        go_test ./examples/transactional -run='^TestNormalHistorySurvivingPendingAbsorbsLostAck$'
        go_test ./examples/idempotency -run='^TestStopAfterCommitAndRestart$'
        CGO_ENABLED=0 go_test ./workq -run="$queue_filter"
        CGO_ENABLED=0 go_test ./runner -run="$runner_filter"
        cd benchmark
        run 300 go test -p=1 -parallel=1 -count=1 -timeout=30s -run='^$' ./...
        go_test . -run='^(TestWorkqRowsTotalOne|TestPercentile)$'
        cd cmd/durable-comparison
        run 60 env PYTHONDONTWRITEBYTECODE=1 \
            python3 -m unittest test_campaign.ValidationTest.test_real_smoke_fixture
        ;;
    bazel)
        run 300 "${bazel_command[@]}" build "${bazel_options[@]}" \
            //:libraries //:examples //workq:workq_test \
            //workq/internal/diagnostics:diagnostics_test //runner:runner_test \
            //durable:durable_test //durable:durable_fault_test \
            //durable/internal/sqlitefault:sqlitefault_test \
            //examples/durable:durable_test //examples/idempotency:idempotency_test \
            //examples/transactional:transactional_test //examples/lifecycle:lifecycle_test \
            //examples/observability:observability_test //examples/overload:overload_test \
            //examples/internal/inventory:inventory_test
        for selection in \
            '//workq:workq_test TestDiagnosticsPackageCompatibility' \
            '//runner:runner_test TestQueueExplicitSettings' \
            '//durable:durable_test TestSchemaUnsignedOrderingAndReopen' \
            '//durable:durable_fault_test TestFaultProductionDirectorySyncError'
        do
            read -r target test_name <<< "$selection"
            run 120 "${bazel_command[@]}" test "${bazel_options[@]}" \
                "--test_filter=^${test_name}$" --test_arg=-test.parallel=1 \
                --test_arg=-test.count=1 "$target"
        done
        ;;
    lint)
        run 120 golangci-lint fmt --diff ./...
        run 120 golangci-lint run --concurrency=2 --timeout=110s ./...
        cd benchmark
        run 120 golangci-lint fmt --diff ./...
        run 120 golangci-lint run --concurrency=2 --timeout=110s ./...
        ;;
    *) echo 'Usage: bash scripts/verify-migration.sh {go|bazel|lint}' >&2; exit 2 ;;
esac
