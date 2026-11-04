"""Acceptance checks for the frozen production comparison protocol."""
import math


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, name, minimum=0):
    require(type(value) is int and value >= minimum, f"invalid integer: {name}")
    return value


def exact(value, expected, name):
    # JSON equality alone accepts True as 1, including inside lists and dictionaries.
    require(type(value) is type(expected), f"wrong type: {name}")
    if isinstance(expected, dict):
        require(value.keys() == expected.keys(), f"wrong fields: {name}")
        for key in expected:
            exact(value[key], expected[key], f"{name}.{key}")
    elif isinstance(expected, list):
        require(len(value) == len(expected), f"wrong length: {name}")
        for index, item in enumerate(expected):
            exact(value[index], item, f"{name}[{index}]")
    else:
        require(value == expected, f"mismatch: {name}")


def expected_config(policy, batch):
    return {
        "Durability": 0 if policy == "FULL" else 1,
        "Policy": {
            "Profile": "sqlite-wal-v1", "MaxPayload": 4096, "MaxCodec": 16,
            "MaxPendingItems": 1024, "MaxPendingPayload": 4 << 20,
            "MaxReceipts": 8192, "MaxProducers": 4, "DatabaseLimit": 1 << 30,
        },
        "Runtime": {
            "Ordinary": {"Commands": 64, "Retained": 8 << 20},
            "Progress": {"Commands": 64, "Retained": 256 << 10},
            "Batches": {
                "SubmitItems": 256, "SubmitEncoded": 2 << 20,
                "AckItems": 256, "LookupItems": 256,
            },
            "Group": {
                "Commands": 64, "Operations": 256, "Encoded": 2 << 20, "MaxDelay": 1000000,
            },
            "Delivery": {
                "Items": 256, "Retained": 4 << 20, "ReceiveItems": batch,
                "ScanRows": 256, "ReadScratch": 32 << 10,
            },
            "Maintenance": {
                "CleanupRows": 256, "CleanupEncoded": 64 << 10, "MaxCommandTurns": 8,
            },
            "SQLite": {
                "Cache": 64 << 20, "Mmap": 256 << 20, "CheckpointTrigger": 16 << 20,
                "CheckpointInterval": 1000000000, "BusyTimeout": 5000000000,
            },
        },
    }


def check_latency(stats, name):
    count = integer(stats["count"], name + ".count", 1)
    total = integer(stats["total_ns"], name + ".total_ns")
    maximum = integer(stats["max_ns"], name + ".max_ns")
    buckets = stats["base2_nanosecond_buckets"]
    require(type(buckets) is list and len(buckets) == 64, f"invalid buckets: {name}")
    for value in buckets:
        integer(value, name + ".bucket")
    require(sum(buckets) == count, f"histogram count: {name}")
    lower = sum(value * (0 if index == 0 else 1 << index)
                for index, value in enumerate(buckets))
    upper = sum(value * ((1 << (index + 1)) - 1)
                for index, value in enumerate(buckets))
    require(lower <= total <= upper, f"histogram total: {name}")
    require(maximum <= total <= maximum * count, f"latency max/total: {name}")
    last = max(index for index, value in enumerate(buckets) if value)
    require(max(0, maximum.bit_length() - 1) == last, f"histogram maximum: {name}")
    return count


def check_phase(stats, batch, name):
    accepted = integer(stats["accepted"], name + ".accepted", 1)
    exact(stats["acknowledged"], accepted, name + ".acknowledged")
    waves = integer(stats["waves"], name + ".waves", 1)
    require(accepted <= waves * 8192, f"wave capacity: {name}")
    require("failed_wave" not in stats, f"failed wave: {name}")
    counts = stats["accepted_by_producer"]
    require(type(counts) is list and len(counts) == 4, f"producer counts: {name}")
    for count in counts:
        integer(count, name + ".producer")
        require(count <= waves * 2048, f"producer quota: {name}")
    require(sum(counts) == accepted, f"producer total: {name}")
    calls = check_latency(stats["submit_batch_latency"], name + ".submit")
    require(calls * batch == accepted, f"submit batch total: {name}")
    ack_calls = check_latency(stats["ack_batch_latency"], name + ".ack")
    for key in ("receive_batch_sizes", "ack_batch_sizes"):
        sizes = stats[key]
        require(type(sizes) is list and len(sizes) == 257, f"batch sizes: {name}.{key}")
        for size in sizes:
            integer(size, name + "." + key)
        require(sizes[0] == 0 and not any(sizes[batch + 1:]), f"batch range: {name}")
        require(sum(sizes) == ack_calls, f"batch call count: {name}")
        require(sum(index * count for index, count in enumerate(sizes)) == accepted,
                f"batch item count: {name}")
    exact(stats["receive_batch_sizes"], stats["ack_batch_sizes"], name + ".batches")
    return counts


def check_resources(stats, name):
    integer(stats["count"], name + ".count", 1)
    for kind in ("main", "wal", "rss"):
        minimum = 1 if kind == "rss" else 0
        maximum = integer(stats[f"{kind}_max_bytes"], f"{name}.{kind}_max_bytes", minimum)
        last = integer(stats[f"last_{kind}_bytes"], f"{name}.last_{kind}_bytes", minimum)
        require(last <= maximum, f"sample maximum: {name}.{kind}")


def check_store(store, config, uuid, epoch):
    exact(store["UUID"], uuid, "store UUID")
    exact(store["Epoch"], epoch, "store epoch")
    exact(store["Policy"], config["Policy"], "store policy")
    exact(store["Durability"], config["Durability"], "store durability")
    exact(store["Counts"], {
        "PendingItems": 0, "PendingPayload": 0, "Receipts": 0,
        "Producers": 4, "RetiredProducers": 4, "CleanupMarkers": 0,
    }, "store counts")
    for key in ("DatabaseBytes", "WALBytes", "PageCount", "FreePages"):
        integer(store[key], "store." + key)
    require(store["FreePages"] <= store["PageCount"], "store free pages")


def validate_report(report, options):
    require(type(report) is dict, "result must be an object")
    exact(report["valid"], True, "valid")
    require("failure" not in report, "failure present")
    exact(report["options"], options, "options")
    config = expected_config(options["policy"], options["submit_batch_limit"])
    exact(report["resolved_config"], config, "resolved_config")
    exact(report["credit_per_producer"], 256, "credit_per_producer")
    integer(report["gomaxprocs"], "gomaxprocs", 1)
    require(type(report["go_version"]) is str and report["go_version"].startswith("go"),
            "missing Go version")
    require(type(report["build"]) is dict, "missing build information")
    for key in ("GoVersion", "Path", "Main", "Deps", "Settings"):
        require(key in report["build"], "missing build." + key)
    exact(report["build"]["GoVersion"], report["go_version"], "build Go version")
    exact(report["build"]["Path"],
          "github.com/freshdresch/go-data-structures/benchmark/cmd/durable-comparison",
          "build path")
    require(type(report["build"]["Main"]) is dict, "build main module")
    require(type(report["build"]["Deps"]) is list, "build dependencies")
    require(type(report["build"]["Settings"]) is list, "build settings")
    warmup = check_phase(report["warmup"], options["submit_batch_limit"], "warmup")
    measured = check_phase(report["measured"], options["submit_batch_limit"], "measured")
    count = report["measured"]["acknowledged"]
    elapsed = integer(report["elapsed_ns"], "elapsed_ns", options["admission_ns"])
    cpu = integer(report["cpu_ns"], "cpu_ns")
    allocated = integer(report["total_alloc_bytes"], "total_alloc_bytes")
    mallocs = integer(report["mallocs"], "mallocs")
    integer(report["reopen_validation_ns"], "reopen_validation_ns", 1)
    for key, expected in {
        "records_per_second": count * 1e9 / elapsed, "cpu_ns_per_record": cpu / count,
        "allocated_bytes_per_record": allocated / count, "allocations_per_record": mallocs / count,
    }.items():
        actual = report[key]
        require(type(actual) in (int, float) and math.isfinite(actual), f"invalid metric: {key}")
        require(math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12),
                f"derived metric mismatch: {key}")
    for name in ("resources_before_warmup", "sampled_resources"):
        check_resources(report[name], name)
    before, final = report["before"], report["final"]
    check_snapshot(before)
    check_snapshot(final)
    exact(before["State"], 0, "before state")
    exact(final["State"], 5, "final state")
    for key in ("AdmissionStopped", "DeliveryStopped"):
        exact(before[key], False, "before " + key)
        exact(final[key], True, "final " + key)
    for group in ("Ordinary", "Progress", "Delivery", "CurrentGroup"):
        for key, value in final[group].items():
            exact(value, 0, "final occupancy " + group + "." + key)
    checkpoint = final["Checkpoint"]
    exact(checkpoint["Valid"], True, "final checkpoint valid")
    exact(checkpoint["Busy"], 0, "final checkpoint busy")
    exact(checkpoint["Frames"], checkpoint["Completed"], "final checkpoint frames")
    exact(final["Store"]["WALBytes"], 0, "final WAL bytes")
    require(final["Outcomes"]["Checkpoints"] > before["Outcomes"]["Checkpoints"],
            "missing shutdown checkpoint")
    for group in ("CommitLatency", "AckLatency"):
        for key in ("Count", "Total", "Max"):
            require(final[group][key] >= before[group][key],
                    "queue latency decreased: " + group + "." + key)
        added_count = final[group]["Count"] - before[group]["Count"]
        added_total = final[group]["Total"] - before[group]["Total"]
        if added_count:
            upper = (added_count - 1) * final[group]["Max"] + final[group]["Last"]
            require(added_total <= upper, "queue latency increment bounds: " + group)
            require(final[group]["Last"] <= added_total,
                    "queue last latency exceeds increment: " + group)
            if final[group]["Max"] > before[group]["Max"]:
                lower = final[group]["Max"]
                if final[group]["Last"] < final[group]["Max"]:
                    lower += final[group]["Last"]
                require(lower <= added_total, "queue maximum exceeds increment: " + group)
        else:
            exact(final[group], before[group], "unchanged queue latency: " + group)
    warmup_ack_calls = report["warmup"]["ack_batch_latency"]["count"]
    measured_ack_calls = report["measured"]["ack_batch_latency"]["count"]
    exact(before["AckLatency"]["Count"], warmup_ack_calls, "warmup queue Ack calls")
    exact(final["AckLatency"]["Count"], warmup_ack_calls + measured_ack_calls,
          "final queue Ack calls")
    uuid = before["Store"]["UUID"]
    require(type(uuid) is list and len(uuid) == 16, "invalid UUID")
    for byte in uuid:
        require(integer(byte, "UUID byte") <= 255, "invalid UUID byte")
    require(any(uuid), "zero UUID")
    check_store(before["Store"], config, uuid, 1)
    check_store(final["Store"], config, uuid, 1)
    for snapshot in (before, final):
        exact(snapshot["Overflow"], False, "counter overflow")
        outcomes = snapshot["Outcomes"]
        for key in ("Commits", "Rollbacks", "UnknownResponses", "Acks", "CleanupTurns",
                    "Checkpoints", "CheckpointFailures"):
            integer(outcomes[key], "outcomes." + key)
        for key in ("Rollbacks", "UnknownResponses", "CheckpointFailures"):
            exact(outcomes[key], 0, "outcomes." + key)
    for key, value in before["Outcomes"].items():
        require(final["Outcomes"][key] >= value, "queue counter decreased: " + key)
    require(final["Outcomes"]["Commits"] > before["Outcomes"]["Commits"],
            "no measured commits")
    exact(before["Outcomes"]["Acks"], sum(warmup), "warmup queue Acks")
    exact(final["Outcomes"]["Acks"], sum(warmup) + count, "final queue Acks")
    inspection = report["inspection"]
    check_store(inspection["Store"], config, uuid, 2)
    for key in ("LogicalRecords", "Duration"):
        integer(inspection["Validation"][key], "inspection validation." + key)
    statuses = inspection["Statuses"]
    require(type(statuses) is list and len(statuses) == 4, "retirement statuses")
    for producer, status in enumerate(statuses):
        frontier = warmup[producer] + measured[producer]
        exact(status, {
            "ID": {"Producer": [producer + 1] + [0] * 15, "Sequence": frontier},
            "Kind": 4, "HasRetirement": True, "RetiredThrough": frontier,
        }, "retirement status")


def check_snapshot(snapshot):
    for key in ("AdmissionStopped", "DeliveryStopped", "Overflow"):
        require(type(snapshot[key]) is bool, "snapshot boolean: " + key)
    for group, keys in {
        "Ordinary": ("Commands", "Retained", "Waiters"),
        "Progress": ("Commands", "Retained", "Waiters"),
        "Delivery": ("Claims", "Unpublished", "Retained"),
        "CurrentGroup": ("Commands", "Operations", "Encoded", "CollectionDelay"),
        "LastGroup": ("Commands", "Operations", "Encoded", "CollectionDelay"),
        "CommitLatency": ("Count", "Total", "Max", "Last"),
        "AckLatency": ("Count", "Total", "Max", "Last"),
        "Validation": ("LogicalRecords", "Duration"),
        "Maintenance": ("DatabaseBytes", "PageCount", "FreePages", "Reason", "WALBytes"),
        "Disk": ("Sequence", "AllocatedBytes", "NativeBytes", "Cache", "Mmap"),
    }.items():
        for key in keys:
            integer(snapshot[group][key], group + "." + key)
    exact(snapshot["CommitLatency"]["Count"], snapshot["Outcomes"]["Commits"],
          "commit latency count")
    for group in ("CommitLatency", "AckLatency"):
        stats = snapshot[group]
        require(stats["Last"] <= stats["Max"] <= stats["Total"] <= stats["Max"] * stats["Count"],
                "queue latency bounds: " + group)
        if stats["Last"] < stats["Max"]:
            require(stats["Total"] >= stats["Max"] + stats["Last"],
                    "distinct maximum and last exceed queue latency total: " + group)
        if stats["Count"] == 0:
            exact(stats["Max"], 0, "empty queue latency: " + group)
        if stats["Count"]:
            upper = (stats["Count"] - 1) * stats["Max"] + stats["Last"]
            require(stats["Total"] <= upper, "queue latency total exceeds last bound: " + group)
    for checkpoint in (snapshot["Checkpoint"], snapshot["Maintenance"]["Checkpoint"]):
        require(type(checkpoint["Valid"]) is bool, "checkpoint validity")
        for key in ("Busy", "Frames", "Completed"):
            integer(checkpoint[key], "checkpoint." + key, -1)
        integer(checkpoint["Duration"], "checkpoint duration")
    for key in ("Valid", "AllocatedValid", "NativeValid"):
        require(type(snapshot["Disk"][key]) is bool, "disk validity: " + key)
    require(type(snapshot["Disk"]["At"]) is str, "disk timestamp")
    cause = snapshot["Cause"]
    exact(cause, {"Operation": 0, "Detail": "", "Truncated": False, "Storage": False,
                  "Maintenance": False, "Exhausted": False}, "terminal cause")
    integer(snapshot["Cleanup"]["Deleted"], "cleanup deleted")
    require(type(snapshot["Cleanup"]["MarkerCleared"]) is bool, "cleanup marker boolean")
