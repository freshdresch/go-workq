#!/usr/bin/env python3
"""Build and run the reviewed, separately approved 80-process production comparison."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

from provenance import build_binary, command_output, host_environment, verify_provenance
from validation import validate_report


SAMPLE_TIMEOUT_SECONDS = 120


def save_manifest(output, manifest):
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def cells():
    for payload in (256, 4096):
        for batch in (1, 256):
            for repetition in range(10):
                policies = ("FULL", "NORMAL") if repetition % 2 == 0 else ("NORMAL", "FULL")
                for policy in policies:
                    yield payload, batch, repetition, policy


def run_sample(root, output, binary, env, manifest, cell):
    payload, batch, repetition, policy = cell
    name = f"p{payload}-b{batch}-r{repetition:02}-{policy}"
    run_dir = output / name
    run_dir.mkdir(mode=0o700)
    options = {
        "directory": str(run_dir / "queue"), "policy": policy, "payload_bytes": payload,
        "submit_batch_limit": batch, "library_revision": manifest["revision"],
        "warmup_ns": 100000000, "admission_ns": 1000000000,
    }
    command = [str(binary), "-directory", options["directory"], "-policy", policy,
               "-payload", str(payload), "-batch", str(batch), "-revision", manifest["revision"],
               "-warmup", "100ms", "-measure", "1s"]
    record = {"name": name, "command": command, "status": "started"}
    manifest["runs"].append(record)
    save_manifest(output, manifest)
    try:
        verify_provenance(root, binary, env, manifest)
        with (run_dir / "result.json").open("w") as stdout:
            with (run_dir / "stderr.log").open("w") as stderr:
                completed = subprocess.run(
                    command,
                    env=env,
                    stdout=stdout,
                    stderr=stderr,
                    check=False,
                    timeout=SAMPLE_TIMEOUT_SECONDS,
                )
        record["exit_code"] = completed.returncode
        verify_provenance(root, binary, env, manifest)
        if completed.returncode != 0:
            raise ValueError(f"measurement exited {completed.returncode}")
        report = json.loads((run_dir / "result.json").read_text())
        validate_report(report, options)
        record["status"] = "validated"
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired,
            ValueError, KeyError, TypeError) as error:
        record["status"] = "failed"
        record["failure"] = str(error)
    save_manifest(output, manifest)
    return record["status"] == "validated"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--approved-campaign", action="store_true")
    args = parser.parse_args()
    if not args.approved_campaign:
        parser.error("campaign requires completed calibration, WAL envelope, review, and approval")
    # benchmark/cmd/durable-comparison remains three levels below the repository root.
    root = Path(__file__).resolve().parents[3]
    output = args.output.resolve()
    output.mkdir(mode=0o700)
    manifest = {"status": "preparing", "runs": [], "host": host_environment(output)}
    save_manifest(output, manifest)
    try:
        mount = manifest["host"]["output_mount"]
        if mount.get("fstype") != "ext4" or not mount.get("source", "").startswith("/dev/"):
            raise ValueError("campaign requires an ext4 output mount with a /dev/ source")
        manifest["git_head"] = command_output(["git", "rev-parse", "HEAD"], root).strip()
        manifest["git_status"] = command_output(["git", "status", "--short"], root)
        binary, env = build_binary(root, output, manifest)
        manifest["status"] = "running"
        save_manifest(output, manifest)
        for cell in cells():
            if not run_sample(root, output, binary, env, manifest, cell):
                raise ValueError("measurement rejected, see last run's failure and raw output")
        manifest["status"] = "complete"
    except (OSError, subprocess.CalledProcessError, ValueError, KeyError, TypeError) as error:
        manifest["status"] = "failed"
        manifest["failure"] = str(error)
        print(f"campaign stopped: {error}. Evidence retained in {output}", file=sys.stderr)
    save_manifest(output, manifest)
    return 0 if manifest["status"] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
