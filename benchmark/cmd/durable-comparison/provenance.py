"""Bind a campaign-owned build to its inputs and record the host environment."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command_output(command, root, env=None):
    return subprocess.check_output(command, cwd=root, env=env, text=True)


def source_manifest(root, env):
    raw = command_output(
        ["go", "list", "-mod=readonly", "-deps", "-json", "./cmd/durable-comparison"],
        root / "benchmark", env,
    )
    decoder = json.JSONDecoder()
    files = {root / "go.mod", root / "go.sum", root / "benchmark/go.mod",
             root / "benchmark/go.sum"}
    files.update((root / "benchmark/cmd/durable-comparison").glob("*.py"))
    while raw.strip():
        package, end = decoder.raw_decode(raw.lstrip())
        raw = raw.lstrip()[end:]
        directory = Path(package["Dir"])
        for key in ("GoFiles", "CgoFiles", "CFiles", "CXXFiles", "MFiles", "HFiles",
                    "FFiles", "SFiles", "SwigFiles", "SwigCXXFiles", "SysoFiles", "EmbedFiles"):
            files.update(directory / name for name in package.get(key, []))
        module = package.get("Module", {})
        module = module.get("Replace", module)
        if "GoMod" in module:
            files.add(Path(module["GoMod"]))
    return {str(path.resolve()): digest(path) for path in sorted(files)}


def external_ring_identity(root, env):
    module = json.loads(command_output(
        ["go", "list", "-mod=readonly", "-m", "-json",
         "github.com/freshdresch/go-data-structures"],
        root / "benchmark", env,
    ))
    if "Replace" in module:
        raise ValueError("external ring module must resolve without a replacement")
    return {key: module[key] for key in ("Path", "Version", "Sum", "GoModSum")}


def source_revision(hashes):
    encoded = json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def read_optional(path):
    try:
        return {"value": Path(path).read_text().strip()}
    except OSError as error:
        return {"unavailable": str(error)}


def backing_device(major_minor, sysfs=Path("/sys/dev/block")):
    metadata = {"major_minor": major_minor}
    if not isinstance(major_minor, str) or not re.fullmatch(r"[0-9]+:[0-9]+", major_minor):
        return dict(metadata, unavailable="mount has no valid MAJ:MIN")
    try:
        device = (sysfs / major_minor).resolve(strict=True)
    except OSError as error:
        return dict(metadata, unavailable=str(error))
    metadata["sysfs_path"] = str(device)
    for name in ("uevent", "device/model", "device/vendor", "loop/backing_file", "dm/name"):
        metadata[name] = read_optional(device / name)
    try:
        metadata["slaves"] = {
            "value": sorted(str(path.resolve()) for path in (device / "slaves").iterdir()),
        }
    except OSError as error:
        metadata["slaves"] = {"unavailable": str(error)}
    return metadata


def host_environment(output):
    mount_command = ["findmnt", "--json", "--target", str(output),
                     "--output", "TARGET,SOURCE,FSTYPE,OPTIONS,MAJ:MIN,FSROOT"]
    try:
        mount = json.loads(command_output(mount_command, output))["filesystems"][0]
    except (OSError, subprocess.CalledProcessError, ValueError, KeyError, IndexError) as error:
        mount = {"unavailable": str(error)}
    try:
        affinity = {"value": sorted(os.sched_getaffinity(0))}
    except (AttributeError, OSError) as error:
        affinity = {"unavailable": str(error)}
    cgroup = read_optional("/proc/self/cgroup")
    limits = {}
    # Record every visible cgroup-v2 ancestor because the effective limit may be inherited.
    unified = next((line[3:] for line in cgroup.get("value", "").splitlines()
                    if line.startswith("0::")), None)
    if unified is None:
        limits["unavailable"] = "no unified cgroup-v2 membership"
    else:
        directory = Path("/sys/fs/cgroup") / unified.lstrip("/")
        while directory.is_relative_to("/sys/fs/cgroup"):
            limits[str(directory)] = {name: read_optional(directory / name) for name in
                                      ("cpu.max", "cpuset.cpus.effective", "memory.max")}
            directory = directory.parent
    cpuinfo = read_optional("/proc/cpuinfo")
    models = sorted({line.split(":", 1)[1].strip()
                     for line in cpuinfo.get("value", "").splitlines()
                     if line.startswith(("model name", "Hardware"))})
    return {
        "platform": platform.platform(), "python": sys.version,
        "python_executable": sys.executable, "cpu_models": models or {"unavailable": cpuinfo},
        "cpu_affinity": affinity, "cgroup_membership": cgroup, "cgroup_limits": limits,
        "output_mount": mount, "mount_command": mount_command,
        "output_backing_device": backing_device(mount.get("maj:min")),
        "runtime_environment": {key: os.environ.get(key) for key in
                                ("GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GODEBUG")},
    }


# This campaign uses the reviewed Linux host compiler defaults only. System
# toolchain headers remain host inputs, recorded through the compiler identity.
COMPILER_DEFAULTS = {
    "CGO_CFLAGS": "-O2 -g", "CGO_CPPFLAGS": "", "CGO_CXXFLAGS": "-O2 -g",
    "CGO_FFLAGS": "-O2 -g", "CGO_LDFLAGS": "-O2 -g",
}
COMPILER_OVERRIDES = (
    "CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "OBJC_INCLUDE_PATH",
    "LIBRARY_PATH", "LD_LIBRARY_PATH", "LD_PRELOAD", "GCC_EXEC_PREFIX", "COMPILER_PATH",
    "SDKROOT", "SYSROOT", "CFLAGS", "CPPFLAGS", "CXXFLAGS", "FFLAGS", "LDFLAGS",
    "PKG_CONFIG", "PKG_CONFIG_PATH", "PKG_CONFIG_LIBDIR", "PKG_CONFIG_SYSROOT_DIR",
    "GOTOOLDIR", "GCCGO", "GOEXPERIMENT", "GOCACHEPROG",
)


def campaign_build_environment(inherited):
    for name, expected in COMPILER_DEFAULTS.items():
        if name in inherited and inherited[name] != expected:
            raise ValueError(f"unsupported compiler environment: {name}, unset it for the campaign")
    for name in inherited:
        forbidden = name in COMPILER_OVERRIDES or name.startswith("CGO_")
        if forbidden and name not in COMPILER_DEFAULTS and name != "CGO_ENABLED":
            raise ValueError(f"unsupported compiler environment: {name}, unset it for the campaign")
    for name, allowed in {
        "CC": ("gcc", "/usr/bin/gcc"), "CXX": ("g++", "/usr/bin/g++"),
        "CGO_ENABLED": ("1",), "GOROOT": ("/usr/local/go",),
    }.items():
        if name in inherited and inherited[name] not in allowed:
            raise ValueError(f"unsupported compiler environment: {name}, unset it for the campaign")
    # Do not pass unknown native-tool inputs through to the compiler subprocesses.
    env = {name: inherited[name] for name in (
        "HOME", "TMPDIR", "GOPATH", "GOCACHE", "GOMODCACHE", "GOPROXY", "GOSUMDB",
        "GOPRIVATE", "GONOPROXY", "GONOSUMDB", "GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GODEBUG",
    ) if name in inherited}
    env.update(COMPILER_DEFAULTS)
    env.update(
        PATH="/usr/local/go/bin:/usr/bin:/bin", CC="/usr/bin/gcc", CXX="/usr/bin/g++",
        CGO_ENABLED="1", GOENV="off", GOWORK="off", GOFLAGS="", GOTOOLCHAIN="local",
    )
    return env


def build_binary(root, output, manifest):
    env = campaign_build_environment(os.environ)
    for compiler in (env["CC"], env["CXX"]):
        version = command_output([compiler, "-dumpfullversion"], root, env).strip()
        if version != "14.2.0":
            raise ValueError(f"unsupported compiler version: {compiler} {version}, need 14.2.0")
    build_dir = output / "build"
    build_dir.mkdir(mode=0o700)
    binary = build_dir / "durable-comparison"
    go_env = json.loads(command_output(["go", "env", "-json"], root / "benchmark", env))
    command = ["go", "build", "-mod=readonly", "-pgo=off", "-o", str(binary),
               "./cmd/durable-comparison"]
    manifest["build"] = {"command": command, "cwd": str(root / "benchmark"), "go_env": go_env,
                         "enforced_environment": env}
    tools = {}
    for name, arguments in {
        "go": ["go", "version"], "cc": shlex.split(go_env["CC"]) + ["--version"],
        "cxx": shlex.split(go_env["CXX"]) + ["--version"],
    }.items():
        executable = shutil.which(arguments[0], path=env["PATH"])
        if executable is None:
            tools[name] = {"unavailable": arguments[0]}
            continue
        tools[name] = {"path": executable, "sha256": digest(Path(executable)),
                       "version": command_output(arguments, root, env)}
    manifest["build"]["tools"] = tools
    hashes = source_manifest(root, env)
    manifest["source_sha256"] = hashes
    manifest["revision"] = source_revision(hashes)
    with (build_dir / "stdout.log").open("w") as stdout:
        with (build_dir / "stderr.log").open("w") as stderr:
            completed = subprocess.run(command, cwd=root / "benchmark", env=env,
                                       stdout=stdout, stderr=stderr, check=False)
    manifest["build"]["exit_code"] = completed.returncode
    if completed.returncode != 0:
        raise ValueError("campaign build failed, see build/stderr.log")
    if source_manifest(root, env) != hashes:
        raise ValueError("source inputs changed during build")
    # source_manifest binds both module files and selected dependency paths and bytes.
    manifest["external_ring_module"] = external_ring_identity(root, env)
    manifest["build"]["binary_info"] = command_output(
        ["go", "version", "-m", str(binary)], root, env,
    )
    manifest["binary_sha256"] = digest(binary)
    manifest["binary"] = str(binary)
    binary.chmod(0o500)
    return binary, env


def verify_provenance(root, binary, env, manifest):
    if digest(binary) != manifest["binary_sha256"]:
        raise ValueError("campaign executable changed")
    if source_manifest(root, env) != manifest["source_sha256"]:
        raise ValueError("campaign source inputs changed")
