"""How much memory is free, and how much each running service holds.

Two kinds of machine ([memory] source):

  "system"  unified memory, as on the DGX Spark (GB10): CPU and GPU share one pool, so MemAvailable is the number
            that matters for "does it fit". One pool, "memory": a service's memory_gib and max_gib.
  "gpu"     one discrete NVIDIA GPU (a workstation): the GPU's own memory, from nvidia-smi, is one pool ("gpu":
            memory_gib and max_gib), and host RAM (MemAvailable) another ("ram": ram_gib and ram_max_gib), for apps
            that offload weights or activations to the CPU.

What a service holds is measured, not taken from its config: GPU allocations per process (nvidia-smi) plus the
anonymous and shared resident memory of every process that belongs to it (its launcher's process tree, plus the
processes of the docker containers it names). On unified memory both count against the one pool; with a discrete
GPU the first counts against "gpu" and the second against "ram". File-backed pages (mmapped checkpoints) are left
out: they are page cache, which MemAvailable already counts as free.
"""
import asyncio
import fnmatch
import os
import re
import shutil
import time
from collections import defaultdict
from pathlib import Path

# How a pool is named in messages: "needs 20 GiB free" on unified memory, "needs 20 GiB of GPU memory free" otherwise.
LABELS = {"memory": "", "gpu": "GPU memory", "ram": "RAM"}


def amount(gib, pool):
    """'20 GiB', or '20 GiB of GPU memory' / '20 GiB of RAM' for the pools of a discrete GPU."""
    return f"{gib:.0f} GiB" + (f" of {LABELS[pool]}" if LABELS.get(pool) else "")


# A container's 64-hex id in /proc/<pid>/cgroup, for the cgroup layouts in use:
#   systemd driver   0::/system.slice/docker-<id>.scope   (Docker's default with cgroup v2, as on DGX OS)
#   cgroupfs driver  0::/docker/<id>   or, on cgroup v1, 12:memory:/docker/<id>
#   Podman           0::/machine.slice/libpod-<id>.scope   or   .../libpod_parent/libpod-<id>
_CONTAINER_ID = re.compile(r"(?:docker|libpod)[-/]([0-9a-f]{64})(?![0-9a-f])")


def container_id(cgroup_text):
    """The container id a process belongs to, from the text of /proc/<pid>/cgroup; None for a host process."""
    m = _CONTAINER_ID.search(cgroup_text)
    return m.group(1) if m else None


def _number(text):
    try:
        return float(text)
    except ValueError:
        return None                     # "[N/A]" (GB10 has no memory of its own), "[Not Supported]"


def parse_gpus(text):
    """`nvidia-smi --query-gpu=index,uuid,name,memory.total,memory.free --format=csv,noheader,nounits` ->
    [{"index", "uuid", "name", "total_mib", "free_mib"}]; a memory value nvidia-smi doesn't report is None."""
    gpus = []
    for line in text.splitlines():
        parts = [x.strip() for x in line.split(",")]
        if len(parts) != 5:
            continue
        index, uuid, name, total, free = parts
        gpus.append({"index": index, "uuid": uuid, "name": name,
                     "total_mib": _number(total), "free_mib": _number(free)})
    return gpus


def pick_gpu(gpus, which):
    """The GPU a config's `gpu` names: its nvidia-smi index (0) or its UUID ("GPU-..."). None if there is none."""
    which = str(which).strip()
    return next((g for g in gpus if which in (g["index"], g["uuid"])), None)


def parse_compute_apps(text, uuid=None):
    """`nvidia-smi --query-compute-apps=pid,used_memory[,gpu_uuid] --format=csv,noheader,nounits` -> {pid: MiB}, only
    for the GPU with this uuid when one is given (and the lines carry it)."""
    gpu = {}
    for line in text.splitlines():
        parts = [x.strip() for x in line.split(",")]
        try:
            pid, mib = int(parts[0]), float(parts[1])
        except (ValueError, IndexError):
            continue
        if uuid and len(parts) > 2 and parts[2] != uuid:
            continue
        gpu[pid] = gpu.get(pid, 0) + mib
    return gpu


RETRY_AFTER_TIMEOUT_S = 30    # a command that timed out isn't run again for this long
_timed_out = {}               # program's path -> (its process that timed out, when, why)


async def _run(*cmd, timeout=10):
    """(stdout, None) of a short command, or (None, why) when it is missing, fails or is slow. One that is slow is
    killed. After a timeout the program isn't run again for RETRY_AFTER_TIMEOUT_S, nor while the process that timed
    out is still alive (one stuck in the driver can't be killed): a hanging nvidia-smi neither piles up processes nor
    holds up every scheduler tick for `timeout` seconds."""
    name = os.path.basename(cmd[0])
    key = shutil.which(cmd[0]) or cmd[0]
    if key in _timed_out:
        p, at, why = _timed_out[key]
        if p.returncode is None or time.monotonic() - at < RETRY_AFTER_TIMEOUT_S:
            return None, why
        del _timed_out[key]
    try:
        p = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    except FileNotFoundError:
        return None, f"{name} not found (not installed, or not on PATH)"
    except OSError as e:
        return None, f"{name} could not run ({e.strerror or e})"
    try:
        out, err = await asyncio.wait_for(p.communicate(), timeout)
    except asyncio.TimeoutError:
        try:
            p.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(p.wait(), 2)
        except asyncio.TimeoutError:
            pass
        why = f"{name} did not answer within {timeout:g} s"
        _timed_out[key] = (p, time.monotonic(), why)
        return None, why
    if p.returncode != 0:
        # nvidia-smi prints its errors ("Failed to initialize NVML: Driver/library version mismatch") on stdout
        lines = [x.strip() for x in (out + b"\n" + err).decode(errors="replace").splitlines() if x.strip()]
        return None, f"{name} failed (exit {p.returncode})" + (f": {lines[0][:200]}" if lines else "")
    return out.decode(errors="replace"), None


def _meminfo():
    out = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":", 1)
            out[k] = int(v.split()[0])          # kB
    return out


class SystemMemory:
    """Unified memory (DGX Spark): one pool, MemAvailable."""
    pools = ("memory",)
    error = None                        # why the numbers can't be trusted right now (see GPUMemory), else None

    def __init__(self, opts):
        self._containers = {}
        self._containers_at = 0.0
        self.free_floor_gib = float(opts.get("free_floor_gib", 0))
        self.reclaim_command = opts.get("reclaim_command")

    def total(self):
        return {"memory": _meminfo()["MemTotal"] / 2**20}

    def available(self):
        return {"memory": _meminfo()["MemAvailable"] / 2**20}

    def free_gib(self):
        return _meminfo()["MemFree"] / 2**20

    async def refresh(self):
        """Re-read what available() and total() don't read on every call (nothing here: /proc/meminfo is cheap)."""

    async def before_start(self, hub, svc):
        """GB10: the GPU driver allocates its own buffers (a new CUDA context, for one) from truly free pages and
        does not wait for the kernel to evict page cache, so with the cache full a start can fail with
        NV_ERR_NO_MEMORY / CUDA_ERROR_OUT_OF_MEMORY although MemAvailable is ample. Below free_floor_gib of MemFree,
        run reclaim_command (drop the clean page cache) first."""
        if not self.reclaim_command or self.free_gib() >= self.free_floor_gib:
            return
        before = self.free_gib()
        try:
            p = await asyncio.create_subprocess_exec(*self.reclaim_command, stdout=asyncio.subprocess.DEVNULL,
                                                     stderr=asyncio.subprocess.PIPE)
            _, err = await asyncio.wait_for(p.communicate(), 60)
        except (OSError, asyncio.TimeoutError) as e:
            hub.event(svc.name, f"could not drop the page cache before starting: {e}", "warning")
            return
        if p.returncode:
            hub.event(svc.name, f"dropping the page cache failed: {err.decode().strip()[:200]}", "warning")
        else:
            hub.event(svc.name, f"dropped the page cache before starting {svc.title} (truly free memory "
                                f"{before:.0f} -> {self.free_gib():.0f} GiB)")

    async def usage(self, services):
        """{service name: {pool: GiB held}} for the given running services."""
        held = await self._held(services)
        return {name: {"memory": ram + gpu} for name, (ram, gpu) in held.items()}

    async def _held(self, services):
        """{service name: (GiB of resident memory, GiB of GPU allocations)}."""
        gpu = await self._gpu()
        cmap = await self._container_names() if any(s.cfg.containers for s in services) else {}
        return await asyncio.to_thread(self._usage, services, gpu, cmap)

    def _usage(self, services, gpu, cmap):
        ppid, cgroup = {}, {}
        for d in os.listdir("/proc"):
            if not d.isdigit():
                continue
            pid = int(d)
            try:
                with open(f"/proc/{d}/stat") as f:
                    ppid[pid] = int(f.read().rsplit(")", 1)[1].split()[1])
                if cmap:
                    with open(f"/proc/{d}/cgroup") as f:
                        cid = container_id(f.read())
                    if cid:
                        cgroup[pid] = cid
            except (OSError, IndexError, ValueError):
                continue
        children = defaultdict(list)
        for p, pp in ppid.items():
            children[pp].append(p)
        out = {}
        for s in services:
            pids = set()
            stack = [s.pid] if s.pid else []
            while stack:
                p = stack.pop()
                if p in pids:
                    continue
                pids.add(p)
                stack += children.get(p, [])
            if s.cfg.containers:
                for p, cid in cgroup.items():
                    name = cmap.get(cid)
                    if name and any(fnmatch.fnmatch(name, pat) for pat in s.cfg.containers):
                        pids.add(p)
            kb = 0
            for p in pids:
                try:
                    with open(f"/proc/{p}/status") as f:
                        for line in f:
                            if line.startswith(("RssAnon:", "RssShmem:")):
                                kb += int(line.split()[1])
                except OSError:
                    pass
            out[s.cfg.name] = (kb / 2**20, sum(gpu.get(p, 0) for p in pids) / 1024)
        return out

    async def _gpu(self):
        """{pid: MiB} of GPU allocations; empty when nvidia-smi is missing or slow."""
        out, _ = await _run("nvidia-smi", "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits")
        return parse_compute_apps(out or "")

    async def _container_names(self):
        """{full container id: name} from `docker ps` (with Podman, its docker-compatible CLI); the last names when it
        fails."""
        if time.monotonic() - self._containers_at < 15:
            return self._containers
        out, _ = await _run("docker", "ps", "--no-trunc", "--format", "{{.ID}} {{.Names}}")
        if out is not None:
            self._containers = dict(line.split(" ", 1) for line in out.splitlines() if " " in line)
        self._containers_at = time.monotonic()
        return self._containers


class GPUMemory(SystemMemory):
    """One discrete NVIDIA GPU: its free memory (nvidia-smi, refreshed by refresh()) and host RAM (MemAvailable).
    `gpu` is the GPU's nvidia-smi index or UUID. Until nvidia-smi has answered once, the GPU counts as full. After a
    failure the last free figure stays for STALE_S (one slow answer changes nothing), then the GPU counts as full again
    until nvidia-smi answers: something else may have taken the memory meanwhile. `error` says why."""
    pools = ("gpu", "ram")
    STALE_S = 60

    def __init__(self, opts):
        super().__init__(opts)
        self.gpu = str(opts.get("gpu", 0))
        self.error = "GPU memory not read yet"
        self.name = None
        self.uuid = None
        self._total = self._free = 0.0          # GiB, from the last good nvidia-smi
        self._read_at = None                    # time.monotonic() of the last good nvidia-smi

    def total(self):
        return {"gpu": self._total, "ram": _meminfo()["MemTotal"] / 2**20}

    def available(self):
        fresh = self._read_at is not None and (self.error is None or time.monotonic() - self._read_at < self.STALE_S)
        return {"gpu": self._free if fresh else 0.0, "ram": _meminfo()["MemAvailable"] / 2**20}

    async def refresh(self):
        out, why = await _run("nvidia-smi", "--query-gpu=index,uuid,name,memory.total,memory.free",
                              "--format=csv,noheader,nounits")
        self.error = self.read(out, why)

    def read(self, out, why=None):
        """Take in nvidia-smi's --query-gpu output (None: it failed, for the reason `why`). Returns the error text, or
        None."""
        if out is None:
            return why or "nvidia-smi failed"
        g = pick_gpu(parse_gpus(out), self.gpu)
        if g is None:
            return f"nvidia-smi lists no GPU {self.gpu!r} (set [memory] gpu to an index or UUID it lists)"
        if g["total_mib"] is None or g["free_mib"] is None:
            return (f"nvidia-smi reports no memory of its own for GPU {self.gpu} ({g['name']}); on unified memory "
                    "(DGX Spark) use [memory] source = \"system\"")
        self.name, self.uuid = g["name"], g["uuid"]
        self._total, self._free = g["total_mib"] / 1024, g["free_mib"] / 1024
        self._read_at = time.monotonic()
        return None

    async def usage(self, services):
        held = await self._held(services)
        return {name: {"gpu": gpu, "ram": ram} for name, (ram, gpu) in held.items()}

    async def _gpu(self):
        """{pid: MiB} of allocations on this GPU only."""
        out, _ = await _run("nvidia-smi", "--query-compute-apps=pid,used_memory,gpu_uuid",
                            "--format=csv,noheader,nounits")
        return parse_compute_apps(out or "", self.uuid)


class FakeMemory:
    """For tests: a fixed total, and one file per holder in `dir` (<name>.gib holding a number).
    A service's usage is its own file; MemAvailable is the total minus every file. With ram_total_gib it stands in
    for a discrete GPU: <name>.gib is GPU memory and <name>.ram.gib host RAM."""
    error = None

    def __init__(self, opts):
        self.totals = {"memory": float(opts["total_gib"])}
        if "ram_total_gib" in opts:
            self.totals = {"gpu": float(opts["total_gib"]), "ram": float(opts["ram_total_gib"])}
        self.pools = tuple(self.totals)
        self.dir = Path(os.path.expanduser(opts["dir"]))
        self.dir.mkdir(parents=True, exist_ok=True)

    def _read(self, f):
        try:
            return float(f.read_text().strip() or 0)
        except (OSError, ValueError):
            return 0.0

    def _files(self, pool):
        if pool == "ram":
            return list(self.dir.glob("*.ram.gib"))
        return [f for f in self.dir.glob("*.gib") if not f.name.endswith(".ram.gib")]

    def total(self):
        return dict(self.totals)

    def available(self):
        return {p: t - sum(self._read(f) for f in self._files(p)) for p, t in self.totals.items()}

    async def refresh(self):
        pass

    async def usage(self, services):
        return {s.cfg.name: {p: self._read(self.dir / f"{s.cfg.name}{'.ram' if p == 'ram' else ''}.gib")
                             for p in self.pools} for s in services}

    async def before_start(self, hub, svc):
        pass


def make(opts):
    src = opts.get("source", "system")
    if src == "system":
        return SystemMemory(opts)
    if src == "gpu":
        return GPUMemory(opts)
    if src == "fake":
        return FakeMemory(opts)
    raise ValueError(f"unknown memory source {src!r}")
