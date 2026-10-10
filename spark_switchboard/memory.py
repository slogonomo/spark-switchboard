"""How much memory is free, and how much each running service holds.

On the DGX Spark (GB10) CPU and GPU share one pool, so MemAvailable is the number that matters for "does it fit".
What a service holds is measured, not taken from its config: GPU allocations per process (nvidia-smi) plus the
anonymous and shared resident memory of every process that belongs to it (its launcher's process tree, plus the
processes of the docker containers it names). File-backed pages (mmapped checkpoints) are left out: they are page
cache, which MemAvailable already counts as free.
"""
import asyncio
import fnmatch
import os
import re
import time
from collections import defaultdict
from pathlib import Path


# A container's 64-hex id in /proc/<pid>/cgroup, for the cgroup layouts in use:
#   systemd driver   0::/system.slice/docker-<id>.scope   (Docker's default with cgroup v2, as on DGX OS)
#   cgroupfs driver  0::/docker/<id>   or, on cgroup v1, 12:memory:/docker/<id>
#   Podman           0::/machine.slice/libpod-<id>.scope   or   .../libpod_parent/libpod-<id>
_CONTAINER_ID = re.compile(r"(?:docker|libpod)[-/]([0-9a-f]{64})(?![0-9a-f])")


def container_id(cgroup_text):
    """The container id a process belongs to, from the text of /proc/<pid>/cgroup; None for a host process."""
    m = _CONTAINER_ID.search(cgroup_text)
    return m.group(1) if m else None


def _meminfo():
    out = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":", 1)
            out[k] = int(v.split()[0])          # kB
    return out


class SystemMemory:
    def __init__(self, opts):
        self._containers = {}
        self._containers_at = 0.0
        self.free_floor_gib = float(opts.get("free_floor_gib", 0))
        self.reclaim_command = opts.get("reclaim_command")

    def total_gib(self):
        return _meminfo()["MemTotal"] / 2**20

    def available_gib(self):
        return _meminfo()["MemAvailable"] / 2**20

    def free_gib(self):
        return _meminfo()["MemFree"] / 2**20

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
        """{service name: GiB held} for the given running services."""
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
            out[s.cfg.name] = kb / 2**20 + sum(gpu.get(p, 0) for p in pids) / 1024
        return out

    async def _gpu(self):
        """{pid: MiB} of GPU allocations; empty when nvidia-smi is missing or slow."""
        try:
            p = await asyncio.create_subprocess_exec(
                "nvidia-smi", "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            out, _ = await asyncio.wait_for(p.communicate(), 10)
        except (OSError, asyncio.TimeoutError):
            return {}
        gpu = {}
        for line in out.decode().splitlines():
            try:
                pid, mib = (x.strip() for x in line.split(","))
                gpu[int(pid)] = gpu.get(int(pid), 0) + float(mib)
            except ValueError:
                pass
        return gpu

    async def _container_names(self):
        """{full container id: name} from `docker ps` (with Podman, its docker-compatible CLI)."""
        if time.monotonic() - self._containers_at < 15:
            return self._containers
        try:
            p = await asyncio.create_subprocess_exec(
                "docker", "ps", "--no-trunc", "--format", "{{.ID}} {{.Names}}",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            out, _ = await asyncio.wait_for(p.communicate(), 10)
            self._containers = dict(line.split(" ", 1) for line in out.decode().splitlines() if " " in line)
            self._containers_at = time.monotonic()
        except (OSError, asyncio.TimeoutError):
            pass
        return self._containers


class FakeMemory:
    """For tests: a fixed total, and one file per holder in `dir` (<name>.gib holding a number).
    A service's usage is its own file; MemAvailable is the total minus every file."""

    def __init__(self, opts):
        self.total = float(opts["total_gib"])
        self.dir = Path(os.path.expanduser(opts["dir"]))
        self.dir.mkdir(parents=True, exist_ok=True)

    def _read(self, f):
        try:
            return float(f.read_text().strip() or 0)
        except (OSError, ValueError):
            return 0.0

    def total_gib(self):
        return self.total

    def available_gib(self):
        return self.total - sum(self._read(f) for f in self.dir.glob("*.gib"))

    async def usage(self, services):
        return {s.cfg.name: self._read(self.dir / f"{s.cfg.name}.gib") for s in services}

    async def before_start(self, hub, svc):
        pass


def make(opts):
    src = opts.get("source", "system")
    if src == "system":
        return SystemMemory(opts)
    if src == "fake":
        return FakeMemory(opts)
    raise ValueError(f"unknown memory source {src!r}")
