"""One service's life: stopped -> starting -> ready -> stopping -> stopped (or failed).

switchboard runs the service's launcher as a child process in its own session, with SWITCHBOARD=1 and
SWITCHBOARD_SERVICE=<name> in the environment, and its output appended to <state_dir>/logs/<name>.log. Lines the
launcher prints as ">>> STATUS: ..." (the launcher protocol, see the README) become the status shown while it
starts. The service is ready when its upstream answers GET ready_path with 2xx. Stopping it is SIGTERM to the
launcher, which runs its own cleanup (stop the container, kill the server); after stop_timeout_s its whole process
group gets SIGKILL.

The launcher is not tied to switchboard's lifetime: after a switchboard restart it is found again through its
pidfile (or, if something else started the service, its upstream simply answering: "external"). An external
launcher is never stopped by switchboard, not even when it doesn't answer within start_timeout_s.
"""
import asyncio
import json
import os
import signal
import statistics
import subprocess
import time
from collections import deque
from pathlib import Path

import aiohttp

STOPPED, STARTING, READY, STOPPING, FAILED = "stopped", "starting", "ready", "stopping", "failed"


def ago(seconds):
    s = int(max(0, seconds))
    if s < 90:
        return f"{s} s"
    if s < 5400:
        return f"{round(s / 60)} min"
    return f"{s / 3600:.1f} h"


class Service:
    def __init__(self, cfg, adapter, hub):
        self.cfg = cfg
        self.name = cfg.name
        self.title = cfg.title
        self.adapter = adapter
        self.hub = hub
        self.state = STOPPED
        self.since = time.time()
        self.pid = None
        self.proc = None
        self.external = False          # answering, but not started by switchboard (no launcher pid known)
        self.adopted = False           # launcher found again after a switchboard restart
        self.detail = ""
        self.failure = None
        self.stop_reason = None
        self.log = deque(maxlen=300)
        self.inflight = 0              # proxied requests in progress that count as use
        self.websockets = 0
        self.last_activity = 0.0
        self.calls = deque(maxlen=500)  # start times of requests that count as use
        self.recent_uses = deque(maxlen=20)  # (time, "POST /prompt from 192.0.2.10") - shown so it's clear what counted
        self.busy_text = None           # what busy_reason() last said
        self.busy_since = None
        self.started_at = None
        self.ready_at = None
        self.stopping_at = None
        self.usage = {}                # {memory pool: GiB} it holds, measured (memory.py)
        self.port_errors = {}
        self.warm_note = None          # why a keep_warm service isn't running yet
        saved = hub.saved(self.name)
        self.start_times = saved.get("start_times", [])
        self.stop_times = saved.get("stop_times", [])
        run = saved.get("run", {})                     # what the previous switchboard knew (see save_run)
        self.last_activity = run.get("last_activity", 0.0)
        self.calls.extend(run.get("calls", []))
        self.recent_uses.extend(tuple(u) for u in run.get("recent_uses", []))
        self._saved_run = run
        self._changed = asyncio.Event()
        self._task = None
        self._tail_task = None
        self._log_pos = 0
        self._log_buf = b""
        self._status_detail = None

    # -- bookkeeping ---------------------------------------------------------------------------------------------
    @property
    def usage_gib(self):
        """What it holds of the main pool: unified memory, or GPU memory with a discrete GPU."""
        return self.usage.get(self.hub.memory.pools[0], 0.0)

    @property
    def log_path(self):
        return self.hub.state_dir / "logs" / f"{self.name}.log"

    def notify(self):
        ev, self._changed = self._changed, asyncio.Event()
        ev.set()

    async def wait_change(self, timeout):
        ev = self._changed
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        except asyncio.TimeoutError:
            pass

    def _set(self, state, msg=None, level="info"):
        self.state = state
        self.since = time.time()
        self.busy_text = self.busy_since = None
        if msg:
            self.hub.event(self.name, msg, level)
        self.notify()

    def start_estimate(self):
        recent = self.start_times[-5:]
        return statistics.median(recent) if recent else self.cfg.start_estimate_s

    def stop_estimate(self):
        recent = self.stop_times[-5:]
        return statistics.median(recent) if recent else 10.0

    def _remember(self, key, value):
        lst = getattr(self, key)
        lst.append(round(value, 1))
        del lst[:-20]
        self.hub.save(self.name, {key: lst})

    def begin(self, activity, what=""):
        if activity:
            now = time.time()
            after = self.hub.cfg.use_event_after_s      # the first use after this long unused goes in the event log
            if after and self.state == READY and now - max(self.last_activity, self.ready_at or 0) >= after:
                idle = now - self.last_activity if self.last_activity else None
                self.hub.event(self.name, f"in use{f' again after {ago(idle)} idle' if idle else ''}: {what}")
            self.inflight += 1
            self.last_activity = now
            self.calls.append(self.last_activity)
            self.recent_uses.append((self.last_activity, what))

    def end(self, activity):
        if activity:
            self.inflight -= 1
            self.last_activity = time.time()

    def idle_for(self):
        return time.time() - max(self.last_activity, self.ready_at or 0)

    def quiet_needed(self):
        """(seconds of quiet needed before another service may take this one's memory, session description or None).
        A lone call needs idle_grace_s. Calls that look like a session (an agent working, a person chatting: at least
        session_min_calls within session_window_s) need session_factor x the longest gap between them, so a pause
        that is normal for this session isn't mistaken for the end of it; capped at session_grace_max_s."""
        c = self.cfg
        last = self.last_activity
        if not last:
            return c.idle_grace_s, None
        bursts = []                     # [first, last] of runs of calls less than session_burst_s apart
        for t in self.calls:
            if not last - c.session_window_s <= t <= last:
                continue
            if bursts and t - bursts[-1][1] < c.session_burst_s:
                bursts[-1][1] = t
            else:
                bursts.append([t, t])
        if len(bursts) < max(2, c.session_min_calls):
            return c.idle_grace_s, None
        gap = max(b[0] - a[1] for a, b in zip(bursts, bursts[1:]))
        need = min(max(c.session_factor * gap, c.idle_grace_s), max(c.session_grace_max_s, c.idle_grace_s))
        return need, f"used {len(bursts)} times over {ago(bursts[-1][1] - bursts[0][0])}, up to {ago(gap)} apart"

    def quiet_reason(self):
        """Why it is still protected by its quiet period (None once quiet long enough). `force` skips this."""
        need, session = self.quiet_needed()
        idle = self.idle_for()
        if idle >= need:
            return None
        why = (f"last used {ago(idle)} ago" if self.last_activity >= (self.ready_at or 0)
               else f"started {ago(idle)} ago")
        if session:
            why += f"; looks like an active session ({session})"
        return why + f"; free after {ago(need)} without use ({ago(need - idle)} from now)"

    def used_recently(self):
        """Like quiet_reason, but only real use counts (not that it was started a moment ago): for "stop it unless
        someone is using it" (a delegating launcher being closed, e.g. an NVIDIA Sync app)."""
        if not self.last_activity:
            return None
        need, _ = self.quiet_needed()
        idle = time.time() - self.last_activity
        return f"last used {ago(idle)} ago" if idle < need else None

    async def busy_reason(self):
        """Work in progress that switchboard never interrupts (not even when forced): requests in flight, or a job
        the service reports (its busy probe)."""
        r = None
        if self.inflight > 0:
            r = f"{self.inflight} request{'s' if self.inflight > 1 else ''} in progress"
        elif self.state == READY:
            r = await self.adapter.busy(self, self.hub.session)
        now = time.time()
        if r is None:
            self.busy_since = None
        elif self.busy_since is None:
            self.busy_since = now
        if r and self.busy_since and now - self.busy_since >= 30:
            r += f", busy for {ago(now - self.busy_since)}"
        self.busy_text = r
        return r

    def can_stop(self):
        """Only a launcher switchboard started (or re-adopted) is switchboard's to stop."""
        return self.pid is not None and not self.external

    # -- the launcher process ----------------------------------------------------------------------------------
    def _alive(self):
        if self.proc is not None:
            return self.proc.poll() is None        # also reaps it
        if not self.pid:
            return False
        try:
            os.kill(self.pid, 0)
            with open(f"/proc/{self.pid}/stat") as f:
                return f.read().rsplit(")", 1)[1].split()[0] != "Z"
        except (OSError, IndexError):
            return False

    async def start(self, reason):
        if self.state in (STARTING, READY, STOPPING):
            return
        self.failure = None
        self.external = self.adopted = False
        self.stop_reason = None
        await self.hub.memory.before_start(self.hub, self)
        if self.hub.paused or self.state in (STARTING, READY, STOPPING):     # changed while the page cache dropped
            return
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.log_path, "ab") as f:
            self._log_pos = f.tell()
            self._log_buf = b""
            f.write(f"\n=== {time.strftime('%F %T')} switchboard: starting {self.name} ({reason})\n".encode())
            f.flush()
            env = {**os.environ, **self.cfg.env, "SWITCHBOARD": "1", "SWITCHBOARD_SERVICE": self.name}
            try:
                # Plain Popen, not asyncio's subprocess: asyncio kills its children when the loop closes, and the
                # launcher must outlive switchboard (restarts re-adopt it; SIGKILL would skip its cleanup).
                self.proc = subprocess.Popen(self.cfg.command, stdin=subprocess.DEVNULL, stdout=f,
                                             stderr=subprocess.STDOUT, cwd=self.cfg.cwd, env=env, start_new_session=True)
            except OSError as e:
                self.failure = f"could not run the launcher: {e}"
                self._set(FAILED, f"{self.title}: {self.failure}", "error")
                return
        self.pid = self.proc.pid
        self.started_at = time.time()
        self.ready_at = None
        self.detail = "starting"
        self._status_detail = None
        self._set(STARTING, f"starting {self.title} ({reason})")
        self._ensure_tail()
        self._task = asyncio.create_task(self._supervise())

    async def adopt(self):
        """At switchboard start: find a launcher left running by a previous switchboard, or a service started
        by something else."""
        pid = None
        if self.cfg.pidfile:
            try:
                pid = int(Path(self.cfg.pidfile).read_text().split()[0])
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    cmdline = f.read().replace(b"\0", b" ").decode(errors="replace")
                if os.path.basename(self.cfg.command[-1]) not in cmdline:
                    pid = None
            except (OSError, ValueError, IndexError):
                pid = None
        ready = await self.probe_ready()
        if pid:
            self.pid = pid
            if not self._managed(pid):
                # A launcher that wasn't started by switchboard (run by hand or from NVIDIA Sync without the
                # delegate): it stops itself when its own parent goes away, but it is not ours to stop.
                self.external = True
            self.adopted = True
            same = self._saved_run.get("pid") == pid           # the launcher the previous switchboard ran
            self.started_at = (same and self._saved_run.get("started_at")) or time.time()
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self._log_pos = self.log_path.stat().st_size if self.log_path.exists() else 0
            self._ensure_tail()
            how = ("started outside switchboard, which leaves it alone until it stops" if self.external
                   else "taking it over")
            if ready:
                self.ready_at = (same and self._saved_run.get("ready_at")) or time.time()
                self._set(READY, f"{self.title} is running (launcher pid {pid}, {how})")
            else:
                if self.external:
                    self.detail = (f"started outside switchboard (launcher pid {pid}); waiting for it to answer on "
                                   f"{self.cfg.upstream}")
                self._set(STARTING, f"{self.title} is starting (launcher pid {pid}, {how})")
            self._task = asyncio.create_task(self._supervise())
        elif ready:
            self.external = True
            self.ready_at = time.time()
            self._set(READY, f"{self.title} is already answering on {self.cfg.upstream} (not started by switchboard)")
            self._task = asyncio.create_task(self._supervise())

    def save_run(self):
        """At shutdown: uptime and use history, so a restarted switchboard reports them and keeps quiet periods."""
        self.hub.save(self.name, {"run": {
            "pid": self.pid, "started_at": self.started_at, "ready_at": self.ready_at,
            "last_activity": self.last_activity, "calls": list(self.calls), "recent_uses": list(self.recent_uses)}})

    def _managed(self, pid):
        try:
            with open(f"/proc/{pid}/environ", "rb") as f:
                return b"SWITCHBOARD=1" in f.read().split(b"\0")
        except OSError:
            return False

    async def probe_ready(self):
        try:
            async with self.hub.session.get(self.cfg.upstream + self.cfg.ready_path, allow_redirects=False,
                                            timeout=aiohttp.ClientTimeout(total=3)) as r:
                await r.read()
                return 200 <= r.status < 300
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            return False

    async def _supervise(self):
        fails = 0
        overdue = False
        try:
            while True:
                if self.state == STARTING:
                    if not self._alive():
                        await self._reap()
                        if self.external:
                            # Its output isn't in our log, so there is no failure to report: it was closed.
                            pid, self.pid, self.external, self.adopted, self.detail = self.pid, None, False, False, ""
                            self._set(STOPPED, f"{self.title}: the launcher started outside switchboard (pid {pid}) "
                                               "exited before it answered")
                            return
                        self.pid = None
                        self.failure = self._failure_text()
                        self._set(FAILED, f"{self.title} failed to start: {self.failure}", "error")
                        return
                    if await self.probe_ready():
                        self.ready_at = time.time()
                        took = self.ready_at - self.started_at
                        if not self.adopted:
                            self._remember("start_times", took)
                        self.detail = ""
                        self._set(READY, f"{self.title} ready" + ("" if self.adopted else f" after {took:.0f} s"))
                        continue
                    if time.time() - self.started_at > self.cfg.start_timeout_s and self.external:
                        # Not switchboard's to stop (run by hand, or from NVIDIA Sync before switchboard started):
                        # leave it running and keep watching until it answers or exits.
                        if not overdue:
                            overdue = True
                            self.detail = (f"started outside switchboard (launcher pid {self.pid}) and not answering "
                                           f"on {self.cfg.upstream} after {ago(time.time() - self.started_at)}; "
                                           "switchboard leaves it alone until it exits")
                            self.hub.event(self.name, f"{self.title}: {self.detail}", "warning")
                            self.notify()
                    elif time.time() - self.started_at > self.cfg.start_timeout_s:
                        self.failure = f"not ready after {self.cfg.start_timeout_s:.0f} s"
                        self.hub.event(self.name, f"{self.title}: {self.failure}; stopping it", "error")
                        await self._terminate()
                        self.pid = None
                        self._set(FAILED)
                        return
                    self._read_status_file()
                    await asyncio.sleep(1)
                elif self.state == READY:
                    if self.pid and not self._alive():
                        await self._reap()
                        if self.external:
                            pid, self.pid, self.external, self.adopted = self.pid, None, False, False
                            self._set(STOPPED, f"{self.title} stopped (the launcher started outside switchboard, "
                                               f"pid {pid}, exited)")
                            return
                        self.pid = None
                        self._set(STOPPED, f"{self.title} stopped on its own (launcher exited; see its log)", "warning")
                        return
                    if self.external and not self.pid:
                        fails = 0 if await self.probe_ready() else fails + 1
                        if fails >= 3:
                            self.external = False
                            self._set(STOPPED, f"{self.title} is no longer answering")
                            return
                    await asyncio.sleep(2)
                else:
                    return
        except asyncio.CancelledError:
            raise
        except Exception as e:  # never leave a service without supervision silently
            self.hub.event(self.name, f"supervisor error: {e!r}", "error")
            raise

    async def _reap(self):
        if self.proc is not None:
            for _ in range(50):
                if self.proc.poll() is not None:
                    return
                await asyncio.sleep(0.1)

    def _failure_text(self):
        self._read_log()
        lines = [line for line in self.log if line.strip()]
        for line in reversed(lines):
            if ">>> STATUS:" in line and ("REFUSED" in line or "ERROR" in line or "fail" in line.lower()):
                return line.split(">>> STATUS:", 1)[1].strip()
        for line in reversed(lines):
            if "ERROR" in line or "error" in line:
                return line.strip()[-300:]
        rc = self.proc.poll() if self.proc else None
        return f"the launcher exited (code {rc})" if rc is not None else "the launcher exited"

    async def stop(self, reason):
        """Stop it (no-op unless starting or ready). Returns once the launcher has exited."""
        if self.state == STOPPING:
            while self.state == STOPPING:
                await self.wait_change(1)
            return True
        if self.state not in (STARTING, READY) or not self.can_stop():
            return False
        was = self.state
        self.stop_reason = reason
        self.stopping_at = time.time()
        self._set(STOPPING, f"stopping {self.title} ({reason})")
        if self._task:
            self._task.cancel()
        t0 = time.time()
        await self._terminate()
        if was == READY:
            self._remember("stop_times", time.time() - t0)
        self.pid = None
        self.proc = None
        self.detail = ""
        self._set(STOPPED, f"{self.title} stopped after {time.time() - t0:.0f} s")
        return True

    async def _terminate(self):
        pid = self.pid
        if not pid:
            return
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.time() + self.cfg.stop_timeout_s
        while self._alive() and time.time() < deadline:
            await asyncio.sleep(0.5)
        if self._alive():
            self.hub.event(self.name, f"{self.title}: launcher still running after {self.cfg.stop_timeout_s:.0f} s; killing it", "warning")
            for target in (lambda: os.killpg(pid, signal.SIGKILL), lambda: os.kill(pid, signal.SIGKILL)):
                try:
                    target()
                except OSError:
                    pass
            if self.cfg.kill_command:
                try:
                    p = await asyncio.create_subprocess_exec(*self.cfg.kill_command, stdout=asyncio.subprocess.DEVNULL,
                                                             stderr=asyncio.subprocess.DEVNULL)
                    await asyncio.wait_for(p.wait(), 120)
                except (OSError, asyncio.TimeoutError):
                    pass
        await self._reap()

    # -- what the launcher says ----------------------------------------------------------------------------------
    def _ensure_tail(self):
        if self._tail_task is None or self._tail_task.done():
            self._tail_task = asyncio.create_task(self._tail())

    async def _tail(self):
        while True:
            self._read_log()
            await asyncio.sleep(0.5)

    def _read_log(self):
        """Take in what the launcher wrote since last time (also called right before reporting a failure)."""
        try:
            with open(self.log_path, "rb") as f:
                f.seek(self._log_pos)
                data = f.read()
                self._log_pos = f.tell()
        except OSError:
            return
        if not data:
            return
        self._log_buf += data
        *lines, self._log_buf = self._log_buf.split(b"\n")
        for raw in lines:
            line = raw.decode(errors="replace").replace("\r", "").rstrip()
            if not line:
                continue
            self.log.append(line)
            if ">>> " in line and self.state == STARTING:
                text = line.split(">>> ", 1)[1]
                self.detail = text.split("STATUS: ", 1)[-1].strip()
                self.notify()

    def _read_status_file(self):
        if not self.cfg.status_file:
            return
        try:
            st = os.stat(self.cfg.status_file)
            if st.st_mtime < (self.started_at or 0) - 1:
                return
            detail = json.loads(Path(self.cfg.status_file).read_text()).get("detail")
        except (OSError, ValueError, AttributeError):
            return
        if detail and detail != self._status_detail:
            self._status_detail = detail
            self.detail = detail
            self.notify()

    # -- reporting -----------------------------------------------------------------------------------------------
    def status(self):
        now = time.time()
        ram = {"ram_usage_gib": round(self.usage.get("ram", 0.0), 1), "ram_gib": self.cfg.ram_gib,
               "ram_max_gib": self.cfg.ram_max_gib} if "ram" in self.hub.memory.pools else {}
        return {
            "name": self.name, "title": self.title, "adapter": self.cfg.adapter, "state": self.state,
            "since": self.since, "detail": self.detail, "failure": self.failure, "stop_reason": self.stop_reason,
            "pid": self.pid, "external": self.external, "adopted": self.adopted,
            "inflight": self.inflight, "websockets": self.websockets,
            "last_activity": self.last_activity or None,
            "idle_s": round(self.idle_for()) if self.state == READY else None,
            "busy": self.busy_text if self.state == READY else None,
            "quiet": self.quiet_reason() if self.state == READY else None,
            "calls_10min": sum(1 for t in self.calls if t > now - 600),
            "recent_uses": [{"t": t, "what": w} for t, w in list(self.recent_uses)[-8:]],
            "started_at": self.started_at, "ready_at": self.ready_at,
            "elapsed_s": round(now - self.started_at, 1) if self.state == STARTING and self.started_at else None,
            "start_estimate_s": round(self.start_estimate()), "stop_estimate_s": round(self.stop_estimate()),
            "usage_gib": round(self.usage_gib, 1), "memory_gib": self.cfg.memory_gib, "max_gib": self.cfg.max_gib,
            **ram,
            "listen": self.cfg.listen, "upstream": self.cfg.upstream, "keep_warm": self.cfg.keep_warm,
            "idle_grace_s": self.cfg.idle_grace_s, "idle_stop_s": self.cfg.idle_stop_s, "priority": self.cfg.priority,
            "port_errors": self.port_errors, "warm_note": self.warm_note if self.state in (STOPPED, FAILED) else None, "info_html": self.cfg.info_html or self.adapter.options.get("info_html", ""),
        }
