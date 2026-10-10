"""Who runs: starts on demand, makes room, idle stops, keep-warm.

A request (or a person, or the API) *wants* a service. Each tick the scheduler goes through the wants in order:

  fits      enough memory for it now -> start it
  evict     stopping idle services frees enough -> stop them (lowest priority first, then least recently
            used), then start it on a later tick
  waiting   only services in use (requests in flight, a job queued, or used within their idle_grace_s) stand in
            the way -> wait for them; `force` (the "switch now" button) stops them anyway
  no room   even stopping every other service wouldn't free enough -> wait, saying so

A want lapses when nobody is waiting for it any more (no held request, no open status page) for want_ttl_s.

While switchboard is paused (Hub.pause) there are no wants and no keep-warm starts, and each service switchboard
runs is stopped once it isn't working.

Memory: "free" is MemAvailable minus the headroom running services may still grow into (max_gib minus what each
holds now). A service starts when free >= its memory_gib. Stopping a service frees max(max_gib, what it holds).
"""
import asyncio
import time

from .service import FAILED, READY, STARTING, STOPPED, STOPPING, ago

FITS, EVICT, BLOCKED, NO_ROOM = "fits", "evict", "blocked", "no_room"


class Want:
    def __init__(self, svc, reason, force):
        self.svc = svc
        self.reason = reason
        self.force = force
        self.created = self.touched = time.time()
        self.phase = "queued"          # queued | making_room | waiting | no_room
        self.text = ""
        self.blockers = []             # [(service, reason, in_progress)]
        self.victims = []
        self.holders = 0               # requests held waiting for it

    def as_dict(self):
        return {"service": self.svc.name, "reason": self.reason, "force": self.force, "since": self.created,
                "phase": self.phase, "text": self.text, "victims": [v.name for v in self.victims],
                "blockers": [{"name": s.name, "title": s.title, "reason": r, "in_progress": hard}
                             for s, r, hard in self.blockers],
                "waiting_requests": self.holders}


class Plan:
    def __init__(self, kind, free, need, victims=(), blockers=(), text=""):
        self.kind, self.free, self.need = kind, free, need
        self.victims, self.blockers, self.text = list(victims), list(blockers), text


class Scheduler:
    def __init__(self, hub):
        self.hub = hub
        self.wants = {}
        self.transition = None
        self._kick = asyncio.Event()
        self._ticked = asyncio.Event()
        self._usage_at = 0.0
        self._stopping = set()          # services being stopped by stop_in_background
        self._bg = set()                # their tasks (asyncio keeps only weak references)
        self._busy_at = 0.0

    @property
    def services(self):
        return self.hub.services

    # -- wants -----------------------------------------------------------------------------------------------------
    def want(self, svc, reason, force=False):
        if self.hub.paused:
            return None
        w = self.wants.get(svc.name)
        if w is not None:
            w.touched = time.time()
            if force and not w.force:
                w.force = True
                self.hub.event(svc.name, f"switch to {svc.title} forced ({reason})")
                self.kick()
            return w
        if svc.state in (READY, STARTING):
            return None
        w = Want(svc, reason, force)
        self.wants[svc.name] = w
        self.hub.event(svc.name, f"{svc.title} wanted: {reason}" + (" (forced)" if force else ""))
        self.kick()
        svc.notify()
        return w

    def touch(self, svc):
        w = self.wants.get(svc.name)
        if w is not None:
            w.touched = time.time()

    def cancel(self, svc, why="cancelled"):
        w = self.wants.pop(svc.name, None)
        if w is not None:
            self.hub.event(svc.name, f"start of {svc.title} {why}")
            svc.notify()
        return w is not None

    def kick(self):
        self._kick.set()

    async def settled(self, timeout=3.0):
        """Wait for the next completed tick (so a new want has been planned)."""
        ev = self._ticked
        self.kick()
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        except asyncio.TimeoutError:
            pass

    async def wait_ready(self, svc, timeout, interest=True):
        """Hold a request until the service is ready (True), it failed / nobody wants it any more (False), or
        `timeout` seconds pass (False)."""
        deadline = time.time() + timeout
        w = self.wants.get(svc.name)
        if w is not None:
            w.holders += 1
        try:
            while True:
                if svc.state == READY:
                    return True
                if svc.name not in self.wants and svc.state in (STOPPED, FAILED):
                    return False
                left = deadline - time.time()
                if left <= 0:
                    return False
                await svc.wait_change(min(left, 1.0))
                if interest:
                    self.touch(svc)
        finally:
            if w is not None:
                w.holders -= 1

    def eta(self, svc):
        """Seconds until it is probably ready, or None when that depends on other services going idle."""
        now = time.time()
        est = svc.start_estimate()
        if svc.state == READY:
            return 0
        if svc.state == STARTING:
            return max(3, est - (now - (svc.started_at or now)))
        if svc.state == STOPPING:
            return svc.stop_estimate() + est
        w = self.wants.get(svc.name)
        if w is None or w.phase == "queued":
            return est
        if w.phase == "making_room":
            left = max((v.stop_estimate() - (now - (v.stopping_at or now)) for v in w.victims), default=0)
            return max(0, left) + 3 + est
        return None

    # -- the loop ----------------------------------------------------------------------------------------------------
    async def run(self):
        while True:
            try:
                await asyncio.wait_for(self._kick.wait(), 1.0)
            except asyncio.TimeoutError:
                pass
            self._kick.clear()
            try:
                await self.tick()
            except Exception as e:
                self.hub.event("switchboard", f"scheduler error: {e!r}", "error")
                await asyncio.sleep(2)
            ev, self._ticked = self._ticked, asyncio.Event()
            ev.set()

    async def refresh_usage(self, max_age=2.0):
        if time.monotonic() - self._usage_at < max_age:
            return
        running = [s for s in self.services.values() if s.state in (STARTING, READY, STOPPING) or s.pid]
        usage = await self.hub.memory.usage(running) if running else {}
        for s in self.services.values():
            s.usage_gib = usage.get(s.name, 0.0)
        self._usage_at = time.monotonic()

    async def tick(self):
        now = time.time()
        paused = self.hub.paused
        for name, w in list(self.wants.items()):
            s = w.svc
            if paused:
                self.cancel(s, "dropped: switchboard is paused")
            elif s.state in (READY, STARTING):
                del self.wants[name]
            elif w.holders == 0 and now - w.touched > self.hub.cfg.want_ttl_s:
                self.cancel(s, "dropped: nobody is waiting for it any more")

        await self.refresh_usage(max_age=10)          # keeps the per-service memory on the pages current
        busy_transition = self.transition is not None and not self.transition.done()
        if self.wants and not busy_transition:
            await self.refresh_usage()
            for w in list(self.wants.values()):
                s = w.svc
                if s.state == STOPPING:
                    w.phase, w.text = "waiting", f"{s.title} is still stopping"
                    continue
                plan = await self.plan(s, w.force)
                if self.hub.paused or self.wants.get(s.name) is not w:     # paused or cancelled meanwhile
                    break
                w.blockers, w.text = plan.blockers, plan.text
                if plan.kind == FITS:
                    del self.wants[s.name]
                    await s.start(w.reason)
                    self._usage_at = 0          # the new service's headroom counts from now on
                    await self.refresh_usage(0)
                    continue
                if plan.kind == EVICT:
                    w.phase, w.victims = "making_room", plan.victims
                    self.transition = asyncio.create_task(self._evict(w, plan.victims))
                    s.notify()
                    break
                w.phase = "waiting" if plan.kind == BLOCKED else "no_room"
                s.notify()

        if time.monotonic() - self._busy_at > 5:     # keeps "busy for N min" and the status pages current
            self._busy_at = time.monotonic()
            for s in self.services.values():
                if s.state == READY:
                    await s.busy_reason()
        await self._idle_stops(now)
        if self.hub.paused:
            await self._drain()
        elif not self.wants and not busy_transition:
            await self._keep_warm(now)

    async def plan(self, target, force=False, evict=True):
        await self.refresh_usage()
        free = self.hub.memory.available_gib() - self.hub.cfg.reserve_gib
        others = [s for s in self.services.values() if s is not target]
        for s in others:
            if s.state in (STARTING, READY):
                free -= max(0.0, s.cfg.max_gib - s.usage_gib)
        need = target.cfg.memory_gib
        if free >= need:
            return Plan(FITS, free, need)
        if not evict:
            return Plan(NO_ROOM, free, need)

        def frees(s):
            return max(s.cfg.max_gib, s.usage_gib)

        evictable, blocked = [], []     # blocked: (service, why, hard); hard = work in progress, never interrupted
        for s in others:
            if s.external:
                continue                # not switchboard's to stop; its memory is measured like anything else's
            if s.state == STARTING:
                blocked.append((s, "starting", True))
            elif s.state == STOPPING:
                blocked.append((s, "stopping", True))
            elif s.state == READY and s.can_stop():
                why = await s.busy_reason()
                if why:
                    blocked.append((s, why, True))
                elif not force and (why := s.quiet_reason()):
                    blocked.append((s, why, False))
                else:
                    evictable.append(s)
        evictable.sort(key=lambda s: (s.cfg.priority, max(s.last_activity, s.ready_at or 0)))
        total, victims = free, []
        for s in evictable:
            if total >= need:
                break
            victims.append(s)
            total += frees(s)
        if total >= need:
            names = ", ".join(v.title for v in victims)
            return Plan(EVICT, free, need, victims=victims, text=f"stopping {names} to make room")
        if total + sum(frees(s) for s, _, _ in blocked) >= need:
            names = "; ".join(f"{s.title}: {why}" for s, why, _ in blocked)
            return Plan(BLOCKED, free, need, blockers=blocked, text=f"waiting for memory in use ({names})")
        external = [s.title for s in others if s.state in (STARTING, READY) and s.external]
        text = (f"needs {need:.0f} GiB free; {max(free, 0):.0f} GiB is free and stopping the services switchboard "
                f"runs would bring it to {total + sum(frees(s) for s, _, _ in blocked):.0f} GiB")
        if external:
            text += f" ({', '.join(external)} was not started by switchboard, so it isn't stopped)"
        else:
            text += " (something outside switchboard is holding memory)"
        return Plan(NO_ROOM, free, need, blockers=blocked, text=text)

    async def _evict(self, w, victims):
        try:
            await asyncio.gather(*(v.stop(f"making room for {w.svc.title}") for v in victims))
            # Containers and the driver hand memory back a few seconds after the processes exit.
            t0 = time.time()
            while time.time() - t0 < 20:
                self._usage_at = 0
                plan = await self.plan(w.svc, evict=False)
                if plan.kind == FITS:
                    break
                await asyncio.sleep(1)
        except Exception as e:
            self.hub.event(w.svc.name, f"error while making room: {e!r}", "error")
        finally:
            w.victims = []
            if w.phase == "making_room":
                w.phase = "queued"
            self.kick()

    def stop_in_background(self, s, reason):
        """Stop a service without waiting for it (idle stop, pause); one stop per service at a time."""
        if s.name in self._stopping:
            return
        self._stopping.add(s.name)

        async def stop():
            try:
                await s.stop(reason)
            finally:
                self._stopping.discard(s.name)
                self.kick()
        t = asyncio.create_task(stop())
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)

    async def _idle_stops(self, now):
        for s in self.services.values():
            t = s.cfg.idle_stop_s
            if not t or s.state != READY or not s.can_stop() or s.name in self._stopping:
                continue
            if s.idle_for() < t or await s.busy_reason():
                continue
            self.stop_in_background(s, f"unused for {ago(s.idle_for())}")

    async def _drain(self):
        """Paused: stop each service switchboard runs as soon as it isn't working (a pause never interrupts work)."""
        for s in self.services.values():
            if s.state not in (STARTING, READY) or not s.can_stop() or s.name in self._stopping:
                continue
            if await s.busy_reason():
                continue
            self.stop_in_background(s, "switchboard paused")

    async def _keep_warm(self, now):
        if any(s.state in (STARTING, STOPPING) and not s.external for s in self.services.values()):
            return
        for s in sorted(self.services.values(), key=lambda s: -s.cfg.priority):
            if not s.cfg.keep_warm:
                continue
            if s.state == FAILED and now - s.since < 600:
                continue
            if s.state not in (STOPPED, FAILED):
                continue
            plan = await self.plan(s, evict=False)
            if self.hub.paused or self.wants:            # paused, or someone wanted something meanwhile
                return
            if plan.kind == FITS:
                s.warm_note = None
                await s.start("keep warm: memory is free")
                return
            note = f"kept warm: starts when {plan.need:.0f} GiB is free ({max(plan.free, 0):.0f} GiB now)"
            if note.split(" (")[0] != (s.warm_note or "").split(" (")[0]:
                self.hub.event(s.name, note)
            s.warm_note = note

    def status(self):
        return {name: w.as_dict() for name, w in self.wants.items()}
