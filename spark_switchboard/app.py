"""The daemon: loads the config, re-adopts running services, binds the public ports, runs the scheduler, and serves
the admin dashboard/API (admin_port) plus a small control API on every public port under /__switchboard/."""
import asyncio
import json
import os
import signal
import socket
import time
from collections import deque
from ipaddress import ip_address

import aiohttp
from aiohttp import web

from . import adapters, memory, pages
from .proxy import Proxy
from .scheduler import Scheduler
from .service import READY, STARTING, STOPPING, Service, ago

PAUSED_HINT = "resume it on the dashboard or with `spark-switchboard resume`"
LOCAL_RESUME_HINT = "only {host} itself can resume it (remote_resume = false); try again later"


def is_local_client(remote, local=None):
    """True when a request comes from this machine: a loopback address, or the very address it connected to (a
    client on the box that used the LAN address). The default dual-stack listener reports IPv4 clients as
    IPv4-mapped IPv6 addresses (::ffff:127.0.0.1), which Python before 3.13 doesn't call loopback."""
    def addr(a):
        try:
            a = ip_address(str(a or "").split("%")[0])
        except ValueError:
            return None
        if a.version == 6 and a.ipv4_mapped:
            return a.ipv4_mapped
        return a
    r = addr(remote)
    if r is None:
        return False
    return r.is_loopback or (local is not None and r == addr(local))


class Hub:
    def __init__(self, cfg):
        self.cfg = cfg
        self.state_dir = cfg.state_dir
        (self.state_dir / "logs").mkdir(parents=True, exist_ok=True)
        self.events = deque(maxlen=400)
        try:                            # the dashboard keeps its history across restarts
            with open(self.state_dir / "events.log", "rb") as f:
                start = max(0, f.seek(0, os.SEEK_END) - 200_000)
                f.seek(start)
                for line in f.read().splitlines()[1 if start else 0:]:     # [1:]: a partial first line
                    try:
                        self.events.append(json.loads(line))
                    except ValueError:
                        pass
        except OSError:
            pass
        self._state_path = self.state_dir / "state.json"
        try:
            self._state = json.loads(self._state_path.read_text())
        except (OSError, ValueError):
            self._state = {}
        self.memory = memory.make(cfg.memory)
        self.session = None
        self.services = {}
        for name, scfg in cfg.services.items():
            if scfg.enabled:
                self.services[name] = Service(scfg, adapters.make(scfg), self)
        self.scheduler = Scheduler(self)
        self.proxy = Proxy(self)
        self._runners = []
        self._tasks = []
        # Paused (see pause()): start nothing, stop what runs. Kept in memory only, so a restarted switchboard isn't
        # paused, unless the pause persists (pause_persists, `pause --persist`): then state.json keeps it until resume.
        self.paused_since = None
        self.paused_reason = None
        self.paused_persists = False
        kept = self._state.get("paused")
        if isinstance(kept, dict) and kept.get("since"):
            self.paused_since, self.paused_reason, self.paused_persists = kept["since"], kept.get("reason"), True
            since = time.strftime("%F %T", time.localtime(self.paused_since))
            self.event("switchboard", f"still paused ({self.paused_reason}, since {since}): the pause persists across "
                                      "restarts until `spark-switchboard resume`")

    @property
    def paused(self):
        return self.paused_since is not None

    # -- persistence & events --------------------------------------------------------------------------------------
    def saved(self, name):
        return self._state.get("services", {}).get(name, {})

    def save(self, name, values):
        self._state.setdefault("services", {}).setdefault(name, {}).update(values)
        self._write_state()

    def _write_state(self):
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._state, indent=1))
        os.replace(tmp, self._state_path)

    def remember(self, name, key, text):
        if self.saved(name).get("remembered", {}).get(key) != text:
            self.save(name, {"remembered": {**self.saved(name).get("remembered", {}), key: text}})

    def remembered(self, name, key):
        return self.saved(name).get("remembered", {}).get(key)

    def event(self, service, text, level="info"):
        e = {"t": time.time(), "service": service, "level": level, "text": text}
        self.events.append(e)
        print(f"[{time.strftime('%F %T')}] [{service}] {level.upper() + ': ' if level != 'info' else ''}{text}", flush=True)
        try:
            with open(self.state_dir / "events.log", "a") as f:
                f.write(json.dumps(e) + "\n")
        except OSError:
            pass

    # -- status ----------------------------------------------------------------------------------------------------
    def memory_status(self):
        """The main pool as total_gib and available_gib; with a discrete GPU ("kind": "gpu") that is GPU memory, and
        host RAM is under "ram"."""
        try:
            total, available = self.memory.total(), self.memory.available()
        except OSError:
            return {}
        main = self.memory.pools[0]
        out = {"total_gib": round(total[main], 1), "available_gib": round(available[main], 1)}
        if "ram" in self.memory.pools:
            out.update(kind="gpu", gpu=getattr(self.memory, "name", None), error=self.memory.error,
                       ram={"total_gib": round(total["ram"], 1), "available_gib": round(available["ram"], 1)})
        return out

    def service_status(self, svc):
        w = self.scheduler.wants.get(svc.name)
        info = self.proxy.info(svc)
        return {**svc.status(), "want": w.as_dict() if w else None, "eta_s": info["eta_s"], "message": info["message"],
                "code": info["code"], "host": self.cfg.hostname, "memory": self.memory_status(),
                "paused": self.paused_status(),
                "log_tail": list(svc.log)[-12:],
                "others": [{"name": s.name, "title": s.title, "state": s.state, "listen": s.cfg.listen}
                           for s in self.services.values() if s is not svc]}

    def paused_status(self):
        if not self.paused:
            return None
        return {"since": self.paused_since, "reason": self.paused_reason, "persists": self.paused_persists,
                "remote_resume": self.cfg.remote_resume}

    def status(self):
        return {"host": self.cfg.hostname, "time": time.time(), "memory": self.memory_status(),
                "paused": self.paused_status(),
                "services": [self.service_status(s) for s in self.services.values()],
                "events": list(self.events)[-60:]}

    # -- pause -------------------------------------------------------------------------------------------------------
    async def pause(self, reason, kill=False, persist=None):
        """Stop every service switchboard runs and start nothing until resume(): pending starts are dropped,
        keep-warm does nothing, and requests that would start a service are told that switchboard is paused. Work in
        progress is not interrupted (unless kill): such a service is stopped as soon as its work is done. Services
        switchboard didn't start are left alone. persist (default: the config's pause_persists) keeps the pause in
        state.json, so a restarted switchboard is still paused; a persisting pause stays so until resume().
        Returns {service: what happens to it}."""
        persist = self.cfg.pause_persists if persist is None else persist
        if not self.paused:
            self.paused_since = time.time()
            self.paused_reason = reason
            self.event("switchboard", f"paused ({reason}): starting nothing, stopping the apps it runs"
                       + ("; the pause persists across restarts" if persist else ""))
        elif persist and not self.paused_persists:
            self.event("switchboard", f"the pause now persists across restarts ({reason})")
        if persist and not self.paused_persists:
            self.paused_persists = True
            self._state["paused"] = {"since": self.paused_since, "reason": self.paused_reason}
            self._write_state()
        for svc in self.services.values():
            self.scheduler.cancel(svc, "dropped: switchboard is paused")
        result = {}
        for svc in self.services.values():
            if svc.state == STOPPING:
                result[svc.name] = "stopping"
            elif svc.state not in (STARTING, READY):
                continue
            elif not svc.can_stop():
                result[svc.name] = "not started by switchboard: left running"
            elif (busy := await svc.busy_reason()) and not kill:
                result[svc.name] = f"busy ({busy}): stops when that work is done"
            else:
                why = "switchboard paused" + (f", interrupting: {busy}" if busy else "")
                self.scheduler.stop_in_background(svc, why)
                result[svc.name] = "stopping" + (f", interrupting: {busy}" if busy else "")
        for svc in self.services.values():
            svc.notify()
        self.scheduler.kick()
        return result

    def resume(self, reason):
        if not self.paused:
            return False
        took = time.time() - self.paused_since
        self.paused_since = self.paused_reason = None
        if self.paused_persists:
            self.paused_persists = False
            self._state.pop("paused", None)
            self._write_state()
        self.event("switchboard", f"resumed ({reason}) after {ago(took)} paused")
        for svc in self.services.values():
            svc.notify()
        self.scheduler.kick()
        return True

    def paused_hint(self):
        return PAUSED_HINT if self.cfg.remote_resume else LOCAL_RESUME_HINT.format(host=self.cfg.hostname)

    def paused_error(self):
        return web.json_response({"ok": False, "paused": True,
                                  "error": f"switchboard is paused, so it starts nothing; {self.paused_hint()}"},
                                 status=409)

    def may_resume(self, request):
        """Whether this client may resume a pause: anyone, unless remote_resume = false (this machine only)."""
        return self.cfg.remote_resume or (request is not None and self._local(request))

    def _resume_refused(self):
        return web.json_response({"ok": False, "resumed": False,
                                  "error": f"resume is only accepted on {self.cfg.hostname} itself "
                                           "(remote_resume = false): `spark-switchboard resume` there"}, status=403)

    # -- control API -------------------------------------------------------------------------------------------------
    def _guard(self, request):
        """POSTs need X-Switchboard (a custom header can't be sent cross-site without CORS, which we never allow)."""
        if request.method == "POST" and not request.headers.get("X-Switchboard"):
            raise web.HTTPForbidden(text="send the header X-Switchboard: 1\n")

    async def control(self, request, svc):
        """/__switchboard/... on a service's own port (what its status page talks to)."""
        self._guard(request)
        action = request.path[len("/__switchboard"):].strip("/")
        if action in ("", "status"):
            if request.query.get("want") == "1":
                self.scheduler.touch(svc)
            if action == "" and request.method == "GET" and "text/html" in request.headers.get("Accept", ""):
                return pages.service_page(self, svc, 200, request)
            return web.json_response(self.service_status(svc), headers={"Cache-Control": "no-store"})
        if request.method != "POST":
            raise web.HTTPMethodNotAllowed(request.method, ["POST"])
        if action == "start":
            if self.paused and svc.state not in (READY, STARTING):
                return self.paused_error()
            force = request.query.get("force") == "1"
            w = self.scheduler.want(svc, "switch now (status page)" if force else "status page", force)
            return web.json_response({"ok": True, "want": w.as_dict() if w else None})
        if action == "cancel":
            return web.json_response({"ok": self.scheduler.cancel(svc, "cancelled on the status page")})
        if action == "resume":
            if not self.may_resume(request):
                return self._resume_refused()
            return web.json_response({"ok": True, "resumed": self.resume(f"status page of {svc.title}")})
        raise web.HTTPNotFound()

    def _local(self, request):
        sock = request.transport.get_extra_info("sockname") if request.transport else None
        return is_local_client(request.remote, sock[0] if sock else None)

    def admin_app(self):
        app = web.Application()
        r = app.router

        async def dashboard(request):
            return pages.dashboard(self, request)

        async def status(request):
            return web.json_response(self.status(), headers={"Cache-Control": "no-store"})

        async def events(request):
            return web.json_response(list(self.events))
        r.add_get("/", dashboard)
        r.add_get("/api/status", status)
        r.add_get("/api/events", events)
        r.add_get("/api/services/{name}", self._api_service)
        r.add_get("/api/services/{name}/log", self._api_log)
        r.add_post("/api/services/{name}/{action}", self._api_action)
        r.add_post("/api/pause", self._api_pause)
        r.add_post("/api/resume", self._api_resume)
        return app

    async def _api_pause(self, request):
        self._guard(request)
        kill = request.query.get("kill") == "1"
        if kill and not self._local(request):
            return web.json_response({"ok": False, "error": "kill=1 is only accepted on the machine itself "
                                      "(spark-switchboard pause --kill)"}, status=403)
        persist = {"1": True, "0": False}.get(request.query.get("persist", ""))       # else pause_persists
        services = await self.pause(request.query.get("reason") or "dashboard", kill, persist)
        return web.json_response({"ok": True, "paused": self.paused_status(), "services": services})

    async def _api_resume(self, request):
        self._guard(request)
        if not self.may_resume(request):
            return self._resume_refused()
        return web.json_response({"ok": True, "resumed": self.resume(request.query.get("reason") or "dashboard")})

    def _svc(self, request):
        svc = self.services.get(request.match_info["name"])
        if svc is None:
            raise web.HTTPNotFound(text=f"no service {request.match_info['name']!r}\n")
        return svc

    async def _api_service(self, request):
        svc = self._svc(request)
        if request.query.get("want") == "1":
            self.scheduler.touch(svc)
        return web.json_response(self.service_status(svc), headers={"Cache-Control": "no-store"})

    async def _api_log(self, request):
        svc = self._svc(request)
        n = int(request.query.get("lines", "200"))
        return web.Response(text="\n".join(list(svc.log)[-n:]) + "\n")

    async def _api_action(self, request):
        self._guard(request)
        svc = self._svc(request)
        action = request.match_info["action"]
        reason = request.query.get("reason") or "dashboard"
        if action == "start":
            if svc.state in (READY, STARTING):
                return web.json_response({"ok": True, "state": svc.state})
            if self.paused:
                return self.paused_error()
            w = self.scheduler.want(svc, reason, request.query.get("force") == "1")
            return web.json_response({"ok": True, "want": w.as_dict() if w else None})
        if action == "cancel":
            return web.json_response({"ok": self.scheduler.cancel(svc, f"cancelled ({reason})")})
        if action == "stop":
            self.scheduler.cancel(svc, f"cancelled ({reason})")
            if not svc.can_stop():
                return web.json_response({"ok": False, "error": f"{svc.title} is {svc.state}" +
                                          (" and was not started by switchboard" if svc.external else "")}, status=409)
            busy = await svc.busy_reason()
            kill = request.query.get("kill") == "1"
            if kill and not self._local(request):
                return web.json_response({"ok": False, "error": "kill=1 is only accepted on the machine itself "
                                          "(spark-switchboard stop --kill)"}, status=403)
            if busy and not kill:
                return web.json_response({"ok": False, "busy": busy, "error": f"{svc.title} is busy ({busy}); "
                                          "switchboard does not interrupt work in progress"}, status=409)
            if request.query.get("if_idle") == "1" and (why := svc.used_recently()):
                return web.json_response({"ok": False, "error": f"{svc.title} is in use ({why})"}, status=409)
            asyncio.create_task(svc.stop(f"{reason}" + (f", interrupting: {busy}" if busy else "")))
            return web.json_response({"ok": True})
        raise web.HTTPNotFound()

    # -- running ------------------------------------------------------------------------------------------------------
    def _listen_socket(self, port):
        """`bind` unset, "" or "::": every interface, IPv4 and IPv6 (one dual-stack socket, so the CLI's
        http://127.0.0.1:<admin_port> reaches it too). Any other address: just that one."""
        bind = self.cfg.bind
        if bind and bind != "::":
            fam = socket.AF_INET6 if ":" in bind else socket.AF_INET
            return socket.create_server((bind, port), family=fam, backlog=512)
        if socket.has_dualstack_ipv6():
            return socket.create_server(("::", port), family=socket.AF_INET6, dualstack_ipv6=True, backlog=512)
        if bind == "::":
            return socket.create_server(("::", port), family=socket.AF_INET6, backlog=512)
        return socket.create_server(("0.0.0.0", port), backlog=512)

    async def _serve(self, runner, port, svc=None):
        """Bind `port`, retrying while something else holds it (e.g. an app still started the old way)."""
        warned = False
        while True:
            try:
                sock = self._listen_socket(port)
                break
            except OSError as e:
                if svc is not None:
                    svc.port_errors[str(port)] = str(e)
                if not warned:
                    self.event(svc.name if svc else "switchboard", f"port {port} is taken ({e.strerror}); retrying "
                               "every 5 s", "warning")
                    warned = True
                await asyncio.sleep(5)
        site = web.SockSite(runner, sock)
        await site.start()
        if svc is not None:
            svc.port_errors.pop(str(port), None)
        self.event(svc.name if svc else "switchboard", f"listening on :{port}" + (" (was taken)" if warned else ""))

    async def run(self, stop_event=None):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        for svc in self.services.values():
            await svc.adopt()
        self._tasks.append(asyncio.create_task(self.scheduler.run()))
        runner = web.AppRunner(self.admin_app(), access_log=None)
        await runner.setup()
        self._runners.append(runner)
        self._tasks.append(asyncio.create_task(self._serve(runner, self.cfg.admin_port)))
        for svc in self.services.values():
            r = web.AppRunner(self.proxy.app_for(svc), access_log=None, handler_cancellation=True)
            await r.setup()
            self._runners.append(r)
            for port in svc.cfg.listen:
                self._tasks.append(asyncio.create_task(self._serve(r, port, svc)))
        self.event("switchboard", f"up: {', '.join(f'{s.name} :{s.cfg.listen[0]}' for s in self.services.values() if s.cfg.listen)}"
                   f"; dashboard :{self.cfg.admin_port}")
        stop_event = stop_event or asyncio.Event()
        await stop_event.wait()

    async def shutdown(self, stop_services=False):
        """Leaves the services running (a restarted switchboard re-adopts them) unless stop_services."""
        if stop_services:
            await asyncio.gather(*(s.stop("switchboard shutting down") for s in self.services.values() if s.can_stop()))
        for s in self.services.values():
            try:
                s.save_run()
            except OSError:
                pass
        for t in self._tasks:
            t.cancel()
        for s in self.services.values():
            for t in (s._task, s._tail_task):
                if t:
                    t.cancel()
        for r in self._runners:
            await r.cleanup()
        await self.proxy.close()
        if self.session:
            await self.session.close()


async def serve(cfg):
    hub = Hub(cfg)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        await hub.run(stop)
    finally:
        hub.event("switchboard", "shutting down (services keep running)")
        await hub.shutdown()
