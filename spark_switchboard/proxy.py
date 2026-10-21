"""The public ports. A ready service gets its traffic passed straight through (HTTP, SSE streams, websockets);
otherwise the request is classified by the service's adapter and either starts the service and waits for it,
gets the status page, or gets an answer shaped for its protocol (503 + Retry-After). While switchboard is paused,
nothing starts: browsers get the status page (with a Resume button), everything else a 503 saying so."""
import asyncio
import time

import aiohttp
from aiohttp import web
from multidict import CIMultiDict
from yarl import URL

from . import pages
from .adapters import NAVIGATE, PASSIVE, TAB, WAKE, is_websocket
from .service import FAILED, READY, STARTING, STOPPING, ago

HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "trailers",
       "transfer-encoding", "upgrade", "expect", "content-length"}


class UpstreamDown(Exception):
    pass


class Proxy:
    def __init__(self, hub):
        self.hub = hub
        self._sessions = {}

    def session(self, svc):
        s = self._sessions.get(svc.name)
        if s is None or s.closed:
            s = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=0, enable_cleanup_closed=True),
                timeout=aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=None),
                auto_decompress=False, cookie_jar=aiohttp.DummyCookieJar(),
                skip_auto_headers=("User-Agent", "Accept", "Accept-Encoding", "Content-Type"))
            self._sessions[svc.name] = s
        return s

    async def close(self):
        for s in self._sessions.values():
            await s.close()

    def app_for(self, svc):
        app = web.Application(client_max_size=1 << 31, handler_args={"max_field_size": 65536, "max_line_size": 65536})

        async def handle(request):
            return await self.handle(request, svc)
        app.router.add_route("*", "/{tail:.*}", handle)
        return app

    # -- dispatch --------------------------------------------------------------------------------------------------
    async def handle(self, request, svc):
        if request.path.startswith("/__switchboard/") or request.path == "/__switchboard":
            return await self.hub.control(request, svc)
        kind = svc.adapter.classify(request)
        if svc.adapter.home_page(request):
            if svc.state != READY:
                self._want_from_browser(svc)
            return pages.service_page(self.hub, svc, 200, request)
        if svc.state == READY:
            try:
                return await self.forward(request, svc, kind)
            except UpstreamDown as e:
                # The launcher may just have died; its supervisor notices within a few seconds.
                self.hub.event(svc.name, f"{svc.title} not answering on {svc.cfg.upstream}: {e}", "warning")
                info = self.info(svc, kind, request=request)
                info.update(status=502, code="upstream_down", retry_after=10,
                            message=f"{svc.title} on {self.hub.cfg.hostname} is not answering; try again shortly.")
                return svc.adapter.unavailable(request, svc, info)
        synthetic = await svc.adapter.synthetic(request, svc, self.hub)
        if synthetic is not None:
            return synthetic
        return await self.not_ready(request, svc, kind)

    def _want_from_browser(self, svc):
        if svc.state == FAILED and time.time() - svc.since < 60:
            return None            # just failed: the page shows why, with a Retry button
        return self.hub.scheduler.want(svc, "opened in a browser")

    async def not_ready(self, request, svc, kind):
        sched = self.hub.scheduler
        if self.hub.paused:                 # starts nothing and holds nothing
            if kind == NAVIGATE:
                return pages.service_page(self.hub, svc, 503, request)
            return svc.adapter.unavailable(request, svc, self.info(svc, kind, request=request))
        if kind == NAVIGATE:
            self._want_from_browser(svc)
            return pages.service_page(self.hub, svc, 503, request)
        pending = svc.name in sched.wants or svc.state == STARTING
        recently_failed = svc.state == FAILED and time.time() - svc.since < 60
        if kind in (TAB, PASSIVE) and not pending or recently_failed and not pending:
            return svc.adapter.unavailable(request, svc, self.info(svc, kind, request=request))
        if kind == WAKE:
            client = request.headers.get("User-Agent", "").split(" ")[0][:40] or "a client"
            sched.want(svc, f"{request.method} {request.path} ({client} from {request.remote})")
            await sched.settled()            # planned, so the ETA knows whether other services must go idle first
        wait = self.hold_window(request, svc)
        eta = sched.eta(svc)
        if wait <= 0 or (eta is not None and eta > wait):
            return svc.adapter.unavailable(request, svc, self.info(svc, kind, request=request))
        if eta is None and kind != WAKE:
            # Waiting on other services: don't hold a tab's background requests (they'd use up the browser's few
            # connections to this host, which the status page needs too).
            return svc.adapter.unavailable(request, svc, self.info(svc, kind, request=request))
        w = sched.wants.get(svc.name)
        if await sched.wait_ready(svc, wait, interest=(kind == WAKE)) and svc.state == READY:
            try:
                return await self.forward(request, svc, kind)
            except UpstreamDown:
                pass
        # The last request waiting for it gave up. Only one that could start it: an open tab's background requests and
        # probes (TAB, PASSIVE) never start or keep a want, so they don't drop one either.
        if kind == WAKE and w is not None and sched.abandoned(w):
            info = self.info(svc, kind, gave_up=True, request=request)
            sched.cancel(svc, "dropped: the request waiting for it gave up")
            return svc.adapter.unavailable(request, svc, info)
        return svc.adapter.unavailable(request, svc, self.info(svc, kind, request=request))

    def hold_window(self, request, svc):
        h = request.headers.get("X-Switchboard-Wait")
        if h is not None:
            try:
                return max(0.0, min(float(h), 3600.0))
            except ValueError:
                pass
        return svc.cfg.hold_s

    def info(self, svc, kind=None, gave_up=False, request=None):
        """What to tell a client that can't be served right now. gave_up: the want goes with this answer (nobody else
        waits for it), so it won't start by itself. request: the client's, so that while paused a client that may
        resume (remote_resume, or this machine itself) is told how."""
        sched = self.hub.scheduler
        host = self.hub.cfg.hostname
        w = sched.wants.get(svc.name)
        eta = sched.eta(svc)
        port = svc.cfg.listen[0] if svc.cfg.listen else None
        page = f" Status: http://{host}:{port}/" if port else ""
        code = "loading"
        if svc.state == READY:
            msg, code = f"{svc.title} is running on {host}.", "ready"
        elif svc.state == STARTING and svc.external:
            msg = f"{svc.title} on {host} was started outside switchboard and is not answering yet."
        elif svc.state == STARTING:
            msg = f"{svc.title} is starting on {host}; ready in about {ago(eta)}."
        elif svc.state == STOPPING and w is None:
            msg = f"{svc.title} on {host} is stopping ({svc.stop_reason})."
            code = "stopping"
        elif self.hub.paused and self.hub.may_resume(request):
            msg = (f"switchboard on {host} is paused, so it doesn't start {svc.title}; resume it on the dashboard "
                   f"(http://{host}:{self.hub.cfg.admin_port}/) or with `spark-switchboard resume`.")
            code = "paused"
        elif self.hub.paused:
            # Typically a workstation whose owner is using it: say why and for how long, and don't invite a caller on
            # another machine to resume it (it can't).
            msg = (f"switchboard on {host} has been paused for {ago(time.time() - self.hub.paused_since)} "
                   f"({self.hub.paused_reason}), so it doesn't start {svc.title}; only {host} itself can resume it. "
                   "Try again later.")
            code = "paused"
        elif w is not None and w.phase == "making_room":
            msg = f"{svc.title} is starting on {host} ({w.text}); ready in about {ago(eta)}."
        elif w is not None and w.phase == "waiting" and gave_up:
            msg = f"{svc.title} can't start on {host} yet ({w.text}); it won't start by itself, so try again later."
            code = "waiting"
        elif w is not None and w.phase == "waiting":
            msg = f"{svc.title} will start on {host} once memory is free: {w.text}."
            code = "waiting"
        elif w is not None and w.phase == "no_room":
            msg = f"{svc.title} can't start on {host} right now: {w.text}."
            if gave_up:
                msg += " It won't start by itself; try again later."
            code = "no_memory"
        elif w is not None:
            msg = f"{svc.title} is starting on {host}; ready in about {ago(eta)}."
        elif svc.state == FAILED:
            msg = f"{svc.title} failed to start on {host}: {svc.failure}."
            code = "failed"
        else:
            msg = (f"{svc.title} is not running on {host}; it starts on the next request that uses it "
                   f"(or open it in a browser).")
            code = "stopped"
        if code in ("stopped", "ready"):
            retry = None
        elif code == "paused":
            retry = 60
        else:
            retry = int(min(max(eta if eta is not None else 30, 5), 600))
        return {"message": msg + page, "code": code, "retry_after": retry, "state": svc.state, "eta_s": eta,
                "paused": self.hub.paused_status() if code == "paused" else None}

    # -- passing traffic through -------------------------------------------------------------------------------------
    def request_headers(self, request):
        h = CIMultiDict()
        for k, v in request.headers.items():
            if k.lower() not in HOP:
                h.add(k, v)
        xff = request.headers.get("X-Forwarded-For")
        h["X-Forwarded-For"] = f"{xff}, {request.remote}" if xff else (request.remote or "")
        h.setdefault("X-Forwarded-Host", request.host)
        h.setdefault("X-Forwarded-Proto", request.scheme)
        return h

    async def forward(self, request, svc, kind):
        if is_websocket(request):
            return await self.forward_ws(request, svc)
        activity = svc.adapter.is_activity(request, kind)
        svc.begin(activity, f"{request.method} {request.path} from {request.remote}")
        try:
            headers = self.request_headers(request)
            data = None
            if request.body_exists:
                if request.headers.get("Content-Length") is not None:
                    headers["Content-Length"] = request.headers["Content-Length"]
                data = request.content.iter_chunked(1 << 16)
            url = URL(svc.cfg.upstream + request.raw_path, encoded=True)
            try:
                up = await self.session(svc).request(request.method, url, headers=headers, data=data,
                                                     allow_redirects=False)
            except aiohttp.ClientConnectorError as e:
                raise UpstreamDown(str(e)) from None
            try:
                resp = web.StreamResponse(status=up.status, reason=up.reason)
                for k, v in up.headers.items():
                    if k.lower() not in HOP:
                        resp.headers.add(k, v)
                cl = up.headers.get("Content-Length")
                if cl is not None and up.status not in (204, 304):
                    resp.content_length = int(cl)
                await resp.prepare(request)
                key = svc.adapter.capture_key(request) if up.status == 200 else None
                captured = bytearray() if key else None
                async for chunk in up.content.iter_any():
                    await resp.write(chunk)
                    if captured is not None and len(captured) < 1 << 20:
                        captured += chunk
                await resp.write_eof()
                if captured:
                    self.hub.remember(svc.name, key, bytes(captured).decode(errors="replace"))
                return resp
            except BaseException:
                up.close()           # client went away mid-stream: drop the upstream connection (stops generation)
                raise
            finally:
                up.release()
        finally:
            svc.end(activity)

    async def forward_ws(self, request, svc):
        headers = CIMultiDict((k, v) for k, v in request.headers.items()
                              if k.lower() not in HOP and not k.lower().startswith("sec-websocket"))
        protocols = [p.strip() for p in request.headers.get("Sec-WebSocket-Protocol", "").split(",") if p.strip()]
        url = URL(svc.cfg.upstream + request.raw_path, encoded=True)
        try:
            up = await self.session(svc).ws_connect(url, headers=headers, protocols=protocols, max_msg_size=0,
                                                    autoclose=False)
        except aiohttp.WSServerHandshakeError as e:
            return web.Response(status=e.status or 502, text=f"upstream refused the websocket: {e.message}\n")
        except aiohttp.ClientConnectorError as e:
            raise UpstreamDown(str(e)) from None
        ws = web.WebSocketResponse(protocols=(up.protocol,) if up.protocol else (), max_msg_size=0, autoclose=False)
        try:
            await ws.prepare(request)
        except BaseException:
            await up.close()
            raise
        svc.websockets += 1

        async def pump(src, dst):
            async for msg in src:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await dst.send_str(msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    await dst.send_bytes(msg.data)
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    await dst.close(code=msg.data or 1000, message=(msg.extra or "").encode()[:120])
                    return
                else:
                    return
        tasks = [asyncio.create_task(pump(ws, up)), asyncio.create_task(pump(up, ws))]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            svc.websockets -= 1
            await up.close()
            await ws.close()
        return ws
