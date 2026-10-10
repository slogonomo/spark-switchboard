"""The generic web adapter: what a request means, and how to answer it while the service isn't running.

Every request to a public port is put in one of four kinds:

  NAVIGATE  a person opening the app in a browser (GET, text/html). Starts the service; gets the status page
            until it is ready.
  WAKE      someone deliberately using the service: any POST/PUT/PATCH/DELETE, or a GET from a program (no
            Referer/Origin: curl, an SDK, an MCP server). Starts the service and is held until it is ready.
            (Only the writes count as *use* afterwards, see is_activity: reads are often a client idling.)
  TAB       background traffic from a page that is already open (a GET or websocket carrying Origin/Referer:
            polling, reconnect attempts). Never starts the service, so a tab left open on a stopped app can't
            bring it back; while the service is starting anyway, it is held like WAKE.
  PASSIVE   probes (paths listed in passive_paths, HEAD, OPTIONS). Never start the service.

Subclasses change the classification for their protocol, answer some requests themselves while the service is
down (OpenAI's model list), and shape the "not ready" errors the way that protocol's clients expect.
"""
import fnmatch
import json
import time
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web

NAVIGATE, WAKE, TAB, PASSIVE = "navigate", "wake", "tab", "passive"


def is_websocket(request):
    return request.headers.get("Upgrade", "").lower() == "websocket"


def is_navigation(request):
    mode = request.headers.get("Sec-Fetch-Mode")
    if mode is not None:
        return mode == "navigate"
    # Plain-HTTP origins (http://<host>:8188 over a LAN or VPN) get no Sec-Fetch-* headers: fall back to Accept.
    return (request.method == "GET" and "text/html" in request.headers.get("Accept", "")
            and request.headers.get("X-Requested-With") is None)


def from_open_page(request):
    """Sent by a page of this same app (fetch/XHR/websocket from a tab), not typed in or sent by a program."""
    if request.headers.get("Origin"):
        return True
    ref = request.headers.get("Referer")
    return bool(ref) and urlsplit(ref).netloc.lower() == request.host.lower()


def wants_json(request):
    accept = request.headers.get("Accept", "")
    ctype = request.headers.get("Content-Type", "")
    return "json" in accept or "json" in ctype or "text/html" not in accept and request.method != "GET"


class WebAdapter:
    name = "web"
    has_home_page = False     # True: "/" in a browser is always switchboard's status page (see home_page)
    OPTIONS = frozenset({"info_html"})      # the keys a service's [options] may hold (others are config errors)

    def __init__(self, cfg):
        self.cfg = cfg
        self.options = cfg.options
        self._probe_cache = (0.0, None)

    # -- what a request means --------------------------------------------------------------------------------
    def classify(self, request):
        path = request.path
        if any(fnmatch.fnmatch(path, p) for p in self.cfg.passive_paths):
            return PASSIVE
        if is_websocket(request):
            return TAB if request.headers.get("Origin") else WAKE
        if request.method in ("HEAD", "OPTIONS"):
            return PASSIVE
        if request.method == "GET":
            if is_navigation(request):
                return NAVIGATE
            return TAB if from_open_page(request) else WAKE
        return WAKE

    def is_activity(self, request, kind):
        """Counts as use of the service: resets its idle clock and makes up sessions. Opening the app, and requests
        that do something (POST/PUT/PATCH/DELETE: queue a prompt, upload, a chat completion). Reads don't: a client
        left open polls, a script waits on /history, a monitor checks health; jobs they started keep the service
        busy through its busy probe instead."""
        return kind == NAVIGATE or (kind == WAKE and request.method not in ("GET", "HEAD", "OPTIONS"))

    def home_page(self, request):
        """Serve switchboard's status page for this request even while the service is ready (e.g. an API server's
        "/", which has nothing to show a browser)."""
        return False

    def capture_key(self, request):
        """Name under which a successful (small) response to this request is remembered, to be served by
        synthetic() while the service is down; None for most requests."""
        return None

    # -- answers while the service is not ready ----------------------------------------------------------------
    async def synthetic(self, request, svc, hub):
        """A complete answer without the service, or None."""
        return None

    def unavailable(self, request, svc, info):
        """503 (or info["status"]) shaped for this protocol. info: message, code, retry_after, state."""
        headers = {"Cache-Control": "no-store"}
        if info.get("retry_after"):
            headers["Retry-After"] = str(int(info["retry_after"]))
        status = info.get("status", 503)
        if wants_json(request):
            return web.json_response(self.error_body(request, svc, info), status=status, headers=headers)
        return web.Response(text=info["message"] + "\n", status=status, headers=headers)

    def error_body(self, request, svc, info):
        return {"error": {"type": "service_unavailable", "code": info.get("code"), "message": info["message"]},
                "switchboard": {"service": svc.name, "state": info.get("state"), "retry_after_s": info.get("retry_after")}}

    # -- is the service working right now? ------------------------------------------------------------------
    async def busy(self, svc, session):
        """A short reason when the service is doing work that stopping it would lose, else None.
        In-flight proxied requests are counted by the proxy; this asks the service itself (queued jobs that
        run after the HTTP request has returned). Configured per service as [busy_probe]:

            path = "/queue"                                   # GET upstream + path, JSON
            nonempty = { queue_running = "running", queue_pending = "queued" }   # busy if any list is non-empty
          or
            path = "/api/state"
            list = "jobs"; field = "status"; idle_values = ["done", "failed"]    # busy if any item isn't idle
        """
        p = self.cfg.busy_probe
        if not p:
            return None
        t, cached = self._probe_cache
        if time.monotonic() - t < 2:
            return cached
        reason = None
        try:
            async with session.get(self.cfg.upstream + p["path"], timeout=aiohttp.ClientTimeout(total=4)) as r:
                data = json.loads(await r.read()) if r.status == 200 else None
        except (aiohttp.ClientError, TimeoutError, ValueError):
            data = None
        if isinstance(data, dict):
            ne = p.get("nonempty")
            if ne:
                labels = ne if isinstance(ne, dict) else {k: k for k in ne}
                parts = [f"{len(data.get(k) or [])} {label}" for k, label in labels.items() if data.get(k)]
                reason = ", ".join(parts) or None
            elif p.get("list"):
                idle = set(p.get("idle_values", []))
                items = [i for i in data.get(p["list"]) or [] if isinstance(i, dict) and i.get(p["field"]) not in idle]
                if items:
                    states = sorted({str(i.get(p["field"])) for i in items})
                    reason = f"{len(items)} job{'s' if len(items) > 1 else ''} ({', '.join(states)})"
        self._probe_cache = (time.monotonic(), reason)
        return reason
