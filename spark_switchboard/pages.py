"""HTML: a service's status page (shown while it starts, and as an API server's "/") and the admin dashboard."""
import html
import json
from pathlib import Path

from aiohttp import web

WEB = Path(__file__).parent / "web"


def _render(name, data):
    page = (WEB / name).read_text()
    blob = json.dumps(data).replace("</", "<\\/")
    return page.replace("/*SB_CONFIG*/null", blob).replace("{{TITLE}}", html.escape(data.get("title", "switchboard")))


def service_page(hub, svc, status):
    # home: this page is the service's own front page (an API server's "/"): it stays, rather than reloading into
    # the app once the service is ready.
    data = {"title": svc.title, "service": svc.name, "home": svc.adapter.has_home_page,
            "admin_port": hub.cfg.admin_port, "initial": hub.service_status(svc)}
    headers = {"Cache-Control": "no-store"}
    if status == 503:
        info = hub.proxy.info(svc)
        if info.get("retry_after"):
            headers["Retry-After"] = str(info["retry_after"])
    return web.Response(text=_render("service.html", data), status=status, content_type="text/html", headers=headers)


def dashboard(hub):
    data = {"title": f"switchboard · {hub.cfg.hostname}", "initial": hub.status(),
            "use_event_after_s": hub.cfg.use_event_after_s}
    return web.Response(text=_render("dashboard.html", data), content_type="text/html", headers={"Cache-Control": "no-store"})
