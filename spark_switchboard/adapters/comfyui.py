"""ComfyUI: its web UI, its HTTP API (POST /prompt, /history, /view, uploads) and its websocket (/ws).

- Opening the UI starts ComfyUI. API clients (scripts, MCP servers) start it with any request; their POST /prompt
  is held until ComfyUI is up, then queued as usual.
- The UI's own background traffic (websocket reconnects, polling) from a tab left open never restarts it.
- Busy = something in ComfyUI's queue (GET /queue), unless the service config sets its own [busy_probe].
- Errors on /prompt use ComfyUI's own error shape, so clients that print `error.message` show ours.
"""
from .base import WebAdapter

DEFAULT_PROBE = {"path": "/queue", "nonempty": {"queue_running": "running", "queue_pending": "queued"}}


class ComfyUIAdapter(WebAdapter):
    name = "comfyui"

    def __init__(self, cfg):
        super().__init__(cfg)
        if cfg.busy_probe is None:
            cfg.busy_probe = dict(DEFAULT_PROBE)

    def error_body(self, request, svc, info):
        if request.path.rstrip("/").endswith("/prompt"):
            return {"error": {"type": "service_unavailable", "message": info["message"], "details": "",
                              "extra_info": {"retry_after_s": info.get("retry_after")}}, "node_errors": {}}
        return super().error_body(request, svc, info)
