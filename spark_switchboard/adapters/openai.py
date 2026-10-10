"""OpenAI-compatible API servers (TensorFold, vLLM, llama.cpp, ...).

- GET /v1/models is answered even while the server is stopped (the last list it returned, or options.models), and
  never starts it: Open WebUI and agent harnesses poll it, and a model that vanishes from the list while it is
  merely unloaded confuses them.
- Inference requests (any POST) start the server and wait for it; clients see a slow first token, not an error.
- If it can't be ready within the hold window, the answer is OpenAI's error shape with HTTP 503 and Retry-After.
  The official SDKs retry 503s on their own (twice by default, honouring Retry-After up to 60 s).
- "/" in a browser always shows switchboard's status page (with options.info_html: how to connect).
"""
import json
import time

from aiohttp import web

from .base import NAVIGATE, PASSIVE, WebAdapter

MODEL_PATHS = ("/v1/models",)


class OpenAIAdapter(WebAdapter):
    name = "openai"
    has_home_page = True
    OPTIONS = WebAdapter.OPTIONS | {"models"}

    def classify(self, request):
        p = request.path
        if request.method == "GET" and (p in MODEL_PATHS or p.startswith("/v1/models/")):
            return PASSIVE
        if p in ("/health", "/v1/health", "/metrics"):
            return PASSIVE
        return super().classify(request)

    def home_page(self, request):
        return request.path in ("/", "/index.html") and self.classify(request) == NAVIGATE

    def capture_key(self, request):
        return "models" if request.method == "GET" and request.path in MODEL_PATHS else None

    async def synthetic(self, request, svc, hub):
        p = request.path
        if request.method == "GET" and (p in MODEL_PATHS or p.startswith("/v1/models/")):
            body = hub.remembered(svc.name, "models")
            try:
                data = json.loads(body) if body else None
            except ValueError:
                data = None
            if not data:
                data = {"object": "list", "data": [
                    {"id": m, "object": "model", "created": int(time.time()), "owned_by": "switchboard"}
                    for m in self.options.get("models", [])]}
            if p != "/v1/models":
                mid = p[len("/v1/models/"):]
                m = next((m for m in data.get("data", []) if m.get("id") == mid), None)
                if m is None:
                    return web.json_response({"error": {"message": f"model {mid!r} not found", "type": "invalid_request_error",
                                                        "param": None, "code": "model_not_found"}}, status=404)
                return web.json_response(m)
            return web.json_response(data, headers={"X-Switchboard-State": svc.state})
        if p in ("/health", "/v1/health"):
            # Reachable and will load on demand: report healthy so clients don't write the endpoint off.
            return web.json_response({"status": "ok", "backend": svc.state, "loads_on_demand": not hub.paused})
        return None

    def error_body(self, request, svc, info):
        return {"error": {"message": info["message"], "type": "service_unavailable", "param": None,
                          "code": info.get("code") or "model_unavailable"}}

    def unavailable(self, request, svc, info):
        headers = {"Cache-Control": "no-store"}
        if info.get("retry_after"):
            headers["Retry-After"] = str(int(info["retry_after"]))
        return web.json_response(self.error_body(request, svc, info), status=info.get("status", 503), headers=headers)
