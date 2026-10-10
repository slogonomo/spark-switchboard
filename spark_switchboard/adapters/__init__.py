"""Protocol adapters. A service's `adapter = "<name>"` picks one; add a module and register it here for a new
protocol. Most web apps need none of their own: "web" with passive_paths and a [busy_probe] covers them."""
from .base import NAVIGATE, PASSIVE, TAB, WAKE, WebAdapter, is_websocket
from .comfyui import ComfyUIAdapter
from .openai import OpenAIAdapter

ADAPTERS = {a.name: a for a in (WebAdapter, OpenAIAdapter, ComfyUIAdapter)}


def make(cfg):
    try:
        return ADAPTERS[cfg.adapter](cfg)
    except KeyError:
        raise ValueError(f"service {cfg.name}: unknown adapter {cfg.adapter!r} (known: {', '.join(ADAPTERS)})") from None


__all__ = ["ADAPTERS", "make", "NAVIGATE", "WAKE", "TAB", "PASSIVE", "WebAdapter", "is_websocket"]
