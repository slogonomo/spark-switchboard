#!/usr/bin/env python3
"""A stand-in for a real service's launcher, for tests: prints ">>> STATUS:" lines like a launcher does, waits
--start-delay seconds (loading), claims --mem GiB in the fake memory directory, then serves:

  openai   GET /v1/models, /health; POST /v1/chat/completions (stream or not; "delay" in the body = seconds)
  comfy    GET /system_stats, /queue; POST /prompt (a job of "seconds" s, reported by /queue); /ws echo + status
  web      GET / (html), /api/ping

SIGTERM: waits --stop-delay seconds, releases the memory, exits. --fail: exits 1 during the load.
"""
import argparse
import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path

from aiohttp import web

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, required=True)
ap.add_argument("--kind", default="openai")
ap.add_argument("--name", required=True)
ap.add_argument("--mem", type=float, default=0)
ap.add_argument("--mem-dir")
ap.add_argument("--start-delay", type=float, default=0.5)
ap.add_argument("--stop-delay", type=float, default=0.2)
ap.add_argument("--fail", action="store_true")
ap.add_argument("--pidfile")
args = ap.parse_args()


def say(msg):
    print(f">>> STATUS: {msg}", flush=True)


def claim(gib):
    if args.mem_dir:
        Path(args.mem_dir, f"{args.name}.gib").write_text(str(gib))


jobs = []          # comfy: [(prompt_id, end_time)]


async def models(request):
    return web.json_response({"object": "list", "data": [{"id": f"{args.name}-model", "object": "model"}]})


async def chat(request):
    body = await request.json()
    delay = float(body.get("delay", 0))
    text = f"hello from {args.name}"
    if body.get("stream"):
        resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await resp.prepare(request)
        for i, word in enumerate(text.split()):
            await asyncio.sleep(delay / 3 if delay else 0.05)
            chunk = {"choices": [{"index": 0, "delta": {"content": word + " "}}], "t": time.time()}
            await resp.write(f"data: {json.dumps(chunk)}\n\n".encode())
        await resp.write(b"data: [DONE]\n\n")
        return resp
    await asyncio.sleep(delay)
    return web.json_response({"choices": [{"index": 0, "message": {"role": "assistant", "content": text}}],
                              "echo_headers": {k: v for k, v in request.headers.items()},
                              "body_len": len(json.dumps(body))})


async def queue(request):
    now = time.time()
    running = [[0, pid] for pid, end in jobs if end > now]
    return web.json_response({"queue_running": running[:1], "queue_pending": running[1:]})


async def prompt(request):
    body = await request.json()
    pid = f"p{len(jobs)}"
    jobs.append((pid, time.time() + float(body.get("seconds", 1))))
    return web.json_response({"prompt_id": pid, "number": len(jobs)})


async def ws(request):
    w = web.WebSocketResponse(max_msg_size=0)
    await w.prepare(request)
    await w.send_str(json.dumps({"type": "status", "origin": request.headers.get("Origin"), "host": request.host}))
    async for msg in w:
        if msg.type == web.WSMsgType.TEXT:
            await w.send_str("echo:" + msg.data)
        elif msg.type == web.WSMsgType.BINARY:
            await w.send_bytes(msg.data[::-1])
    return w


async def upload(request):
    data = await request.read()
    return web.json_response({"received": len(data), "te": request.headers.get("Transfer-Encoding"),
                              "cl": request.headers.get("Content-Length")})


async def html(request):
    return web.Response(text=f"<html><body>{args.name} app</body></html>", content_type="text/html")


def reply(data):
    async def handler(request):
        return web.json_response(data)
    return handler


async def big(request):
    n = int(request.query.get("n", "1000000"))
    return web.Response(body=b"x" * n, headers={"Content-Type": "application/octet-stream"})


async def main():
    if args.pidfile:
        Path(args.pidfile).write_text(str(os.getpid()))
    print(f"[fake] {args.name} pid {os.getpid()} SWITCHBOARD={os.environ.get('SWITCHBOARD')}", flush=True)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    say(f"loading {args.name}")
    claim(args.mem / 2)
    t0 = time.time()
    while time.time() - t0 < args.start_delay:
        if stop.is_set():
            claim(0)
            return 0
        if args.fail and time.time() - t0 > args.start_delay / 2:
            print("ERROR: fake failure while loading", flush=True)
            say("REFUSED - fake failure")
            claim(0)
            return 1
        await asyncio.sleep(0.05)
    claim(args.mem)
    app = web.Application(client_max_size=1 << 30)
    app.router.add_get("/health", reply({"ok": True}))
    app.router.add_get("/v1/models", models)
    app.router.add_post("/v1/chat/completions", chat)
    app.router.add_get("/system_stats", reply({"system": {}}))
    app.router.add_get("/queue", queue)
    app.router.add_post("/prompt", prompt)
    app.router.add_get("/ws", ws)
    app.router.add_post("/upload", upload)
    app.router.add_get("/big", big)
    app.router.add_get("/", html)
    app.router.add_get("/api/ping", reply({"pong": True}))
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", args.port).start()
    say(f"{args.name} ready")
    await stop.wait()
    say(f"stopping {args.name}")
    await asyncio.sleep(args.stop_delay)
    await runner.cleanup()
    claim(0)
    if args.pidfile:
        try:
            os.unlink(args.pidfile)
        except OSError:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
