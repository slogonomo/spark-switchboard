"""End-to-end tests: a real switchboard (in-process) in front of fake services (tests/fake_service.py, separate
processes), with fake memory. No GPU, no docker.   Run from the repo root: python -m unittest -v"""
import asyncio
import json
import os
import shutil
import socket
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spark_switchboard import config as config_mod  # noqa: E402
from spark_switchboard import scheduler as scheduler_mod  # noqa: E402
from spark_switchboard.app import Hub  # noqa: E402

FAKE = str(Path(__file__).with_name("fake_service.py"))
KINDS = {"openai": "openai", "comfy": "comfyui", "web": "web"}


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def toml_value(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return json.dumps(v)
    if isinstance(v, list):
        return "[" + ", ".join(toml_value(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{k} = {toml_value(x)}" for k, x in v.items()) + "}"
    raise TypeError(v)


class SwitchboardTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="sbtest-"))
        self.memdir = self.tmp / "mem"
        self.hub = None
        self.http = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.http.close()
        if self.hub:
            await self.stop_hub(stop_services=True)
        for f in self.tmp.glob("*.pid"):
            try:
                os.kill(int(f.read_text()), 9)
            except (OSError, ValueError):
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    def svc(self, name, kind="openai", mem=10, max_gib=None, start=0.4, extra_args=(), ram=None, **kw):
        up = free_port()
        pidfile = str(self.tmp / f"{name}.pid")
        d = {"title": name.upper(), "adapter": KINDS[kind], "upstream": f"http://127.0.0.1:{up}",
             "listen": free_port(), "memory_gib": mem, "max_gib": max_gib or mem, "pidfile": pidfile,
             "command": [sys.executable, FAKE, "--port", str(up), "--kind", kind, "--name", name,
                         "--mem", str(max_gib or mem), "--mem-dir", str(self.memdir), "--start-delay", str(start),
                         "--pidfile", pidfile, *extra_args],
             "ready_path": "/health", "start_estimate_s": start + 1, "idle_grace_s": 0, "stop_timeout_s": 10}
        if ram:                         # host RAM, with a fake discrete GPU (start_hub(ram_total=...))
            d.update(ram_gib=ram)
            d["command"] += ["--ram", str(ram)]
        d.update(kw)
        return name, d

    async def start_hub(self, *services, total=121, ram_total=None, **main):
        self.admin = free_port()
        lines = [f"admin_port = {self.admin}", 'bind = "127.0.0.1"', f'state_dir = "{self.tmp / "state"}"',
                 'hostname = "testbox"']
        lines += [f"{k} = {toml_value(v)}" for k, v in main.items()]
        lines += ["[memory]", 'source = "fake"', f"total_gib = {total}", f'dir = "{self.memdir}"']
        if ram_total:                   # a discrete GPU: total_gib is its memory, ram_total_gib the host's RAM
            lines.append(f"ram_total_gib = {ram_total}")
        for name, d in services:
            lines.append(f"[service.{name}]")
            lines += [f"{k} = {toml_value(v)}" for k, v in d.items()]
        path = self.tmp / "switchboard.toml"
        path.write_text("\n".join(lines) + "\n")
        self.cfg = config_mod.load(path)
        self.hub = Hub(self.cfg)
        self.stop_event = asyncio.Event()
        self.task = asyncio.create_task(self.hub.run(self.stop_event))
        await self.until(lambda: self._listening(self.admin) and all(self._listening(s.cfg.listen[0]) for s in self.hub.services.values()))
        return self.hub

    async def restart_hub(self):
        """A new switchboard on the same config and state (the services keep running and are re-adopted)."""
        path = self.cfg.path
        await self.stop_hub()
        self.cfg = config_mod.load(path)
        self.hub = Hub(self.cfg)
        self.stop_event = asyncio.Event()
        self.task = asyncio.create_task(self.hub.run(self.stop_event))
        await self.until(lambda: self._listening(self.admin) and all(self._listening(s.cfg.listen[0]) for s in self.hub.services.values()))

    async def stop_hub(self, stop_services=False):
        await self.hub.shutdown(stop_services=stop_services)
        self.stop_event.set()
        try:
            await asyncio.wait_for(self.task, 5)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        self.hub = None

    def _listening(self, port):
        with socket.socket() as s:
            return s.connect_ex(("127.0.0.1", port)) == 0

    async def until(self, cond, timeout=15, step=0.05):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if cond():
                return
            await asyncio.sleep(step)
        self.fail(f"condition not met within {timeout} s")

    def url(self, name, path):
        return f"http://127.0.0.1:{self.hub.services[name].cfg.listen[0]}{path}"

    def s(self, name):
        return self.hub.services[name]

    async def admin_post(self, path, headers=None):
        async with self.http.post(f"http://127.0.0.1:{self.admin}{path}",
                                  headers={"X-Switchboard": "1"} if headers is None else headers) as r:
            text = await r.text()
            try:
                return r.status, json.loads(text)
            except ValueError:
                return r.status, text

    async def chat(self, name, delay=0, stream=False, headers=None):
        async with self.http.post(self.url(name, "/v1/chat/completions"), headers=headers or {},
                                  json={"messages": [], "delay": delay, "stream": stream}) as r:
            body = await r.read()
            return r.status, r.headers, body

    # -------------------------------------------------------------------------------------------------------------
    async def test_models_list_is_answered_without_starting(self):
        await self.start_hub(self.svc("llm", options={"models": ["configured-model"]}))
        async with self.http.get(self.url("llm", "/v1/models")) as r:
            self.assertEqual(r.status, 200)
            self.assertEqual((await r.json())["data"][0]["id"], "configured-model")
        await asyncio.sleep(1.2)
        self.assertEqual(self.s("llm").state, "stopped")
        self.assertEqual(self.hub.scheduler.wants, {})
        code, _, body = await self.chat("llm")              # held while it starts, then answered
        self.assertEqual(code, 200, body)
        self.assertIn(b"hello from llm", body)
        async with self.http.get(self.url("llm", "/v1/models")) as r:     # proxied now, and remembered
            self.assertEqual((await r.json())["data"][0]["id"], "llm-model")
        await self.s("llm").stop("test")
        async with self.http.get(self.url("llm", "/v1/models")) as r:
            self.assertEqual((await r.json())["data"][0]["id"], "llm-model")
        self.assertEqual(len(self.s("llm").start_times), 1)

    async def test_streaming_passes_through_incrementally(self):
        await self.start_hub(self.svc("llm"))
        await self.chat("llm")
        times = []
        async with self.http.post(self.url("llm", "/v1/chat/completions"),
                                  json={"messages": [], "stream": True, "delay": 0.9}) as r:
            async for line in r.content:
                if line.startswith(b"data: {"):
                    times.append(time.time())
        self.assertGreaterEqual(len(times), 3)
        self.assertGreater(times[-1] - times[0], 0.4)       # chunks arrived as produced, not all at the end

    async def test_browser_gets_status_page_then_app(self):
        await self.start_hub(self.svc("app", kind="web", start=1.5))
        async with self.http.get(self.url("app", "/"), headers={"Accept": "text/html"}) as r:
            self.assertEqual(r.status, 503)
            self.assertIn("__switchboard", await r.text())
            self.assertTrue(r.headers.get("Retry-After"))
        await self.until(lambda: self.s("app").state == "ready")
        async with self.http.get(self.url("app", "/"), headers={"Accept": "text/html"}) as r:
            self.assertEqual(r.status, 200)
            self.assertIn("app app", await r.text())
        async with self.http.get(self.url("app", "/__switchboard/status")) as r:
            self.assertEqual((await r.json())["state"], "ready")

    async def test_open_tab_traffic_never_starts_a_service(self):
        await self.start_hub(self.svc("app", kind="web"))
        u = self.url("app", "/api/ping")
        async with self.http.get(u, headers={"Referer": self.url("app", "/")}) as r:
            self.assertEqual(r.status, 503)
        with self.assertRaises(aiohttp.WSServerHandshakeError):
            await self.http.ws_connect(self.url("app", "/ws"), headers={"Origin": self.url("app", "")})
        await asyncio.sleep(1.2)
        self.assertEqual(self.s("app").state, "stopped")
        async with self.http.get(u) as r:                   # a program's GET does start it
            self.assertEqual(r.status, 200)

    async def test_switches_from_an_idle_service(self):
        await self.start_hub(self.svc("a", mem=80), self.svc("b", mem=80))
        self.assertEqual((await self.chat("a"))[0], 200)
        code, _, body = await self.chat("b")
        self.assertEqual(code, 200, body)
        self.assertEqual(self.s("a").state, "stopped")
        self.assertEqual(self.s("b").state, "ready")
        self.assertIn("making room for B", self.s("a").stop_reason)

    async def test_request_in_flight_is_never_interrupted(self):
        await self.start_hub(self.svc("a", mem=80), self.svc("b", mem=80))
        await self.chat("a")
        long = asyncio.create_task(self.chat("a", delay=3))
        await asyncio.sleep(0.3)
        t0 = time.time()
        code_b, _, body_b = await self.chat("b")            # waits for a's request, then switches
        code_a, _, body_a = await long
        self.assertEqual(code_a, 200, body_a)
        self.assertEqual(code_b, 200, body_b)
        self.assertGreater(time.time() - t0, 2.0)

    async def test_quiet_period_then_force(self):
        await self.start_hub(self.svc("a", mem=80, idle_grace_s=60), self.svc("b", mem=80))
        await self.chat("a")
        code, headers, body = await self.chat("b", headers={"X-Switchboard-Wait": "1"})
        self.assertEqual(code, 503)
        err = json.loads(body)["error"]
        self.assertEqual(err["code"], "waiting")
        self.assertIn("last used", err["message"])
        self.assertTrue(headers.get("Retry-After"))
        self.assertEqual(self.s("a").state, "ready")
        # the force needs the X-Switchboard header (no cross-site POSTs)
        async with self.http.post(self.url("b", "/__switchboard/start?force=1")) as r:
            self.assertEqual(r.status, 403)
        async with self.http.post(self.url("b", "/__switchboard/start?force=1"), headers={"X-Switchboard": "1"}) as r:
            self.assertEqual(r.status, 200)
        await self.until(lambda: self.s("b").state == "ready")
        self.assertEqual(self.s("a").state, "stopped")

    async def test_running_job_blocks_even_a_forced_switch(self):
        await self.start_hub(self.svc("comfy", kind="comfy", mem=80), self.svc("b", mem=80))
        async with self.http.post(self.url("comfy", "/prompt"), json={"seconds": 3}) as r:
            self.assertEqual(r.status, 200)
        async with self.http.post(self.url("b", "/__switchboard/start?force=1"), headers={"X-Switchboard": "1"}) as r:
            self.assertEqual(r.status, 200)
        await asyncio.sleep(1.5)
        self.assertEqual(self.s("comfy").state, "ready")
        w = self.hub.scheduler.wants["b"]
        self.assertEqual(w.phase, "waiting")
        self.assertTrue(w.blockers[0][2])                   # in progress: never interrupted
        self.assertIn("running", w.blockers[0][1])
        await self.until(lambda: self.s("b").state == "ready", timeout=15)
        self.assertEqual(self.s("comfy").state, "stopped")

    async def test_session_aware_quiet_period(self):
        await self.start_hub(self.svc("a", mem=10, idle_grace_s=5, session_grace_max_s=600))
        s = self.s("a")
        now = time.time()
        s.calls.extend([now - 100, now - 70, now - 40])     # a session with 30 s gaps
        s.last_activity = now - 40
        need, session = s.quiet_needed()
        self.assertEqual(need, 90)
        self.assertIn("used 3 times", session)
        self.assertIn("free after", s.quiet_reason())
        s.calls.clear()
        s.calls.extend([now - 40.9, now - 40.6, now - 40.3, now - 40.2, now - 40])   # one page load's burst
        self.assertEqual(s.quiet_needed(), (5, None))
        s.calls.clear()
        s.calls.append(now - 40)                            # a lone call: just idle_grace_s
        self.assertEqual(s.quiet_needed(), (5, None))

    async def test_gives_up_holding_with_retry_after(self):
        await self.start_hub(self.svc("llm", start=5))
        code, headers, body = await self.chat("llm", headers={"X-Switchboard-Wait": "1"})
        self.assertEqual(code, 503)
        err = json.loads(body)["error"]
        self.assertEqual(err["type"], "service_unavailable")
        self.assertIn("starting", err["message"])
        self.assertGreaterEqual(int(headers["Retry-After"]), 3)

    async def test_failed_start_is_reported_and_not_retried_at_once(self):
        await self.start_hub(self.svc("bad", start=1, extra_args=("--fail",)))
        code, _, body = await self.chat("bad")
        self.assertEqual(code, 503)
        self.assertIn("fake failure", json.loads(body)["error"]["message"])
        self.assertEqual(self.s("bad").state, "failed")
        n = sum(1 for e in self.hub.events if "starting BAD" in e["text"])
        code, _, body = await self.chat("bad")
        self.assertEqual(code, 503)
        self.assertEqual(sum(1 for e in self.hub.events if "starting BAD" in e["text"]), n)

    async def test_websocket_passes_through_with_the_clients_host(self):
        await self.start_hub(self.svc("comfy", kind="comfy"))
        async with self.http.get(self.url("comfy", "/system_stats")) as r:
            self.assertEqual(r.status, 200)
        host = f"127.0.0.1:{self.s('comfy').cfg.listen[0]}"
        async with self.http.ws_connect(self.url("comfy", "/ws?clientId=x"), headers={"Origin": f"http://{host}"},
                                        max_msg_size=0) as ws:
            first = json.loads((await ws.receive()).data)
            self.assertEqual(first["host"], host)
            self.assertEqual(first["origin"], f"http://{host}")
            await ws.send_str("hi")
            self.assertEqual((await ws.receive()).data, "echo:hi")
            await ws.send_bytes(b"abc" * 2_000_000)          # 6 MB: no message size limit in the proxy
            self.assertEqual(len((await ws.receive()).data), 6_000_000)
            self.assertEqual(self.s("comfy").websockets, 1)
        await self.until(lambda: self.s("comfy").websockets == 0)

    async def test_bodies_and_heads(self):
        await self.start_hub(self.svc("app", kind="web"))
        async with self.http.get(self.url("app", "/api/ping")) as r:
            self.assertEqual(r.status, 200)
        payload = os.urandom(5_000_000)
        async with self.http.post(self.url("app", "/upload"), data=payload) as r:
            j = await r.json()
            self.assertEqual(j["received"], len(payload))
            self.assertEqual(j["cl"], str(len(payload)))
            self.assertIsNone(j["te"])

        async def gen():
            for _ in range(5):
                yield b"y" * 100_000
        async with self.http.post(self.url("app", "/upload"), data=gen()) as r:
            j = await r.json()
            self.assertEqual(j["received"], 500_000)
            self.assertEqual(j["te"], "chunked")
        async with self.http.get(self.url("app", "/big?n=3000000")) as r:
            self.assertEqual(len(await r.read()), 3_000_000)
        async with self.http.head(self.url("app", "/big?n=3000000")) as r:
            self.assertEqual(r.status, 200)
            self.assertEqual(await r.read(), b"")

    async def test_restart_readopts_running_services(self):
        # The first switchboard is a real process that exits (SIGTERM), as under systemd.
        _, d = self.svc("llm")
        await self.start_hub(("llm", d))
        cfg_path = self.cfg.path
        await self.stop_hub(stop_services=False)
        self.memdir.mkdir(exist_ok=True)
        proc = await asyncio.create_subprocess_exec(sys.executable, "-m", "spark_switchboard", "serve",
                                                    "-c", str(cfg_path), cwd=str(Path(__file__).resolve().parents[1]),
                                                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        port = d["listen"]
        await self.until(lambda: self._listening(port))
        async with self.http.post(f"http://127.0.0.1:{port}/v1/chat/completions", json={"messages": []}) as r:
            self.assertEqual(r.status, 200)
        pid = int(Path(d["pidfile"]).read_text())
        proc.terminate()
        await proc.wait()
        await asyncio.sleep(1)
        os.kill(pid, 0)                                     # the launcher is still running without switchboard
        self.cfg = config_mod.load(cfg_path)
        self.hub = Hub(self.cfg)
        self.stop_event = asyncio.Event()
        self.task = asyncio.create_task(self.hub.run(self.stop_event))
        await self.until(lambda: self.s("llm").state == "ready")
        self.assertTrue(self.s("llm").adopted)
        self.assertFalse(self.s("llm").external)
        self.assertTrue(any("listening on" in e["text"] for e in list(self.hub.events)[:3]))   # history kept
        self.assertGreater(self.s("llm").last_activity, 0)                                     # and use
        self.assertTrue(self.s("llm").recent_uses)
        self.assertEqual(self.s("llm").pid, pid)
        await self.until(lambda: self._listening(self.s("llm").cfg.listen[0]))
        self.assertEqual((await self.chat("llm"))[0], 200)
        self.assertTrue(await self.s("llm").stop("test"))

    async def test_keep_warm_comes_back_after_idle_stop(self):
        await self.start_hub(self.svc("main", mem=80, keep_warm=True, priority=10),
                             self.svc("side", mem=80, idle_stop_s=2))
        await self.until(lambda: self.s("main").state == "ready")
        self.assertEqual((await self.chat("side"))[0], 200)
        self.assertEqual(self.s("main").state, "stopped")
        await self.until(lambda: self.s("side").state == "stopped", timeout=10)
        await self.until(lambda: self.s("main").state == "ready", timeout=10)
        self.assertIn("unused", self.s("side").stop_reason)

    async def test_unwanted_start_lapses(self):
        await self.start_hub(self.svc("a", mem=80, idle_grace_s=60), self.svc("b", mem=80), want_ttl_s=1.5)
        await self.chat("a")
        code, _, _ = await self.chat("b", headers={"X-Switchboard-Wait": "0"})
        self.assertEqual(code, 503)
        self.assertIn("b", self.hub.scheduler.wants)
        await self.until(lambda: "b" not in self.hub.scheduler.wants, timeout=6)
        self.assertEqual(self.s("a").state, "ready")

    async def test_a_want_goes_with_the_last_request_that_gave_up(self):
        # B waits for memory that A (in its quiet period) holds. Its only caller gives up: the want goes with that
        # answer, so B doesn't start later for nobody (want_ttl_s is long here, so only the give-up drops it).
        await self.start_hub(self.svc("a", mem=80, idle_grace_s=60), self.svc("b", mem=80), want_ttl_s=600)
        await self.chat("a")
        code, _, body = await self.chat("b", headers={"X-Switchboard-Wait": "1"})
        self.assertEqual(code, 503)
        err = json.loads(body)["error"]
        self.assertEqual(err["code"], "waiting")
        self.assertIn("won't start by itself", err["message"])
        self.assertNotIn("b", self.hub.scheduler.wants)
        # An answer given at once (Wait 0: nobody was held) leaves the want alone.
        code, _, body = await self.chat("b", headers={"X-Switchboard-Wait": "0"})
        self.assertEqual(code, 503)
        self.assertIn("b", self.hub.scheduler.wants)
        self.assertNotIn("won't start by itself", json.loads(body)["error"]["message"])

    async def test_a_polled_want_stays_when_a_request_gives_up(self):
        # A status page (?want=1 on the app's port) or a launcher (?want=1 on the admin API) still waits for it.
        await self.start_hub(self.svc("a", mem=80, idle_grace_s=60), self.svc("b", mem=80), want_ttl_s=600)
        await self.chat("a")
        polls = (self.url("b", "/__switchboard/status?want=1"), f"http://127.0.0.1:{self.admin}/api/services/b?want=1")
        for poll in polls:
            self.hub.scheduler.cancel(self.s("b"))
            self.assertEqual((await self.admin_post("/api/services/b/start"))[0], 200)
            async with self.http.get(poll) as r:
                self.assertEqual(r.status, 200)
            code, _, body = await self.chat("b", headers={"X-Switchboard-Wait": "1"})
            self.assertEqual(code, 503)
            self.assertIn("will start", json.loads(body)["error"]["message"])
            self.assertIn("b", self.hub.scheduler.wants, poll)
        # Not while making room: the stops are under way, and dropping the want would start nothing in their place.
        w = self.hub.scheduler.wants["b"]
        w.polled_at, w.phase = 0.0, "making_room"
        self.assertFalse(self.hub.scheduler.abandoned(w))
        w.phase = "waiting"
        self.assertTrue(self.hub.scheduler.abandoned(w))

    async def test_a_want_that_made_room_stays_when_its_request_gives_up(self):
        # A is stopped to make room for B, but its memory comes back late (a container, the driver): after switchboard
        # stopped waiting for it, and after B's only request gave up. B's want stays (and lapses after want_ttl_s as
        # usual), so B starts once the memory is back, rather than A and B both staying stopped.
        await self.start_hub(self.svc("a", mem=80), self.svc("b", mem=80), want_ttl_s=600)
        self.assertEqual((await self.chat("a"))[0], 200)
        self.s("a").stop_times = [0.5]                      # so the request is held (its ETA fits its wait)

        async def ghost():                                  # holds A's memory from when A starts stopping
            await self.until(lambda: self.s("a").state == "stopping")
            (self.memdir / "ghost.gib").write_text("80")
        g = asyncio.create_task(ghost())
        with mock.patch.object(scheduler_mod, "ROOM_WAIT_S", 1):
            t0 = time.time()
            code, _, body = await self.chat("b", headers={"X-Switchboard-Wait": "7"})
        await g
        self.assertGreater(time.time() - t0, 6)              # held, through making room and then no room
        self.assertEqual(code, 503, body)
        err = json.loads(body)["error"]
        self.assertEqual(err["code"], "no_memory")
        self.assertNotIn("won't start by itself", err["message"])
        self.assertIn("b", self.hub.scheduler.wants)
        self.assertEqual(self.s("a").state, "stopped")
        (self.memdir / "ghost.gib").unlink()
        await self.until(lambda: self.s("b").state == "ready", timeout=10)

    async def test_a_probe_that_gives_up_leaves_the_want(self):
        # An answer given at once (Wait 0) leaves the want, so B starts once its stop is done. A probe (passive_paths)
        # held meanwhile can't start B, so its giving up doesn't drop that want either.
        await self.start_hub(self.svc("b", passive_paths=["/probe"], extra_args=("--stop-delay", "6")),
                             want_ttl_s=600)
        self.assertEqual((await self.chat("b"))[0], 200)
        self.s("b").start_times, self.s("b").stop_times = [0.5], [0.1]     # so the probe is held
        self.assertEqual((await self.admin_post("/api/services/b/stop"))[0], 200)
        await self.until(lambda: self.s("b").state == "stopping")
        code, _, body = await self.chat("b", headers={"X-Switchboard-Wait": "0"})
        self.assertEqual(code, 503)
        self.assertIn("will start", json.loads(body)["error"]["message"])
        t0 = time.time()
        async with self.http.get(self.url("b", "/probe"), headers={"X-Switchboard-Wait": "3"}) as r:
            self.assertEqual(r.status, 503)
            self.assertNotIn("won't start by itself", await r.text())
        self.assertGreater(time.time() - t0, 2.5)            # it was held
        self.assertEqual(self.s("b").state, "stopping")
        self.assertIn("b", self.hub.scheduler.wants)
        await self.until(lambda: self.s("b").state == "ready", timeout=15)

    async def test_no_memory_counts_the_reserve_in(self):
        # The answer gives what must really be free (memory_gib + reserve_gib) and free memory as the machine shows it.
        self.memdir.mkdir(parents=True, exist_ok=True)
        (self.memdir / "other.gib").write_text("100")       # memory held outside switchboard: 21 of 121 GiB free
        await self.start_hub(self.svc("big", mem=60), reserve_gib=2)
        code, _, body = await self.chat("big", headers={"X-Switchboard-Wait": "1"})
        self.assertEqual(code, 503)
        self.assertIn("needs 62 GiB free (2 GiB of it kept in reserve); 21 GiB is free",
                      json.loads(body)["error"]["message"])

    async def test_admin_stop_refuses_busy_services(self):
        await self.start_hub(self.svc("a"))
        await self.chat("a")
        long = asyncio.create_task(self.chat("a", delay=2))
        await asyncio.sleep(0.3)
        base = f"http://127.0.0.1:{self.admin}/api/services/a"
        async with self.http.post(base + "/stop", headers={"X-Switchboard": "1"}) as r:
            self.assertEqual(r.status, 409)
            self.assertIn("in progress", (await r.json())["busy"])
        self.assertEqual((await long)[0], 200)
        async with self.http.post(base + "/stop", headers={"X-Switchboard": "1"}) as r:
            self.assertEqual(r.status, 200)
        await self.until(lambda: self.s("a").state == "stopped")
        async with self.http.get(f"http://127.0.0.1:{self.admin}/") as r:
            self.assertIn("switchboard", await r.text())

    async def test_busy_and_use_are_reported_truthfully(self):
        await self.start_hub(self.svc("a"))
        await self.chat("a")
        long = asyncio.create_task(self.chat("a", delay=7))         # outlasts a 5 s busy refresh
        await self.until(lambda: self.s("a").status()["busy"])
        self.assertEqual((await long)[0], 200)
        await self.until(lambda: self.s("a").status()["busy"] is None, timeout=10)   # not a stale "in progress"
        self.hub.cfg.use_event_after_s = 0.5                       # the main config's key, normally 900
        await asyncio.sleep(0.6)
        await self.chat("a")
        self.assertTrue(any(e["text"].startswith("in use again after") for e in self.hub.events))

    async def test_stop_if_idle_ignores_a_fresh_start_but_not_use(self):
        # What a delegating launcher sends when it is closed (e.g. an NVIDIA Sync app): stop it unless someone used it.
        await self.start_hub(self.svc("a", idle_grace_s=60))
        base = f"http://127.0.0.1:{self.admin}/api/services/a"
        h = {"X-Switchboard": "1"}
        async with self.http.post(base + "/start", headers=h) as r:
            self.assertEqual(r.status, 200)
        await self.until(lambda: self.s("a").state == "ready")
        async with self.http.post(base + "/stop?if_idle=1", headers=h) as r:
            self.assertEqual(r.status, 200)         # started a moment ago, never used: stops
        await self.until(lambda: self.s("a").state == "stopped")
        await self.chat("a")                        # used now
        async with self.http.post(base + "/stop?if_idle=1", headers=h) as r:
            self.assertEqual(r.status, 409)
            self.assertIn("last used", (await r.json())["error"])

    async def test_no_room_explains_itself(self):
        self.memdir.mkdir(parents=True, exist_ok=True)
        (self.memdir / "other.gib").write_text("100")       # memory held outside switchboard
        await self.start_hub(self.svc("big", mem=60))
        code, _, body = await self.chat("big", headers={"X-Switchboard-Wait": "1"})
        self.assertEqual(code, 503)
        self.assertIn("outside switchboard", json.loads(body)["error"]["message"])
        # Memory that can't be read (a GPU nvidia-smi doesn't answer for) isn't blamed on other software.
        self.hub.memory.error = "nvidia-smi did not answer within 10 s"
        code, _, body = await self.chat("big", headers={"X-Switchboard-Wait": "1"})
        msg = json.loads(body)["error"]["message"]
        self.assertNotIn("outside switchboard", msg)
        self.assertIn("would bring it to 21 GiB; nvidia-smi did not answer within 10 s", msg)

    async def test_keep_warm_note_counts_the_reserve_in(self):
        # As in the no_memory answer: what must really be free (memory_gib + reserve_gib), and free memory as the
        # machine shows it.
        await self.start_hub(self.svc("main", mem=8, keep_warm=True), total=8.4, reserve_gib=1)
        await self.until(lambda: self.s("main").warm_note)
        self.assertEqual(self.s("main").warm_note, "kept warm: starts when 9 GiB is free (8 GiB now)")
        self.assertEqual(self.s("main").state, "stopped")

    # -- a discrete GPU: GPU memory and host RAM are planned separately ----------------------------------------------
    async def test_discrete_gpu_plans_gpu_memory_and_ram(self):
        # 32 GiB of GPU memory, 64 GiB of RAM. A holds 10 + 40; B needs 10 GPU (free) and 30 RAM (24 free): stopping
        # the idle A makes room in RAM.
        await self.start_hub(self.svc("a", mem=10, ram=40), self.svc("b", mem=10, ram=30), total=32, ram_total=64)
        self.assertEqual((await self.chat("a"))[0], 200)
        await self.hub.scheduler.refresh_usage(0)
        st = self.hub.status()
        self.assertEqual((st["memory"]["kind"], st["memory"]["total_gib"], st["memory"]["ram"]["total_gib"]),
                         ("gpu", 32, 64))
        a = next(s for s in st["services"] if s["name"] == "a")
        self.assertEqual((a["usage_gib"], a["ram_usage_gib"], a["ram_gib"], a["ram_max_gib"]), (10, 40, 40, 40))
        code, _, body = await self.chat("b")
        self.assertEqual(code, 200, body)
        self.assertEqual(self.s("a").state, "stopped")
        self.assertIn("making room for B", self.s("a").stop_reason)
        await self.s("b").stop("test")
        # Memory held outside switchboard (the person at the desk): the answer names the memory that is short.
        (self.memdir / "desktop.ram.gib").write_text("50")
        code, headers, body = await self.chat("b", headers={"X-Switchboard-Wait": "1"})
        self.assertEqual(code, 503)
        err = json.loads(body)["error"]
        self.assertEqual(err["code"], "no_memory")
        self.assertIn("needs 30 GiB of RAM free; 14 GiB is free", err["message"])
        self.assertNotIn("GPU memory", err["message"])
        self.assertIn("outside switchboard", err["message"])
        self.assertTrue(headers.get("Retry-After"))
        (self.memdir / "desktop.ram.gib").unlink()
        (self.memdir / "desktop.gib").write_text("25")
        code, _, body = await self.chat("b", headers={"X-Switchboard-Wait": "1"})
        self.assertEqual(code, 503)
        self.assertIn("needs 10 GiB of GPU memory free; 7 GiB is free", json.loads(body)["error"]["message"])
        (self.memdir / "desktop.gib").unlink()               # the desk is free again: the waiting start goes ahead
        self.assertEqual((await self.chat("b"))[0], 200)
        self.assertEqual(self.s("a").state, "stopped")

    async def test_making_room_stops_only_what_is_needed(self):
        # RAM is short for B. A (used least recently) holds a bit of RAM, D plenty: stopping D alone makes room, so A
        # stays up.
        await self.start_hub(self.svc("a", mem=8, ram=0.05), self.svc("d", mem=1, ram=20), self.svc("b", mem=4, ram=40),
                             total=32, ram_total=64, ram_reserve_gib=8)
        self.assertEqual((await self.chat("a"))[0], 200)
        self.assertEqual((await self.chat("d"))[0], 200)
        code, _, body = await self.chat("b")
        self.assertEqual(code, 200, body)
        self.assertEqual((self.s("a").state, self.s("d").state), ("ready", "stopped"))
        self.assertIn("making room for B", self.s("d").stop_reason)

    # -- pause -----------------------------------------------------------------------------------------------------
    async def test_pause_keeps_keep_warm_down_until_resume(self):
        await self.start_hub(self.svc("main", mem=80, keep_warm=True))
        await self.until(lambda: self.s("main").state == "ready")
        self.assertEqual((await self.admin_post("/api/pause", headers={}))[0], 403)     # needs X-Switchboard
        code, r = await self.admin_post("/api/pause?reason=test")
        self.assertEqual(code, 200, r)
        self.assertEqual(r["services"], {"main": "stopping"})
        self.assertEqual(r["paused"]["reason"], "test")
        await self.until(lambda: self.s("main").state == "stopped")
        await asyncio.sleep(3)                              # several scheduler ticks: keep-warm would have started it
        self.assertEqual(self.s("main").state, "stopped")
        self.assertEqual(self.hub.status()["paused"]["reason"], "test")
        code, r = await self.admin_post("/api/resume")
        self.assertEqual((code, r["resumed"]), (200, True))
        self.assertIsNone(self.hub.status()["paused"])
        await self.until(lambda: self.s("main").state == "ready", timeout=10)      # keep-warm is back
        self.assertEqual((await self.admin_post("/api/resume"))[1]["resumed"], False)

    async def test_requests_while_paused_start_nothing(self):
        await self.start_hub(self.svc("llm"), self.svc("app", kind="web"))
        await self.admin_post("/api/pause")
        n_events = len(self.hub.events)
        code, headers, body = await self.chat("llm")       # a program's request: answered at once, not held
        self.assertEqual(code, 503)
        err = json.loads(body)["error"]
        self.assertEqual(err["code"], "paused")
        self.assertIn("spark-switchboard resume", err["message"])
        self.assertEqual(headers["Retry-After"], "60")
        async with self.http.get(self.url("app", "/"), headers={"Accept": "text/html"}) as r:   # a browser
            self.assertEqual(r.status, 503)
            self.assertIn("btn-resume", await r.text())
        async with self.http.get(self.url("app", "/__switchboard/status")) as r:
            st = await r.json()
            self.assertEqual(st["code"], "paused")
            self.assertTrue(st["paused"])
        async with self.http.post(self.url("app", "/__switchboard/start"), headers={"X-Switchboard": "1"}) as r:
            self.assertEqual(r.status, 409)
        code, r = await self.admin_post("/api/services/app/start")
        self.assertEqual(code, 409)
        self.assertTrue(r["paused"])
        async with self.http.get(self.url("llm", "/health")) as r:
            self.assertFalse((await r.json())["loads_on_demand"])
        await asyncio.sleep(1.5)
        self.assertEqual((self.s("llm").state, self.s("app").state), ("stopped", "stopped"))
        self.assertEqual(self.hub.scheduler.wants, {})
        self.assertFalse(any("wanted" in e["text"] or "starting" in e["text"] for e in list(self.hub.events)[n_events:]))
        # The status page's Resume button
        async with self.http.post(self.url("app", "/__switchboard/resume"), headers={"X-Switchboard": "1"}) as r:
            self.assertTrue((await r.json())["resumed"])
        self.assertEqual((await self.chat("llm"))[0], 200)

    async def test_a_pause_persists_across_restarts_only_when_asked(self):
        await self.start_hub(self.svc("a"))
        code, r = await self.admin_post("/api/pause")
        self.assertFalse(r["paused"]["persists"])
        await self.restart_hub()
        self.assertFalse(self.hub.paused)                   # the default: a restart resumes
        code, r = await self.admin_post("/api/pause?persist=1&reason=maintenance")
        self.assertTrue(r["paused"]["persists"])
        await self.restart_hub()
        self.assertEqual((self.hub.paused_reason, self.hub.paused_persists), ("maintenance", True))
        self.assertEqual((await self.chat("a"))[0], 503)
        await self.admin_post("/api/pause?persist=0")        # pausing again doesn't undo the persisting pause
        self.assertTrue(self.hub.paused_persists)
        self.assertTrue((await self.admin_post("/api/resume"))[1]["resumed"])
        await self.restart_hub()
        self.assertFalse(self.hub.paused)
        self.assertEqual((await self.chat("a"))[0], 200)

    async def test_pause_persists_keeps_keep_warm_down_across_a_restart(self):
        # pause_persists = true (a workstation): every pause is kept until resume, keep-warm apps included.
        await self.start_hub(self.svc("main", mem=10, keep_warm=True), pause_persists=True)
        await self.until(lambda: self.s("main").state == "ready")
        code, r = await self.admin_post("/api/pause?reason=at%20the%20desk")
        self.assertTrue(r["paused"]["persists"])
        await self.until(lambda: self.s("main").state == "stopped")
        await self.restart_hub()
        self.assertEqual(self.hub.paused_reason, "at the desk")
        self.assertTrue(any("still paused (at the desk" in e["text"] for e in self.hub.events))
        await asyncio.sleep(2.5)                            # several ticks: keep-warm would have started it
        self.assertEqual(self.s("main").state, "stopped")
        self.assertTrue((await self.admin_post("/api/resume"))[1]["resumed"])
        await self.until(lambda: self.s("main").state == "ready", timeout=10)
        await self.restart_hub()                            # re-adopted, not paused
        self.assertFalse(self.hub.paused)
        await self.until(lambda: self.s("main").state == "ready")

    async def test_another_machine_pauses_and_resumes_by_default(self):
        await self.start_hub(self.svc("llm"))
        self.hub._local = lambda request: False             # a caller on another machine
        code, r = await self.admin_post("/api/pause?kill=1")
        self.assertEqual(code, 403)
        self.assertFalse(self.hub.paused)
        code, r = await self.admin_post("/api/pause?reason=remote")
        self.assertEqual(code, 200)
        self.assertTrue(self.hub.paused)
        code, r = await self.admin_post("/api/resume")
        self.assertEqual((code, r["resumed"]), (200, True))

    async def test_only_this_machine_pauses_or_resumes_when_remote_resume_is_off(self):
        await self.start_hub(self.svc("llm"), self.svc("app", kind="web"), remote_resume=False)
        # A caller on another machine (every test client is on 127.0.0.1, so the check is replaced): no pause, since
        # it couldn't undo it
        self.hub._local = lambda request: False
        code, r = await self.admin_post("/api/pause?reason=remote")
        self.assertEqual((code, r["paused"]), (403, None))
        self.assertIn("pause is only accepted on testbox itself (remote_resume = false)", r["error"])
        self.assertFalse(self.hub.paused)
        del self.hub._local                                 # this machine itself pauses
        await self.admin_post("/api/pause?reason=in%20use%20at%20the%20desk")
        self.hub._local = lambda request: False
        code, r = await self.admin_post("/api/pause?reason=remote")         # nor pauses again
        self.assertEqual((code, r["paused"]["reason"]), (403, "in use at the desk"))
        code, headers, body = await self.chat("llm")
        self.assertEqual(code, 503)
        err = json.loads(body)["error"]
        self.assertEqual(err["code"], "paused")
        self.assertIn("(in use at the desk)", err["message"])
        self.assertIn("only testbox itself can resume it. Try again later.", err["message"])
        self.assertNotIn("spark-switchboard resume", err["message"])
        self.assertEqual(headers["Retry-After"], "60")
        async with self.http.get(self.url("app", "/api/ping"), headers={"Accept": "application/json"}) as r:
            self.assertEqual(r.status, 503)
            paused = (await r.json())["switchboard"]["paused"]
            self.assertEqual((paused["reason"], paused["remote_resume"]), ("in use at the desk", False))
        async with self.http.post(self.url("app", "/__switchboard/resume"), headers={"X-Switchboard": "1"}) as r:
            self.assertEqual(r.status, 403)
        code, r = await self.admin_post("/api/resume")
        self.assertEqual(code, 403)
        self.assertIn("remote_resume = false", r["error"])
        code, r = await self.admin_post("/api/services/app/start")
        self.assertEqual(code, 409)
        self.assertIn("only testbox itself can resume it", r["error"])
        async with self.http.get(self.url("app", "/"), headers={"Accept": "text/html"}) as r:
            self.assertIn('"can_resume": false', await r.text())         # no Resume button
        async with self.http.get(f"http://127.0.0.1:{self.admin}/") as r:
            page = await r.text()
            self.assertIn('"can_resume": false', page)                  # nor Pause: it couldn't undo it
            self.assertIn("toggle('hidden', !!paused || !SB.can_resume)", page)
        self.assertTrue(self.hub.paused)
        del self.hub._local                                 # this machine itself: told how to resume
        async with self.http.get(f"http://127.0.0.1:{self.admin}/") as r:
            self.assertIn('"can_resume": true', await r.text())
        code, _, body = await self.chat("llm")
        self.assertEqual(code, 503)
        self.assertIn("spark-switchboard resume", json.loads(body)["error"]["message"])
        code, r = await self.admin_post("/api/services/app/start")
        self.assertEqual(code, 409)
        self.assertIn("resume it on the dashboard or with `spark-switchboard resume`", r["error"])
        code, r = await self.admin_post("/api/resume")
        self.assertEqual((code, r["resumed"]), (200, True))
        self.assertEqual((await self.chat("llm"))[0], 200)

    async def test_pause_lets_work_finish_unless_killed(self):
        await self.start_hub(self.svc("comfy", kind="comfy", mem=40), self.svc("a", mem=40))
        async with self.http.post(self.url("comfy", "/prompt"), json={"seconds": 3}) as r:
            self.assertEqual(r.status, 200)
        await self.chat("a")
        long = asyncio.create_task(self.chat("a", delay=2))
        await asyncio.sleep(0.3)
        code, r = await self.admin_post("/api/pause")
        self.assertEqual(code, 200)
        self.assertTrue(r["services"]["comfy"].startswith("busy (1 running)"), r)
        self.assertTrue(r["services"]["a"].startswith("busy (1 request in progress)"), r)
        await asyncio.sleep(0.5)
        self.assertEqual((self.s("comfy").state, self.s("a").state), ("ready", "ready"))
        self.assertEqual((await long)[0], 200)              # not interrupted
        await self.until(lambda: self.s("a").state == "stopped", timeout=10)
        await self.until(lambda: self.s("comfy").state == "stopped", timeout=10)    # once its job is done
        self.assertEqual(self.s("comfy").stop_reason, "switchboard paused")
        # kill=1 (from this machine) interrupts work
        await self.admin_post("/api/resume")
        async with self.http.post(self.url("comfy", "/prompt"), json={"seconds": 60}) as r:
            self.assertEqual(r.status, 200)
        await self.until(lambda: self.s("comfy").status()["busy"], timeout=10)
        code, r = await self.admin_post("/api/pause?kill=1")
        self.assertIn("interrupting", r["services"]["comfy"])
        await self.until(lambda: self.s("comfy").state == "stopped", timeout=10)

    async def test_cli_pause_resume_and_the_stop_all_alias(self):
        await self.start_hub(self.svc("a"))
        await self.chat("a")
        env = {k: v for k, v in os.environ.items() if not k.startswith("SWITCHBOARD")}

        async def cli(*args):
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "spark_switchboard", *args, "-c", str(self.cfg.path), env=env,
                cwd=str(Path(__file__).resolve().parents[1]), stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT)
            out = (await proc.communicate())[0].decode()
            return proc.returncode, out
        rc, out = await cli("pause")
        self.assertEqual(rc, 0, out)
        self.assertIn("a: stopping", out)
        self.assertIn("switchboard is paused", out)
        await self.until(lambda: self.s("a").state == "stopped")
        rc, out = await cli("status")
        self.assertIn("PAUSED", out)
        rc, out = await cli("start", "a")
        self.assertNotEqual(rc, 0)
        self.assertIn("paused", out)
        self.assertEqual(await cli("resume"), (0, "resumed\n"))
        self.assertFalse(self.hub.paused)
        rc, out = await cli("stop-all")                    # the older name: pauses too
        self.assertEqual(rc, 0, out)
        self.assertTrue(self.hub.paused)
        self.assertIn("cli stop-all", self.hub.paused_reason)
        self.assertEqual(await cli("resume"), (0, "resumed\n"))
        rc, out = await cli("pause", "--persist", "--reason", "at the desk")
        self.assertEqual(rc, 0, out)
        self.assertIn("also across restarts and reboots", out)
        self.assertEqual((self.hub.paused_reason, self.hub.paused_persists), ("at the desk", True))
        rc, out = await cli("status")
        self.assertIn("(at the desk)", out)
        self.assertIn("persists across restarts", out)
        proc = await asyncio.create_subprocess_exec(sys.executable, "-m", "spark_switchboard", "--help", env=env,
                                                    cwd=str(Path(__file__).resolve().parents[1]),
                                                    stdout=asyncio.subprocess.PIPE)
        self.assertIn("the same as pause", (await proc.communicate())[0].decode())

    # -- launchers switchboard didn't start --------------------------------------------------------------------------
    async def test_external_launcher_is_never_stopped(self):
        # A launcher started by hand (no SWITCHBOARD=1) that writes the service's pidfile but never answers on the
        # upstream: switchboard adopts it as external, and its start timeout must not kill it.
        script, pidfile = self.tmp / "ext.sh", self.tmp / "ext.pid"
        script.write_text('echo $$ > "$1"\nwhile :; do sleep 0.2; done\n')
        env = {k: v for k, v in os.environ.items() if not k.startswith("SWITCHBOARD")}
        proc = await asyncio.create_subprocess_exec("bash", str(script), str(pidfile), env=env, start_new_session=True)
        await self.until(lambda: pidfile.exists() and pidfile.read_text().strip())
        name, d = self.svc("ext", kind="web", mem=10, start_timeout_s=1.5)
        d.update(command=["bash", str(script)], pidfile=str(pidfile))
        await self.start_hub((name, d), self.svc("warm", mem=10, keep_warm=True))
        ext = self.s("ext")
        self.assertEqual((ext.state, ext.external, ext.pid), ("starting", True, proc.pid))
        await self.until(lambda: self.s("warm").state == "ready")      # an external start doesn't hold up keep-warm
        await self.until(lambda: "leaves it alone until it exits" in ext.detail, timeout=10)
        await asyncio.sleep(1.5)
        self.assertIsNone(proc.returncode)                  # still running
        self.assertEqual(ext.state, "starting")
        self.assertFalse(any("stopping it" in e["text"] for e in self.hub.events))
        self.assertTrue(any(e["level"] == "warning" and "not answering" in e["text"] for e in self.hub.events))
        code, r = await self.admin_post("/api/services/ext/stop?kill=1")
        self.assertEqual(code, 409)
        self.assertIn("not started by switchboard", r["error"])
        proc.kill()
        await proc.wait()
        await self.until(lambda: ext.state == "stopped", timeout=10)
        self.assertFalse(ext.external)


if __name__ == "__main__":
    unittest.main()
