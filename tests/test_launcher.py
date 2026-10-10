"""End-to-end launcher protocol: spark-switchboard running examples/launchers/example-app.sh (bash + common.sh +
python3 -m http.server), delegate.sh following a switchboard, and common.sh / comfyui.sh on their own (with a fake
ComfyUI). No GPU, no docker; real processes on random localhost ports, with fake memory on the switchboard side.
Run from the repo root: python -m unittest -v"""
import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

import aiohttp

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from spark_switchboard import config as config_mod  # noqa: E402
from spark_switchboard.app import Hub  # noqa: E402

LAUNCHERS = REPO / "examples" / "launchers"
LAUNCHER = LAUNCHERS / "example-app.sh"
DELEGATE = LAUNCHERS / "delegate.sh"


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def listening(port):
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def alive(pid):
    try:
        os.kill(pid, 0)
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except (OSError, IndexError):
        return False


def clean_env(**extra):
    """The test's environment without any SWITCHBOARD* variable (so nothing can reach a real switchboard)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SWITCHBOARD", "LAUNCHER_", "EXAMPLE_"))}
    env.update(extra)
    return env


class LauncherTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="sblaunch-"))
        (self.tmp / "www").mkdir()
        (self.tmp / "www" / "hello.txt").write_text("hello from the example app\n")
        self.admin, self.listen, self.upstream = free_port(), free_port(), free_port()
        self.run_dir = self.tmp / "run"
        self.pidfile = self.run_dir / "example-app.pid"
        self.hub = self.proc = None
        self.extra_pids = []
        self.http = aiohttp.ClientSession()
        env = {"EXAMPLE_PORT": str(self.upstream), "EXAMPLE_HOST": "127.0.0.1", "EXAMPLE_DIR": str(self.tmp / "www"),
               "EXAMPLE_DELAY_S": "2", "LAUNCHER_RUN_DIR": str(self.run_dir),
               "LAUNCHER_LOG_DIR": str(self.tmp / "launcher-logs"), "LAUNCHER_POLL_S": "1",
               "SWITCHBOARD_API": f"http://127.0.0.1:{self.admin}"}
        self.cfg_path = self.tmp / "switchboard.toml"
        self.cfg_path.write_text(f"""admin_port = {self.admin}
bind = "127.0.0.1"
state_dir = "{self.tmp / 'state'}"
hostname = "testbox"
[memory]
source = "fake"
total_gib = 121
dir = "{self.tmp / 'mem'}"
[service.example-app]
title = "Example app"
adapter = "web"
listen = {self.listen}
upstream = "http://127.0.0.1:{self.upstream}"
pidfile = "{self.pidfile}"
command = ["bash", "{LAUNCHER}"]
env = {{ {', '.join(f'{k} = {json.dumps(v)}' for k, v in env.items())} }}
ready_path = "/"
start_estimate_s = 3
stop_timeout_s = 15
idle_grace_s = 0
""")

    async def asyncTearDown(self):
        await self.http.close()
        if self.hub:
            await self.stop_hub()
        if self.proc and self.proc.returncode is None:
            self.proc.kill()
            await self.proc.wait()
        pids = list(self.extra_pids)
        try:
            pids.append(int(self.pidfile.read_text()))
        except (OSError, ValueError):
            pass
        for pid in pids:                    # the launcher runs in its own session: its group holds http.server too
            for kill, target in ((os.killpg, pid), (os.kill, pid)):
                try:
                    kill(target, signal.SIGKILL)
                except OSError:
                    pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def until(self, cond, timeout=20, step=0.1):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if cond():
                return
            await asyncio.sleep(step)
        self.fail(f"condition not met within {timeout} s")

    async def start_hub(self):
        self.hub = Hub(config_mod.load(self.cfg_path))
        self.stop_event = asyncio.Event()
        self.task = asyncio.create_task(self.hub.run(self.stop_event))
        await self.until(lambda: listening(self.admin) and listening(self.listen))
        return self.hub

    async def stop_hub(self, stop_services=True):
        await self.hub.shutdown(stop_services=stop_services)
        self.stop_event.set()
        try:
            await asyncio.wait_for(self.task, 5)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        self.hub = None

    async def get(self, port, path, **kw):
        async with self.http.get(f"http://127.0.0.1:{port}{path}", **kw) as r:
            return r.status, await r.text()

    # -------------------------------------------------------------------------------------------------------------
    async def test_launcher_contract(self):
        # 1. A real switchboard process starts the launcher on the first request, which it holds until ready.
        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "spark_switchboard", "serve", "-c", str(self.cfg_path), cwd=str(REPO),
            env=clean_env(), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await self.until(lambda: listening(self.listen) and listening(self.admin))
        request = asyncio.create_task(self.get(self.listen, "/hello.txt"))
        details = set()
        t0 = time.time()
        while not request.done() and time.time() - t0 < 30:          # what the status page shows while it starts
            status, body = await self.get(self.admin, "/api/services/example-app")
            st = json.loads(body)
            if st["state"] == "starting" and st["detail"]:
                details.add(st["detail"])
            await asyncio.sleep(0.2)
        status, body = await request
        self.assertEqual((status, body), (200, "hello from the example app\n"))
        self.assertTrue(any(d.startswith("loading (pretend") for d in details), details)   # its >>> STATUS: lines

        # 2. The pidfile names the launcher, which runs with SWITCHBOARD=1 and its service name.
        pid = int(self.pidfile.read_text())
        self.assertIn(b"example-app.sh", Path(f"/proc/{pid}/cmdline").read_bytes())
        environ = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
        self.assertIn(b"SWITCHBOARD=1", environ)
        self.assertIn(b"SWITCHBOARD_SERVICE=example-app", environ)
        log = self.tmp / "state" / "logs" / "example-app.log"
        await self.until(lambda: ">>> STATUS: example-app ready on" in log.read_text(), timeout=10)

        # 3. switchboard exits (as on a restart): the launcher is re-parented and keeps running (SWITCHBOARD is set).
        self.proc.terminate()
        await self.proc.wait()
        await asyncio.sleep(3.5)                                       # > 3 keepalive checks (LAUNCHER_POLL_S=1)
        self.assertTrue(alive(pid))
        ppid = int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[1])
        self.assertNotEqual(ppid, self.proc.pid)
        self.assertEqual((await self.get(self.upstream, "/hello.txt"))[0], 200)

        # 4. A new switchboard re-adopts it through the pidfile.
        hub = await self.start_hub()
        svc = hub.services["example-app"]
        await self.until(lambda: svc.state == "ready")
        self.assertTrue(svc.adopted)
        self.assertFalse(svc.external)
        self.assertEqual(svc.pid, pid)
        self.assertEqual((await self.get(self.listen, "/hello.txt"))[0], 200)

        # 5. Stopping it is a SIGTERM; the launcher cleans up (http.server, pidfile) and exits well within the timeout.
        t0 = time.time()
        self.assertTrue(await svc.stop("test"))
        self.assertLess(time.time() - t0, 10)
        self.assertFalse(alive(pid))
        self.assertFalse(self.pidfile.exists())
        await self.until(lambda: not listening(self.upstream), timeout=5)
        self.assertFalse(any("killing it" in e["text"] for e in hub.events))
        log = (self.tmp / "state" / "logs" / "example-app.log").read_text()
        self.assertIn("received SIGTERM", log)
        self.assertIn(">>> STATUS: example-app stopped", log)

    async def test_delegate_hands_a_manual_start_to_switchboard(self):
        hub = await self.start_hub()
        svc = hub.services["example-app"]
        # Run the launcher by hand (as NVIDIA Sync would): switchboard knows the app, so the launcher only follows it.
        manual = await asyncio.create_subprocess_exec(
            "bash", str(LAUNCHER), env=clean_env(SWITCHBOARD_API=f"http://127.0.0.1:{self.admin}", LAUNCHER_POLL_S="1",
                                                 LAUNCHER_RUN_DIR=str(self.tmp / "run-manual"),
                                                 LAUNCHER_LOG_DIR=str(self.tmp / "logs-manual")),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, start_new_session=True)
        self.extra_pids.append(manual.pid)
        lines = []

        async def read_until(fragment, timeout=30):
            async def go():
                while True:
                    line = (await manual.stdout.readline()).decode()
                    if not line:
                        self.fail(f"launcher output ended; got {lines}")
                    lines.append(line.rstrip())
                    if fragment in line:
                        return
            await asyncio.wait_for(go(), timeout)

        await read_until(">>> STATUS: Example app ready (run by spark-switchboard)")
        self.assertEqual(svc.state, "ready")
        self.assertNotEqual(svc.pid, manual.pid)                       # switchboard's own launcher runs the app
        self.assertTrue(any("wanted: delegating launcher" in e["text"] for e in hub.events))
        self.assertFalse((self.tmp / "run-manual" / "example-app.pid").exists())
        # Closing it asks switchboard to stop the app unless it was used: it wasn't, so it stops.
        manual.send_signal(signal.SIGTERM)
        self.assertEqual(await asyncio.wait_for(manual.wait(), 15), 0)
        await self.until(lambda: svc.state == "stopped", timeout=20)
        self.assertIn("delegating launcher closed", svc.stop_reason)

    async def test_unknown_service_runs_standalone(self):
        await self.start_hub()
        port = free_port()
        manual = await asyncio.create_subprocess_exec(
            "bash", str(LAUNCHER), env=clean_env(SWITCHBOARD_API=f"http://127.0.0.1:{self.admin}",
                                                 SWITCHBOARD_SERVICE="not-configured", EXAMPLE_PORT=str(port),
                                                 EXAMPLE_HOST="127.0.0.1", EXAMPLE_DIR=str(self.tmp / "www"),
                                                 LAUNCHER_RUN_DIR=str(self.tmp / "run-manual"),
                                                 LAUNCHER_LOG_DIR=str(self.tmp / "logs-manual")),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL, start_new_session=True)
        self.extra_pids.append(manual.pid)
        await self.until(lambda: listening(port))
        self.assertEqual((await self.get(port, "/hello.txt"))[0], 200)
        self.assertEqual(int((self.tmp / "run-manual" / "example-app.pid").read_text()), manual.pid)
        self.assertEqual(self.hub.services["example-app"].state, "stopped")
        manual.send_signal(signal.SIGTERM)
        self.assertEqual(await asyncio.wait_for(manual.wait(), 15), 0)
        await self.until(lambda: not listening(port), timeout=5)

    async def run_delegate(self, *args, wrapper=False):
        """delegate.sh as NVIDIA Sync or an ssh session would run it (wrapper: under a shell that can go away)."""
        env = clean_env(SWITCHBOARD_API=f"http://127.0.0.1:{self.admin}", LAUNCHER_POLL_S="1",
                        LAUNCHER_RUN_DIR=str(self.tmp / "run-delegate"), LAUNCHER_LOG_DIR=str(self.tmp / "logs-delegate"))
        cmd = ["bash", str(DELEGATE), *args]
        if wrapper:
            cmd = ["bash", "-c", '"$@" & echo "delegate pid $!"; wait', "wrapper", *cmd]
        proc = await asyncio.create_subprocess_exec(*cmd, env=env, stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.STDOUT, start_new_session=True)
        self.extra_pids.append(proc.pid)
        return proc

    async def read_until(self, proc, fragment, lines, timeout=30):
        async def go():
            while True:
                line = (await proc.stdout.readline()).decode()
                if not line:
                    self.fail(f"output ended; got {lines}")
                lines.append(line.rstrip())
                if fragment in line:
                    return line
        return await asyncio.wait_for(go(), timeout)

    async def test_delegate_script_opens_any_service(self):
        hub = await self.start_hub()
        svc = hub.services["example-app"]
        # 1. Closed with SIGTERM (NVIDIA Sync closing the app): switchboard is asked to stop it unless it is in use.
        proc, lines = await self.run_delegate("example-app"), []
        await self.read_until(proc, ">>> STATUS: Example app ready (run by spark-switchboard) on port", lines)
        self.assertTrue(any(">>> STATUS: Example app starting" in line for line in lines), lines)
        self.assertEqual(svc.state, "ready")
        self.assertTrue(any(e["text"] == "Example app wanted: delegating launcher" for e in hub.events))
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(await asyncio.wait_for(proc.wait(), 15), 0)
        await self.until(lambda: svc.state == "stopped", timeout=20)
        self.assertIn("delegating launcher closed", svc.stop_reason)
        # 2. Its parent goes away (the ssh session or Sync's wrapper shell is killed): the same.
        proc, lines = await self.run_delegate("example-app", wrapper=True), []
        pid = int((await self.read_until(proc, "delegate pid ", lines)).split()[-1])
        self.extra_pids.append(pid)
        await self.read_until(proc, "ready (run by spark-switchboard)", lines)
        os.kill(proc.pid, signal.SIGKILL)                               # the wrapper, not delegate.sh
        await self.until(lambda: not alive(pid), timeout=15)
        await self.until(lambda: svc.state == "stopped", timeout=20)
        self.assertIn("delegating launcher closed", svc.stop_reason)
        # 3. Opened while switchboard is paused: it says so, and starts the app once switchboard is resumed.
        await hub.pause("test")
        proc, lines = await self.run_delegate("example-app"), []
        await self.read_until(proc, "not started, spark-switchboard is paused", lines)
        self.assertEqual(svc.state, "stopped")
        hub.resume("test")
        await self.read_until(proc, "ready (run by spark-switchboard)", lines)
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(await asyncio.wait_for(proc.wait(), 15), 0)
        await self.until(lambda: svc.state == "stopped", timeout=20)
        # 4. A service switchboard doesn't run, and a switchboard that isn't there: exit 1, saying why.
        proc, lines = await self.run_delegate("no-such-app"), []
        out = (await asyncio.wait_for(proc.communicate(), 15))[0].decode()
        self.assertEqual(proc.returncode, 1)
        self.assertIn("has no enabled service named 'no-such-app'", out)
        await self.stop_hub()
        proc = await self.run_delegate("example-app")
        out = (await asyncio.wait_for(proc.communicate(), 15))[0].decode()
        self.assertEqual(proc.returncode, 1)
        self.assertIn("is not answering", out)


def bash(script, **env):
    """Run a bash snippet with common.sh sourced (APP=t, no delegate); returns (exit code, output)."""
    r = subprocess.run(["bash", "-c", f'APP=t; SWITCHBOARD_DELEGATE=0; source "{LAUNCHERS}/common.sh"; {script}'],
                       env=clean_env(**env), capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout + r.stderr


class CommonShTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="sbcommon-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.env = {"LAUNCHER_RUN_DIR": str(self.tmp / "run"), "LAUNCHER_LOG_DIR": str(self.tmp / "logs")}

    def test_fractional_and_invalid_gib_values(self):
        self.assertEqual(bash('whole_gib 7.5; whole_gib 7; whole_gib 0.2; whole_gib .5', **self.env),
                         (0, "8\n7\n1\n1\n"))
        for bad in ("abc", "-1", "1e3", ""):
            self.assertEqual(bash(f'whole_gib "{bad}"', **self.env)[0], 1, bad)
        rc, out = bash("require_memory 999999.5", **self.env)       # used to pass (bash can't compare 999999.5)
        self.assertEqual(rc, 1, out)
        self.assertIn("REFUSED - t needs 1000000 GiB free memory", out)
        rc, out = bash("require_memory 0.5 && echo admitted=$_ADMITTED", **self.env)
        self.assertEqual(rc, 0, out)
        self.assertIn("admitted=1", out)
        rc, out = bash("require_memory lots", **self.env)
        self.assertEqual(rc, 1)
        self.assertIn("ERROR: t: the memory needed must be a number of GiB, not 'lots'", out)
        rc, out = bash("start_mem_guard 2.5 true && echo guard=$_GUARD_PID && kill $_GUARD_PID", **self.env)
        self.assertEqual(rc, 0, out)
        self.assertRegex(out, r"guard=\d+")


FAKE_COMFY = """import argparse, http.server
ap = argparse.ArgumentParser()
ap.add_argument("--listen")
ap.add_argument("--port", type=int)
a, _ = ap.parse_known_args()
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")
    def log_message(self, *x):
        pass
http.server.HTTPServer((a.listen, a.port), H).serve_forever()
"""


class ComfyLauncherTest(unittest.TestCase):
    """examples/launchers/comfyui.sh with a fake ComfyUI (main.py answering /system_stats)."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="sbcomfy-"))
        (self.tmp / "comfy").mkdir()
        (self.tmp / "comfy" / "main.py").write_text(FAKE_COMFY)
        self.run_dir = self.tmp / "run"
        self.procs, self.pids = [], []
        self.env = {"COMFYUI_DIR": str(self.tmp / "comfy"), "COMFYUI_PYTHON": sys.executable,
                    "COMFY_LISTEN": "127.0.0.1", "COMFY_MIN_FREE_GIB": "0", "COMFY_MEM_FLOOR_GIB": "0",
                    "LAUNCHER_RUN_DIR": str(self.run_dir), "LAUNCHER_LOG_DIR": str(self.tmp / "logs"),
                    "LAUNCHER_POLL_S": "1", "SWITCHBOARD_DELEGATE": "0"}

    def tearDown(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
            p.wait()
        for pid in self.pids:
            for kill, target in ((os.killpg, pid), (os.kill, pid)):
                try:
                    kill(target, signal.SIGKILL)
                except OSError:
                    pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    def launch(self, **env):
        p = subprocess.Popen(["bash", str(LAUNCHERS / "comfyui.sh")], env=clean_env(**{**self.env, **env}),
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        self.procs.append(p)
        self.pids.append(p.pid)
        return p

    def wait_for(self, cond, timeout=30):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if cond():
                return
            time.sleep(0.1)
        self.fail(f"condition not met within {timeout} s")

    def server_pidfile(self):
        try:
            return (self.run_dir / "comfyui-server.pid").read_text().split()
        except OSError:
            return []

    def test_handover_across_ports_stops_the_old_server(self):
        # L1 runs as under switchboard (internal port P1); L2 is opened by hand / from Sync on P2 while switchboard
        # isn't answering. L1 hands over; L2 must not leave L1's ComfyUI running next to its own.
        p1, p2 = free_port(), free_port()
        l1 = self.launch(SWITCHBOARD="1", SWITCHBOARD_SERVICE="comfyui", COMFY_PORT=str(p1))
        threading.Thread(target=l1.wait, daemon=True).start()     # reap it, as its parent would (no zombie)
        self.wait_for(lambda: listening(p1) and len(self.server_pidfile()) == 3)
        s1 = int(self.server_pidfile()[0])
        self.pids.append(s1)
        self.assertEqual(self.server_pidfile()[1:], ["127.0.0.1", str(p1)])
        l2 = self.launch(COMFY_PORT=str(p2))
        self.wait_for(lambda: listening(p2))
        self.assertEqual(l1.wait(30), 0)                      # handed over
        self.wait_for(lambda: not alive(s1), timeout=30)      # and its ComfyUI was stopped, not orphaned
        s2, listen, port = self.server_pidfile()
        self.pids.append(int(s2))
        self.assertEqual((listen, port), ("127.0.0.1", str(p2)))
        self.assertNotEqual(int(s2), s1)
        self.assertFalse(listening(p1))
        l2.send_signal(signal.SIGTERM)
        self.assertEqual(l2.wait(30), 0)
        self.assertFalse(alive(int(s2)))
        self.assertEqual(self.server_pidfile(), [])

    def test_a_refused_start_leaves_a_stale_pids_process_alone(self):
        victim = subprocess.Popen(["sleep", "300"], start_new_session=True)
        self.procs.append(victim)
        self.run_dir.mkdir(parents=True)
        for content in (f"{victim.pid}", f"{victim.pid} 127.0.0.1 {free_port()}"):     # old and new format
            (self.run_dir / "comfyui-server.pid").write_text(content + "\n")
            p = self.launch(COMFY_PORT=str(free_port()), COMFY_MIN_FREE_GIB="1000000")
            self.assertEqual(p.wait(30), 1)                   # refused: not enough memory
            self.assertIsNone(victim.poll(), content)         # the unrelated process is still there


if __name__ == "__main__":
    unittest.main()
