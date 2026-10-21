"""Config discovery, the CLI's admin API URL, the cgroup parser, the local-client check, config keys, the shipped
examples, and reading a discrete GPU (with a fake nvidia-smi). No network, apart from one tiny local HTTP server for
the CLI test.   Run from the repo root: python -m unittest -v"""
import asyncio
import http.server
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from spark_switchboard import config as config_mod  # noqa: E402
from spark_switchboard import memory, pages  # noqa: E402
from spark_switchboard.__main__ import api_url  # noqa: E402
from spark_switchboard.app import Hub, is_local_client  # noqa: E402
from spark_switchboard.memory import container_id  # noqa: E402

CID = "3f1c9e0d5b7a4c2e8f6a1b0c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e"


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Tmp(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory(prefix="sbcfg-")
        self.tmp = Path(self._td.name)

    def tearDown(self):
        self._td.cleanup()

    def write(self, rel, text):
        p = self.tmp / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
        return p


class FindConfig(Tmp):
    def setUp(self):
        super().setUp()
        self.system = self.tmp / "etc" / "spark-switchboard" / "switchboard.toml"
        patcher = mock.patch.object(config_mod, "SYSTEM_CONFIG", self.system)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.env = {"HOME": str(self.tmp / "home")}

    def test_explicit_path_wins_and_must_exist(self):
        p = self.write("a.toml", "")
        self.write("xdg/spark-switchboard/switchboard.toml", "")
        env = {**self.env, "SWITCHBOARD_CONFIG": str(p), "XDG_CONFIG_HOME": str(self.tmp / "xdg")}
        self.assertEqual(config_mod.find(str(p), env), p)
        with self.assertRaisesRegex(config_mod.ConfigError, r"given with -c"):
            config_mod.find(str(self.tmp / "missing.toml"), env)

    def test_env_var_comes_next_and_must_exist(self):
        p = self.write("b.toml", "")
        self.write("xdg/spark-switchboard/switchboard.toml", "")
        env = {**self.env, "SWITCHBOARD_CONFIG": str(p), "XDG_CONFIG_HOME": str(self.tmp / "xdg")}
        self.assertEqual(config_mod.find(None, env), p)
        env["SWITCHBOARD_CONFIG"] = str(self.tmp / "missing.toml")
        with self.assertRaisesRegex(config_mod.ConfigError, r"SWITCHBOARD_CONFIG"):
            config_mod.find(None, env)

    def test_xdg_config_home_then_home_dot_config_then_etc(self):
        xdg = self.write("xdg/spark-switchboard/switchboard.toml", "")
        self.assertEqual(config_mod.find(None, {**self.env, "XDG_CONFIG_HOME": str(self.tmp / "xdg")}), xdg)
        home = self.write("home/.config/spark-switchboard/switchboard.toml", "")
        self.assertEqual(config_mod.find(None, self.env), home)                 # XDG_CONFIG_HOME unset
        home.unlink()
        self.write("etc/spark-switchboard/switchboard.toml", "")
        self.assertEqual(config_mod.find(None, self.env), self.system)

    def test_nothing_found_lists_every_place(self):
        with self.assertRaises(config_mod.ConfigError) as cm:
            config_mod.find(None, self.env)
        msg = str(cm.exception)
        user = self.tmp / "home" / ".config" / "spark-switchboard" / "switchboard.toml"
        for part in ("-c PATH", "$SWITCHBOARD_CONFIG", str(user), str(self.system), "examples/switchboard.toml"):
            self.assertIn(part, msg)


class ApiUrl(Tmp):
    def args(self, **kw):
        return Namespace(api=kw.get("api"), config=kw.get("config"))

    def test_precedence(self):
        cfg = self.write("c.toml", "admin_port = 9123\n")
        env = {"SWITCHBOARD_CONFIG": str(cfg)}
        self.assertEqual(api_url(self.args(api="http://h:1"), {**env, "SWITCHBOARD_API": "http://e:2"}), "http://h:1")
        self.assertEqual(api_url(self.args(), {**env, "SWITCHBOARD_API": "http://e:2"}), "http://e:2")
        self.assertEqual(api_url(self.args(), env), "http://127.0.0.1:9123")
        self.assertEqual(api_url(self.args(config=str(cfg)), {}), "http://127.0.0.1:9123")

    def test_bind_address_and_defaults(self):
        for bind, want in (('"192.0.2.10"', "http://192.0.2.10:9000"), ('"::1"', "http://[::1]:9000"),
                           ('"::"', "http://127.0.0.1:9000"), ('"0.0.0.0"', "http://127.0.0.1:9000"),
                           ('""', "http://127.0.0.1:9000")):
            cfg = self.write("b.toml", f"admin_port = 9000\nbind = {bind}\n")
            self.assertEqual(api_url(self.args(config=str(cfg)), {}), want)
        cfg = self.write("d.toml", "")
        self.assertEqual(api_url(self.args(config=str(cfg)), {}), "http://127.0.0.1:8800")

    def test_no_config_falls_back_but_a_named_one_must_exist(self):
        with mock.patch.object(config_mod, "SYSTEM_CONFIG", self.tmp / "none.toml"):
            self.assertEqual(api_url(self.args(), {"HOME": str(self.tmp)}), "http://127.0.0.1:8800")
            with self.assertRaises(config_mod.ConfigError):
                api_url(self.args(config=str(self.tmp / "missing.toml")), {"HOME": str(self.tmp)})
            with self.assertRaises(config_mod.ConfigError):
                api_url(self.args(), {"HOME": str(self.tmp), "SWITCHBOARD_CONFIG": str(self.tmp / "missing.toml")})

    def test_cli_talks_to_the_configs_admin_port(self):
        port = free_port()
        status = {"host": "fakebox", "time": 0, "memory": {"total_gib": 121, "available_gib": 100}, "services": [],
                  "events": []}

        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps(status).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass
        srv = http.server.HTTPServer(("127.0.0.1", port), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        cfg = self.write("cli.toml", f"admin_port = {port}\n")
        env = {k: v for k, v in os.environ.items() if not k.startswith("SWITCHBOARD")}
        env["SWITCHBOARD_CONFIG"] = str(cfg)
        r = subprocess.run([sys.executable, "-m", "spark_switchboard", "status"], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=30)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("fakebox: 100 of 121 GiB free", r.stdout)
        r = subprocess.run([sys.executable, "-m", "spark_switchboard", "status", "--json"], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=30)
        self.assertEqual(json.loads(r.stdout)["host"], "fakebox")


class Cgroups(unittest.TestCase):
    def test_container_ids(self):
        cases = {
            f"0::/system.slice/docker-{CID}.scope\n": CID,                                  # systemd driver
            f"0::/docker/{CID}\n": CID,                                                     # cgroupfs, v2
            f"12:memory:/docker/{CID}\n11:cpu:/docker/{CID}\n": CID,                        # cgroupfs, v1
            f"0::/machine.slice/libpod-{CID}.scope/container\n": CID,                       # podman
            f"0::/libpod_parent/libpod-{CID}\n": CID,                                       # podman, cgroupfs
            f"0::/user.slice/user-1000.slice/user@1000.service/user.slice/docker-{CID}.scope\n": CID,   # rootless
            "0::/user.slice/user-1000.slice/session-3.scope\n": None,                       # a host process
            "0::/system.slice/docker.service\n": None,                                      # dockerd itself
            f"0::/system.slice/docker-{CID[:40]}.scope\n": None,                            # not a full id
            f"0::/system.slice/docker-{CID}ab.scope\n": None,                               # too long
        }
        for text, want in cases.items():
            self.assertEqual(container_id(text), want, text)


class LocalClient(unittest.TestCase):
    def test_loopback_mapped_and_same_address(self):
        self.assertTrue(is_local_client("127.0.0.1"))
        self.assertTrue(is_local_client("::1"))
        self.assertTrue(is_local_client("::ffff:127.0.0.1"))       # what a dual-stack listener reports for IPv4
        self.assertFalse(is_local_client("192.0.2.20"))
        self.assertFalse(is_local_client("::ffff:192.0.2.20"))
        self.assertTrue(is_local_client("192.0.2.10", "192.0.2.10"))       # a client on the box using its LAN address
        self.assertTrue(is_local_client("::ffff:192.0.2.10", "::ffff:192.0.2.10"))
        self.assertFalse(is_local_client("192.0.2.20", "192.0.2.10"))
        self.assertFalse(is_local_client(None))
        self.assertFalse(is_local_client("not an address"))


class Keys(Tmp):
    def test_defaults_and_new_keys(self):
        cfg = config_mod.load(self.write("m.toml", ""))
        self.assertEqual(cfg.admin_port, 8800)
        self.assertEqual(cfg.state_dir, Path(os.path.expanduser("~/.local/state/spark-switchboard")))
        self.assertEqual(cfg.use_event_after_s, 900)
        self.assertEqual(cfg.hostname, socket.gethostname())
        cfg = config_mod.load(self.write("n.toml", 'use_event_after_s = 120\nhostname = "spark-xxxx.local"\n'))
        self.assertEqual(cfg.use_event_after_s, 120)
        self.assertEqual(cfg.hostname, "spark-xxxx.local")

    def test_unknown_keys_are_errors(self):
        base = '[service.a]\ntitle = "A"\nadapter = "web"\nupstream = "http://127.0.0.1:1"\ncommand = ["true"]\n'
        for text, frag in (("bogus = 1\n", "unknown keys"), ('path = "x"\n', "unknown keys"),
                           ("services = 1\n", "unknown keys"), (base + "colour = 1\n", "unknown keys"),
                           (base + "[service.a.run]\nstop_timeout = 5\n", r"in \[run\]"),
                           (base + "[service.a.ready]\npath = '/'\nestimate = 5\n", r"in \[ready\]")):
            with self.assertRaisesRegex(config_mod.ConfigError, frag):
                config_mod.load(self.write("k.toml", text))

    def test_unknown_keys_in_the_other_tables_and_unknown_adapters(self):
        svc = '[service.a]\ntitle = "A"\nadapter = "{adapter}"\nupstream = "http://127.0.0.1:1"\ncommand = ["true"]\n'
        web, openai = svc.format(adapter="web"), svc.format(adapter="openai")
        for text, frag in (
                ("[memory]\nfree_floor_gb = 20\n", r"unknown keys in \[memory\]: \['free_floor_gb'\]"),
                ('[memory]\nsource = "proc"\n', "source must be"),
                ('[memory]\nsource = "fake"\n', "needs total_gib and dir"),
                (web + '[service.a.busy_probe]\npath = "/q"\nlist = "jobs"\nfield = "s"\nidle_value = ["done"]\n',
                 r"unknown keys in \[busy_probe\]: \['idle_value'\]"),
                (web + '[service.a.busy_probe]\nlist = "jobs"\nfield = "s"\n', "needs path"),
                (web + '[service.a.busy_probe]\npath = "/q"\n', "needs either nonempty or list"),
                (web + '[service.a.busy_probe]\npath = "/q"\nlist = "jobs"\n', "with list needs field"),
                (web + '[service.a.options]\nmodels = ["m"]\n', r"unknown keys in \[options\] for adapter 'web'"),
                (openai + '[service.a.options]\nmodles = ["m"]\n', r"\['modles'\]"),
                (svc.format(adapter="webb"), "unknown adapter 'webb'")):
            with self.assertRaisesRegex(config_mod.ConfigError, frag):
                config_mod.load(self.write("k.toml", text))
        cfg = config_mod.load(self.write("ok.toml", openai + '[service.a.options]\nmodels = ["m"]\ninfo_html = "x"\n'
                                         '[service.a.busy_probe]\npath = "/q"\nnonempty = ["queue"]\n'))
        self.assertEqual(cfg.services["a"].options["models"], ["m"])

    def test_discrete_gpu_and_pause_keys(self):
        cfg = config_mod.load(self.write("d.toml", ""))
        self.assertEqual((cfg.pause_persists, cfg.remote_resume, cfg.ram_reserve_gib), (False, True, 0))
        svc = '[service.a]\ntitle = "A"\nadapter = "web"\nupstream = "http://127.0.0.1:1"\ncommand = ["true"]\n'
        gpu = '[memory]\nsource = "gpu"\ngpu = "GPU-x"\n'
        cfg = config_mod.load(self.write("g.toml", "pause_persists = true\nremote_resume = false\nram_reserve_gib = 8\n"
                                         + gpu + svc + "memory_gib = 20\nram_gib = 16\n"))
        s = cfg.services["a"]
        self.assertEqual((s.memory_gib, s.max_gib, s.ram_gib, s.ram_max_gib), (20, 20, 16, 16))
        self.assertEqual((cfg.pause_persists, cfg.remote_resume, cfg.ram_reserve_gib), (True, False, 8))
        for text, frag in ((svc + "ram_gib = 16\n", r"ram_gib and ram_max_gib only apply with \[memory\] source"),
                           ("ram_reserve_gib = 4\n", "ram_reserve_gib only applies"),
                           ("[memory]\ngpu = 0\n", r"gpu only applies with source = \"gpu\""),
                           ('[memory]\nsource = "gpu"\ngpu = 1.5\n', "nvidia-smi index"),
                           # quoted, "false" is a string that Python takes as true: remote resume would be allowed
                           ('remote_resume = "false"\n',
                            r"remote_resume must be true or false \(unquoted\), not 'false'"),
                           ('pause_persists = "no"\n', "pause_persists must be true or false"),
                           (svc + 'keep_warm = "yes"\n', "keep_warm must be true or false"),
                           ('[memory]\nsource = "cuda"\n', r"\"gpu\" \(one discrete NVIDIA GPU\)")):
            with self.assertRaisesRegex(config_mod.ConfigError, frag):
                config_mod.load(self.write("k.toml", text))

    def test_run_and_ready_tables(self):
        p = self.write("r.toml", 'include = ["services/*.toml"]\n')
        self.write("services/x.toml", 'title = "X"\nadapter = "web"\nupstream = "http://127.0.0.1:5/"\n'
                   '[run]\ncommand = "echo hi"\nenv = { A = "~/a" }\nstop_timeout_s = 7\n'
                   '[ready]\npath = "/ok"\nestimate_s = 3\ntimeout_s = 9\n')
        s = config_mod.load(p).services["x"]
        self.assertEqual(s.command, ["bash", "-c", "echo hi"])
        self.assertEqual(s.env["A"], os.path.expanduser("~/a"))
        self.assertEqual((s.stop_timeout_s, s.ready_path, s.start_estimate_s, s.start_timeout_s), (7, "/ok", 3, 9))
        self.assertEqual(s.upstream, "http://127.0.0.1:5")


class Examples(unittest.TestCase):
    def test_example_config_loads_and_is_consistent(self):
        cfg = config_mod.load(REPO / "examples" / "switchboard.toml")
        self.assertEqual(set(cfg.services), {"open-webui", "comfyui", "tensorfold", "lf-studio", "sol-h3-studio"})
        # sol-h3-studio.toml is the deprecated copy of lf-studio.toml for the studio's old names (same port)
        deprecated = {n for n in cfg.services
                      if (REPO / "examples" / "services" / f"{n}.toml").read_text().startswith("# DEPRECATED")}
        self.assertEqual(deprecated, {"sol-h3-studio"})
        self.assertEqual(cfg.services["sol-h3-studio"].listen, cfg.services["lf-studio"].listen)
        ports = [p for s in cfg.services.values() if s.name not in deprecated for p in s.listen]
        self.assertEqual(len(ports), len(set(ports)))
        self.assertNotIn(cfg.admin_port, ports)
        launchers = REPO / "examples" / "launchers"
        for s in cfg.services.values():
            self.assertFalse(s.enabled, f"{s.name}: examples ship disabled")
            for k, v in s.env.items():
                if k.endswith("_MIN_FREE_GIB"):
                    self.assertEqual(float(v), s.memory_gib + cfg.reserve_gib,
                                     f"{s.name}: {k} should be memory_gib + reserve_gib")
            script = Path(s.command[-1]).name
            if (launchers / script).exists():
                text = (launchers / script).read_text()
                self.assertIn(f"\nAPP={s.name}\n", text, f"{script}: APP should be the service name")
                self.assertEqual(s.pidfile, os.path.expanduser(f"~/.local/state/spark-switchboard/run/{s.name}.pid"))
                self.assertIn(f"/examples/launchers/{script}", s.command[-1])

    def test_workstation_example(self):
        # examples/workstation.toml: one discrete GPU, one app, nothing kept warm, a pause that persists and is
        # resumed on the machine itself.
        cfg = config_mod.load(REPO / "examples" / "workstation.toml")
        self.assertEqual((cfg.memory["source"], cfg.pause_persists, cfg.remote_resume), ("gpu", True, False))
        self.assertEqual(list(cfg.services), ["comfyui"])
        s = cfg.services["comfyui"]
        self.assertFalse(s.enabled, "examples ship disabled")
        self.assertFalse(s.keep_warm)
        self.assertGreater(s.idle_stop_s, 0)
        self.assertGreater(s.ram_gib, 0)
        self.assertEqual(float(s.env["COMFY_MIN_FREE_GIB"]), s.ram_gib + cfg.ram_reserve_gib,
                         "the launcher's check is about host RAM here, reserve included")
        self.assertEqual(s.pidfile, os.path.expanduser("~/.local/state/spark-switchboard/run/comfyui.pid"))
        self.assertTrue(s.command[-1].endswith("/examples/launchers/comfyui.sh"))
        self.assertIn("\nAPP=comfyui\n", (REPO / "examples" / "launchers" / "comfyui.sh").read_text())

    def matches_the_companion_repo(self, name):
        """examples/services/<name>.toml is the companion repo's integrations/spark-switchboard/<name>.toml, shipped
        disabled (header comments aside). Checked when that repo is checked out next to this one, under its new name or
        its old one (sol-h3-studio, before its 0.5), and has the file; skipped (saying so) when none has it."""
        checked = []
        for repo in ("lf-studio", "sol-h3-studio"):
            companion = REPO.parent / repo / "integrations" / "spark-switchboard" / f"{name}.toml"
            if not companion.is_file():
                continue
            with self.subTest(companion=str(companion)):
                ours = tomllib.loads((REPO / "examples" / "services" / f"{name}.toml").read_text())
                theirs = tomllib.loads(companion.read_text())
                self.assertIs(ours.pop("enabled"), False)
                self.assertIs(theirs.pop("enabled", True), True)
                self.assertEqual(ours, theirs)
            checked.append(companion)
        if not checked:
            self.skipTest(f"{name}.toml not compared: no lf-studio or sol-h3-studio checkout next to {REPO} has it")

    def test_lf_studio_matches_the_companion_repo(self):
        self.matches_the_companion_repo("lf-studio")            # LF Studio 0.5 and newer

    def test_deprecated_sol_h3_studio_matches_the_companion_repo(self):
        self.matches_the_companion_repo("sol-h3-studio")        # Sol-H3 Studio 0.4 (and LF Studio 0.5, which keeps it)

    def test_check_command_on_the_examples(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("SWITCHBOARD")}
        env["SWITCHBOARD_CONFIG"] = str(REPO / "examples" / "switchboard.toml")
        r = subprocess.run([sys.executable, "-m", "spark_switchboard", "check"], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=30)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.count(" off :"), 5, r.stdout)

    def test_lf_studio_says_which_version_it_needs(self):
        text = (REPO / "examples" / "services" / "lf-studio.toml").read_text()
        self.assertIn("needs LF Studio 0.5 or newer", text)
        self.assertIn("bin/lf-studio", " ".join(tomllib.loads(text)["run"]["command"]))


SMI = """#!/bin/sh
# A fake nvidia-smi: one RTX 5090, or (FAKE_SMI=gb10) a GB10, which has no memory of its own to report. FAKE_SMI=fail
# fails as after a driver update without a reboot, FAKE_SMI=hang never answers (its pid in $FAKE_SMI_PIDFILE).
case "$FAKE_SMI" in
  fail) echo "Failed to initialize NVML: Driver/library version mismatch"; echo "NVML library version: 580"; exit 18 ;;
  hang) echo $$ > "$FAKE_SMI_PIDFILE"; exec sleep 60 ;;
esac
case "$1" in
  --query-gpu=*)
    if [ "$FAKE_SMI" = gb10 ]; then echo "0, GPU-20f0, NVIDIA GB10, [N/A], [N/A]"
    else echo "0, GPU-aaaa, NVIDIA GeForce RTX 5090, 32607, 20000"; fi ;;
  --query-compute-apps=*)
    echo "$FAKE_PID, 8192, GPU-aaaa"
    echo "$FAKE_PID, 1024, GPU-bbbb"
    echo "999999999, 100, GPU-aaaa" ;;
esac
"""


class DiscreteGPU(Tmp):
    def fake_smi(self, **env):
        d = self.tmp / "bin"
        d.mkdir(exist_ok=True)
        (d / "nvidia-smi").write_text(SMI)
        (d / "nvidia-smi").chmod(0o755)
        return mock.patch.dict(os.environ, {"PATH": f"{d}:{os.environ['PATH']}", "FAKE_PID": str(os.getpid()), **env})

    def test_parsing(self):
        gpus = memory.parse_gpus("0, GPU-aaaa, NVIDIA GeForce RTX 5090, 32607, 30001\n"
                                 "1, GPU-bbbb, NVIDIA RTX A2000, 6138, 6000\n")
        self.assertEqual(memory.pick_gpu(gpus, 1)["name"], "NVIDIA RTX A2000")
        self.assertEqual(memory.pick_gpu(gpus, "GPU-aaaa")["total_mib"], 32607)
        self.assertIsNone(memory.pick_gpu(gpus, 2))
        self.assertIsNone(memory.parse_gpus("0, GPU-20f0, NVIDIA GB10, [N/A], [N/A]\n")[0]["free_mib"])
        apps = "11, 1000, GPU-aaaa\n12, 500, GPU-bbbb\n11, 24, GPU-aaaa\n13, [N/A], GPU-aaaa\n"
        self.assertEqual(memory.parse_compute_apps(apps, "GPU-aaaa"), {11: 1024.0})
        self.assertEqual(memory.parse_compute_apps("11, 1000\n12, 500\n"), {11: 1000.0, 12: 500.0})
        self.assertEqual(memory.amount(20, "memory"), "20 GiB")
        self.assertEqual(memory.amount(20, "gpu"), "20 GiB of GPU memory")

    def test_gpu_source_reads_nvidia_smi(self):
        with self.fake_smi():
            mem = memory.GPUMemory({"gpu": 0})
            asyncio.run(mem.refresh())
            self.assertIsNone(mem.error)
            self.assertEqual(mem.name, "NVIDIA GeForce RTX 5090")
            self.assertAlmostEqual(mem.total()["gpu"], 32607 / 1024)
            self.assertAlmostEqual(mem.available()["gpu"], 20000 / 1024)
            self.assertGreater(mem.available()["ram"], 0)
            me = SimpleNamespace(pid=os.getpid(), cfg=SimpleNamespace(name="me", containers=[]))
            held = asyncio.run(mem.usage([me]))["me"]
            self.assertEqual(held["gpu"], 8.0)              # only this GPU's allocations of this process tree
            self.assertGreater(held["ram"], 0)
        with self.fake_smi(FAKE_SMI="gb10"):
            mem = memory.GPUMemory({})
            asyncio.run(mem.refresh())
            self.assertIn('use [memory] source = "system"', mem.error)
            self.assertEqual(mem.available()["gpu"], 0)     # never read: counts as full
        with mock.patch.dict(os.environ, {"PATH": str(self.tmp / "nothing")}):
            mem = memory.GPUMemory({"gpu": 3})
            asyncio.run(mem.refresh())
            self.assertEqual(mem.error, "nvidia-smi not found (not installed, or not on PATH)")

    def test_nvidia_smi_failures_say_why_and_old_figures_expire(self):
        with self.fake_smi():
            mem = memory.GPUMemory({})
            asyncio.run(mem.refresh())
            self.assertAlmostEqual(mem.available()["gpu"], 20000 / 1024)
        with self.fake_smi(FAKE_SMI="fail"):
            asyncio.run(mem.refresh())
            self.assertEqual(mem.error, "nvidia-smi failed (exit 18): Failed to initialize NVML: Driver/library "
                                        "version mismatch")
        self.assertAlmostEqual(mem.available()["gpu"], 20000 / 1024)     # one failure: the last figure stays
        mem._read_at -= memory.GPUMemory.STALE_S + 1                   # ... for STALE_S, then the GPU counts as full
        self.assertEqual(mem.available()["gpu"], 0)
        self.assertAlmostEqual(mem.total()["gpu"], 32607 / 1024)
        with self.fake_smi():
            asyncio.run(mem.refresh())
            self.assertIsNone(mem.error)
            self.assertAlmostEqual(mem.available()["gpu"], 20000 / 1024)

    def test_a_hanging_nvidia_smi_is_killed_and_not_run_again_at_once(self):
        self.addCleanup(memory._timed_out.clear)
        pidfile = self.tmp / "smi.pid"
        with self.fake_smi(FAKE_SMI="hang", FAKE_SMI_PIDFILE=str(pidfile)):
            mem = memory.GPUMemory({})

            async def run():
                with mock.patch.object(memory, "_run", lambda *cmd: run.orig(*cmd, timeout=0.5)):
                    await mem.refresh()
            run.orig = memory._run
            asyncio.run(run())
            self.assertEqual(mem.error, "nvidia-smi did not answer within 0.5 s")
            pid = int(pidfile.read_text())
            with self.assertRaises(ProcessLookupError):     # killed (and reaped), not left running
                os.kill(pid, 0)
            pidfile.unlink()
            t0 = time.monotonic()
            asyncio.run(mem.refresh())                      # within RETRY_AFTER_TIMEOUT_S: not run again
            self.assertLess(time.monotonic() - t0, 0.5)
            self.assertFalse(pidfile.exists())
            self.assertEqual(mem.error, "nvidia-smi did not answer within 0.5 s")

    def test_check_reads_the_gpu(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("SWITCHBOARD")}
        with self.fake_smi():
            env["PATH"] = os.environ["PATH"]
            r = subprocess.run([sys.executable, "-m", "spark_switchboard", "check", "-c",
                                str(REPO / "examples" / "workstation.toml")], cwd=REPO, env=env,
                               capture_output=True, text=True, timeout=30)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn("memory: GPU 0 (NVIDIA GeForce RTX 5090, 31.8 GiB, 19.5 free)", r.stdout)
            self.assertIn("pause: persists across restarts, resumed only on this machine", r.stdout)
            self.assertIn("needs 20 GiB GPU (max 31) + 16 GiB RAM (max 40)", r.stdout)
            r = subprocess.run([sys.executable, "-m", "spark_switchboard", "check", "-c",
                                str(REPO / "examples" / "workstation.toml")], cwd=REPO,
                               env={**env, "FAKE_SMI": "gb10"}, capture_output=True, text=True, timeout=30)
            self.assertEqual(r.returncode, 1)
            self.assertIn("no memory of its own", r.stderr)


class Listen(Tmp):
    def test_bind_all_ipv6_also_answers_on_ipv4_loopback(self):
        # bind = "::" means every interface, as unset does: the CLI's http://127.0.0.1:<admin_port> must reach it.
        if not socket.has_dualstack_ipv6():
            self.skipTest("no dual-stack IPv6 here")
        p = self.write("l.toml", f'bind = "::"\nstate_dir = "{self.tmp / "state"}"\n'
                                 f'[memory]\nsource = "fake"\ntotal_gib = 10\ndir = "{self.tmp / "mem"}"\n')
        cfg = config_mod.load(p)
        self.assertEqual(config_mod.admin_url(p), f"http://127.0.0.1:{cfg.admin_port}")
        port = free_port()
        with Hub(cfg)._listen_socket(port) as srv, socket.create_connection(("127.0.0.1", port), timeout=5):
            self.assertEqual(srv.family, socket.AF_INET6)


class Dashboard(Tmp):
    def test_event_threshold_reaches_the_page(self):
        p = self.write("d.toml", f'state_dir = "{self.tmp / "state"}"\nuse_event_after_s = 600\n'
                                 f'[memory]\nsource = "fake"\ntotal_gib = 10\ndir = "{self.tmp / "mem"}"\n')
        hub = Hub(config_mod.load(p))
        text = pages.dashboard(hub).text
        self.assertIn('"use_event_after_s": 600', text)
        self.assertIn("plus the first use after ' + ago(SB.use_event_after_s)", text)
        self.assertNotIn("15 min unused", text)


class State(Tmp):
    def test_a_damaged_state_file_is_kept_aside_and_reported(self):
        p = self.write("s.toml", f'state_dir = "{self.tmp / "state"}"\n'
                                 f'[memory]\nsource = "fake"\ntotal_gib = 10\ndir = "{self.tmp / "mem"}"\n')
        self.write("state/state.json", '{"paused": {"since": 1, "reason": "x"}, "serv')     # cut off
        hub = Hub(config_mod.load(p))
        self.assertFalse(hub.paused)
        self.assertTrue(any(e["level"] == "warning" and "state.json is damaged" in e["text"] for e in hub.events))
        self.assertTrue((self.tmp / "state" / "state.json.bad").read_text().startswith('{"paused"'))
        self.assertFalse((self.tmp / "state" / "state.json").exists())
        hub._state["paused"] = {"since": 2, "reason": "y"}
        hub._write_state(durable=True)
        self.assertEqual(json.loads((self.tmp / "state" / "state.json").read_text())["paused"]["reason"], "y")
        self.assertTrue((self.tmp / "state" / "state.json.bad").exists())         # not overwritten


if __name__ == "__main__":
    unittest.main()
