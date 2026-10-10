"""Config discovery, the CLI's admin API URL, the cgroup parser, the local-client check, config keys, and the shipped
examples. No network, apart from one tiny local HTTP server for the CLI test.   Run from the repo root:
python -m unittest -v"""
import http.server
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import tomllib
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from spark_switchboard import config as config_mod  # noqa: E402
from spark_switchboard import pages  # noqa: E402
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
        self.assertEqual(set(cfg.services), {"open-webui", "comfyui", "tensorfold", "sol-h3-studio"})
        ports = [p for s in cfg.services.values() for p in s.listen]
        self.assertEqual(len(ports), len(set(ports)))
        self.assertNotIn(cfg.admin_port, ports)
        launchers = REPO / "examples" / "launchers"
        for s in cfg.services.values():
            self.assertFalse(s.enabled, f"{s.name}: examples ship disabled")
            for k, v in s.env.items():
                if k.endswith("_MIN_FREE_GIB"):
                    self.assertEqual(float(v), s.memory_gib, f"{s.name}: {k} should match memory_gib")
            script = Path(s.command[-1]).name
            if (launchers / script).exists():
                text = (launchers / script).read_text()
                self.assertIn(f"\nAPP={s.name}\n", text, f"{script}: APP should be the service name")
                self.assertEqual(s.pidfile, os.path.expanduser(f"~/.local/state/spark-switchboard/run/{s.name}.pid"))
                self.assertIn(f"/examples/launchers/{script}", s.command[-1])

    def test_sol_h3_studio_matches_the_companion_repo(self):
        # examples/services/sol-h3-studio.toml is the companion repo's integrations/spark-switchboard/sol-h3-studio.toml,
        # shipped disabled (header comments aside). Checked when that repo is checked out next to this one.
        companion = REPO.parent / "sol-h3-studio" / "integrations" / "spark-switchboard" / "sol-h3-studio.toml"
        if not companion.is_file():
            self.skipTest(f"{companion} is not there")
        ours = tomllib.loads((REPO / "examples" / "services" / "sol-h3-studio.toml").read_text())
        theirs = tomllib.loads(companion.read_text())
        self.assertIs(ours.pop("enabled"), False)
        self.assertIs(theirs.pop("enabled", True), True)
        self.assertEqual(ours, theirs)

    def test_check_command_on_the_examples(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("SWITCHBOARD")}
        env["SWITCHBOARD_CONFIG"] = str(REPO / "examples" / "switchboard.toml")
        r = subprocess.run([sys.executable, "-m", "spark_switchboard", "check"], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=30)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.count(" off :"), 4, r.stdout)


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


if __name__ == "__main__":
    unittest.main()
