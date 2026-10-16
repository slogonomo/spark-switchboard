"""spark-switchboard command line (also `python -m spark_switchboard`).

  spark-switchboard serve  [-c CONFIG]             run the daemon
  spark-switchboard check  [-c CONFIG]             validate the config and list the services
  spark-switchboard status [--json]                what is running (talks to the daemon's admin API)
  spark-switchboard start NAME [--force] [--wait]
  spark-switchboard stop  NAME [--kill]            --kill also interrupts work in progress (only from this machine)
  spark-switchboard pause [--kill] [--persist | --no-persist] [--reason TEXT]
                                                   stop every service switchboard runs and start nothing until resume
  spark-switchboard resume
  spark-switchboard stop-all [--kill]              the same as pause (an older name)
  spark-switchboard log   NAME [-n 100]

The config file is -c, else $SWITCHBOARD_CONFIG, else ~/.config/spark-switchboard/switchboard.toml
($XDG_CONFIG_HOME), else /etc/spark-switchboard/switchboard.toml. The client commands talk to --api, else
$SWITCHBOARD_API, else the admin_port of that config file, else http://127.0.0.1:8800.
"""
import argparse
import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from . import __version__
from . import config as config_mod


def api_url(args, environ=None):
    """Where the admin API is: --api, $SWITCHBOARD_API, the discovered config's admin_port, or the default."""
    env = os.environ if environ is None else environ
    if getattr(args, "api", None):
        return args.api
    if env.get("SWITCHBOARD_API"):
        return env["SWITCHBOARD_API"]
    try:
        return config_mod.admin_url(config_mod.find(getattr(args, "config", None), env))
    except config_mod.ConfigError:
        if getattr(args, "config", None) or env.get("SWITCHBOARD_CONFIG"):
            raise                       # a config was named: don't silently talk to another daemon
        return f"http://127.0.0.1:{config_mod.DEFAULT_ADMIN_PORT}"


def _api(args, method, path):
    req = urllib.request.Request(args.api.rstrip("/") + path, method=method, headers={"X-Switchboard": "1"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read()
            return r.status, (json.loads(body) if r.headers.get_content_type() == "application/json" else body.decode())
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, body.decode()
    except OSError as e:
        sys.exit(f"switchboard is not answering on {args.api}: {e}")


def _ago(s):
    s = int(max(0, s))
    return f"{s} s" if s < 90 else f"{round(s / 60)} min" if s < 5400 else f"{s / 3600:.1f} h"


def _line(s):
    state = s["state"]
    if s.get("want"):
        state = f"wanted: {s['want']['phase']}"
    extra = []
    if s["state"] == "ready":
        held = f"{s['usage_gib']} GiB"
        if "ram_usage_gib" in s:                        # a discrete GPU
            held = f"{s['usage_gib']} GiB GPU + {s['ram_usage_gib']} GiB RAM"
        extra.append(f"{held}, {s['inflight']} in flight, last used "
                     f"{'%d s ago' % (time.time() - s['last_activity']) if s.get('last_activity') else 'never'}")
        if s.get("busy"):
            extra.append(f"working: {s['busy']}")
    if s["state"] == "starting" and s.get("detail"):
        extra.append(s["detail"])
    if s.get("want") and s["want"].get("text"):
        extra.append(s["want"]["text"])
    if s["state"] == "failed" and s.get("failure"):
        extra.append(s["failure"])
    port = f":{s['listen'][0]}" if s["listen"] else ""
    return f"{s['name']:<14} {port:<7} {state:<16} {' · '.join(extra)}"


def _check_gpu(cfg):
    """`check` with [memory] source = "gpu": read the GPU once, as the daemon will. False (after saying why) if it
    can't be read."""
    from .memory import GPUMemory
    mem = GPUMemory(cfg.memory)
    asyncio.run(mem.refresh())
    if mem.error:
        print(f"memory: {mem.error}", file=sys.stderr)
        return False
    total = mem.total()
    print(f"memory: GPU {mem.gpu} ({mem.name}, {total['gpu']:.1f} GiB, {mem.available()['gpu']:.1f} free) and "
          f"{total['ram']:.1f} GiB of RAM; reserved {cfg.reserve_gib:g} GiB GPU, {cfg.ram_reserve_gib:g} GiB RAM")
    return True


def main(argv=None):
    ap = argparse.ArgumentParser(prog="spark-switchboard", description="On-demand front door for model services.")
    ap.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    ap.add_argument("--api", default=None, help="admin API URL (default: $SWITCHBOARD_API, else from the config's "
                                                "admin_port, else http://127.0.0.1:8800)")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-c", "--config", default=None, help="main config file (default: $SWITCHBOARD_CONFIG, "
                        "~/.config/spark-switchboard/switchboard.toml, /etc/spark-switchboard/switchboard.toml)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve", parents=[common], help="run the daemon")
    sub.add_parser("check", parents=[common], help="validate the config and list the services")
    p = sub.add_parser("status", parents=[common], help="what is running")
    p.add_argument("--json", action="store_true", help="print the raw /api/status JSON")
    p = sub.add_parser("start", parents=[common], help="start a service")
    p.add_argument("name")
    p.add_argument("--force", action="store_true", help="skip other services' quiet periods (never interrupts work)")
    p.add_argument("--wait", action="store_true", help="wait until it is ready")
    p = sub.add_parser("stop", parents=[common], help="stop a service")
    p.add_argument("name")
    p.add_argument("--kill", action="store_true", help="stop it even if it is working")
    pause_help = ("stop every service switchboard runs and start nothing (no keep-warm, no starts on request) until "
                  "`resume` (or a restart of switchboard, unless the pause persists); work in progress finishes first")
    for cmd, text in (("pause", pause_help), ("stop-all", "the same as pause (an older name for it)")):
        p = sub.add_parser(cmd, parents=[common], help=text, description=pause_help)
        p.add_argument("--kill", action="store_true", help="interrupt work in progress too (only from this machine)")
        p.add_argument("--persist", action=argparse.BooleanOptionalAction, default=None,
                       help="keep the pause across restarts and reboots until `resume` (default: the config's "
                            "pause_persists)")
        p.add_argument("--reason", default=None, help="shown to callers while paused (default: \"cli pause\")")
    sub.add_parser("resume", parents=[common], help="undo pause (a persisting one too): start apps on request and "
                                                    "keep-warm again")
    p = sub.add_parser("log", parents=[common], help="the launcher's output")
    p.add_argument("name")
    p.add_argument("-n", type=int, default=100)
    args = ap.parse_args(argv)

    if args.cmd in ("serve", "check"):
        try:
            cfg = config_mod.load(config_mod.find(args.config))
        except config_mod.ConfigError as e:
            sys.exit(f"config error: {e}")
        if args.cmd == "check":
            print(f"{cfg.path}: admin port {cfg.admin_port}, state in {cfg.state_dir}, hostname {cfg.hostname}")
            gpu = cfg.memory.get("source") == "gpu"
            if gpu and not _check_gpu(cfg):
                sys.exit(1)
            if cfg.pause_persists or not cfg.remote_resume:
                print("pause: " + ", ".join(t for t in ("persists across restarts" if cfg.pause_persists else "",
                                                        "resumed only on this machine" if not cfg.remote_resume else "")
                                            if t))
            for s in cfg.services.values():
                need = f"needs {s.memory_gib:g} GiB (max {s.max_gib:g})"
                if gpu:
                    need = (f"needs {s.memory_gib:g} GiB GPU (max {s.max_gib:g}) + {s.ram_gib:g} GiB RAM "
                            f"(max {s.ram_max_gib:g})")
                print(f"{s.name:<14} {'on ' if s.enabled else 'off'} :{','.join(map(str, s.listen)) or '-':<6} -> "
                      f"{s.upstream:<26} {s.adapter:<8} {need}"
                      f"{' keep-warm' if s.keep_warm else ''}{' idle-stop %gs' % s.idle_stop_s if s.idle_stop_s else ''}")
            return
        from .app import serve
        asyncio.run(serve(cfg))
        return

    try:
        args.api = api_url(args)
    except config_mod.ConfigError as e:
        sys.exit(f"config error: {e}")
    if args.cmd == "status":
        code, st = _api(args, "GET", "/api/status")
        if args.json:
            print(json.dumps(st, indent=1))
            return
        m = st["memory"]
        if m.get("kind") == "gpu":
            print(f"{st['host']}: {m.get('available_gib')} of {m.get('total_gib')} GiB GPU memory free"
                  f"{' (' + m['gpu'] + ')' if m.get('gpu') else ''}, {m['ram'].get('available_gib')} of "
                  f"{m['ram'].get('total_gib')} GiB RAM free" + (f"; {m['error']}" if m.get("error") else ""))
        else:
            print(f"{st['host']}: {m.get('available_gib')} of {m.get('total_gib')} GiB free")
        if st.get("paused"):
            print(f"PAUSED for {_ago(time.time() - st['paused']['since'])} ({st['paused']['reason']}): starting "
                  "nothing; `spark-switchboard resume` to resume"
                  + (" (the pause persists across restarts)" if st["paused"].get("persists") else ""))
        for s in st["services"]:
            print(_line(s))
        return
    if args.cmd == "log":
        code, text = _api(args, "GET", f"/api/services/{args.name}/log?lines={args.n}")
        print(text, end="")
        return
    if args.cmd == "start":
        code, r = _api(args, "POST", f"/api/services/{args.name}/start?reason=cli" + ("&force=1" if args.force else ""))
        if code != 200:
            sys.exit(f"{code}: {r}")
        last = None
        while args.wait:
            code, s = _api(args, "GET", f"/api/services/{args.name}?want=1")
            line = _line(s)
            if line != last:
                print(line, flush=True)
                last = line
            if s["state"] == "ready":
                return
            if s["state"] == "failed" and not s.get("want"):
                sys.exit(1)
            time.sleep(2)
        print(_line(_api(args, "GET", f"/api/services/{args.name}")[1]))
        return
    if args.cmd == "stop":
        code, r = _api(args, "POST", f"/api/services/{args.name}/stop?reason=cli" + ("&kill=1" if args.kill else ""))
        if code != 200:
            sys.exit(r.get("error") if isinstance(r, dict) else r)
        print(f"stopping {args.name}")
        return
    if args.cmd in ("pause", "stop-all"):
        query = {"reason": args.reason or f"cli {args.cmd}"}
        if args.kill:
            query["kill"] = "1"
        if args.persist is not None:
            query["persist"] = "1" if args.persist else "0"
        code, r = _api(args, "POST", "/api/pause?" + urllib.parse.urlencode(query, quote_via=urllib.parse.quote))
        if code != 200:
            sys.exit(r.get("error") if isinstance(r, dict) else f"{code}: {r}")
        for name, what in r["services"].items():
            print(f"{name}: {what}")
        if (r.get("paused") or {}).get("persists"):
            print("switchboard is paused: it starts nothing until `spark-switchboard resume`, also across restarts "
                  "and reboots")
        else:
            print("switchboard is paused: it starts nothing until `spark-switchboard resume` (or a restart of "
                  "switchboard)")
        return
    if args.cmd == "resume":
        code, r = _api(args, "POST", "/api/resume?reason=cli")
        if code != 200:
            sys.exit(r.get("error") if isinstance(r, dict) else f"{code}: {r}")
        print("resumed" if r.get("resumed") else "switchboard was not paused")
        return


if __name__ == "__main__":
    main()
