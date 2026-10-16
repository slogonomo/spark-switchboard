"""Configuration: one main TOML file plus one TOML file per service.

The main file holds the daemon's own settings and an `include` list of service files. A service file says how to
run the service (its launcher), where it listens once it runs (upstream), which public port switchboard answers on
for it, which protocol adapter shapes the answers while it is not running, and how much memory it needs.

The main file is found by `find()`: an explicit path (-c), then $SWITCHBOARD_CONFIG, then
$XDG_CONFIG_HOME/spark-switchboard/switchboard.toml (XDG_CONFIG_HOME defaults to ~/.config), then
/etc/spark-switchboard/switchboard.toml.
"""
import glob
import os
import socket
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_ADMIN_PORT = 8800
SYSTEM_CONFIG = Path("/etc/spark-switchboard/switchboard.toml")


class ConfigError(Exception):
    pass


def expand(p):
    return os.path.expandvars(os.path.expanduser(str(p)))


def user_config_path(environ=None):
    env = os.environ if environ is None else environ
    base = env.get("XDG_CONFIG_HOME") or os.path.join(env.get("HOME") or os.path.expanduser("~"), ".config")
    return Path(base) / "spark-switchboard" / "switchboard.toml"


def find(explicit=None, environ=None):
    """The main config file to use. An explicit path (-c) or $SWITCHBOARD_CONFIG must exist; otherwise the first
    existing of the per-user and the system-wide file. Raises ConfigError naming every place it looked."""
    env = os.environ if environ is None else environ
    if explicit:
        p = Path(expand(explicit))
        if p.is_file():
            return p
        raise ConfigError(f"config file {p} (given with -c) does not exist")
    if env.get("SWITCHBOARD_CONFIG"):
        p = Path(expand(env["SWITCHBOARD_CONFIG"]))
        if p.is_file():
            return p
        raise ConfigError(f"config file {p} (from $SWITCHBOARD_CONFIG) does not exist")
    tried = [user_config_path(env), SYSTEM_CONFIG]
    for p in tried:
        if p.is_file():
            return p
    raise ConfigError("no config file found. Looked for: -c PATH (not given), $SWITCHBOARD_CONFIG (not set), "
                      + ", ".join(str(p) for p in tried)
                      + ". Copy examples/switchboard.toml and examples/services/ to the first of these, or run "
                      "deploy/install.sh, which does that.")


def admin_url(path):
    """The admin API's base URL for a main config file: http://127.0.0.1:<admin_port>, or the `bind` address when
    the daemon is bound to one specific address. Reads only the main file (no service files)."""
    try:
        raw = tomllib.loads(Path(path).read_text())
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise ConfigError(f"{path}: {e}") from None
    port = raw.get("admin_port", DEFAULT_ADMIN_PORT)
    host = raw.get("bind") or ""
    if host in ("", "0.0.0.0", "::"):
        host = "127.0.0.1"
    if ":" in host:
        host = f"[{host}]"
    return f"http://{host}:{port}"


@dataclass
class ServiceConfig:
    name: str
    title: str
    adapter: str                      # protocol adapter: web, openai, comfyui (spark_switchboard/adapters)
    upstream: str                     # base URL of the running service, e.g. http://127.0.0.1:18188
    listen: list[int]                 # public ports switchboard answers on for this service
    command: list[str]                # the launcher; runs in the foreground until stopped (SIGTERM)
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    memory_gib: float = 0             # free memory it needs to start (MemAvailable; GPU memory with source "gpu")
    max_gib: float = 0                # the most it grows to while running (default: memory_gib)
    ram_gib: float = 0                # source "gpu" only: free host RAM it needs to start (CPU offload)
    ram_max_gib: float = 0            # ... and the most host RAM it grows to (default: ram_gib)
    priority: int = 0                 # when memory is needed, lower priority services are stopped first
    idle_grace_s: float = 120         # not stopped for another service until unused this long (the minimum; see session_*)
    session_window_s: float = 900     # calls in this window before the last one decide whether a session is going on
    session_min_calls: int = 3        # this many separate uses in the window = a session (a person chatting, an agent)
    session_burst_s: float = 20       # uses closer together than this are one use (a page load's requests, an agent's turn)
    session_factor: float = 3         # during a session, quiet needed = factor x the longest gap between its calls
    session_grace_max_s: float = 900  # ... capped here (and never below idle_grace_s)
    idle_stop_s: float = 0            # stopped after unused this long (0: never)
    keep_warm: bool = False           # started whenever it fits without stopping anything
    hold_s: float = 120               # how long a request may wait for the service to become ready
    start_estimate_s: float = 60      # until switchboard has measured starts itself
    start_timeout_s: float = 900
    stop_timeout_s: float = 60        # SIGTERM to the launcher, then SIGKILL to its process group
    kill_command: list[str] | None = None   # run after a SIGKILL (e.g. remove a container the launcher started)
    ready_path: str = "/"             # GET upstream + ready_path answering 2xx = ready
    pidfile: str | None = None        # where the launcher writes its pid (re-adopted after a switchboard restart)
    containers: list[str] = field(default_factory=list)   # docker container names (globs) owned by the service
    status_file: str | None = None    # JSON {"detail": ...} the launcher keeps current while starting
    passive_paths: list[str] = field(default_factory=list)  # requests that never start the service
    busy_probe: dict | None = None    # how to ask the service whether it is working (see adapters/base.py)
    options: dict = field(default_factory=dict)  # adapter options
    info_html: str = ""               # shown on the service's status page
    enabled: bool = True


@dataclass
class Config:
    path: Path
    bind: str | None = None           # None, "" or "::": every interface, IPv4 and IPv6
    admin_port: int = DEFAULT_ADMIN_PORT
    state_dir: Path = Path("~/.local/state/spark-switchboard")
    hostname: str = ""                # "": socket.gethostname()
    reserve_gib: float = 0            # memory kept free on top of each service's own memory_gib
    ram_reserve_gib: float = 0        # source "gpu" only: host RAM kept free on top of each service's ram_gib
    want_ttl_s: float = 90            # a start nobody waits for any more is dropped after this long
    use_event_after_s: float = 900    # the first use after this long unused goes in the event log (0: never)
    pause_persists: bool = False      # a pause is kept in state_dir across restarts until resumed (the default for
                                      # `pause`; `pause --persist` / `--no-persist` choose per pause)
    remote_resume: bool = True        # False: only this machine itself may resume a pause
    memory: dict = field(default_factory=lambda: {"source": "system"})
    services: dict[str, ServiceConfig] = field(default_factory=dict)


MAIN_KEYS = set(Config.__dataclass_fields__) - {"path", "services"}
SERVICE_KEYS = set(ServiceConfig.__dataclass_fields__) - {"name"}
RUN_KEYS = {"command", "env", "cwd", "stop_timeout_s", "kill_command"}
READY_KEYS = {"path": "ready_path", "estimate_s": "start_estimate_s", "timeout_s": "start_timeout_s"}
MEMORY_KEYS = {"source", "free_floor_gib", "reclaim_command", "gpu", "total_gib", "ram_total_gib", "dir"}
MEMORY_SOURCES = ("system", "gpu", "fake")
BUSY_PROBE_KEYS = {"path", "nonempty", "list", "field", "idle_values"}


def _table(value, where):
    if not isinstance(value, dict):
        raise ConfigError(f"{where} must be a table")
    return value


def _check_memory(m, origin):
    m = _table(m, f"{origin}: [memory]")
    if set(m) - MEMORY_KEYS:
        raise ConfigError(f"{origin}: unknown keys in [memory]: {sorted(set(m) - MEMORY_KEYS)}")
    source = m.get("source", "system")
    if source not in MEMORY_SOURCES:
        raise ConfigError(f"{origin}: [memory] source must be \"system\" (unified memory, as on a DGX Spark), \"gpu\" "
                          f"(one discrete NVIDIA GPU) or \"fake\" (for the tests), not {source!r}")
    if source == "fake" and not {"total_gib", "dir"} <= set(m):
        raise ConfigError(f"{origin}: [memory] source = \"fake\" needs total_gib and dir")
    if "gpu" in m and source != "gpu":
        raise ConfigError(f"{origin}: [memory] gpu only applies with source = \"gpu\"")
    if not isinstance(m.get("gpu", 0), (int, str)) or isinstance(m.get("gpu"), bool):
        raise ConfigError(f"{origin}: [memory] gpu must be an nvidia-smi index (0) or a GPU UUID (\"GPU-...\")")


def has_ram_pool(memory):
    """Whether host RAM is planned separately from the main pool (a discrete GPU), so ram_gib etc. apply."""
    return memory.get("source", "system") == "gpu" or (memory.get("source") == "fake" and "ram_total_gib" in memory)


def _check_busy_probe(p, origin):
    p = _table(p, f"{origin}: [busy_probe]")
    if set(p) - BUSY_PROBE_KEYS:
        raise ConfigError(f"{origin}: unknown keys in [busy_probe]: {sorted(set(p) - BUSY_PROBE_KEYS)}")
    if not isinstance(p.get("path"), str):
        raise ConfigError(f"{origin}: [busy_probe] needs path")
    if ("nonempty" in p) == ("list" in p):
        raise ConfigError(f"{origin}: [busy_probe] needs either nonempty or list (with field and idle_values)")
    if "list" in p and "field" not in p:
        raise ConfigError(f"{origin}: [busy_probe] with list needs field")


def _check_adapter(d, origin):
    from .adapters import ADAPTERS            # (imports aiohttp; only needed here)
    adapter = ADAPTERS.get(d["adapter"])
    if adapter is None:
        raise ConfigError(f"{origin}: unknown adapter {d['adapter']!r} (known: {', '.join(ADAPTERS)})")
    unknown = set(_table(d.get("options", {}), f"{origin}: [options]")) - adapter.OPTIONS
    if unknown:
        raise ConfigError(f"{origin}: unknown keys in [options] for adapter {d['adapter']!r}: {sorted(unknown)} "
                          f"(known: {', '.join(sorted(adapter.OPTIONS))})")


def _service(name, d, origin):
    unknown = set(d) - SERVICE_KEYS - {"run", "ready"}
    if unknown:
        raise ConfigError(f"{origin}: unknown keys {sorted(unknown)}")
    d = dict(d)
    run = d.pop("run", {})          # [run] command/env/cwd/stop_timeout_s/kill_command, for readability
    ready = d.pop("ready", {})      # [ready] path/estimate_s/timeout_s
    if set(run) - RUN_KEYS:
        raise ConfigError(f"{origin}: unknown keys in [run]: {sorted(set(run) - RUN_KEYS)}")
    if set(ready) - set(READY_KEYS):
        raise ConfigError(f"{origin}: unknown keys in [ready]: {sorted(set(ready) - set(READY_KEYS))}")
    for k in RUN_KEYS:
        if k in run:
            d[k] = run[k]
    for k, attr in READY_KEYS.items():
        if k in ready:
            d[attr] = ready[k]
    for k in ("title", "adapter", "upstream", "command"):
        if k not in d:
            raise ConfigError(f"{origin}: service {name!r} needs {k!r}")
    _check_adapter(d, origin)
    if d.get("busy_probe") is not None:
        _check_busy_probe(d["busy_probe"], origin)
    if isinstance(d.get("listen"), int):
        d["listen"] = [d["listen"]]
    d.setdefault("listen", [])
    if isinstance(d["command"], str):
        d["command"] = ["bash", "-c", d["command"]]
    d["command"] = [expand(c) for c in d["command"]]
    d["env"] = {k: expand(v) for k, v in d.get("env", {}).items()}
    for k in ("cwd", "pidfile", "status_file"):
        if d.get(k):
            d[k] = expand(d[k])
    if d.get("kill_command"):
        d["kill_command"] = [expand(c) for c in d["kill_command"]]
    d["upstream"] = d["upstream"].rstrip("/")
    s = ServiceConfig(name=name, **d)
    if not s.max_gib:
        s.max_gib = s.memory_gib
    if not s.ram_max_gib:
        s.ram_max_gib = s.ram_gib
    return s


def load(path):
    path = Path(expand(path)).resolve()
    try:
        raw = tomllib.loads(path.read_text())
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise ConfigError(f"{path}: {e}") from None
    includes = raw.pop("include", [])
    inline = raw.pop("service", {})
    unknown = set(raw) - MAIN_KEYS
    if unknown:
        raise ConfigError(f"{path}: unknown keys {sorted(unknown)}")
    cfg = Config(path=path, **raw)
    _check_memory(cfg.memory, path)
    cfg.state_dir = Path(expand(cfg.state_dir))
    cfg.hostname = cfg.hostname or socket.gethostname()
    found = []
    for pattern in includes:
        p = Path(expand(pattern))
        p = p if p.is_absolute() else path.parent / p
        found += sorted(glob.glob(str(p)))
    for f in found:
        try:
            d = tomllib.loads(Path(f).read_text())
        except tomllib.TOMLDecodeError as e:
            raise ConfigError(f"{f}: {e}") from None
        name = d.pop("name", Path(f).stem)
        _add(cfg, _service(name, d, f))
    for name, d in inline.items():
        _add(cfg, _service(name, d, path))
    if not has_ram_pool(cfg.memory):
        why = ("with [memory] source = \"gpu\" (a discrete GPU); on unified memory, memory_gib and max_gib cover "
               "RAM too")
        if cfg.ram_reserve_gib:
            raise ConfigError(f"{path}: ram_reserve_gib only applies {why}")
        for s in cfg.services.values():
            if s.ram_gib or s.ram_max_gib:
                raise ConfigError(f"service {s.name!r}: ram_gib and ram_max_gib only apply {why}")
    ports = {}
    for s in cfg.services.values():
        if not s.enabled:
            continue
        for p in s.listen:
            if p in ports:
                raise ConfigError(f"port {p} is claimed by both {ports[p]} and {s.name}")
            if p == cfg.admin_port:
                raise ConfigError(f"port {p} of {s.name} is the admin port")
            ports[p] = s.name
    return cfg


def _add(cfg, s):
    if s.name in cfg.services:
        raise ConfigError(f"service {s.name!r} is defined twice")
    cfg.services[s.name] = s
