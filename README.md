# spark-switchboard

An on-demand front door for the model services on one NVIDIA DGX Spark. It answers on every app's public port all
the time and starts the app behind it when someone actually uses it. If memory is needed, it first stops apps that
nobody is using. Once an app is up, traffic passes straight through, including streams and websockets.

- **For:** one DGX Spark (GB10) running several large apps that don't fit in its memory together.
- **Needs:** Linux with systemd, Python 3.11 or newer, and the apps themselves, installed separately, each with a
  launcher script. Examples are included for ComfyUI, Open WebUI, MiaAI-Lab's TensorFold recipe and
  [Sol-H3 Studio](https://github.com/slogonomo/sol-h3-studio).

Unofficial. Not affiliated with or endorsed by NVIDIA.

```
 browser / agent / script / Open WebUI
          │  :8888 :8188 :8190 :12000 (public ports)            :8800 dashboard + admin API
          ▼
 ┌────────────────────── spark-switchboard ─────────────────────┐
 │ proxy:      classify each request (one adapter per protocol)  │
 │ scheduler:  requests, memory plan, quiet periods, keep-warm   │
 │ supervisor: launchers, readiness, re-adoption after restarts  │
 └──────┬──────────────────┬──────────────────┬──────────────────┘
        │ 127.0.0.1:18888  │ :18188           │ :18190 ...   (internal ports)
        ▼                  ▼                  ▼
   tensorfold.sh       comfyui.sh       sol-h3-studio      ← each app's own launcher
```

The apps stay independent. switchboard runs each app's launcher with `SWITCHBOARD=1` and an internal port taken from
the environment. Without switchboard, the same launchers run the apps on their own, from NVIDIA Sync or by hand.

## Contents

- [Why](#why)
- [Scope and requirements](#scope-and-requirements)
- [Quick start](#quick-start)
- [What clients see](#what-clients-see)
- [When a switch happens](#when-a-switch-happens)
- [Memory](#memory)
- [Writing a launcher](#writing-a-launcher)
- [Configuration reference](#configuration-reference)
- [NVIDIA Sync](#nvidia-sync)
- [Dashboard, CLI and admin API](#dashboard-cli-and-admin-api)
- [Operations](#operations)
- [Security](#security)
- [Example: four apps on one DGX Spark](#example-four-apps-on-one-dgx-spark)
- [Limits](#limits)
- [Tests](#tests)
- [Prior art and credits](#prior-art-and-credits)
- [License and trademarks](#license-and-trademarks)

## Why

A DGX Spark has 128 GB of unified memory (Linux reports about 121 GiB as MemTotal). CPU and GPU share it. A large
LLM server, a ComfyUI video workflow and a video pipeline each want most of it, so they can't run together. Without a
front door, switching means stopping one app (in NVIDIA Sync or a terminal) before starting the next, and clients
see connection errors while the wrong app is up.

With spark-switchboard, `http://spark-xxxx.local:8188` (spark-xxxx being your Spark's hostname) is always ComfyUI: it
is there, or a status page says what is happening and when it will be ready. An agent calls
`http://spark-xxxx.local:8888/v1/...` and gets its answer, after a slow first token if the model had to be loaded.

## Scope and requirements

Built for and tested on one DGX Spark (GB10). It assumes **unified memory**: MemAvailable is what an app can still
get, CPU or GPU. Other GB10 machines should behave the same but are untested. On a machine with a discrete GPU,
MemAvailable doesn't describe GPU memory, so the memory plan would be wrong there.

- Linux with systemd. Tested on DGX OS (Ubuntu 24.04, aarch64).
- sudo rights, to install the systemd unit (and the optional page-cache rule). The service itself runs as you.
- Python 3.11 or newer with `venv` (DGX OS has 3.12; `sudo apt install python3-venv` if `python3 -m venv` fails).
- aiohttp 3.9 or newer (installed by pip; tested with 3.14.4). No GPU Python libraries.
- Optional: `nvidia-smi` (per-process GPU memory; without it only resident memory is counted), the docker CLI (for
  apps that run in containers), and a sudoers rule for the page-cache fix ([Memory](#memory)).
- The example launchers use bash, curl, python3, `ss` (iproute2) and `flock` (util-linux), all present on DGX OS.

## Quick start

1. Clone the repository and run the installer once:

   ```bash
   git clone https://github.com/slogonomo/spark-switchboard.git ~/src/spark-switchboard
   cd ~/src/spark-switchboard
   deploy/install.sh
   ```

   This first run creates `.venv`, installs the package, finds no config, copies `examples/switchboard.toml` and
   `examples/services/*.toml` to `~/.config/spark-switchboard/`, and stops.
2. Optional: [try it first](#try-it-first) without systemd or any GPU app.
3. Edit the copied files. Each example service ships with `enabled = false`; set it to `true` for each app you have
   installed and adjust its paths.
4. Stop the apps you started by hand or from NVIDIA Sync (for the TensorFold recipe: its `./stop.sh`). switchboard
   never stops an app it didn't start, and can't take an app's port while something else holds it.
5. Check the config, then install and start the service. This run uses sudo for the systemd unit:

   ```bash
   .venv/bin/spark-switchboard check      # validates the config and lists the services
   deploy/install.sh                      # installs and starts spark-switchboard.service
   ```

Open the dashboard at `http://spark-xxxx.local:8800/` (install.sh prints the URL). Add `--with-drop-caches` to
install the optional sudoers rule for the GB10 page-cache fix (see [Memory](#memory)); `--dry-run` shows what
install.sh would do.

**Config changes** take effect after `sudo systemctl restart spark-switchboard` (or rerunning `deploy/install.sh`).
Running apps keep running; the new process re-adopts them.

### Try it first

This runs the example launcher behind a throwaway config, without systemd or any GPU app. It needs `.venv` from the
first install.sh run (or `python3 -m venv .venv && .venv/bin/pip install .` in the repo), and uses admin port 8801 so
it doesn't clash with an installed switchboard on 8800.

```bash
mkdir -p /tmp/sb-try
```

```toml
# /tmp/sb-try/switchboard.toml
admin_port = 8801
state_dir = "/tmp/sb-try/state"

[service.example-app]
title = "Example app"
adapter = "web"
listen = 8000
upstream = "http://127.0.0.1:18000"
command = ["bash", "~/src/spark-switchboard/examples/launchers/example-app.sh"]   # your clone's path
env = { EXAMPLE_PORT = "18000", EXAMPLE_HOST = "127.0.0.1", EXAMPLE_DELAY_S = "5", LAUNCHER_RUN_DIR = "/tmp/sb-try/run" }
pidfile = "/tmp/sb-try/run/example-app.pid"
ready_path = "/"
```

```bash
.venv/bin/spark-switchboard serve -c /tmp/sb-try/switchboard.toml
```

Open `http://spark-xxxx.local:8000/`: a status page for about 5 s, then the example page. The dashboard on :8801
shows the start. switchboard leaves running apps alone when it exits ([Operations](#operations)), so stop the app
before pressing Ctrl-C, from a second terminal in the repo:
`.venv/bin/spark-switchboard stop example-app -c /tmp/sb-try/switchboard.toml`.

## What clients see

Every request on a public port gets one of four meanings (`spark_switchboard/adapters/base.py`):

| Kind | What it is | App stopped | App starting |
| --- | --- | --- | --- |
| **navigate** | a person opening the app (GET, `text/html`) | starts it; **status page** (503 + `Retry-After`) that reloads into the app when ready | status page |
| **wake** | deliberate use: any POST/PUT/PATCH/DELETE, or a GET from a program (no `Origin`/`Referer`: curl, SDKs, MCP servers) | starts it and **holds the request** until ready (up to `hold_s`), then passes it through | held |
| **tab** | background traffic from a page already open: polling, websocket reconnects (they carry `Origin`/`Referer`) | **503, never starts it**: a tab left open can't bring an app back | held while it is actually starting |
| **passive** | probes: `passive_paths`, HEAD, OPTIONS | 503, or a synthetic answer | same |

Per protocol (`adapter = ...` in the service file):

- **`openai`**, OpenAI-compatible API servers (TensorFold, vLLM, llama.cpp). `GET /v1/models` (and
  `/v1/models/{id}`) always answers, even while the server is stopped: it returns the last list the server gave, or
  `options.models`; the list carries an `X-Switchboard-State` header with the server's state. So Open WebUI and
  agent harnesses keep seeing the model.
  `/health` and `/v1/health` answer `{"status": "ok", "backend": "<state>", "loads_on_demand": true}` while it is
  stopped (`false` while switchboard is [paused](#dashboard-cli-and-admin-api)); `/metrics` never starts it. A
  completion request loads the model and waits for it. If the model can't be ready in time, the client gets OpenAI's
  error shape with HTTP 503 and `Retry-After`; the official SDKs retry that by themselves. In a browser, `/` is
  always switchboard's status page, with connection details from `info_html`.
- **`comfyui`**. Opening the UI starts it. An API client's `POST /prompt` is held, then queued as usual. Errors on
  `/prompt` use ComfyUI's own error shape. It counts as busy while anything is in its queue (`GET /queue`).
- **`web`**, any other web app: the status page, holding, and the tab rule. A `[busy_probe]` teaches switchboard how
  to see a running job.

Other answers while an app isn't ready: a JSON client gets
`{"error": {"type": "service_unavailable", "code": ..., "message": ...}, "switchboard": {"service", "state", "retry_after_s"}}`;
anything else gets the message as plain text. The `code` is one of `loading`, `waiting` (for apps in use),
`no_memory`, `failed`, `stopping`, `stopped`, `paused` (switchboard is paused and starts nothing; `Retry-After: 60`),
or `upstream_down` (HTTP 502: the app stopped answering).

A request can send `X-Switchboard-Wait: <seconds>` (0 to 3600) to choose its own hold time; `0` means answer at once.
Requests passed through get `X-Forwarded-For`, `X-Forwarded-Host` and `X-Forwarded-Proto`.

## When a switch happens

A request for an app that doesn't fit becomes a *want*. The scheduler works through the wants in order:

1. **Fits now:** start it.
2. **Stopping idle apps would make room:** stop them (lowest `priority` first, then least recently used), wait up to
   20 s for their memory to come back, then start it.
3. **Only apps in use are in the way:** wait for them. The status page names each one and what it is doing.
4. **Even stopping everything wouldn't fit:** wait, and say so (usually memory held by something switchboard doesn't
   run).

An app counts as **in use** in two ways:

- **Work in progress:** a request in flight, or a job its busy probe reports (ComfyUI's queue, a web app's job list).
  This is **never interrupted**, not even by "Switch now". A switch waits for it to finish.
- **The quiet period after use:** `idle_grace_s` after the last use (for example 2 min for an LLM, 5 min for ComfyUI).
  If recent uses look like a **session** (3 or more separate uses within 15 min: `session_min_calls`,
  `session_window_s`), the quiet period stretches to 3 times the longest gap between them (`session_factor`), capped
  at 15 min (`session_grace_max_s`). So a person reading an answer before typing the next prompt, or an agent running
  tests between calls, isn't mistaken for having left. Uses less than `session_burst_s` (20 s) apart count as one: a
  page load's burst of requests, or an agent's quick turns.

**"Switch now"** on the status page skips only the quiet period. It says exactly what it will stop, and it never
interrupts work. No request header forces a switch. The admin API's `stop?kill=1`, which does interrupt work, is
accepted only from the machine itself (`spark-switchboard stop NAME --kill`).

Only real actions count as **use**: opening the app in a browser, and requests that do something (POST and the other
writes). Reads don't count, because they are often a client idling: a script polling `/history`, a monitor checking
health, a tab reconnecting. Tested with the real ComfyUI frontend on a DGX Spark: an idle tab made zero HTTP requests
in 2.5 min, and with ComfyUI stopped, its 40 websocket reconnects in 75 s were all turned away without restarting it.
The dashboard lists, per app, the requests that counted as use.

A want that nobody waits for any more lapses after `want_ttl_s` (90 s): no held request and no open status page. So a
one-off request can't trigger a switch an hour later.

**Keep warm and idle stop.** A `keep_warm` app starts whenever it fits without stopping anything (and nothing else is
wanted, starting or stopping, and switchboard isn't [paused](#dashboard-cli-and-admin-api)). An `idle_stop_s` app stops after that long unused and not busy, which lets the
keep-warm app come back. A keep-warm app whose start failed waits 10 min before the next try.

**Failed starts.** The status page shows why (from the launcher's output) with a Retry button. For 60 s after a
failure, requests get that failure instead of starting it again.

## Memory

For each app the service file gives:

- `memory_gib`: the free memory it needs to start. Use the same threshold its launcher checks.
- `max_gib`: the most it grows to while running.

What each running app holds is **measured**: GPU allocations per process from `nvidia-smi`, plus the anonymous and
shared resident memory (RssAnon + RssShmem) of its launcher's process tree and of the docker containers it names in
`containers`. File-backed pages are left out, because they are page cache.

"Free" for planning is MemAvailable minus `reserve_gib`, minus the headroom other running apps may still grow into
(`max_gib` minus what each holds now). An app starts when free ≥ its `memory_gib`. Stopping an app frees
max(`max_gib`, what it holds).

**The GB10 page-cache quirk.** The GPU driver takes its own buffers, such as a new CUDA context, from truly free pages
(MemFree), and doesn't wait for the kernel to evict page cache. With the cache full, a start can fail with
`NV_ERR_NO_MEMORY` (in dmesg) and `CUDA_ERROR_OUT_OF_MEMORY` at `cuDevicePrimaryCtxRetain` although MemAvailable is
ample. Observed on a DGX Spark when an LLM server started right after a video pipeline: MemFree was about 3 GiB with
114 GiB of page cache, MemAvailable 117 GiB. After dropping the clean page cache, the same start succeeded (72 s).

The fix, optional, needs root for one command:

1. `deploy/install.sh --with-drop-caches` installs `/etc/sudoers.d/spark-switchboard` (checked with `visudo -cf`
   first), which lets your user run exactly `sudo -n /usr/bin/tee /proc/sys/vm/drop_caches` without a password.
2. Uncomment in `switchboard.toml`:

   ```toml
   [memory]
   free_floor_gib = 20
   reclaim_command = ["bash", "-c", "sync; echo 1 | sudo -n /usr/bin/tee /proc/sys/vm/drop_caches >/dev/null"]
   ```

3. Restart switchboard to apply it: `sudo systemctl restart spark-switchboard`.

Before each start, if MemFree is under `free_floor_gib`, switchboard runs `reclaim_command` and logs the result.
`echo 1` drops only the clean page cache. Without the sudoers rule, `sudo -n` fails at once (it never prompts) and the
event log says so. The example TensorFold launcher does the same check itself with a higher floor
(`TF_MIN_MEMFREE_GIB`, 95), because it reads ~71 GiB of checkpoint into the page cache before the engine creates its
CUDA context.

Admission is per start. An app that grows later (ComfyUI loading models for a job, an Ollama model in Open WebUI) is
counted by its `max_gib` headroom and measured usage; the launchers' memory guards are the backstop.

## Writing a launcher

A launcher is a script that runs one app in the foreground. The contract:

1. **Foreground until SIGTERM.** On SIGTERM, clean up (stop the container, kill the server) and exit. After
   `stop_timeout_s`, switchboard sends SIGKILL to the launcher's process group, then runs `kill_command`.
2. **Listen where the environment says.** switchboard passes the internal port and address through the service's
   `env` (for example `COMFY_PORT=18188`, `COMFY_LISTEN=127.0.0.1`); the public port is switchboard's.
3. **Pidfile.** Write the launcher's own pid to the service's `pidfile`. After a switchboard restart, the launcher is
   re-adopted through it if `/proc/<pid>/cmdline` contains the file name of the last element of `command` (so make
   the script the last element). A launcher without `SWITCHBOARD=1` in its environment (started by hand or from
   NVIDIA Sync) is treated as external: passed through and never stopped, not even when it doesn't answer within
   `start_timeout_s`.
4. **Don't exit when re-parented while `SWITCHBOARD` is set.** switchboard starts launchers in their own session and
   leaves them running when it restarts or upgrades. (Exiting on re-parent is right for NVIDIA Sync, where it means
   the app was closed.)
5. **Report progress** as lines `>>> STATUS: <text>` on stdout. The latest one is shown on the status page while the
   app starts. Alternatively, keep a JSON file `{"detail": "<text>"}` current and name it in `status_file`.
6. **Fail clearly.** On a failed start, print a STATUS line saying why, with `REFUSED` or `ERROR` in it, and exit
   non-zero. The status page shows the last such line (or else the last line containing "error"), plus the tail of
   the launcher's output.

switchboard runs the launcher with its own environment plus the service's `env`, `SWITCHBOARD=1` and
`SWITCHBOARD_SERVICE=<name>`, with stdin from /dev/null and stdout/stderr appended to
`<state_dir>/logs/<name>.log`.

[`examples/launchers/common.sh`](examples/launchers/common.sh) implements the contract for bash launchers:

| Function | What it does |
| --- | --- |
| `log`, `say_status`, `fail` | log lines; `>>> STATUS:` lines; `fail` prints `STATUS: ERROR: ...` and exits 1 |
| `install_traps`, `cleanup_hook` | INT/TERM/HUP/QUIT run your `cleanup_hook` once, then exit; SIGPIPE is ignored |
| `claim_pidfile` | writes `$LAUNCHER_RUN_DIR/$APP.pid`; asks a previous launcher of the same app to hand over |
| `require_port_free PORT` | refuses if something listens there, naming it when it is yours (no sudo) |
| `require_memory GIB` | refuses unless MemAvailable ≥ GIB, naming the largest users; waits for another launcher that is still stopping |
| `start_mem_guard GIB FUNC` | runs FUNC when MemAvailable stays under GIB for 3 s (on GB10, exhausting memory can freeze the machine) |
| `wait_http URL [S] [PID]` | waits for a 2xx, giving up after S seconds or when PID exits |
| `keepalive [FUNC]` | stays in the foreground; exits on re-parent only when `SWITCHBOARD` is unset; exits 1 after FUNC fails 4 times |
| `drop_caches` | drops the clean page cache through the optional sudoers rule; never prompts |
| `whole_gib VALUE` | a GiB setting as a whole number, rounded up (`7.5` → 8), for bash arithmetic; returns 1 for anything that isn't a number. `require_memory` and `start_mem_guard` use it, so fractional values work there and invalid ones fail clearly |
| `docker` | falls back to `sg docker` when a long-lived login predates your docker group membership (NVIDIA Sync); exported to child scripts |
| the delegate | `switchboard_delegate` runs when common.sh is sourced (unless `SWITCHBOARD` is set or `SWITCHBOARD_DELEGATE=0`); `switchboard_follow API NAME` is the part delegate.sh uses; see [NVIDIA Sync](#nvidia-sync) |

Its settings (environment): `LAUNCHER_RUN_DIR` (default `~/.local/state/spark-switchboard/run`), `LAUNCHER_LOG_DIR`
(`~/.local/state/spark-switchboard/launchers`; app server logs, and the launcher's own log when switchboard isn't
running it), `LAUNCHER_POLL_S` (5), `LAUNCHER_STOP_WAIT_S` (150), and `SWITCHBOARD_API`, `SWITCHBOARD_SERVICE`,
`SWITCHBOARD_DELEGATE` for the delegate.

[`examples/launchers/example-app.sh`](examples/launchers/example-app.sh) is the smallest complete launcher (it serves
a directory with `python3 -m http.server`). To start your own, copy it into `examples/launchers/` (next to
`common.sh`), or anywhere else after changing its `source` line to the path of `common.sh`. The other examples:

| Launcher | Runs | Main settings |
| --- | --- | --- |
| `open-webui.sh` | Open WebUI in docker; recreates the container when its settings change (data volume kept). An existing container named `open-webui` is replaced on the first start: set `OWUI_DATA_VOLUME` (and `OWUI_IMAGE`, `OWUI_OLLAMA_DIR`) to match it first, or `OWUI_CONTAINER` to a new name | `OWUI_CONTAINER`, `OWUI_IMAGE`, `OWUI_PORT`, `OWUI_BIND`, `OWUI_DATA_VOLUME`, `OWUI_OPENAI_URLS`, `OWUI_OLLAMA_URL`, `OWUI_OLLAMA_DIR`, `OWUI_MIN_FREE_GIB` |
| `comfyui.sh` | ComfyUI's `main.py` from a local checkout and venv; memory guard interrupts the job and unloads models; sets `TRITON_PTXAS_PATH` to CUDA 13's ptxas for sm_121 | `COMFYUI_DIR`, `COMFYUI_PYTHON`, `COMFY_PORT`, `COMFY_LISTEN`, `COMFY_EXTRA_ARGS`, `COMFY_MIN_FREE_GIB`, `COMFY_MEM_FLOOR_GIB` |
| `delegate.sh SERVICE` | nothing itself: asks a running switchboard to start SERVICE and follows it ([NVIDIA Sync](#nvidia-sync)) | `SWITCHBOARD_API` |
| `tensorfold.sh` | MiaAI-Lab's TensorFold recipe through its own `start.sh` / `stop.sh`; optional checkpoint pre-read (load 93–98 s → 54–56 s, measured on a DGX Spark), page-cache drop when MemFree is low, fast stop while loading | `TF_RECIPE_DIR`, `TF_HOST`, `TF_PORT`, `TF_CONTAINER`, `TF_SERVED_NAME`, `TF_MIN_FREE_GIB`, `TF_PREREAD`, `TF_PREREAD_TABLE`, `TF_MIN_MEMFREE_GIB`, `TF_MEM_FLOOR_GIB` |

Each file's header lists every setting with its default. A launcher doesn't have to use common.sh or bash; the
companion [sol-h3-studio](https://github.com/slogonomo/sol-h3-studio) repository ships its own launcher that
follows the same contract.

## Configuration reference

The main file is found in this order: `-c PATH`; `$SWITCHBOARD_CONFIG`; `$XDG_CONFIG_HOME/spark-switchboard/switchboard.toml`
(`XDG_CONFIG_HOME` defaults to `~/.config`); `/etc/spark-switchboard/switchboard.toml`. A named file (`-c` or the
variable) must exist. Unknown keys are errors, in every table (`[memory]`, `[run]`, `[ready]`, `[busy_probe]` and
`[options]` too), and so is an unknown `adapter`: `spark-switchboard check` catches typos. `~` and `$VARS` are
expanded in `state_dir`, `include` patterns, `command`, `env` values, `cwd`, `pidfile`, `status_file` and
`kill_command`.

### Main file

| Key | Default | Meaning |
| --- | --- | --- |
| `admin_port` | `8800` | dashboard and admin API |
| `bind` | unset | address for the admin port and every public port; unset, `""` or `"::"` = every interface, IPv4 and IPv6 |
| `hostname` | `""` | the name clients use to reach the machine (`spark-xxxx.local`, a VPN name, an IP); used in messages to API clients and page titles; `""` = the system hostname |
| `state_dir` | `~/.local/state/spark-switchboard` | logs, events, measured timings |
| `reserve_gib` | `0` | memory kept free on top of each app's `memory_gib` |
| `want_ttl_s` | `90` | a want nobody waits for any more lapses after this |
| `use_event_after_s` | `900` | the first use of an app after this long unused goes to the event log; `0` = never |
| `include` | `[]` | service file globs, relative to the main file |
| `[memory] source` | `"system"` | `"system"`: /proc/meminfo plus measured usage. (`"fake"`, with `total_gib` and `dir`, is for the tests.) |
| `[memory] free_floor_gib` | `0` | run `reclaim_command` before a start when MemFree is under this |
| `[memory] reclaim_command` | unset | command (list) that frees page cache; see [Memory](#memory) |
| `[service.NAME]` | | a service written inline instead of in its own file |

### Service file

One TOML file per app; the service name is the file name without `.toml` unless the file sets `name`. The same keys
work in an inline `[service.NAME]` table (without `name`).

| Key | Default | Meaning |
| --- | --- | --- |
| `title` | required | shown on the pages and in messages |
| `adapter` | required | `web`, `openai` or `comfyui` (see [What clients see](#what-clients-see)) |
| `upstream` | required | where the app listens while switchboard runs it, e.g. `http://127.0.0.1:18188` |
| `command` | required | the launcher, as a list (`["bash", "~/path/app.sh"]`) or a string (run with `bash -c`). In `[run]` or at the top level. |
| `listen` | `[]` | public port(s) switchboard answers on; an integer or a list |
| `enabled` | `true` | `false`: ignored (no ports bound, not shown) |
| `name` | file name | service name (service files only) |
| `env` | `{}` | extra environment for the launcher (`[run]` or top level) |
| `cwd` | unset | launcher working directory (`[run]` or top level) |
| `memory_gib` | `0` | free memory needed to start |
| `max_gib` | `memory_gib` | the most it grows to while running |
| `priority` | `0` | lower is stopped first when memory is needed; keep-warm apps start in descending order |
| `keep_warm` | `false` | start whenever it fits without stopping anything |
| `idle_grace_s` | `120` | quiet needed after use before another app may take its memory |
| `idle_stop_s` | `0` | stop after this long unused (`0`: never) |
| `session_window_s` | `900` | uses in this window decide whether a session is going on |
| `session_min_calls` | `3` | this many separate uses in the window make a session |
| `session_burst_s` | `20` | uses closer together than this count as one |
| `session_factor` | `3` | during a session, quiet needed = factor × the longest gap between uses |
| `session_grace_max_s` | `900` | cap on that (never below `idle_grace_s`) |
| `hold_s` | `120` | how long a request may wait for the app to be ready |
| `stop_timeout_s` | `60` | SIGTERM, then this long, then SIGKILL to the process group (`[run]` or top level) |
| `kill_command` | unset | run after a SIGKILL, e.g. remove a container (`[run]` or top level) |
| `ready_path` | `"/"` | `GET upstream + ready_path` answering 2xx = ready (`[ready] path`) |
| `start_estimate_s` | `60` | start time until switchboard has measured its own (median of the last 5) (`[ready] estimate_s`) |
| `start_timeout_s` | `900` | not ready after this long: stopped and marked failed (`[ready] timeout_s`). A launcher switchboard didn't start is left running |
| `pidfile` | unset | where the launcher writes its pid; needed for re-adoption after a restart |
| `containers` | `[]` | docker container names (globs) whose memory counts as this app's |
| `status_file` | unset | JSON `{"detail": "..."}` the launcher keeps current while starting |
| `passive_paths` | `[]` | path globs that never start the app (health checks) |
| `busy_probe` | unset | how to see a running job (below); `comfyui` defaults to its `/queue` |
| `options` | `{}` | adapter options: `info_html` (any adapter), `models` (openai only: the list served before the server was ever seen) |
| `info_html` | `""` | HTML shown on the status page; `{{HOST}}` becomes the host:port the browser used |

`[run]` may hold `command`, `env`, `cwd`, `stop_timeout_s` and `kill_command`; `[ready]` may hold `path`,
`estimate_s` and `timeout_s`. Other keys in them are errors.

`[busy_probe]` is a `GET upstream + path` returning JSON, in one of two forms:

```toml
[busy_probe]                 # busy while any of these lists is non-empty; values are labels for the message
path = "/queue"
nonempty = { queue_running = "running", queue_pending = "queued" }

[busy_probe]                 # busy while any item of the list has a field value other than these
path = "/api/state"
list = "jobs"
field = "status"
idle_values = ["done", "failed"]
```

A new protocol needs an adapter only if its clients need special answers: subclass `WebAdapter`
(`spark_switchboard/adapters/base.py`), override `classify`, `synthetic`, `error_body` or `busy`, and register it in
`spark_switchboard/adapters/__init__.py`.

## NVIDIA Sync

NVIDIA Sync custom apps run a command on the Spark and forward its port to your computer.

**While switchboard runs, open any app it manages with
[`delegate.sh`](examples/launchers/delegate.sh).** Give the custom app the command
`bash ~/src/spark-switchboard/examples/launchers/delegate.sh NAME` (NAME is the service name: its file name without
`.toml`) and the app's public port (`listen`), for example `delegate.sh sol-h3-studio` with port 8190. delegate.sh
never starts the app itself. It asks switchboard to start it (`POST /api/services/NAME/start`), prints switchboard's
progress as STATUS lines, and stays in the foreground, so Sync shows the app as running and keeps its port forward.
When you close the app in Sync (SIGTERM or SIGHUP, or its parent going away), it asks switchboard to stop the app
unless someone used it recently (`stop?if_idle=1`). This works for every service, including
[Sol-H3 Studio](https://github.com/slogonomo/sol-h3-studio), whose own launcher has no delegate, and over plain ssh
too. While switchboard is [paused](#dashboard-cli-and-admin-api) it says so, and asks again once switchboard is
resumed. If switchboard isn't answering or doesn't run NAME, delegate.sh says so and exits 1.

**Or point the custom app at an example launcher** (for example command
`bash ~/src/spark-switchboard/examples/launchers/comfyui.sh`, port 8188). That works with or without switchboard:

- **switchboard not running:** the launcher starts ComfyUI itself on 8188 (the launchers' default ports are the
  public ones), refuses with a memory report if it doesn't fit, and stops it when you close the app in Sync. Closing
  an app may signal only the shell that ran the launcher; the launcher notices it was re-parented and cleans up.
- **switchboard running:** the launcher's delegate (in common.sh) sees that switchboard knows the app and does what
  delegate.sh does. Apps switchboard doesn't know (or has disabled) run standalone.

A launcher without that delegate (such as Sol-H3 Studio's `bin/sol-h3-studio`) refuses to start from Sync while
switchboard holds its public port; use delegate.sh for it. An app opened from Sync *before* switchboard started keeps
running as an [external](#operations) app: switchboard never stops it and takes over its port only once you close it.

The delegate's settings: `SWITCHBOARD_API` (default `http://127.0.0.1:8800`; set it if you changed `admin_port`),
`SWITCHBOARD_SERVICE` (default: the launcher's `APP`), and `SWITCHBOARD_DELEGATE=0` to run standalone anyway (the
launcher then finds the public port taken). It needs curl and python3.

Sync runs launchers over one long-lived SSH login whose group list is fixed when it logs in; if that login is older
than your docker group membership, plain `docker` fails. common.sh's `docker` function falls back to `sg docker`.

## Dashboard, CLI and admin API

**Dashboard** (`http://<host>:8800/`): what runs, what each app holds, why something waits, what counted as use, and
recent events (starts, stops, switches, and the first use after `use_event_after_s` unused), with Start, Stop and
Cancel buttons, each launcher's log, and Pause / Resume.

**Pause** stops every app switchboard runs and starts nothing until you resume: pending starts are dropped,
keep-warm apps stay down, and requests that would start an app get `503` with `Retry-After` and the code `paused`
(a browser gets the status page, with a Resume button). Work in progress is not interrupted: an app that is working
is stopped as soon as that work is done (`--kill` interrupts it, from the machine itself only). Apps switchboard
didn't start are left alone. A pause is kept in memory only: restarting switchboard resumes it.

**CLI** (`.venv/bin/spark-switchboard`, or `python -m spark_switchboard`):

```
serve   [-c CONFIG]                    run the daemon
check   [-c CONFIG]                    validate the config and list the services
status  [--json]                       what runs, memory, pending starts (--json: the raw /api/status)
start   NAME [--force] [--wait]        --force skips quiet periods (never interrupts work)
stop    NAME [--kill]                  refuses while it works, unless --kill (only from this machine)
pause   [--kill]                       stop every app switchboard runs and start nothing until resume
resume                                 start apps on request and keep-warm again
stop-all [--kill]                      the same as pause (an older name; it no longer interrupts work by default)
log     NAME [-n 100]                  the launcher's output
--api URL, --version                   (before the command)
```

The client commands find the admin API from `--api`, else `$SWITCHBOARD_API`, else the `admin_port` (and `bind`) of
the config file found as above, else `http://127.0.0.1:8800`. Every command accepts `-c CONFIG`.

**Admin API** (on `admin_port`). POSTs must carry the header `X-Switchboard` (any value); without it they get 403.

| Method and path | What it does |
| --- | --- |
| `GET /api/status` | everything: host, memory, `paused` (`{"since", "reason"}` or null), every service's status, recent events |
| `GET /api/events` | the event history kept in memory (last 400) |
| `GET /api/services/NAME[?want=1]` | one service; `want=1` also keeps its pending start alive |
| `GET /api/services/NAME/log?lines=200` | the tail of its launcher output (plain text) |
| `POST /api/services/NAME/start[?force=1&reason=TEXT]` | want it; `force=1` skips other apps' quiet periods |
| `POST /api/services/NAME/stop[?kill=1&if_idle=1&reason=TEXT]` | stop it. 409 while busy unless `kill=1` (403 unless from this machine); with `if_idle=1`, 409 if it was used within its quiet period |
| `POST /api/services/NAME/cancel` | drop its pending start |
| `POST /api/pause[?kill=1&reason=TEXT]` | pause (see above); returns what happens to each app. `kill=1`: interrupt work too (403 unless from this machine) |
| `POST /api/resume[?reason=TEXT]` | undo pause |

While paused, `POST .../start` answers 409 with `"paused": true`.

Each public port also answers under `/__switchboard/`: `GET /__switchboard/` (the status page for a browser, JSON
otherwise), `GET /__switchboard/status[?want=1]`, `POST /__switchboard/start[?force=1]`,
`POST /__switchboard/cancel` and `POST /__switchboard/resume`. The status page uses these.

## Operations

- **Install or update:** `deploy/install.sh` (again after a `git pull`). It reinstalls the package into `.venv`,
  checks the config, renders `spark-switchboard.service` (`User` and `Group` are you; `SupplementaryGroups=docker`
  only if that group exists) and restarts it.
- **Restarts and upgrades don't stop the apps.** Launchers are started outside asyncio's process handling and the
  unit uses `KillMode=process`, so they outlive switchboard, and the new process re-adopts them through their
  pidfiles, keeping their uptime and use history.
- **Apps switchboard didn't start** are *external*: an app found answering on its upstream at startup without a
  matching pidfile, or a launcher found through its pidfile that runs without `SWITCHBOARD=1` (started by hand or from
  NVIDIA Sync before switchboard started). switchboard passes traffic through to it and never stops it, not even when
  it doesn't answer within `start_timeout_s`; it takes the app over once it goes away. If a public port is held by
  something else (such as an app opened from Sync before switchboard started), switchboard retries every 5 s, so that
  app's port is switchboard's again once you close it.
- **Pause** ([above](#dashboard-cli-and-admin-api)) stops the apps and keeps them stopped, keep-warm apps included,
  until `spark-switchboard resume` or a restart of switchboard.
- **State** lives in `state_dir` (`~/.local/state/spark-switchboard`): `logs/<name>.log` (launcher output),
  `events.log` (JSON lines), and `state.json` (measured start and stop times, remembered `/v1/models`, uptime and use
  history across restarts). The daemon's journal: `journalctl -u spark-switchboard`.
- **Remove:** `deploy/uninstall.sh` pauses switchboard, waits (up to 10 min, on `status --json`) until every app it
  runs is down, then removes the unit and the sudoers rule. Work in progress finishes first; if an app is still
  working after 10 min, nothing is removed (rerun later, or with `--kill` to interrupt it). `--keep-apps` leaves the
  apps running on their internal ports; stop one later by sending its launcher SIGTERM (for the example launchers:
  `kill $(cat ~/.local/state/spark-switchboard/run/NAME.pid)`). Afterwards the launchers work standalone.
  Both scripts take `--dry-run`.

## Security

- **No authentication**, like the apps themselves. By default switchboard listens on every interface: anyone who can
  reach the machine can use the apps, open the dashboard, read launcher logs, start, stop or cancel apps that are not
  busy, and pause or resume switchboard. Only `kill=1` (interrupting work) is limited to the machine itself.
- Use it on a **trusted LAN or VPN only**. Don't forward its ports to the internet. To keep it local, set
  `bind = "127.0.0.1"` and use SSH port forwarding (NVIDIA Sync does this); note that docker containers then can't
  reach the apps through `host.docker.internal`.
- POSTs to control endpoints need an `X-Switchboard` header. Browsers can't send a custom header cross-site without a
  CORS preflight, which switchboard never grants, so other websites can't trigger them from your browser.
- **Client IP addresses are logged**: the event log and the "what counted as use" list record each request's method,
  path and client address, in `state_dir` (`events.log`, `state.json`). The same records are shown on the dashboard
  and returned by `/api/status`, `/api/events` and every app port's `/__switchboard/status`, to anyone who can reach
  the machine. They also include the client's User-Agent product token. Treat the state directory as private, and
  bind to a trusted network only.
- Traffic is passed through unchanged, including `Authorization` headers and API keys. Plain HTTP: use a VPN or SSH
  tunnel if the network isn't trusted.

## Example: four apps on one DGX Spark

The example service files describe this setup, measured on a DGX Spark (GB10, 121 GiB MemTotal) that runs four apps
which mostly can't fit together:

| App | Example files | Public port | Needs to start | Grows to | Policy |
| --- | --- | --- | --- | --- | --- |
| Qwen3.8 Flash Next on TensorFold (OpenAI API) | `tensorfold.toml`, `tensorfold.sh` | 8888 | 115 GiB | ~110 GiB | kept warm: the default resident; loads in ~70 s; 2 min quiet |
| ComfyUI | `comfyui.toml`, `comfyui.sh` | 8188 | 60 GiB | ~95 GiB (two-stage video) | stops after 30 min unused; 5 min quiet |
| [Sol-H3 Studio](https://github.com/slogonomo/sol-h3-studio) | `sol-h3-studio.toml` | 8190 | 110 GiB | 117 GiB (a cap: nearly the whole machine) | stops after 30 min unused; busy while a job runs |
| Open WebUI | `open-webui.toml`, `open-webui.sh` | 12000 | 4 GiB | ~2 GiB (idles at 1.5) | fits next to any of them |

In practice: the LLM is up most of the time, so agents and Open WebUI (which reaches it through switchboard's port
8888) get answers at once. Opening ComfyUI stops the LLM once it has been quiet for 2 min (longer during a session),
starts ComfyUI in about 15 s, and holds the browser on the status page meanwhile. 30 min after ComfyUI's last use
(and never while a job runs), ComfyUI stops and the LLM comes back by itself. A video job in progress is never
interrupted; a request for the LLM waits for it, with the status page saying so.

The TensorFold service is [Qwen3.8 Flash Next on one DGX Spark (TensorFold)](https://github.com/MiaAI-Lab/Qwen3.8-Flash-Next-Single-DGX-Spark-TensorFold)
by MiaAI-Lab, installed per its README; `tensorfold.sh` calls its `start.sh` and `stop.sh`. Measured with it on a DGX
Spark: reading the checkpoint shards into the page cache before `start.sh` cut the load from 93–98 s to 54–56 s
(`TF_PREREAD`), and reading the n-gram table after the server is ready cut the first 30k-token prompt from 47 s to
18 s (`TF_PREREAD_TABLE`, off by default).

Sol-H3 Studio is the companion [sol-h3-studio](https://github.com/slogonomo/sol-h3-studio) repository: a web studio
and launcher for NVIDIA Research's Sol-H3-Spark video and audio pipeline on one DGX Spark. The pipeline runs MiniMax
H3 and Lightricks LTX-2.5 models, whose weights are downloaded separately under their own licenses (see that
repository's [Model terms](https://github.com/slogonomo/sol-h3-studio#model-terms)). Install it per its README:

```bash
git clone https://github.com/slogonomo/sol-h3-studio.git ~/src/sol-h3-studio
```

`examples/services/sol-h3-studio.toml` is the same as its `integrations/spark-switchboard/sol-h3-studio.toml`,
shipped disabled. Open it from NVIDIA Sync with [`delegate.sh`](#nvidia-sync).

## Limits

- One machine, unified memory. See [Scope](#scope-and-requirements).
- Admission is per start, against measured usage plus `max_gib` headroom. Apps that grow beyond `max_gib` are the
  launchers' memory guards' business.
- No authentication ([Security](#security)).
- An app that needs a specific *version* of another (an API server switching models) is two services. Per-model
  routing on one port (pick the service by the request's `model`) would be a natural next adapter.
- Requests are held across a switch, up to `hold_s`; there is no queue that accepts now and answers later. A request
  whose wait would be longer gets 503 with `Retry-After`.
- Per-process GPU memory comes from `nvidia-smi`; container membership from the docker CLI (Podman should work through
  its docker-compatible CLI; untested). Without them, usage is undercounted and planning relies more on `max_gib`.
- Re-adoption needs a pidfile and the launcher script as the last element of `command`.

## Tests

```bash
python3 -m venv .venv && .venv/bin/pip install -e .
.venv/bin/python -m unittest -v
```

51 tests, about 2.5 min, no GPU and no docker. They start real processes on random localhost ports:

- `tests/test_switchboard.py` (25): a real switchboard in front of fake services (`tests/fake_service.py`) with fake
  memory: holding and streaming, the status page, the tab rule, switching, quiet periods and force, never
  interrupting work, failed starts, websockets (6 MB frames), large uploads, HEAD, re-adoption across a real process
  restart, keep-warm with idle stop, wants lapsing, admin stop, busy state that clears when work ends,
  stop-if-unused, pause (keep-warm stays down, requests start nothing, work finishes first, resume, the CLI), and a
  launcher started outside switchboard that is never stopped.
- `tests/test_launcher.py` (7): switchboard running `examples/launchers/example-app.sh`: started by a request, STATUS
  lines on the status page, the pidfile, surviving a switchboard exit, re-adoption by a new switchboard, SIGTERM
  cleanup; the delegate handing a manual start to switchboard and stopping it on close; a launcher switchboard
  doesn't know running standalone; `delegate.sh` closed by a signal and by its parent going away, and across a
  pause; fractional GiB values in common.sh; `comfyui.sh` (with a fake ComfyUI) handing over across ports and leaving
  a stale pid's process alone.
- `tests/test_config.py` (19): config discovery, the CLI's admin URL, container ids from cgroup paths (systemd,
  cgroupfs, Podman), the local-client check, config keys in every table, `bind = "::"`, the shipped examples, and
  (when the companion [sol-h3-studio](https://github.com/slogonomo/sol-h3-studio) repo is checked out next to
  this one) that the Sol-H3 Studio example matches its copy there.

## Prior art and credits

- [LocalRouter](https://github.com/jayleaton/localrouter) by Jay Leaton (Apache-2.0) solves a neighbouring problem:
  one daemon owns the GPU and runs its own image and video engines behind MCP and OpenAI-style APIs. spark-switchboard
  takes its eviction order (idle models are unloaded lowest priority first, then least recently used) and its
  keep-warm and idle-TTL ideas (`keep_loaded` and `idle_ttl_s` there; `keep_warm` and `idle_stop_s` here). It differs
  in what it fronts: existing services with their own UIs and protocols, passed through unchanged, each one a config
  file plus its launcher. No code is shared.
- [llama-swap](https://github.com/mostlygeek/llama-swap) (MIT) is the best-known swap proxy: it hot-swaps local model
  servers (llama.cpp, vLLM, stable-diffusion.cpp, ComfyUI, ...) on demand, routed by model name. spark-switchboard
  instead gives each app its own port, passes any web app through unchanged, and plans by measured memory.
  No code is shared.
- The example TensorFold service runs
  [Qwen3.8 Flash Next on one DGX Spark (TensorFold)](https://github.com/MiaAI-Lab/Qwen3.8-Flash-Next-Single-DGX-Spark-TensorFold)
  by MiaAI-Lab (MIT), built on [TensorFold](https://github.com/ashhart/TensorFold) by Ash Hart and the TensorFold
  contributors (Apache-2.0). Neither is bundled.
- The example Sol-H3 Studio service runs the companion
  [sol-h3-studio](https://github.com/slogonomo/sol-h3-studio) repository, a web studio for NVIDIA Research's
  Sol-H3-Spark pipeline. That pipeline runs MiniMax H3 and Lightricks LTX-2.5 models, whose weights are downloaded
  separately under their own licenses (see its
  [Model terms](https://github.com/slogonomo/sol-h3-studio#model-terms)). None of it is bundled.
- [ComfyUI](https://github.com/Comfy-Org/ComfyUI) (GPL-3.0) and [Open WebUI](https://github.com/open-webui/open-webui)
  are run by the example launchers as separate apps; neither is bundled.
- [aiohttp](https://github.com/aio-libs/aiohttp) (Apache-2.0; its bundled llhttp parser is MIT) is the only runtime
  dependency, with its `multidict` and `yarl` (Apache-2.0).
- The `comfyui` and `openai` adapters speak ComfyUI's and the OpenAI API's protocols (error shapes, `/queue`,
  `/v1/models`) for compatibility; no code from those projects is used. Open WebUI is supported as a client and as an
  app.

## License and trademarks

MIT; see [LICENSE](LICENSE) and [NOTICE](NOTICE).

spark-switchboard is an unofficial project. It is not affiliated with, sponsored by, or endorsed by NVIDIA. NVIDIA,
DGX, DGX Spark, GB10 and NVIDIA Sync are trademarks of NVIDIA Corporation. ComfyUI, Open WebUI, OpenAI, MiniMax,
Lightricks and the other names here belong to their owners and are used only to describe compatibility or give
credit.
