#!/usr/bin/env bash
# tensorfold.sh: Qwen3.8 Flash Next through MiaAI-Lab's recipe "Qwen3.8 Flash Next on one DGX Spark (TensorFold)"
# (MIT, https://github.com/MiaAI-Lab/Qwen3.8-Flash-Next-Single-DGX-Spark-TensorFold), which runs TensorFold by Ash
# Hart and the TensorFold contributors (Apache-2.0, https://github.com/ashhart/TensorFold). Install the recipe per its
# README first. This launcher calls the recipe's ./start.sh and ./stop.sh unchanged, as the recipe documents them;
# it adds a foreground process for spark-switchboard (or an NVIDIA Sync custom app), STATUS lines, a memory guard,
# and the optional load-time steps below. For spark-switchboard, an NVIDIA Sync custom app, or by hand.
#
# Settings (environment). The recipe's own settings stay in its .env; the environment wins over .env.
#   TF_RECIPE_DIR       ~/Qwen3.8-Flash-Next-Single-DGX-Spark-TensorFold   the recipe checkout
#   TF_HOST, TF_PORT    passed to the recipe as HOST and PORT (switchboard passes 127.0.0.1 and an internal port);
#                       unset: the recipe's own (.env, or 0.0.0.0 and 8888)
#   TF_CONTAINER        passed as CONTAINER_NAME; unset: the recipe's own (qwen38-flash-next-tf). The service file's
#                       `containers` and `kill_command` must name the same container.
#   TF_SERVED_NAME      passed as SERVED_NAME; unset: the recipe's own (Qwen3.8-Flash-Next). The service file's
#                       options.models should list the same id.
#   TF_MIN_FREE_GIB     115   free memory (MemAvailable) needed to start. The engine sizes its KV pool from what is
#                             free when it starts, so more is better; the recipe itself needs about 93 at its defaults.
#   TF_MEM_FLOOR_GIB    4     once running, stop the server if MemAvailable stays under this for 3 s (0: off). The
#                             engine has no watchdog of its own after loading.
#   TF_PREREAD          1     read the checkpoint shards into the page cache before start.sh (see below); 0: off
#   TF_PREREAD_TABLE    0     after the server is ready, also read the n-gram table (ple-table/) the same way
#   TF_MIN_MEMFREE_GIB  95    before starting, drop the page cache when MemFree is under this (see below)
#   TF_STOP_TIMEOUT     (the recipe's STOP_TIMEOUT, 30 s)   graceful stop budget once it is serving
# plus the LAUNCHER_* and SWITCHBOARD_* settings described in common.sh.
APP=tensorfold
# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

TF_RECIPE_DIR="${TF_RECIPE_DIR:-$HOME/Qwen3.8-Flash-Next-Single-DGX-Spark-TensorFold}"
NEED_GIB=${TF_MIN_FREE_GIB:-115}
START_LOG="$LAUNCHER_LOG_DIR/tensorfold-start.log"
START_PID=
WARM_PID=
TABLE_PID=
ABORTED=

# Run a command in the recipe directory with this launcher's overrides (the environment wins over the recipe's .env).
recipe() {
    (
        cd "$TF_RECIPE_DIR" || exit 1
        [[ -n "${TF_HOST:-}" ]] && export HOST="$TF_HOST"
        [[ -n "${TF_PORT:-}" ]] && export PORT="$TF_PORT"
        [[ -n "${TF_CONTAINER:-}" ]] && export CONTAINER_NAME="$TF_CONTAINER"
        [[ -n "${TF_SERVED_NAME:-}" ]] && export SERVED_NAME="$TF_SERVED_NAME"
        "$@"
    )
}
# The recipe's resolved settings: HOST, PORT, CONTAINER_NAME, SERVED_NAME, and the checkpoint snapshot directory
# (empty until it is downloaded). Uses the helpers in the recipe's scripts/config.sh (model_cache_dir, snapshot_rev).
recipe_settings() {
    # shellcheck disable=SC2016  # expanded by the inner bash
    recipe bash -c 'source scripts/config.sh >/dev/null 2>&1 || exit 1
        d=$(model_cache_dir 2>/dev/null)/snapshots/$(snapshot_rev 2>/dev/null)
        [[ -d "$d" ]] || d=""
        printf "%s\n" "$HOST" "$PORT" "$CONTAINER_NAME" "$SERVED_NAME" "$d"'
}

[[ -x "$TF_RECIPE_DIR/start.sh" && -f "$TF_RECIPE_DIR/scripts/config.sh" ]] ||
    { install_traps; fail "no TensorFold recipe in $TF_RECIPE_DIR (set TF_RECIPE_DIR to your checkout)"; }
mapfile -t _s < <(recipe_settings)
HOST=${_s[0]:-0.0.0.0} PORT=${_s[1]:-8888} CONTAINER=${_s[2]:-qwen38-flash-next-tf} SERVED=${_s[3]:-Qwen3.8-Flash-Next}
SNAPSHOT=${_s[4]:-}
URL="http://$(url_host "$HOST"):$PORT"

running() { [[ -n "$(docker ps -q --filter "name=^${CONTAINER}$" --filter status=running 2>/dev/null)" ]]; }
health() { curl -fs -m 5 -o /dev/null "$URL/health"; }
kill_tree() { local c; for c in $(pgrep -P "$1" 2>/dev/null); do kill_tree "$c"; done; kill "$1" 2>/dev/null; }

cleanup_hook() {
    handing_over && { log "handing over; the server is left running"; return; }
    [[ $_ADMITTED == 1 ]] || return 0              # refused before starting anything: leave any server alone
    local p
    for p in $WARM_PID $TABLE_PID; do                # pre-reads still running (still our children: pids get reused)
        [[ "$(ps -o ppid= -p "$p" 2>/dev/null | tr -d ' ')" == "$$" ]] && kill_tree "$p"
    done
    if [[ -n "$START_PID" ]] && kill -0 "$START_PID" 2>/dev/null; then
        kill_tree "$START_PID"
        ABORTED=1
        log "aborted start.sh while it was loading"
    fi
    if [[ -n "$ABORTED" ]]; then
        # Fast stop: while it loads there are no requests to finish, and the engine doesn't act on SIGTERM until it
        # has loaded (seen with recipe 1.0.0), so a graceful stop would only wait out its timeout. 5 s, then docker
        # kills it. start.sh may still have been starting the container: check again shortly after.
        log "running stop.sh (load aborted: up to 5 s)"
        STOP_TIMEOUT=5 recipe ./stop.sh 2>&1 || log "stop.sh returned non-zero"
        sleep 3
        if docker ps -aq --filter "name=^${CONTAINER}$" | grep -q .; then
            log "the container appeared after the abort; stopping it"
            STOP_TIMEOUT=5 recipe ./stop.sh 2>&1
        fi
    else
        log "running stop.sh (graceful)"
        STOP_TIMEOUT=${TF_STOP_TIMEOUT:-${STOP_TIMEOUT:-30}} recipe ./stop.sh 2>&1 || log "stop.sh returned non-zero"
    fi
}

# Read files into the page cache with 16 parallel readers. $1: model (the top-level shards the engine loads) or table.
preread() {
    local t=$SECONDS
    [[ -n "$SNAPSHOT" ]] || { log "pre-read skipped: the checkpoint isn't downloaded yet"; return 0; }
    if [[ $1 == table ]]; then
        [[ -d "$SNAPSHOT/ple-table" ]] || return 0
        find -L "$SNAPSHOT/ple-table" -name '*.safetensors' -print0 | xargs -0 -r -P16 -n1 cat >/dev/null 2>&1
    else
        find -L "$SNAPSHOT" -maxdepth 1 -name 'model*.safetensors' -print0 | xargs -0 -r -P16 -n1 cat >/dev/null 2>&1
    fi
    log "pre-read the $1 files in $((SECONDS - t)) s"
}

guard_action() {
    say_status "memory nearly exhausted (another app loading?): stopping $SERVED to keep the machine responsive"
    kill -TERM $$
}

install_traps
claim_pidfile
MEMFREE_GIB=$(whole_gib "${TF_MIN_MEMFREE_GIB:-95}") ||
    fail "TF_MIN_MEMFREE_GIB must be a number of GiB, not '${TF_MIN_MEMFREE_GIB}'"

if running; then
    if health; then
        log "container $CONTAINER is already running and healthy; adopting it"
        admitted
    else
        fail "container $CONTAINER is running but not answering on $URL (started by hand?). Use it directly, or stop it with $TF_RECIPE_DIR/stop.sh"
    fi
else
    require_port_free "$PORT"
    require_memory "$NEED_GIB"
    say_status "starting $SERVED (TensorFold) on $HOST:$PORT; the first start builds the image and downloads ~122 GiB"
    # GB10 page-cache quirk: the engine's CUDA context needs truly free pages (MemFree); with the page cache full, the
    # start can fail with NV_ERR_NO_MEMORY although MemAvailable is ample. Measured on a DGX Spark; needs the sudoers
    # rule from deploy/install.sh --with-drop-caches (without it, this only logs).
    if (($(mem_gib MemFree) < MEMFREE_GIB)); then
        log "MemFree $(mem_gib MemFree) GiB < $MEMFREE_GIB GiB: dropping the page cache first"
        drop_caches
    fi
    # Shard pre-read (not part of the recipe; measured on a DGX Spark: load 93-98 s -> 54-56 s): the engine faults its
    # shards in through mmap at under 1 GB/s, while 16 parallel readers fill the page cache in seconds. It adds ~71 GiB
    # of page cache, so it only runs when that much is truly free.
    if [[ "${TF_PREREAD:-1}" == 1 ]]; then
        if (($(mem_gib MemFree) >= MEMFREE_GIB)); then
            say_status "reading the checkpoint into memory"
            preread model &
            WARM_PID=$!
            while kill -0 "$WARM_PID" 2>/dev/null; do nap 1; quit_if_orphaned; done
            WARM_PID=
        else
            log "pre-read skipped: only $(mem_gib MemFree) GiB truly free (TF_MIN_MEMFREE_GIB $MEMFREE_GIB)"
        fi
    fi
    : >"$START_LOG"
    recipe ./start.sh >>"$START_LOG" 2>&1 &
    START_PID=$!
    last=""
    while kill -0 "$START_PID" 2>/dev/null; do        # start.sh's progress as STATUS lines
        line=$(tail -40 "$START_LOG" | tr '\r' '\n' |
               grep -aoE '\[[0-9]/5\] [A-Za-z :]+|[0-9]+s elapsed, [0-9.]+ GiB on the GPU|Not ready yet.*|Pulling.*|Downloading.*' |
               tail -1)
        [[ -n "$line" && "$line" != "$last" ]] && { say_status "$line"; last=$line; }
        nap 2
        quit_if_orphaned
    done
    wait "$START_PID"
    rc=$?
    START_PID=
    if [[ $rc != 0 ]] || ! health; then
        log "tail of $START_LOG:"
        tail -25 "$START_LOG"
        fail "start.sh failed (exit $rc): $(grep -a 'ERROR' "$START_LOG" | tail -1 | cut -c1-200)"
    fi
fi
# n-gram table pre-read (optional, not part of the recipe; measured on a DGX Spark: the first 30k-token prompt
# 47 s -> 18 s). The engine keeps the table mmapped and reads it row by row while it reads prompts.
if [[ "${TF_PREREAD_TABLE:-0}" == 1 ]]; then
    preread table &
    TABLE_PID=$!
fi
start_mem_guard "${TF_MEM_FLOOR_GIB:-4}" guard_action
say_status "$SERVED ready: OpenAI API at $URL/v1 (model $SERVED)"
keepalive health
