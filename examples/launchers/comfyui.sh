#!/usr/bin/env bash
# comfyui.sh: ComfyUI from a local install (a checkout with its own Python venv). For spark-switchboard, an NVIDIA
# Sync custom app, or by hand. Foreground: starts main.py, waits for /system_stats, stays up, stops ComfyUI on exit.
# While it runs, a memory guard interrupts the running job and asks ComfyUI to unload its models if MemAvailable stays
# under COMFY_MEM_FLOOR_GIB for 3 s (on GB10, running out of unified memory can freeze the machine).
#
# Settings (environment):
#   COMFYUI_DIR          ~/ComfyUI   the ComfyUI checkout (holds main.py)
#   COMFYUI_PYTHON       the first of $COMFYUI_DIR/venv/bin/python, $COMFYUI_DIR/.venv/bin/python, python3
#   COMFY_PORT           8188        port to listen on (switchboard passes an internal one)
#   COMFY_LISTEN         0.0.0.0     address to listen on (switchboard passes 127.0.0.1)
#   COMFY_EXTRA_ARGS     (empty)     more main.py arguments, e.g. "--extra-model-paths-config /path/to/paths.yaml"
#   COMFY_MIN_FREE_GIB   60          free memory (MemAvailable) needed to start
#   COMFY_MEM_FLOOR_GIB  6           memory guard floor; 0 turns the guard off
# (On a machine with a discrete GPU, MemAvailable is host RAM: these two are about RAM there. switchboard's
# [memory] source = "gpu" checks the GPU's own memory.)
# plus the LAUNCHER_* and SWITCHBOARD_* settings described in common.sh. ComfyUI's own output goes to
# $LAUNCHER_LOG_DIR/comfyui-server.log; its last lines are copied into this launcher's output when it fails to start.
# $LAUNCHER_RUN_DIR/comfyui-server.pid holds "<pid> <listen> <port>" of the ComfyUI a launcher started. A new launcher
# adopts that server when it runs on the same address, and stops it first when it runs on another one (a launcher
# from switchboard on the internal port, say, that handed over to one started from NVIDIA Sync).
#
# sm_121: Triton's bundled ptxas predates GB10's sm_121, so Triton kernels fail to compile with it. On a GB10
# (nvidia-smi reports compute capability 12.1), when the CUDA 13 toolkit's ptxas exists (/usr/local/cuda/bin/ptxas on
# DGX OS), TRITON_PTXAS_PATH points at it. Other GPUs keep Triton's own ptxas. Set it yourself to use another one.
APP=comfyui
# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

COMFYUI_DIR="${COMFYUI_DIR:-$HOME/ComfyUI}"
PORT=${COMFY_PORT:-8188}
LISTEN=${COMFY_LISTEN:-0.0.0.0}
URL="http://$(url_host "$LISTEN"):$PORT"
SERVER_LOG="$LAUNCHER_LOG_DIR/comfyui-server.log"
SERVER_PIDFILE="$LAUNCHER_RUN_DIR/comfyui-server.pid"
PY_PID=

if [[ -z "${COMFYUI_PYTHON:-}" ]]; then
    for p in "$COMFYUI_DIR/venv/bin/python" "$COMFYUI_DIR/.venv/bin/python" python3; do
        command -v "$p" >/dev/null && { COMFYUI_PYTHON=$p; break; }
    done
fi

# 0 when pid $1 is a ComfyUI that a launcher started with --listen $2 --port $3 (not a reused pid).
is_server() {
    [[ -n "$1" && -n "$3" ]] && kill -0 "$1" 2>/dev/null &&
        [[ "$(tr '\0' ' ' <"/proc/$1/cmdline" 2>/dev/null)" == *"main.py --listen $2 --port $3 "* ]]
}

cleanup_hook() {
    handing_over && { log "handing over; ComfyUI left running"; return; }
    [[ $_ADMITTED == 1 && -n "$PY_PID" ]] || return 0    # nothing started or adopted: leave any other process alone
    stop_pid "$PY_PID" 20 "ComfyUI"
    rm -f "$SERVER_PIDFILE"
}

guard_action() {   # interrupt the running job and ask ComfyUI to drop its models from memory
    curl -s -m 5 -X POST "$URL/interrupt" >/dev/null
    curl -s -m 10 -X POST "$URL/free" -H 'Content-Type: application/json' \
        -d '{"unload_models": true, "free_memory": true}' >/dev/null
    say_status "ComfyUI job interrupted: memory was nearly exhausted (see the launcher log)"
}

health() { curl -fsS -m 5 -o /dev/null "$URL/system_stats"; }

install_traps
claim_pidfile

old="" old_listen="" old_port=""
[[ -r "$SERVER_PIDFILE" ]] && read -r old old_listen old_port <"$SERVER_PIDFILE"
if is_server "$old" "$old_listen" "$old_port"; then
    # A ComfyUI left by an earlier launcher (it handed over to this one, or was killed). Same address: adopt it.
    # Another address (or not answering): stop it rather than run a second ComfyUI next to it.
    if [[ "$old_listen:$old_port" != "$LISTEN:$PORT" ]]; then
        log "ComfyUI (pid $old) from an earlier launcher runs on $old_listen:$old_port, not $LISTEN:$PORT; stopping it"
    elif wait_http "$URL/system_stats" 120 "$old"; then
        log "ComfyUI (pid $old) is already running on $LISTEN:$PORT; adopting it"
        admitted
        PY_PID=$old
    else
        log "ComfyUI (pid $old) from an earlier launcher is not answering on $URL; stopping it"
    fi
    if [[ -z "$PY_PID" ]]; then
        stop_pid "$old" 20 "ComfyUI (pid $old)"
        rm -f "$SERVER_PIDFILE"
    fi
fi
if [[ -z "$PY_PID" ]]; then
    [[ -f "$COMFYUI_DIR/main.py" ]] || fail "no ComfyUI in $COMFYUI_DIR (set COMFYUI_DIR to your checkout)"
    require_port_free "$PORT"
    require_memory "${COMFY_MIN_FREE_GIB:-60}"
    say_status "starting ComfyUI on $LISTEN:$PORT (log: $SERVER_LOG)"
    if [[ -z "${TRITON_PTXAS_PATH:-}" && -x /usr/local/cuda/bin/ptxas ]] &&
        [[ "$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1)" == "12.1" ]]; then
        export TRITON_PTXAS_PATH=/usr/local/cuda/bin/ptxas
    fi
    cd "$COMFYUI_DIR" || fail "cannot cd to $COMFYUI_DIR"
    # shellcheck disable=SC2086  # COMFY_EXTRA_ARGS is a list of arguments
    PYTHONUNBUFFERED=1 "$COMFYUI_PYTHON" main.py --listen "$LISTEN" --port "$PORT" ${COMFY_EXTRA_ARGS:-} \
        >>"$SERVER_LOG" 2>&1 &
    PY_PID=$!
    echo "$PY_PID $LISTEN $PORT" >"$SERVER_PIDFILE"
    if ! wait_http "$URL/system_stats" 300 "$PY_PID"; then
        log "tail of $SERVER_LOG:"
        tail -30 "$SERVER_LOG"
        fail "ComfyUI did not come up; see $SERVER_LOG"
    fi
fi
start_mem_guard "${COMFY_MEM_FLOOR_GIB:-6}" guard_action
say_status "ComfyUI ready on $URL"
keepalive health
