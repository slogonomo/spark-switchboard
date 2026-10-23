# shellcheck shell=bash
# common.sh: shared helpers for spark-switchboard launchers. Sourced, not run.
#
# A launcher sets APP (a short name, also used for its pidfile) and sources this file:
#
#     APP=myapp
#     source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"
#
# then defines cleanup_hook, calls install_traps and claim_pidfile, checks its port and memory, starts the app,
# waits for it, and ends with keepalive. example-app.sh is the smallest complete launcher; a copy of it works from
# this directory (next to common.sh), or from anywhere once its `source` line names this file's path.
#
# The launcher protocol (README, "Writing a launcher"):
#   - Run in the foreground until SIGTERM, then clean up (stop the server or container) and exit.
#   - Listen where the environment says; switchboard passes internal ports.
#   - Write a pidfile ($LAUNCHER_RUN_DIR/$APP.pid) so a restarted switchboard can re-adopt the launcher.
#   - With SWITCHBOARD set (switchboard runs it), don't exit when re-parented: a switchboard restart must not stop it.
#   - Print ">>> STATUS: <text>" lines while starting; switchboard shows them on the status page.
#   - On a failed start, print a STATUS line that says why (REFUSED / ERROR), then exit non-zero.
#
# Settings (environment):
#   LAUNCHER_RUN_DIR      ~/.local/state/spark-switchboard/run        pidfiles and stop markers
#   LAUNCHER_LOG_DIR      ~/.local/state/spark-switchboard/launchers  app server logs, and the launcher's own log
#                                                                     when switchboard isn't the one running it
#   LAUNCHER_POLL_S       5      keepalive interval in whole seconds (health checks run about every 30 s)
#   LAUNCHER_STOP_WAIT_S  150    how long require_memory waits for another launcher that is still stopping
#   SWITCHBOARD_API       http://127.0.0.1:8800   where the delegate looks for a running spark-switchboard
#   SWITCHBOARD_SERVICE   $APP   the service name switchboard knows this app by
#   SWITCHBOARD_DELEGATE  1      0: never delegate; run standalone even while switchboard is up
# Tools used: bash, curl, python3, ss (iproute2), ps, awk; flock, nvidia-smi and docker when present.

APP="${APP:?set APP before sourcing common.sh}"
LAUNCHER_RUN_DIR="${LAUNCHER_RUN_DIR:-$HOME/.local/state/spark-switchboard/run}"
LAUNCHER_LOG_DIR="${LAUNCHER_LOG_DIR:-$HOME/.local/state/spark-switchboard/launchers}"
LAUNCHER_POLL_S="${LAUNCHER_POLL_S:-5}"
mkdir -p "$LAUNCHER_RUN_DIR" "$LAUNCHER_LOG_DIR"
PIDFILE="$LAUNCHER_RUN_DIR/$APP.pid"
APP_LOG="$LAUNCHER_LOG_DIR/$APP.log"
LAUNCHER_SCRIPT="$(readlink -f "$0")"
_ORIG_PPID=$PPID
_ADMITTED=0
_CLEANED=0
_BG_PID=
_GUARD_PID=

# ---- output ---------------------------------------------------------------------------------------------------
# Under switchboard, stdout already is <state_dir>/logs/<service>.log. Otherwise (NVIDIA Sync, by hand) lines also go
# to $APP_LOG, because the terminal they were printed to may be gone.
_to_file() { [[ -n "${SWITCHBOARD:-}" ]] || echo "$1" >>"$APP_LOG" 2>/dev/null; }
log() { local m; m="[$(date '+%F %T')] [$APP] $*"; echo "$m" 2>/dev/null; _to_file "$m"; }
say_status() { echo ">>> STATUS: $*" 2>/dev/null; _to_file "[$(date '+%F %T')] [$APP] >>> STATUS: $*"; }
fail() { say_status "ERROR: $*"; exit 1; }

# ---- docker ---------------------------------------------------------------------------------------------------
# NVIDIA Sync runs launchers over one long-lived SSH login whose group list was fixed when it logged in. If that login
# is older than `usermod -aG docker $USER`, plain docker fails with "permission denied" while `sg docker` works.
# Exported, so scripts the launcher runs (for example a recipe's start.sh) get the same fallback.
docker() {
    if [[ -n "${DOCKER_HOST:-}" || -w /var/run/docker.sock ]] || ! id -nG "$(id -un)" 2>/dev/null | grep -qw docker; then
        command docker "$@"
    else
        sg docker -c "$(printf '%q ' docker "$@")"
    fi
}
export -f docker

# ---- traps, cleanup, pidfile ------------------------------------------------------------------------------------
cleanup_hook() { :; }                  # each launcher overrides this: stop its server or container
admitted() { _ADMITTED=1; }            # the app is being started (or was adopted): cleanup stops it
handing_over() { [[ -s "$LAUNCHER_RUN_DIR/$APP.handover" ]]; }   # a new launcher of this app takes the app over

_on_signal() { log "received SIG$1"; exit 0; }       # exit -> EXIT trap -> _cleanup
_cleanup() {
    local rc=$?
    [[ $_CLEANED == 1 ]] && return
    _CLEANED=1
    trap '' INT TERM HUP QUIT
    if [[ $_ADMITTED == 1 ]] && ! handing_over; then
        # Tell other launchers this app's memory is on its way back (require_memory waits instead of refusing).
        echo "$$ $(date +%s)" >"$LAUNCHER_RUN_DIR/$APP.stopping.$$" 2>/dev/null &&
            mv -f "$LAUNCHER_RUN_DIR/$APP.stopping.$$" "$LAUNCHER_RUN_DIR/$APP.stopping"
    fi
    [[ -n "$_BG_PID" ]] && kill "$_BG_PID" 2>/dev/null
    [[ -n "$_GUARD_PID" ]] && kill "$_GUARD_PID" 2>/dev/null
    log "cleanup (exit code $rc)"
    cleanup_hook
    [[ "$(cat "$PIDFILE" 2>/dev/null)" == "$$" ]] && rm -f "$PIDFILE"
    rm -f "$LAUNCHER_RUN_DIR/$APP.stopping"
    if [[ $_ADMITTED != 1 ]]; then
        log "$APP was not started"
    elif handing_over; then
        log "$APP handed over to the new launcher"
    else
        say_status "$APP stopped"
    fi
}

install_traps() {
    trap '' PIPE        # once a remote session's channel is gone, stdout is a dead pipe: don't die on the next echo
    local s how=standalone
    # shellcheck disable=SC2064  # expand $s now, once per signal
    for s in INT TERM HUP QUIT; do trap "_on_signal $s" "$s"; done
    trap _cleanup EXIT
    [[ -n "${SWITCHBOARD:-}" ]] && how="run by spark-switchboard (service ${SWITCHBOARD_SERVICE:-?})"
    log "launcher pid $$, parent $PPID, $how"
}

# Write the pidfile. If another launcher of this app is still running (NVIDIA Sync relaunched it, say), ask it to hand
# over: it exits without stopping the app (see handing_over), and this launcher adopts the running app.
claim_pidfile() {
    local old i
    old=$(cat "$PIDFILE" 2>/dev/null)
    if [[ -n "$old" && "$old" != "$$" ]] && kill -0 "$old" 2>/dev/null &&
        tr '\0' ' ' <"/proc/$old/cmdline" 2>/dev/null | grep -qF "$(basename "$LAUNCHER_SCRIPT")"; then
        log "another $APP launcher (pid $old) is running; asking it to hand over"
        echo "$$" >"$LAUNCHER_RUN_DIR/$APP.handover"
        kill -TERM "$old" 2>/dev/null
        for ((i = 0; i < 60; i++)); do kill -0 "$old" 2>/dev/null || break; sleep 1; done
        rm -f "$LAUNCHER_RUN_DIR/$APP.handover"
    fi
    echo $$ >"$PIDFILE"
}

# Sleep in the background and wait, so a SIGTERM is handled at once (a foreground sleep defers the trap).
nap() { sleep "$1" & _BG_PID=$!; wait "$_BG_PID" 2>/dev/null; _BG_PID=; }

# Not run by switchboard, and our parent went away (NVIDIA Sync closed the app and signalled only its wrapper, or the
# SSH session died): we were re-parented. Under switchboard (SWITCHBOARD set) this never applies: switchboard
# restarts and upgrades leave the launchers running and re-adopt them.
orphaned() {
    [[ -n "${SWITCHBOARD:-}" ]] && return 1
    local cur
    cur=$(ps -o ppid= -p $$ 2>/dev/null | tr -d ' ')
    [[ -n "$cur" && "$cur" != "$_ORIG_PPID" ]]
}
quit_if_orphaned() {
    if orphaned; then log "parent process $_ORIG_PPID went away; stopping"; exit 0; fi
    return 0
}

stop_pid() {   # pid [timeout_s=20] [label]: SIGTERM, wait, then SIGKILL
    local pid=$1 t=${2:-20} label=${3:-process} i
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null || return 0
    kill -TERM "$pid" 2>/dev/null
    for ((i = 0; i < t * 2; i++)); do
        kill -0 "$pid" 2>/dev/null || { log "$label stopped"; return 0; }
        sleep 0.5
    done
    kill -KILL "$pid" 2>/dev/null
    log "$label killed (still running $t s after SIGTERM)"
}

# ---- ports and HTTP -------------------------------------------------------------------------------------------
url_host() { case "$1" in "" | 0.0.0.0 | ::) echo 127.0.0.1 ;; *:*) echo "[$1]" ;; *) echo "$1" ;; esac; }

port_in_use() {
    if command -v ss >/dev/null; then
        ss -Hltn "sport = :$1" 2>/dev/null | grep -q .
    else
        (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
    fi
}

# Refuse (exit 1) if something already listens on the port. Names the owner when it is one of our own processes
# (ss shows other users' processes only to root; no sudo is used).
require_port_free() {
    local port=$1 who
    port_in_use "$port" || return 0
    who=$(ss -Hltnp "sport = :$port" 2>/dev/null | grep -oE 'users:\(\("[^"]+",pid=[0-9]+' | head -1 |
          sed -E 's/users:\(\("([^"]+)",pid=([0-9]+)/\1 (pid \2)/')
    say_status "REFUSED - port $port is already in use by ${who:-another process}; stop that first"
    exit 1
}

wait_http() {   # url [timeout_s=300] [pid]: until the URL answers 2xx; gives up early when pid (the server) exits
    local url=$1 t=${2:-300} pid=${3:-} end
    end=$((SECONDS + t))
    while ((SECONDS < end)); do
        curl -fsS -m 3 -o /dev/null "$url" 2>/dev/null && return 0
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then return 1; fi
        nap 1
        quit_if_orphaned
    done
    return 1
}

# ---- memory -----------------------------------------------------------------------------------------------------
# On a DGX Spark (GB10) the CPU and GPU share one pool: MemAvailable is what an app can still get. On a machine with a
# discrete GPU it is host RAM only; switchboard ([memory] source = "gpu") plans the GPU's own memory.
mem_gib() { awk -v k="${1:-MemAvailable}:" '$1 == k { printf "%d", $2 / 1048576 }' /proc/meminfo; }

# A GiB setting as a whole number for bash arithmetic, rounded up (7.5 -> 8); returns 1 (printing nothing) when it
# isn't a plain non-negative number. Use as: n=$(whole_gib "$X") || fail "X must be a number of GiB, not '$X'"
whole_gib() {
    [[ "$1" =~ ^[0-9]+([.][0-9]*)?$ || "$1" =~ ^[.][0-9]+$ ]] || return 1
    awk -v v="$1" 'BEGIN { i = int(v); if (i < v) i++; print i }'
}

_label_pid() {   # pid -> "container NAME" or the start of its command line
    local pid=$1 cid name
    cid=$(grep -oE '(docker|libpod)[-/][0-9a-f]{64}' "/proc/$pid/cgroup" 2>/dev/null | grep -oE '[0-9a-f]{64}' | head -1)
    if [[ -n "$cid" ]]; then
        name=$(docker ps --no-trunc --format '{{.ID}} {{.Names}}' 2>/dev/null | awk -v c="$cid" '$1 == c { print $2 }')
        echo "container ${name:-${cid:0:12}}"
    else
        tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null | cut -c1-70
    fi
}

memory_users() {   # one line per GPU allocation (largest first), then each container's host memory
    local pid mib
    nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader,nounits 2>/dev/null |
        while IFS=', ' read -r pid mib; do
            [[ "$pid" =~ ^[0-9]+$ ]] || continue
            printf '%7s GiB GPU  pid %s  %s\n' "$(awk -v m="$mib" 'BEGIN { printf "%.1f", m / 1024 }')" "$pid" \
                "$(_label_pid "$pid")"
        done | sort -rn
    docker stats --no-stream --format '{{.MemUsage}}  container {{.Name}}' 2>/dev/null | sed 's/^/    host RAM /'
}

stopping_apps() {   # other apps whose launchers are shutting down right now
    local f app pid
    for f in "$LAUNCHER_RUN_DIR"/*.stopping; do
        [[ -e "$f" ]] || continue
        app=$(basename "$f" .stopping)
        [[ "$app" == "$APP" ]] && continue
        pid=$(awk '{ print $1; exit }' "$f" 2>/dev/null)
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then echo "$app"; else rm -f "$f"; fi
    done
}

# Refuse (exit 1) unless MemAvailable >= $1 GiB. If another launcher is still stopping (an app closed seconds before
# this one was started), wait for it (up to LAUNCHER_STOP_WAIT_S) plus up to 20 s for its memory to come back, rather
# than refusing. Only one launcher waits at a time (flock). The refusal names the largest memory users.
# This is a point-in-time check without reservations; spark-switchboard plans starts so they don't race.
require_memory() {
    local need have waited=0 grace=0 stopping fd="" max users top
    need=$(whole_gib "${1:-0}") || fail "$APP: the memory needed must be a number of GiB, not '${1:-}'"
    max=$(whole_gib "${LAUNCHER_STOP_WAIT_S:-150}") || fail "LAUNCHER_STOP_WAIT_S must be a number of seconds"
    have=$(mem_gib MemAvailable)
    if ((have < need)) && [[ -n "$(stopping_apps)" ]] && command -v flock >/dev/null; then
        exec {fd}>>"$LAUNCHER_RUN_DIR/memwait.lock"
        if ! flock -n "$fd"; then
            log "another launcher is already waiting for memory; not waiting"
            exec {fd}>&-
            fd=""
        fi
    fi
    while [[ -n "$fd" ]] && ((have < need && waited < max)); do
        stopping=$(stopping_apps | tr '\n' ' ')
        if [[ -n "$stopping" ]]; then
            grace=0
            ((waited % 10 == 0)) && say_status "waiting for ${stopping% } to finish stopping (${have} GiB free, need ${need})"
        elif ((grace < 20)); then
            grace=$((grace + 2))        # it just exited: the driver and containers hand memory back over a few seconds
        else
            break
        fi
        nap 2
        quit_if_orphaned
        waited=$((waited + 2))
        have=$(mem_gib MemAvailable)
    done
    if [[ -n "$fd" ]]; then
        flock -u "$fd"
        exec {fd}>&-
    fi
    if ((have < need)); then
        users=$(memory_users)
        [[ -n "$users" ]] && { log "memory in use:"; while IFS= read -r l; do log "  $l"; done <<<"$users"; }
        top=$(head -1 <<<"$users" | sed -E 's/^ +//; s/ +/ /g')
        say_status "REFUSED - $APP needs ${need} GiB free memory (MemAvailable), only ${have} GiB is free${top:+; largest user: $top}"
        exit 1
    fi
    log "memory OK: ${have} GiB available, ${need} GiB needed"
    admitted
}

# Run function $2 once when MemAvailable stays under $1 GiB for 3 s (then again at most every 20 s). On GB10,
# exhausting unified memory can freeze the machine; a guard lets a launcher stop its own job first.
start_mem_guard() {
    local floor action=$2
    floor=$(whole_gib "${1:-0}") || fail "$APP: the memory guard floor must be a number of GiB, not '${1:-}'"
    ((floor > 0)) || return 0
    (
        trap - EXIT INT TERM HUP QUIT
        low=0
        while sleep 1; do
            kill -0 $$ 2>/dev/null || exit 0
            if (($(mem_gib MemAvailable) < floor)); then low=$((low + 1)); else low=0; fi
            if ((low == 3)); then
                log "MEMORY GUARD: MemAvailable $(mem_gib MemAvailable) GiB < ${floor} GiB for 3 s: $action"
                memory_users | while IFS= read -r l; do log "  $l"; done
                "$action"
                sleep 20
                low=0
            fi
        done
    ) &
    _GUARD_PID=$!
}

# GB10: the GPU driver takes its own buffers (a new CUDA context, for one) from truly free pages (MemFree) and doesn't
# wait for the kernel to evict page cache. With the cache full, a start can fail with NV_ERR_NO_MEMORY although
# MemAvailable is ample. This drops the clean page cache (echo 1) through the optional sudoers rule that
# deploy/install.sh --with-drop-caches installs; without that rule it logs why and returns 1. Never prompts.
drop_caches() {
    local before
    before=$(mem_gib MemFree)
    sync
    if echo 1 | sudo -n /usr/bin/tee /proc/sys/vm/drop_caches >/dev/null 2>&1; then
        log "dropped the page cache: MemFree ${before} -> $(mem_gib MemFree) GiB"
    else
        log "could not drop the page cache (no sudoers rule for /usr/bin/tee /proc/sys/vm/drop_caches;" \
            "see deploy/install.sh --with-drop-caches)"
        return 1
    fi
}

# Stay in the foreground until signalled. $1 (optional): a health function, run about every 30 s; after 4 failures
# in a row the launcher exits 1 (and cleans up). Exits when re-parented only when not run by switchboard.
keepalive() {
    local health=${1:-} fails=0 n=0 every
    every=$((30 / LAUNCHER_POLL_S))
    ((every < 1)) && every=1
    while :; do
        nap "$LAUNCHER_POLL_S"
        quit_if_orphaned
        n=$((n + 1))
        if [[ -n "$health" ]] && ((n % every == 0)); then
            if "$health"; then
                fails=0
            else
                fails=$((fails + 1))
                log "health check failed ($fails/4)"
                ((fails >= 4)) && { log "$APP is gone; exiting"; exit 1; }
            fi
        fi
    done
}

# ---- the delegate -----------------------------------------------------------------------------------------------
# While spark-switchboard runs, it starts and stops the apps itself. A launcher started any other way (an NVIDIA Sync
# custom app, by hand, over ssh) then asks switchboard to start the app instead of starting a second copy, prints
# switchboard's progress as STATUS lines, and stays in the foreground (so Sync shows the app as running and keeps its
# port forward). When it is closed (SIGTERM/HUP/INT, or its parent goes away), it asks switchboard to stop the app
# unless someone used it recently (stop?if_idle=1). Apps switchboard doesn't know (or has disabled) run standalone as
# before. delegate.sh does the same for any service, without a launcher of its own.

# 0 when the switchboard at $1 (the admin API) runs service $2.
switchboard_runs() { command -v curl >/dev/null && curl -fsS -m 2 -o /dev/null "$1/api/services/$2" 2>/dev/null; }

# Returns (so the launcher runs standalone) unless switchboard runs this app; otherwise follows it and never returns.
switchboard_delegate() {
    local api="${SWITCHBOARD_API:-http://127.0.0.1:8800}" svc="${SWITCHBOARD_SERVICE:-$APP}"
    switchboard_runs "$api" "$svc" || return 0
    switchboard_follow "$api" "$svc"
}

# Ask the switchboard at $1 to start service $2, print its progress as STATUS lines, stay in the foreground; on exit,
# ask it to stop the service unless it is in use. Never returns.
switchboard_follow() {
    local api=$1 svc=$2 st out flag line last="" paused=0
    trap '' PIPE
    log "spark-switchboard runs $svc ($api): asking it to start the app; this launcher only follows it"
    _sb_post() { curl -fsS -m 10 -X POST -H 'X-Switchboard: 1' "$api/api/services/$svc/$1" 2>/dev/null; }
    _sb_release() {
        trap '' INT TERM HUP QUIT
        [[ -n "$_BG_PID" ]] && kill "$_BG_PID" 2>/dev/null
        log "closed: asking spark-switchboard to stop $svc unless it is in use"
        _sb_post "stop?if_idle=1&reason=delegating%20launcher%20closed" >/dev/null
    }
    trap 'exit 0' INT TERM HUP QUIT
    trap _sb_release EXIT
    _sb_post "start?reason=delegating%20launcher" >/dev/null
    while :; do
        st=$(curl -fsS -m 5 "$api/api/services/$svc?want=1" 2>/dev/null)
        # One line: "P<tab>text" while switchboard is paused, "-<tab>text" otherwise.
        out=$(python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except ValueError:
    sys.exit()
w, s, t = d.get("want"), str(d.get("state")), str(d.get("title", ""))
port = str((d.get("listen") or ["?"])[0])
paused = bool(d.get("paused")) and s in ("stopped", "failed")
if s == "ready":
    text = t + " ready (run by spark-switchboard) on port " + port
elif s == "starting":
    text = t + " starting: " + str(d.get("detail") or "")
elif paused:
    text = t + ": not started, spark-switchboard is paused (resume it on its dashboard or with spark-switchboard resume)"
elif w:
    text = t + ": " + str(w.get("text") or w.get("phase"))
else:
    text = t + ": " + s + (" (" + str(d["failure"]) + ")" if d.get("failure") else "")
print(("P" if paused else "-") + "\t" + text)
' <<<"$st" 2>/dev/null)
        IFS=$'\t' read -r flag line <<<"$out"
        if [[ "$flag" == P ]]; then
            paused=1
        elif [[ "$flag" == - && $paused == 1 ]]; then     # resumed while this was open: ask again
            paused=0
            _sb_post "start?reason=delegating%20launcher" >/dev/null
        fi
        [[ -n "$line" && "$line" != "$last" ]] && { say_status "$line"; last=$line; }
        nap 2
        quit_if_orphaned
    done
}

if [[ -z "${SWITCHBOARD:-}" && "${SWITCHBOARD_DELEGATE:-1}" == 1 ]]; then
    switchboard_delegate
fi
