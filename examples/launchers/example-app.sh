#!/usr/bin/env bash
# example-app.sh: the smallest complete launcher. Serves a directory with `python3 -m http.server`.
# A template for your own launchers, and what the end-to-end tests run. Works the same from spark-switchboard,
# from an NVIDIA Sync custom app, or by hand: `bash examples/launchers/example-app.sh`. To start your own, copy it
# into this directory (next to common.sh), or anywhere else after changing its `source` line to common.sh's path.
#
# Settings (environment):
#   EXAMPLE_PORT          8000      port to listen on (switchboard passes an internal one)
#   EXAMPLE_HOST          0.0.0.0   address to listen on (switchboard passes 127.0.0.1)
#   EXAMPLE_DIR           (own)     directory to serve; unset: a page of its own in
#                                   ~/.local/state/spark-switchboard/example-app (never the current directory)
#   EXAMPLE_MIN_FREE_GIB  0         free memory (MemAvailable) needed to start
#   EXAMPLE_DELAY_S       0         pretend to load for this many seconds (to watch the status page)
# plus the LAUNCHER_* and SWITCHBOARD_* settings described in common.sh.
APP=example-app
# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

PORT=${EXAMPLE_PORT:-8000}
HOST=${EXAMPLE_HOST:-0.0.0.0}
DIR=${EXAMPLE_DIR:-$HOME/.local/state/spark-switchboard/example-app}
URL="http://$(url_host "$HOST"):$PORT/"
SERVER_LOG="$LAUNCHER_LOG_DIR/$APP-server.log"
SRV_PID=

cleanup_hook() { stop_pid "$SRV_PID" 10 "http.server"; }
health() { curl -fsS -m 5 -o /dev/null "$URL"; }

install_traps
claim_pidfile
if [[ -z "${EXAMPLE_DIR:-}" ]]; then           # serve a page of its own, never whatever directory it was started in
    mkdir -p "$DIR"
    echo "<!doctype html><title>example app</title><p>The spark-switchboard example app is running.</p>" >"$DIR/index.html"
fi
require_port_free "$PORT"
require_memory "${EXAMPLE_MIN_FREE_GIB:-0}"
if awk -v d="${EXAMPLE_DELAY_S:-0}" 'BEGIN { exit !(d + 0 > 0) }'; then
    say_status "loading (pretend: ${EXAMPLE_DELAY_S} s)"
    nap "$EXAMPLE_DELAY_S"
fi
say_status "starting http.server on $HOST:$PORT (serving $DIR)"
python3 -m http.server --bind "$HOST" --directory "$DIR" "$PORT" >>"$SERVER_LOG" 2>&1 &
SRV_PID=$!
wait_http "$URL" 30 "$SRV_PID" || fail "http.server did not come up; see $SERVER_LOG"
say_status "$APP ready on $URL"
keepalive health
