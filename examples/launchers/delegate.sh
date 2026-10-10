#!/usr/bin/env bash
# delegate.sh SERVICE: open an app that spark-switchboard runs, from an NVIDIA Sync custom app or an ssh session.
# It asks switchboard to start SERVICE (reason "delegating launcher"), prints switchboard's progress as
# ">>> STATUS:" lines, and stays in the foreground, so Sync shows the app as running and keeps its port forward.
# When it is closed (SIGTERM, SIGHUP, Ctrl-C, or its parent goes away) it asks switchboard to stop SERVICE unless
# someone used it recently (stop?if_idle=1). It never starts the app itself, so it works for any service in your
# switchboard config, including apps whose own launcher has no delegate (such as Sol-H3 Studio,
# https://github.com/slogonomo/sol-h3-studio).
#
#   NVIDIA Sync custom app:  command  bash ~/src/spark-switchboard/examples/launchers/delegate.sh sol-h3-studio
#                            port     8190   (the service's public port, `listen` in its service file)
#
# Settings (environment):
#   SWITCHBOARD_API   http://127.0.0.1:8800   switchboard's admin API (set it if you changed admin_port)
# plus LAUNCHER_LOG_DIR from common.sh (its log: $LAUNCHER_LOG_DIR/SERVICE.log). Needs curl and python3.
# Exit codes: 0 when closed, 1 when switchboard isn't answering or doesn't run SERVICE, 2 for a usage error.
SVC=${1:-}
usage() { sed -n '2,/^# Exit codes/p' "$0" | sed 's/^# \{0,1\}//'; }
if [[ "$SVC" == -h || "$SVC" == --help ]]; then usage; exit 0; fi
if [[ -z "$SVC" ]]; then usage >&2; exit 2; fi
if [[ ! "$SVC" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "delegate.sh: '$SVC' is not a service name" >&2
    exit 2
fi
if [[ -n "${SWITCHBOARD:-}" ]]; then
    echo "delegate.sh only follows spark-switchboard; it is not a launcher for switchboard to run" >&2
    exit 2
fi
APP=$SVC
SWITCHBOARD_DELEGATE=0             # common.sh: don't run the launchers' delegate on load; this script calls it below
# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

API=${SWITCHBOARD_API:-http://127.0.0.1:8800}
command -v curl >/dev/null || fail "delegate.sh needs curl"
command -v python3 >/dev/null || fail "delegate.sh needs python3"
if ! curl -fsS -m 3 -o /dev/null "$API/api/status" 2>/dev/null; then
    fail "spark-switchboard is not answering on $API (set SWITCHBOARD_API if it uses another admin port)"
fi
switchboard_runs "$API" "$SVC" ||
    fail "spark-switchboard on $API has no enabled service named '$SVC' (see: spark-switchboard status)"
switchboard_follow "$API" "$SVC"
