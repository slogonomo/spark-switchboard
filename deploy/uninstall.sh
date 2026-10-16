#!/usr/bin/env bash
# deploy/uninstall.sh: remove the spark-switchboard systemd service.
#
#   deploy/uninstall.sh [--kill | --keep-apps] [--dry-run]
#
#   default      Pause switchboard (.venv/bin/spark-switchboard pause --no-persist: this pause isn't kept for a later
#                reinstall; a pause that already persists stays): it stops every app it runs and starts nothing
#                any more, not even keep-warm apps. Work in progress is not interrupted: an app that is working is
#                stopped when that work is done. Wait (up to 10 min) until `spark-switchboard status --json` shows none
#                of them starting, running or stopping, then disable and remove the unit and the optional sudoers
#                rule. If an app is still running after 10 min, nothing is removed and switchboard stays paused
#                (`spark-switchboard resume` undoes that): rerun later, or with --kill or --keep-apps.
#   --kill       also interrupt work in progress (spark-switchboard pause --kill)
#   --keep-apps  Leave the apps running. The unit has KillMode=process, so stopping switchboard doesn't stop them;
#                they keep their internal ports, not the public ones. Stop one later with
#                kill $(cat ~/.local/state/spark-switchboard/run/<app>.pid)   (its launcher then cleans up).
#   --dry-run    print what would be done
#
# Afterwards the launchers work standalone again (NVIDIA Sync or by hand). The config (~/.config/spark-switchboard),
# the state (~/.local/state/spark-switchboard) and this repo's .venv are left in place.
set -uo pipefail

REPO=$(cd "$(dirname "$(readlink -f "$0")")/.." && pwd)
VENV="$REPO/.venv"
SB="$VENV/bin/spark-switchboard"
UNIT=/etc/systemd/system/spark-switchboard.service
SUDOERS=/etc/sudoers.d/spark-switchboard
KEEP=0
DRY=0
KILL=()

while (($#)); do
    case "$1" in
        --keep-apps) KEEP=1; shift ;;
        --kill) KILL=(--kill); shift ;;
        -n | --dry-run) DRY=1; shift ;;
        -h | --help) sed -n '2,/^set -uo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1 (see --help)" >&2; exit 2 ;;
    esac
done

say() { printf '%s\n' "$*"; }
run() {   # a step that changes something: printed in a dry run, run otherwise
    if ((DRY)); then say "would run: $(printf '%q ' "$@")"; else "$@"; fi
}

# The config the unit runs with, so the CLI finds the right admin port.
CONFIG=$(sed -n 's/^ExecStart=.* -c "\{0,1\}\([^"]*\)"\{0,1\}$/\1/p' "$UNIT" 2>/dev/null | head -1)
CFG=()
[[ -n "$CONFIG" ]] && CFG=(-c "$CONFIG")

# Services switchboard runs that are not stopped yet, from /api/status (JSON), with what each is busy with.
# Fails when switchboard doesn't answer.
running_apps() {
    "$SB" status --json "${CFG[@]}" 2>/dev/null | "$VENV/bin/python" -c '
import json, sys
try:
    st = json.load(sys.stdin)
except ValueError:
    sys.exit(1)
print(", ".join(s["name"] + (" (working: " + s["busy"] + ")" if s.get("busy") else "") for s in st["services"]
                if not s.get("external") and s["state"] in ("starting", "ready", "stopping")))'
}

if [[ -x "$SB" ]] && systemctl is-active --quiet spark-switchboard; then
    if ((KEEP)); then
        say "leaving these apps running: $(running_apps)"
    else
        if ! run "$SB" pause --no-persist --reason uninstall "${KILL[@]}" "${CFG[@]}"; then
            say "could not pause spark-switchboard; nothing was removed (--keep-apps removes it anyway)"
            exit 1
        fi
        if ((!DRY)); then
            last=""
            for ((i = 0; i < 300; i++)); do                     # up to 10 min
                if ! left=$(running_apps); then
                    say "spark-switchboard stopped answering; nothing was removed (see: systemctl status spark-switchboard)"
                    exit 1
                fi
                [[ -z "$left" ]] && break
                if [[ "$left" != "$last" ]] || ((i % 15 == 0)); then say "waiting for: $left"; last=$left; fi
                sleep 2
            done
            if [[ -n "$left" ]]; then
                say "still running after 10 min: $left"
                say "Nothing was removed, and switchboard stays paused (undo: $SB resume). Rerun when that work is"
                say "done, or rerun with --kill to interrupt it, or with --keep-apps to leave the apps running."
                exit 1
            fi
            say "every app switchboard ran is stopped"
        fi
    fi
else
    say "spark-switchboard is not running (or not installed from this repo); nothing to stop"
fi

run sudo systemctl disable --now spark-switchboard
run sudo rm -f "$UNIT" "$SUDOERS"
run sudo systemctl daemon-reload
if ((DRY)); then say "dry run: nothing was changed"; else say "spark-switchboard removed"; fi
