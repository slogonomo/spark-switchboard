#!/usr/bin/env bash
# deploy/install.sh: install or update spark-switchboard as a systemd service. Run it as the user who will own the
# service (not as root); it uses sudo only to install the unit and the optional sudoers rule.
#
#   deploy/install.sh [--config PATH] [--with-drop-caches] [--dry-run]
#
#   1. Checks for Python 3.11 or newer.
#   2. Creates .venv in this repo and pip-installs the repo into it.
#   3. Finds the config: --config, $SWITCHBOARD_CONFIG, ~/.config/spark-switchboard/switchboard.toml
#      ($XDG_CONFIG_HOME), /etc/spark-switchboard/switchboard.toml. If there is none, it copies
#      examples/switchboard.toml and examples/services/ to ~/.config/spark-switchboard/ and stops: edit it, then rerun.
#   4. Runs `spark-switchboard check` on it.
#   5. Renders deploy/spark-switchboard.service for this user and installs it as
#      /etc/systemd/system/spark-switchboard.service, then enables and (re)starts it.
#
#   --with-drop-caches  also install /etc/sudoers.d/spark-switchboard (checked with visudo -cf first), which lets this
#                       user run `sudo -n /usr/bin/tee /proc/sys/vm/drop_caches` for the GB10 page-cache quirk
#   --dry-run           run only the read-only steps and print the others (including the rendered unit)
#
# Apps already running another way (NVIDIA Sync, by hand) keep their ports until they stop, and switchboard never
# stops them; it binds each port as soon as it is free. Stop them before the second run (README, "Quick start").
# Config changes take effect when this script (or `sudo systemctl restart spark-switchboard`) restarts the service;
# running apps keep running and are re-adopted.
set -euo pipefail

REPO=$(cd "$(dirname "$(readlink -f "$0")")/.." && pwd)
VENV="$REPO/.venv"
SB="$VENV/bin/spark-switchboard"
UNIT=/etc/systemd/system/spark-switchboard.service
SUDOERS=/etc/sudoers.d/spark-switchboard
CONFIG_ARG=""
DROP_CACHES=0
DRY=0

usage() { sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'; }
while (($#)); do
    case "$1" in
        -c | --config) CONFIG_ARG=${2:?--config needs a path}; shift 2 ;;
        --config=*) CONFIG_ARG=${1#*=}; shift ;;
        --with-drop-caches) DROP_CACHES=1; shift ;;
        -n | --dry-run) DRY=1; shift ;;
        -h | --help) usage; exit 0 ;;
        *) echo "unknown argument: $1 (see --help)" >&2; exit 2 ;;
    esac
done

say() { printf '%s\n' "$*"; }
esc() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }      # for the replacement side of sed's s|...|...|
run() {   # a step that changes something: printed in a dry run, run otherwise
    if ((DRY)); then say "would run: $(printf '%q ' "$@")"; else "$@"; fi
}

if [[ $(id -u) == 0 ]]; then
    say "Run this as the user who will own the service, not as root; it uses sudo where it needs to."
    exit 1
fi
USER_NAME=$(id -un)
GROUP_NAME=$(id -gn)

# 1. Python 3.11+ (tomllib)
PY=${PYTHON:-python3}
if ! command -v "$PY" >/dev/null; then
    say "$PY not found. On DGX OS / Ubuntu: sudo apt install python3 python3-venv"
    exit 1
fi
if ! "$PY" -c 'import sys; sys.exit(sys.version_info < (3, 11))'; then
    say "spark-switchboard needs Python 3.11 or newer; this is $("$PY" -V 2>&1)."
    say "DGX OS (Ubuntu 24.04) ships 3.12. On older Ubuntu: sudo apt install python3.11 python3.11-venv, then"
    say "  PYTHON=python3.11 $0"
    exit 1
fi

# 2. venv with the package
if ((DRY)); then
    say "would create $VENV (if missing) and run: $VENV/bin/python -m pip install --upgrade $REPO"
else
    if [[ ! -x "$VENV/bin/python" ]]; then
        if ! "$PY" -m venv "$VENV"; then
            say "Creating the venv failed. On DGX OS / Ubuntu: sudo apt install python3-venv"
            exit 1
        fi
    fi
    "$VENV/bin/python" -m pip install --quiet --upgrade "$REPO"
fi

# 3. config
USER_CONFIG="${XDG_CONFIG_HOME:-$HOME/.config}/spark-switchboard/switchboard.toml"
if [[ -n "$CONFIG_ARG" ]]; then
    CONFIG=$CONFIG_ARG
elif [[ -n "${SWITCHBOARD_CONFIG:-}" ]]; then
    CONFIG=$SWITCHBOARD_CONFIG
elif [[ -f "$USER_CONFIG" ]]; then
    CONFIG=$USER_CONFIG
elif [[ -f /etc/spark-switchboard/switchboard.toml ]]; then
    CONFIG=/etc/spark-switchboard/switchboard.toml
else
    CONFIG=""
fi
if [[ -n "$CONFIG" && ! -f "$CONFIG" ]]; then
    say "Config file $CONFIG does not exist."
    exit 1
fi
if [[ -z "$CONFIG" ]]; then
    dir=$(dirname "$USER_CONFIG")
    say "No config found. Copying the examples to $dir/"
    run mkdir -p "$dir/services"
    copied=()
    [[ -e "$dir/switchboard.toml" ]] || { run cp "$REPO/examples/switchboard.toml" "$dir/switchboard.toml"; copied+=("$dir/switchboard.toml"); }
    for f in "$REPO"/examples/services/*.toml; do
        [[ -e "$dir/services/$(basename "$f")" ]] && continue
        [[ "$(head -1 "$f")" == "# DEPRECATED"* ]] && continue      # kept for old configs only
        run cp "$f" "$dir/services/"
        copied+=("$dir/services/$(basename "$f")")
    done
    # The examples name the launchers under ~/src/spark-switchboard; point them at this checkout.
    if [[ "$REPO" != "$HOME/src/spark-switchboard" && ${#copied[@]} -gt 0 ]]; then
        run sed -i "s|~/src/spark-switchboard/|$(esc "$REPO")/|g" "${copied[@]}"
    fi
    say ""
    say "Edit $dir/switchboard.toml and the service files in $dir/services/"
    say "(set enabled = true for each app you have installed), then rerun: $0"
    exit 0
fi
CONFIG=$(readlink -f "$CONFIG")

# 4. check
if [[ -x "$SB" ]]; then
    "$SB" check -c "$CONFIG"
else
    say "would run: $SB check -c $CONFIG"
fi

# 5. systemd unit (and the optional sudoers rule)
# Does a service in the config name docker containers (`containers`, enabled or not)? Exit 0 yes, 1 no, 2 can't tell.
# Plain tomllib, so it works before the venv exists (dry run).
uses_containers() {
    "$PY" - "$CONFIG" <<'EOF'
import glob, os, sys, tomllib
from pathlib import Path
main = Path(sys.argv[1])
try:
    raw = tomllib.loads(main.read_text())
    files = []
    for pattern in raw.get("include", []):
        p = Path(os.path.expandvars(os.path.expanduser(pattern)))
        files += glob.glob(str(p if p.is_absolute() else main.parent / p))
    services = list(raw.get("service", {}).values()) + [tomllib.loads(Path(f).read_text()) for f in files]
except (OSError, tomllib.TOMLDecodeError):
    sys.exit(2)
sys.exit(0 if any(s.get("containers") for s in services) else 1)
EOF
}
# Docker access is root-equivalent: the service gets the docker group only when an app it runs uses containers.
AFTER_DOCKER=" docker.service"
if uses_containers; [[ $? == 1 ]]; then
    SUPP="# SupplementaryGroups=docker left out: no service in the config names containers"
    AFTER_DOCKER=""
elif getent group docker >/dev/null; then
    SUPP="SupplementaryGroups=docker"
    if ! id -nG "$USER_NAME" | grep -qw docker; then
        say "warning: $USER_NAME is not in the docker group. The service gets it through SupplementaryGroups, but"
        say "launchers run by hand or from NVIDIA Sync don't. To fix: sudo usermod -aG docker $USER_NAME (then log in again)"
    fi
else
    SUPP="# SupplementaryGroups=docker left out: this machine has no docker group"
fi
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
sed -e "s|@USER@|$(esc "$USER_NAME")|g" -e "s|@GROUP@|$(esc "$GROUP_NAME")|g" -e "s|@SUPPLEMENTARY_GROUPS@|$SUPP|" \
    -e "s|@AFTER_DOCKER@|$AFTER_DOCKER|" \
    -e "s|@HOME@|$(esc "$HOME")|g" -e "s|@VENV@|$(esc "$VENV")|g" -e "s|@CONFIG@|$(esc "$CONFIG")|g" \
    "$REPO/deploy/spark-switchboard.service" >"$TMP/spark-switchboard.service"
if ((DRY)); then
    say "--- $UNIT would contain:"
    cat "$TMP/spark-switchboard.service"
    say "---"
fi
if command -v systemd-analyze >/dev/null; then
    systemd-analyze verify "$TMP/spark-switchboard.service" 2>&1 | grep -v -e 'Unit .* not found' -e '^$' || true
fi

if ((DROP_CACHES)); then
    sed "s|@USER@|$(esc "$USER_NAME")|g" "$REPO/deploy/sudoers.d/spark-switchboard" >"$TMP/sudoers"
    chmod 0440 "$TMP/sudoers"
    VISUDO=$(command -v visudo || echo /usr/sbin/visudo)
    if ! "$VISUDO" -cf "$TMP/sudoers" >/dev/null; then
        say "The rendered sudoers rule did not pass visudo -cf; not installing it:"
        cat "$TMP/sudoers"
        exit 1
    fi
    run sudo install -m 0440 -o root -g root "$TMP/sudoers" "$SUDOERS"
    ((DRY)) || say "$SUDOERS: $USER_NAME may now run 'sudo -n /usr/bin/tee /proc/sys/vm/drop_caches' without a password."
    say "To use it, uncomment free_floor_gib and reclaim_command under [memory] in $CONFIG."
fi

run sudo install -m 0644 "$TMP/spark-switchboard.service" "$UNIT"
run sudo systemctl daemon-reload
run sudo systemctl enable spark-switchboard
run sudo systemctl restart spark-switchboard
if ((!DRY)); then
    sleep 2
    systemctl --no-pager --lines=0 status spark-switchboard | head -5 || true
fi

# The dashboard URL, from the config (hostname and admin_port). A configured hostname is used as it is (it may be a
# VPN name, where .local doesn't resolve). Without one: the system hostname plus .local (mDNS, as on DGX OS), as
# clients on the LAN usually reach a DGX Spark.
local_name() { local h; h=$(hostname); [[ "$h" == *.* ]] && echo "$h" || echo "$h.local"; }
URL=""
if [[ -x "$VENV/bin/python" ]]; then
    URL=$("$VENV/bin/python" -c 'import sys, tomllib
from spark_switchboard import config
c = config.load(sys.argv[1])
with open(sys.argv[1], "rb") as f:
    configured = tomllib.load(f).get("hostname")
host = c.hostname if configured or "." in c.hostname else c.hostname + ".local"
print(f"http://{host}:{c.admin_port}/")' "$CONFIG" 2>/dev/null) || URL=""
fi
say "dashboard: ${URL:-http://$(local_name):8800/}"
