#!/usr/bin/env bash
# open-webui.sh: Open WebUI in a docker container. For spark-switchboard, an NVIDIA Sync custom app, or by hand.
# Foreground: starts (or creates) the container, waits for /health, stays up, stops the container on exit.
# The container is recreated when the settings below differ from the ones it was created with (it carries them as a
# label). Chats, users and the secret key live in the data volume, so they survive that.
#
# Settings (environment):
#   OWUI_CONTAINER     open-webui                          container name (match `containers` and `kill_command`
#                                                          in the service file)
#   OWUI_IMAGE         ghcr.io/open-webui/open-webui:main  the :ollama tag bundles Ollama
#   OWUI_PORT          12000     host port to publish (switchboard passes an internal one)
#   OWUI_BIND          (empty)   host address to publish on; empty = every interface (switchboard passes 127.0.0.1)
#   OWUI_DATA_VOLUME   open-webui   docker volume (or host directory) for /app/backend/data. Set it to the volume
#                                   an existing Open WebUI container uses, to keep its data.
#   OWUI_OPENAI_URLS   (empty)   OpenAI-compatible endpoints, ';'-separated. From inside the container the host is
#                                host.docker.internal, e.g. http://host.docker.internal:8888/v1
#   OWUI_OPENAI_KEYS   "none" per URL   matching API keys, ';'-separated
#   OWUI_OLLAMA_URL    (empty)   an Ollama server for Open WebUI to use, e.g. http://host.docker.internal:11434
#   OWUI_OLLAMA_DIR    (empty)   host directory for the bundled Ollama's models (mounted at /root/.ollama)
#   OWUI_DOCKER_ARGS   (empty)   extra `docker run` arguments, e.g. "--gpus=all" for the :ollama image
#   OWUI_MIN_FREE_GIB  4         free memory (MemAvailable) needed to start
# plus the LAUNCHER_* and SWITCHBOARD_* settings described in common.sh.
# With neither OWUI_OLLAMA_URL, OWUI_OLLAMA_DIR nor an :ollama image, the Ollama connection is turned off
# (ENABLE_OLLAMA_API=False). Open WebUI stores its connection settings in its database after the first start, so
# a later change to OWUI_OPENAI_URLS may have to be made in its admin settings instead.
APP=open-webui
# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

NAME=${OWUI_CONTAINER:-open-webui}
IMAGE=${OWUI_IMAGE:-ghcr.io/open-webui/open-webui:main}
PORT=${OWUI_PORT:-12000}
BIND=${OWUI_BIND:-}
DATA=${OWUI_DATA_VOLUME:-open-webui}
OPENAI_URLS=${OWUI_OPENAI_URLS:-}
OLLAMA_URL=${OWUI_OLLAMA_URL:-}
OLLAMA_DIR=${OWUI_OLLAMA_DIR:-}
DOCKER_ARGS=${OWUI_DOCKER_ARGS:-}
URL="http://$(url_host "$BIND"):$PORT"

if [[ -n "$OPENAI_URLS" && -z "${OWUI_OPENAI_KEYS:-}" ]]; then
    OWUI_OPENAI_KEYS=$(awk -F';' '{ for (i = 1; i <= NF; i++) printf "%s%s", (i > 1 ? ";" : ""), "none" }' <<<"$OPENAI_URLS")
fi
OPENAI_KEYS=${OWUI_OPENAI_KEYS:-}

running() { [[ -n "$(docker ps -q --filter "name=^${NAME}$" --filter status=running 2>/dev/null)" ]]; }
exists() { docker container inspect "$NAME" >/dev/null 2>&1; }
health() { running; }

cleanup_hook() {
    handing_over && { log "handing over; container $NAME left running"; return; }
    if docker stop -t 20 "$NAME" >/dev/null 2>&1; then log "container $NAME stopped"; else log "container $NAME was not running"; fi
}

install_traps
claim_pidfile

docker info >/dev/null 2>&1 || fail "the docker daemon is not reachable (is $(id -un) in the docker group?)"

# What the container should look like; a container created with other settings (or by something else) is recreated.
WANT=$(printf '%s\n' "$IMAGE" "$BIND:$PORT" "$DATA" "$OPENAI_URLS" "$OPENAI_KEYS" "$OLLAMA_URL" "$OLLAMA_DIR" \
       "$DOCKER_ARGS" | sha256sum | cut -c1-16)
if exists; then
    have=$(docker container inspect "$NAME" --format '{{index .Config.Labels "spark-switchboard.settings"}}' 2>/dev/null)
    if [[ "$have" != "$WANT" ]]; then
        log "recreating container $NAME: its settings differ from this launcher's (data volume $DATA is kept)"
        running && docker stop -t 20 "$NAME" >/dev/null
        docker rm -f "$NAME" >/dev/null || fail "could not remove the old container $NAME"
    fi
fi

if running; then
    log "container $NAME is already running"
    admitted
else
    require_port_free "$PORT"
    require_memory "${OWUI_MIN_FREE_GIB:-4}"
    if exists; then
        say_status "starting container $NAME"
        docker start "$NAME" >/dev/null || fail "docker start $NAME failed"
    else
        say_status "creating container $NAME from $IMAGE (the first start pulls the image)"
        args=(-d --name "$NAME" -p "${BIND:+$BIND:}$PORT:8080" --add-host=host.docker.internal:host-gateway
              -v "$DATA:/app/backend/data" -e WEBUI_SECRET_KEY_FILE=/app/backend/data/.webui_secret_key
              --label "spark-switchboard.settings=$WANT")
        [[ -n "$OPENAI_URLS" ]] && args+=(-e "OPENAI_API_BASE_URLS=$OPENAI_URLS" -e "OPENAI_API_KEYS=$OPENAI_KEYS")
        [[ -n "$OLLAMA_URL" ]] && args+=(-e "OLLAMA_BASE_URL=$OLLAMA_URL")
        [[ -n "$OLLAMA_DIR" ]] && args+=(-v "$OLLAMA_DIR:/root/.ollama")
        if [[ -z "$OLLAMA_URL$OLLAMA_DIR" && "$IMAGE" != *:ollama* ]]; then args+=(-e ENABLE_OLLAMA_API=False); fi
        # shellcheck disable=SC2206  # OWUI_DOCKER_ARGS is a list of arguments
        extra=($DOCKER_ARGS)
        docker run "${args[@]}" "${extra[@]}" "$IMAGE" >/dev/null || fail "docker run failed"
    fi
fi

if ! wait_http "$URL/health" 300; then
    log "last container logs:"
    docker logs --tail 30 "$NAME" 2>&1
    fail "Open WebUI did not become healthy on $URL"
fi
say_status "Open WebUI ready on $URL"
keepalive health
