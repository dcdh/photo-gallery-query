#!/usr/bin/env sh
podman ps -aq | xargs podman stop | xargs podman rm
export DOCKER_HOST=unix:///run/user/${UID}/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
mvn clean install
podman ps -aq | xargs podman stop | xargs podman rm
