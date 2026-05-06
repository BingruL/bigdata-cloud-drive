#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT_DIR/.run"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-cloud-drive-kafka}"

info() {
  printf '[stop-full] %s\n' "$*"
}

stop_pid() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"

  if [[ ! -s "$pid_file" ]]; then
    info "$name not tracked"
    return 0
  fi

  local pid
  pid="$(cat "$pid_file")"
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    info "$name already stopped"
    rm -f "$pid_file"
    return 0
  fi

  info "stopping $name (pid $pid)"
  kill "$pid" >/dev/null 2>&1 || true
  sleep 2
  if kill -0 "$pid" >/dev/null 2>&1; then
    info "$name still running; sending SIGKILL"
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$pid_file"
}

stop_pid spark-streaming
stop_pid kafka-consumer
stop_pid flask

if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$KAFKA_CONTAINER"; then
  info "stopping Kafka container"
  docker stop cloud-drive-kafka >/dev/null
else
  info "Kafka container not running"
fi

info "HDFS and HBase are left running by default"
