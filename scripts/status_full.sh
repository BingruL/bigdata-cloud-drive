#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT_DIR/.run"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-cloud-drive-kafka}"

print_status() {
  printf '%-24s %s\n' "$1" "$2"
}

jps_has() {
  command -v jps >/dev/null 2>&1 && jps | awk '{print $2}' | grep -qx "$1"
}

expected_cmd_pattern() {
  case "$1" in
    flask) printf 'run.py' ;;
    kafka-consumer) printf 'backend.workers.log_consumer' ;;
    spark-streaming) printf 'spark_jobs/streaming_stats.py' ;;
    *) return 1 ;;
  esac
}

pid_status() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"
  local pid pattern cmdline

  if [[ ! -s "$pid_file" ]]; then
    print_status "$name" "stopped"
    return 0
  fi

  pid="$(cat "$pid_file")"
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    print_status "$name" "stopped"
    return 0
  fi

  pattern="$(expected_cmd_pattern "$name")" || pattern=""
  cmdline="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)"
  if [[ -n "$pattern" && "$cmdline" != *"$pattern"* ]]; then
    print_status "$name" "stopped (stale pid $pid)"
    return 0
  fi

  print_status "$name" "running (pid $pid)"
}

port_open() {
  local host="$1"
  local port="$2"

  [[ -x "$PYTHON_BIN" ]] || return 1
  "$PYTHON_BIN" - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

host, port = sys.argv[1], int(sys.argv[2])
with socket.create_connection((host, port), timeout=1):
    pass
PY
}

print_status "HDFS NameNode" "$(jps_has NameNode && echo running || echo stopped)"
print_status "HDFS DataNode" "$(jps_has DataNode && echo running || echo stopped)"
print_status "HBase HMaster" "$(jps_has HMaster && echo running || echo stopped)"
print_status "HBase RegionServer" "$(jps_has HRegionServer && echo running || echo stopped)"
print_status "HBase ThriftServer" "$(jps_has ThriftServer && echo running || echo stopped)"
print_status "HBase 9090" "$(port_open localhost 9090 && echo reachable || echo closed)"

if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$KAFKA_CONTAINER"; then
  print_status "Kafka container" "running"
else
  print_status "Kafka container" "stopped"
fi
print_status "Kafka 9092" "$(port_open localhost 9092 && echo reachable || echo closed)"

pid_status flask
pid_status kafka-consumer
pid_status spark-streaming

printf '\nLogs:\n'
printf '  logs/flask.log\n'
printf '  logs/kafka-consumer.log\n'
printf '  logs/spark-streaming.log\n'
