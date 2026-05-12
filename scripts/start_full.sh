#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
PID_DIR="$ROOT_DIR/.run"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-cloud-drive-kafka}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.7.2}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_TOPIC_EVENTS="${KAFKA_TOPIC_EVENTS:-cloud_drive_events}"
HBASE_HOST="${HBASE_HOST:-localhost}"
HBASE_PORT="${HBASE_PORT:-9090}"
SPARK_KAFKA_PACKAGE="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0}"

mkdir -p "$LOG_DIR" "$PID_DIR"

info() {
  printf '[start-full] %s\n' "$*"
}

warn() {
  printf '[start-full] WARN: %s\n' "$*" >&2
}

fail() {
  printf '[start-full] ERROR: %s\n' "$*" >&2
  exit 1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

jps_has() {
  command_exists jps && jps | awk '{print $2}' | grep -qx "$1"
}

expected_cmd_pattern() {
  case "$1" in
    flask) printf 'run.py' ;;
    kafka-consumer) printf 'backend.workers.log_consumer' ;;
    spark-streaming) printf 'spark_jobs/streaming_stats.py' ;;
    *) return 1 ;;
  esac
}

pid_is_running() {
  local name="$1"
  local pid_file="$2"
  local pid pattern cmdline

  [[ -s "$pid_file" ]] || return 1
  pid="$(cat "$pid_file")"
  kill -0 "$pid" >/dev/null 2>&1 || return 1

  pattern="$(expected_cmd_pattern "$name")" || return 0
  cmdline="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)"
  [[ "$cmdline" == *"$pattern"* ]]
}

start_background() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"
  local log_file="$LOG_DIR/$name.log"
  shift

  if pid_is_running "$name" "$pid_file"; then
    info "$name already running (pid $(cat "$pid_file"))"
    return 0
  fi

  info "starting $name, log: $log_file"
  (
    cd "$ROOT_DIR"
    nohup "$@" >"$log_file" 2>&1 &
    echo $! >"$pid_file"
  )
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local label="$3"
  local attempt

  for attempt in $(seq 1 30); do
    if "$PYTHON_BIN" - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

host, port = sys.argv[1], int(sys.argv[2])
with socket.create_connection((host, port), timeout=1):
    pass
PY
    then
      info "$label is reachable at $host:$port"
      return 0
    fi
    sleep 1
  done

  warn "$label did not become reachable at $host:$port within 30s"
  return 1
}

ensure_python() {
  [[ -x "$PYTHON_BIN" ]] || fail "Python not found: $PYTHON_BIN. Run: source .venv/bin/activate"
}

ensure_hdfs_hbase() {
  if ! command_exists jps; then
    warn "jps not found; skipping Hadoop/HBase process detection"
    return 0
  fi

  if ! jps_has NameNode || ! jps_has DataNode; then
    command_exists start-dfs.sh || fail "start-dfs.sh not found in PATH"
    info "starting HDFS"
    start-dfs.sh
  else
    info "HDFS already running"
  fi

  if ! jps_has HMaster || ! jps_has HRegionServer; then
    command_exists start-hbase.sh || fail "start-hbase.sh not found in PATH"
    info "starting HBase"
    start-hbase.sh
  else
    info "HBase already running"
  fi

  if ! jps_has ThriftServer; then
    command_exists hbase || fail "hbase command not found in PATH"
    info "starting HBase ThriftServer on port $HBASE_PORT"
    nohup hbase thrift start >"$LOG_DIR/hbase-thrift.log" 2>&1 &
    echo $! >"$PID_DIR/hbase-thrift.pid"
  else
    info "HBase ThriftServer already running"
  fi

  wait_for_port "$HBASE_HOST" "$HBASE_PORT" "HBase ThriftServer" || true
}

ensure_kafka() {
  command_exists docker || fail "docker command not found"
  docker ps >/dev/null 2>&1 || fail "cannot access Docker daemon. Start Docker Desktop and ensure WSL integration is enabled."

  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$KAFKA_CONTAINER"; then
    info "Kafka container already running"
    return 0
  fi

  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$KAFKA_CONTAINER"; then
    info "starting existing Kafka container"
    docker start cloud-drive-kafka
  else
    info "creating Kafka container from $KAFKA_IMAGE"
    docker run -d --name "$KAFKA_CONTAINER" -p 9092:9092 "$KAFKA_IMAGE"
  fi

  wait_for_port localhost 9092 "Kafka" || true
}

start_flask() {
  # Equivalent manual command: KAFKA_ENABLED=1 KAFKA_BOOTSTRAP=localhost:9092 python run.py
  start_background flask \
    env KAFKA_ENABLED=1 KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" "$PYTHON_BIN" run.py
}

start_consumer() {
  start_background kafka-consumer \
    env KAFKA_ENABLED=1 KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" HBASE_HOST="$HBASE_HOST" HBASE_PORT="$HBASE_PORT" \
    "$PYTHON_BIN" -m backend.workers.log_consumer
}

start_streaming() {
  if ! command_exists spark-submit; then
    warn "spark-submit not found; skipping Spark Streaming"
    return 0
  fi

  start_background spark-streaming \
    env PYSPARK_PYTHON="$PYTHON_BIN" KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" KAFKA_TOPIC_EVENTS="$KAFKA_TOPIC_EVENTS" \
    HBASE_HOST="$HBASE_HOST" HBASE_PORT="$HBASE_PORT" \
    spark-submit --master 'local[*]' --packages "$SPARK_KAFKA_PACKAGE" spark_jobs/streaming_stats.py
}

main() {
  ensure_python
  ensure_hdfs_hbase
  ensure_kafka
  start_flask
  start_consumer
  start_streaming

  info "startup requested"
  info "status: scripts/status_full.sh"
  info "logs: tail -f logs/flask.log logs/kafka-consumer.log logs/spark-streaming.log"
  info "app: http://localhost:5000"
}

main "$@"
