#!/usr/bin/env bash
set -euo pipefail

# Run a local 3-node Primary/Backup cluster on one machine.
# - meta-coordinator: 127.0.0.1:9100
# - gateway:          127.0.0.1:8000
# - storagenode1:     127.0.0.1:7001  (initial primary)
# - storagenode2:     127.0.0.1:7002
# - storagenode3:     127.0.0.1:7003
#
# Usage:
#   ./scripts/run_cluster.sh start
#   ./scripts/run_cluster.sh stop
#   ./scripts/run_cluster.sh status
#   ./scripts/run_cluster.sh kill_primary

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUN_DIR="${ROOT}/.run"
LOG_DIR="${RUN_DIR}/logs"
PID_DIR="${RUN_DIR}/pids"

GW_ADDR="127.0.0.1:8000"
META_ADDR="127.0.0.1:9100"
N1_ADDR="127.0.0.1:7001"
N2_ADDR="127.0.0.1:7002"
N3_ADDR="127.0.0.1:7003"

EPOCH=1
WQ=2
NUM_SHARDS=1
IMG_SIZE=$((64 * 1024 * 1024))  # 64MiB

BIN_META="${RUN_DIR}/metacoordinator"
BIN_GW="${RUN_DIR}/gateway"
BIN_SN="${RUN_DIR}/storagenode"
BIN_CLIENT="${RUN_DIR}/client"

PID_META="${PID_DIR}/meta.pid"
PID_GW="${PID_DIR}/gw.pid"
PID_N1="${PID_DIR}/n1.pid"
PID_N2="${PID_DIR}/n2.pid"
PID_N3="${PID_DIR}/n3.pid"

mkdir -p "${RUN_DIR}" "${LOG_DIR}" "${PID_DIR}"

die() { echo "[fatal] $*" >&2; exit 1; }

is_running() {
  local pid_file="$1"
  [[ -f "${pid_file}" ]] || return 1
  local pid
  pid="$(cat "${pid_file}")"
  kill -0 "${pid}" >/dev/null 2>&1
}

kill_pidfile() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}")" || true
    if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
      sleep 0.2
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${pid_file}"
  fi
}

build_bins() {
  echo "[build] go build -> ${RUN_DIR}"
  go build -o "${BIN_META}"   ./cmd/metacoordinator
  go build -o "${BIN_GW}"     ./cmd/gateway
  go build -o "${BIN_SN}"     ./cmd/storagenode
  go build -o "${BIN_CLIENT}" ./cmd/client
  echo "[build] ok"
}

start_meta() {
  if is_running "${PID_META}"; then
    echo "[start] meta already running (pid=$(cat "${PID_META}"))"
    return
  fi
  echo "[start] meta-coordinator @ ${META_ADDR}"
  "${BIN_META}" -addr "${META_ADDR}" >"${LOG_DIR}/meta.log" 2>&1 &
  echo $! >"${PID_META}"
  sleep 0.2
}

start_node() {
  local name="$1" addr="$2" pidfile="$3" img="$4" oplog="$5" log="$6"
  if is_running "${pidfile}"; then
    echo "[start] ${name} already running (pid=$(cat "${pidfile}"))"
    return
  fi
  echo "[start] ${name} @ ${addr}"
  "${BIN_SN}" \
    -addr "${addr}" \
    -img "${img}" \
    -dev_size "${IMG_SIZE}" \
    -oplog "${oplog}" \
    >"${log}" 2>&1 &
  echo $! >"${pidfile}"
  sleep 0.2
}

start_gateway() {
  if is_running "${PID_GW}"; then
    echo "[start] gateway already running (pid=$(cat "${PID_GW}"))"
    return
  fi
  echo "[start] gateway @ ${GW_ADDR} (bootstrap meta)"
  "${BIN_GW}" \
    -addr "${GW_ADDR}" \
    -meta "${META_ADDR}" \
    -shards "${NUM_SHARDS}" \
    -primary "${N1_ADDR}" \
    -backups "${N2_ADDR},${N3_ADDR}" \
    -epoch "${EPOCH}" \
    -wq "${WQ}" \
    -bootstrap=true \
    >"${LOG_DIR}/gateway.log" 2>&1 &
  echo $! >"${PID_GW}"
  sleep 0.2
}

start_all() {
  build_bins

  start_meta

  start_node "node1" "${N1_ADDR}" "${PID_N1}" "${RUN_DIR}/node1.img" "${RUN_DIR}/node1.oplog" "${LOG_DIR}/node1.log"
  start_node "node2" "${N2_ADDR}" "${PID_N2}" "${RUN_DIR}/node2.img" "${RUN_DIR}/node2.oplog" "${LOG_DIR}/node2.log"
  start_node "node3" "${N3_ADDR}" "${PID_N3}" "${RUN_DIR}/node3.img" "${RUN_DIR}/node3.oplog" "${LOG_DIR}/node3.log"

  start_gateway

  echo
  echo "[ok] cluster is up"
  echo "  meta:   ${META_ADDR}"
  echo "  gw:     ${GW_ADDR}"
  echo "  nodes:  ${N1_ADDR}, ${N2_ADDR}, ${N3_ADDR} (primary=${N1_ADDR})"
  echo "  logs:   ${LOG_DIR}"
  echo
  echo "Try:"
  echo "  ${BIN_CLIENT} -addr ${GW_ADDR} put k1 v1"
  echo "  ${BIN_CLIENT} -addr ${GW_ADDR} get k1"
  echo "  ${BIN_CLIENT} -addr ${GW_ADDR} del k1"
}

stop_all() {
  echo "[stop] killing processes (if any)..."
  kill_pidfile "${PID_GW}"
  kill_pidfile "${PID_N1}"
  kill_pidfile "${PID_N2}"
  kill_pidfile "${PID_N3}"
  kill_pidfile "${PID_META}"
  echo "[stop] done"
}

status() {
  echo "[status]"
  for x in "meta:${PID_META}" "gw:${PID_GW}" "node1:${PID_N1}" "node2:${PID_N2}" "node3:${PID_N3}"; do
    name="${x%%:*}"
    pf="${x#*:}"
    if is_running "${pf}"; then
      echo "  ${name}: running (pid=$(cat "${pf}"))"
    else
      echo "  ${name}: stopped"
    fi
  done
  echo
  echo "Logs: ${LOG_DIR}"
}

kill_primary() {
  # MVP：初始 primary=node1。脚本不追踪 failover 后的 primary（可扩展）。
  if is_running "${PID_N1}"; then
    pid="$(cat "${PID_N1}")"
    echo "[kill_primary] killing node1 pid=${pid}"
    kill "${pid}" >/dev/null 2>&1 || true
    sleep 0.2
    kill -9 "${pid}" >/dev/null 2>&1 || true
    rm -f "${PID_N1}"
    echo "[kill_primary] done. Now try a write via gateway; it should trigger failover (epoch++)."
  else
    echo "[kill_primary] node1 not running"
  fi
}

cmd="${1:-}"
case "${cmd}" in
  start)        start_all ;;
  stop)         stop_all ;;
  status)       status ;;
  kill_primary) kill_primary ;;
  *)
    echo "usage: $0 {start|stop|status|kill_primary}"
    exit 1
    ;;
esac

# keep running in foreground when start, so Ctrl+C stops your terminal session
if [[ "${cmd}" == "start" ]]; then
  echo "[running] Ctrl+C to stop (processes keep running; use '$0 stop' to stop all)."
  while true; do sleep 3600; done
fi
