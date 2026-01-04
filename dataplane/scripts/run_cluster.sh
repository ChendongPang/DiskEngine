#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

GW_ADDR="127.0.0.1:8000"
N1_ADDR="127.0.0.1:7001"
N2_ADDR="127.0.0.1:7002"
N3_ADDR="127.0.0.1:7003"

EPOCH=1
WQ=2

IMG_SIZE=$((64 * 1024 * 1024))

LOGDIR="${ROOT}/_logs"
RUNDIR="${ROOT}/_run"
mkdir -p "$LOGDIR" "$RUNDIR"

PID_N1="${RUNDIR}/n1.pid"
PID_N2="${RUNDIR}/n2.pid"
PID_N3="${RUNDIR}/n3.pid"
PID_GW="${RUNDIR}/gw.pid"

cleanup() {
  echo "[cleanup] stopping..."
  for f in "$PID_GW" "$PID_N1" "$PID_N2" "$PID_N3"; do
    if [[ -f "$f" ]]; then
      pid="$(cat "$f" || true)"
      if [[ -n "${pid}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
        kill "$pid" >/dev/null 2>&1 || true
      fi
      rm -f "$f"
    fi
  done
}
trap cleanup EXIT

cmd="${1:-start}"

build_all() {
  echo "[build] gen proto (if needed)..."
  if [[ -f "${ROOT}/scripts/gen_proto.sh" ]]; then
    bash "${ROOT}/scripts/gen_proto.sh"
  fi

  echo "[build] binaries..."
  go build -o "${RUNDIR}/storagenode" ./cmd/storagenode
  go build -o "${RUNDIR}/gateway"    ./cmd/gateway
  go build -o "${RUNDIR}/client"     ./cmd/client
}

start_nodes() {
  echo "[start] storagenodes (all backup initially)..."

  "${RUNDIR}/storagenode" \
    -addr "${N1_ADDR}" -img "${RUNDIR}/node1.img" -size "${IMG_SIZE}" \
    -role backup -epoch "${EPOCH}" -wq "${WQ}" \
    >"${LOGDIR}/node1.log" 2>&1 &
  echo $! >"${PID_N1}"

  "${RUNDIR}/storagenode" \
    -addr "${N2_ADDR}" -img "${RUNDIR}/node2.img" -size "${IMG_SIZE}" \
    -role backup -epoch "${EPOCH}" -wq "${WQ}" \
    >"${LOGDIR}/node2.log" 2>&1 &
  echo $! >"${PID_N2}"

  "${RUNDIR}/storagenode" \
    -addr "${N3_ADDR}" -img "${RUNDIR}/node3.img" -size "${IMG_SIZE}" \
    -role backup -epoch "${EPOCH}" -wq "${WQ}" \
    >"${LOGDIR}/node3.log" 2>&1 &
  echo $! >"${PID_N3}"

  sleep 0.3
}

start_gateway() {
  echo "[start] gateway (bootstrap configure)..."
  "${RUNDIR}/gateway" \
    -addr "${GW_ADDR}" \
    -primary "${N1_ADDR}" \
    -backups "${N2_ADDR},${N3_ADDR}" \
    -epoch "${EPOCH}" \
    -wq "${WQ}" \
    -bootstrap=true \
    >"${LOGDIR}/gateway.log" 2>&1 &
  echo $! >"${PID_GW}"

  sleep 0.3
  echo "[ok] gateway up at ${GW_ADDR}"
  echo "  logs: ${LOGDIR}"
  echo "  client: ${RUNDIR}/client -addr ${GW_ADDR} put k1 v1"
}

kill_primary() {
  # MVP：primary 初始就是 node1；failover 后 primary 会变，但脚本不追踪（可扩展）。
  if [[ -f "${PID_N1}" ]]; then
    pid="$(cat "${PID_N1}")"
    echo "[kill_primary] killing node1 primary pid=${pid}"
    kill "${pid}" || true
    rm -f "${PID_N1}"
    echo "[kill_primary] done. now try write via gateway and it should failover (epoch++)"
  else
    echo "[kill_primary] node1 pid not found"
  fi
}

case "$cmd" in
  start)
    build_all
    start_nodes
    start_gateway
    echo "[test] try:"
    echo "  ${RUNDIR}/client -addr ${GW_ADDR} put k1 v1"
    echo "  ${RUNDIR}/client -addr ${GW_ADDR} get k1"
    echo "  ${RUNDIR}/client -addr ${GW_ADDR} del k1"
    echo "  ${RUNDIR}/client -addr ${GW_ADDR} get k1"
    echo
    echo "[failover] simulate:"
    echo "  $0 kill_primary"
    echo "  ${RUNDIR}/client -addr ${GW_ADDR} put k2 v2"
    ;;
  kill_primary)
    kill_primary
    ;;
  *)
    echo "usage: $0 {start|kill_primary}"
    exit 1
    ;;
esac

# keep running in foreground when start
if [[ "$cmd" == "start" ]]; then
  echo "[running] Ctrl+C to stop."
  while true; do
    sleep 3600
  done
fi
