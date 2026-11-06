#!/usr/bin/env bash
# soak.sh — 10k idle conns + 1k chat loop (spec §Testing: 1h, watch RSS/fds/drops)
#
# Usage:
#   ./tools/soak.sh [--dry-run] [--duration SECS] [--idle N] [--chat N]
#                   [--host H] [--port P] [--binary PATH] [--data-dir DIR]
#
# Defaults (per spec): IDLE=10000 idle + CHAT=1000 chat, DURATION=3600s (1h).
# Requires: python3 for socket churn. Run server separately or let this
# script start one in a temp data dir if --binary is given.
set -euo pipefail

DRY_RUN=0
DURATION=3600
IDLE=10000
CHAT=1000
HOST="127.0.0.1"
PORT=7878
BINARY=""
DATA_DIR=""
LOG=""

usage() { cat <<'EOF'
Usage: ./tools/soak.sh [--dry-run] [--duration SECS] [--idle N] [--chat N]
                       [--host H] [--port P] [--binary PATH] [--data-dir DIR]
  --dry-run       Validate inputs and print plan only (no sockets/connections).
  --duration SECS Soak duration (default 3600 = 1h).
  --idle N        Idle conns to hold open (default 10000).
  --chat N        Active chat conns in pub/sub loop (default 1000).
  --binary PATH   Optional: start server binary (fresh temp data dir unless --data-dir set).
  --data-dir DIR  WAL/snapshot dir for the server (default: mktemp -d if --binary set).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --duration) DURATION="${2:?}"; shift ;;
    --idle) IDLE="${2:?}"; shift ;;
    --chat) CHAT="${2:?}"; shift ;;
    --host) HOST="${2:?}"; shift ;;
    --port) PORT="${2:?}"; shift ;;
    --binary) BINARY="${2:?}"; shift ;;
    --data-dir) DATA_DIR="${2:?}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

for n in "$DURATION" "$IDLE" "$CHAT" "$PORT"; do
  case "$n" in ''|*[!0-9]*) echo "error: expected non-negative integer, got '$n'" >&2; exit 2;; esac
done
[ "$IDLE" -le 100000 ] || { echo "error: idle $IDLE exceeds MAX_CONNS 100000 (src/op.rs)" >&2; exit 2; }
[ "$CHAT" -le 10000 ] || { echo "error: chat $CHAT exceeds MAX_SUBS 10000 per topic (src/op.rs)" >&2; exit 2; }

LOG="$(pwd)/soak-$(date +%Y%m%d-%H%M%S).log"
echo "[soak] host=$HOST port=$PORT idle=$IDLE chat=$CHAT duration=${DURATION}s log=$LOG"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "[dry-run] would: build (if --binary), start server, open $IDLE idle + $CHAT chat conns, run chat loop ${DURATION}s, sample RSS/fds every 30s"
  echo "[dry-run] limits: max_conns=100000 max_subs_per_topic=10000 out_cap=256KiB max_key=4KiB max_value=64KiB"
  exit 0
fi

command -v python3 >/dev/null 2>&1 || { echo "error: python3 required" >&2; exit 1; }

SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

if [ -n "$BINARY" ]; then
  [ -z "$DATA_DIR" ] && DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/soak-data.XXXXXX")"
  mkdir -p "$DATA_DIR"
  echo "[soak] starting $BINARY --data-dir $DATA_DIR --port $PORT"
  "$BINARY" --data-dir "$DATA_DIR" --port "$PORT" >>"$LOG" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 30); do
    (echo > /dev/tcp/"$HOST"/"$PORT") 2>/dev/null && break || sleep 1
  done
fi

echo "[soak] running idle=$IDLE chat=$CHAT duration=${DURATION}s log=$LOG"
python3 - "$HOST" "$PORT" "$IDLE" "$CHAT" "$DURATION" >>"$LOG" 2>&1 <<'PY'
import socket, sys, time, threading
host, port, idle, chat, dur = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
stop = threading.Event()
def hold_idle(i):
    try:
        s = socket.create_connection((host, port), timeout=5)
        while not stop.is_set():
            time.sleep(1.0)
        s.close()
    except Exception as e:
        print(f"idle[{i}] err: {e}", flush=True)
def chat_loop(i):
    try:
        s = socket.create_connection((host, port), timeout=5)
        s.settimeout(2.0)
        n = 0
        while not stop.is_set():
            # TODO: frame_put/frame_pub once TCP adapter lands; send len+opcode placeholder then recv
            s.sendall(b"\x01\x00\x00\x00\x0bmsg-" + str(n).encode())
            n += 1
            try:
                s.recv(4096)
            except socket.timeout:
                pass
        s.close()
    except Exception as e:
        print(f"chat[{i}] err: {e}", flush=True)
threads = []
for i in range(idle):
    t = threading.Thread(target=hold_idle, args=(i,), daemon=True); t.start(); threads.append(t)
for i in range(chat):
    t = threading.Thread(target=chat_loop, args=(i,), daemon=True); t.start(); threads.append(t)
print(f"[py] spawned idle={idle} chat={chat}", flush=True)
time.sleep(dur)
stop.set()
for t in threads:
    t.join(timeout=2)
print("[py] done", flush=True)
PY

echo "[soak] done. Watch: RSS / fd count / drops (see docs/OPERATIONS.md §Soak)"
echo "[soak] tail $LOG"
