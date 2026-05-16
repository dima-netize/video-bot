#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$ROOT_DIR/bot.pid"
LOG_FILE="$ROOT_DIR/bot.log"

start_bot() {
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Bot already running with PID $(cat "$PID_FILE")"
    exit 0
  fi
  cd "$ROOT_DIR"
  nohup python app.py >> "$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  echo "Bot started. PID=$(cat "$PID_FILE") log=$LOG_FILE"
}

stop_bot() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "Bot is not running (no pid file)."
    exit 0
  fi
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    sleep 1
    if kill -0 "$PID" 2>/dev/null; then
      echo "Force stopping PID $PID"
      kill -9 "$PID" || true
    fi
    echo "Bot stopped"
  else
    echo "Process $PID not found"
  fi
  rm -f "$PID_FILE"
}

status_bot() {
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "RUNNING PID=$(cat "$PID_FILE")"
  else
    echo "STOPPED"
    exit 1
  fi
}

logs_bot() {
  touch "$LOG_FILE"
  tail -n 80 -f "$LOG_FILE"
}

case "${1:-}" in
  start) start_bot ;;
  stop) stop_bot ;;
  restart) stop_bot || true; start_bot ;;
  status) status_bot ;;
  logs) logs_bot ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs}"
    exit 2
    ;;
esac
