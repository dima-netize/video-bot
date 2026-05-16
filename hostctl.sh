#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

COMPOSE_FILES=(-f docker-compose.yml)
if [[ "${2:-}" == "prod" || "${HOST_MODE:-}" == "prod" ]]; then
  COMPOSE_FILES=(-f docker-compose.prod.yml)
fi

ensure_files() {
  [[ -f .env ]] || cp .env.example .env
  [[ -f bot_stats.json ]] || echo '{"success":0,"errors":0,"bytes":0,"platforms":{}}' > bot_stats.json
  [[ -f bot_settings.json ]] || echo '{"quality":{}}' > bot_settings.json
  [[ -f bot_subscribers.json ]] || echo '[]' > bot_subscribers.json
  [[ -f cookies.txt ]] || touch cookies.txt
}

compose() {
  docker compose "${COMPOSE_FILES[@]}" "$@"
}

case "${1:-}" in
  init)
    ensure_files
    echo "Initialized. Fill TOKEN in .env"
    ;;
  start)
    ensure_files
    compose up -d --build
    ;;
  stop)
    compose down
    ;;
  restart)
    ensure_files
    compose up -d --build --force-recreate
    ;;
  status)
    compose ps
    ;;
  logs)
    compose logs -f --tail=150
    ;;
  pull)
    git pull --rebase
    ;;
  *)
    echo "Usage: $0 {init|start|stop|restart|status|logs|pull} [prod]"
    exit 2
    ;;
esac
