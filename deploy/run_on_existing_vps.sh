#!/usr/bin/env bash
set -euo pipefail

# Швидкий запуск/перезапуск стеку на вже існуючому VPS по SSH.
# Приклад:
# bash deploy/run_on_existing_vps.sh \
#   --host 178.197.199.106 \
#   --user Dima \
#   --repo https://github.com/<user>/<repo>.git \
#   --bot-token 123:ABC \
#   --admin-username dimagymenjuk \
#   --panel-token MyPanelPass123 \
#   --panel-secret SuperSecret987

HOST=""
SSH_USER="root"
REPO_URL=""
BOT_TOKEN=""
ADMIN_USERNAME="dimagymenjuk"
PANEL_TOKEN=""
PANEL_SECRET=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --user) SSH_USER="$2"; shift 2 ;;
    --repo) REPO_URL="$2"; shift 2 ;;
    --bot-token) BOT_TOKEN="$2"; shift 2 ;;
    --admin-username) ADMIN_USERNAME="$2"; shift 2 ;;
    --panel-token) PANEL_TOKEN="$2"; shift 2 ;;
    --panel-secret) PANEL_SECRET="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,90p' "$0"
      exit 0
      ;;
    *)
      echo "Невідомий аргумент: $1"
      exit 2
      ;;
  esac
done

if [[ -z "$HOST" || -z "$REPO_URL" || -z "$BOT_TOKEN" || -z "$PANEL_TOKEN" || -z "$PANEL_SECRET" ]]; then
  echo "Помилка: потрібно задати --host --repo --bot-token --panel-token --panel-secret"
  exit 1
fi

command -v ssh >/dev/null || { echo "Потрібен ssh"; exit 1; }

echo "[1/2] Підключаюсь до $SSH_USER@$HOST і оновлюю репозиторій..."
ssh -o StrictHostKeyChecking=accept-new "$SSH_USER@$HOST" bash <<REMOTE
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
if command -v sudo >/dev/null 2>&1; then
  SUDO='sudo'
else
  SUDO=''
fi

\$SUDO apt-get update -y
\$SUDO apt-get install -y git curl

if [[ ! -d /opt/video-bot/.git ]]; then
  \$SUDO git clone "$REPO_URL" /opt/video-bot
else
  cd /opt/video-bot
  \$SUDO git pull --rebase
fi
REMOTE

echo "[2/2] Запускаю bootstrap + production стек..."
ssh -o StrictHostKeyChecking=accept-new "$SSH_USER@$HOST" bash <<REMOTE
set -euo pipefail
cd /opt/video-bot
if command -v sudo >/dev/null 2>&1; then
  sudo bash deploy/bootstrap_vps.sh "$BOT_TOKEN" "$ADMIN_USERNAME" "$PANEL_TOKEN" "$PANEL_SECRET"
else
  bash deploy/bootstrap_vps.sh "$BOT_TOKEN" "$ADMIN_USERNAME" "$PANEL_TOKEN" "$PANEL_SECRET"
fi
REMOTE

echo "✅ Готово. Відкривай: http://$HOST"
