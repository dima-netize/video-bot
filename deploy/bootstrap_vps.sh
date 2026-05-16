#!/usr/bin/env bash
set -euo pipefail

# One-command VPS bootstrap for Ubuntu 22.04+
# Usage:
#   sudo bash deploy/bootstrap_vps.sh <BOT_TOKEN> <ADMIN_USERNAME> <PANEL_TOKEN> <PANEL_SECRET>

BOT_TOKEN="${1:-}"
ADMIN_USERNAME="${2:-dimagymenjuk}"
PANEL_TOKEN="${3:-change-me-now}"
PANEL_SECRET="${4:-change-this-secret}"

if [[ -z "$BOT_TOKEN" ]]; then
  echo "Помилка: вкажи BOT_TOKEN"
  echo "Приклад: sudo bash deploy/bootstrap_vps.sh 123:ABC dimagymenjuk mypanelpass supersecret"
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl gnupg lsb-release git ufw

install -m 0755 -d /etc/apt/keyrings
if [[ ! -f /etc/apt/keyrings/docker.gpg ]]; then
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
fi
chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list >/dev/null

apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
systemctl enable docker
systemctl start docker

PROJECT_DIR="/opt/video-bot"
if [[ ! -d "$PROJECT_DIR/.git" ]]; then
  if [[ -d /workspace/video-bot/.git ]]; then
    mkdir -p /opt
    cp -r /workspace/video-bot "$PROJECT_DIR"
  else
    echo "Клонуй репозиторій у $PROJECT_DIR вручну"
    exit 1
  fi
fi

cd "$PROJECT_DIR"
./hostctl.sh init

cat > .env <<ENV
TOKEN=$BOT_TOKEN
ADMIN_USERNAME=$ADMIN_USERNAME
MAX_UPLOAD_BYTES=51380224
PROGRESS_THROTTLE=1.3
REQUEST_TIMEOUT=30
OLD_FILE_TTL=10800
MAX_LINKS_PER_MESSAGE=3
PARALLEL_DOWNLOADS=1
ADMIN_PANEL_TOKEN=$PANEL_TOKEN
ADMIN_PANEL_PORT=8080
ADMIN_PANEL_SECRET=$PANEL_SECRET
ENV

# open only ssh + web
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow 80/tcp
ufw --force enable

./hostctl.sh start prod

echo "Готово ✅"
PUBLIC_IP="$(curl -fsS ifconfig.me || true)"
if [[ -n "$PUBLIC_IP" ]]; then
  echo "Сайт: http://$PUBLIC_IP"
else
  echo "Сайт: http://<IP_сервера>"
fi
