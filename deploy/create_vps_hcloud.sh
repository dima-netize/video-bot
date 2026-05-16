#!/usr/bin/env bash
set -euo pipefail

# Створення власного VPS у Hetzner Cloud + автоматичний деплой video-bot.
# Потрібно: hcloud API token (https://console.hetzner.cloud/projects)
#
# Приклад:
# HCLOUD_TOKEN=... bash deploy/create_vps_hcloud.sh \
#   --name video-bot-ua \
#   --location fsn1 \
#   --type cpx21 \
#   --image ubuntu-24.04 \
#   --ssh-key ~/.ssh/id_ed25519.pub \
#   --repo https://github.com/<user>/<repo>.git \
#   --bot-token 123:ABC \
#   --admin-username dimagymenjuk \
#   --panel-token MyPanelPass123 \
#   --panel-secret SuperSecret987

NAME="video-bot-ua"
LOCATION="fsn1"
SERVER_TYPE="cpx21"
IMAGE="ubuntu-24.04"
SSH_KEY_PATH="${HOME}/.ssh/id_rsa.pub"
REPO_URL=""
BOT_TOKEN=""
ADMIN_USERNAME="dimagymenjuk"
PANEL_TOKEN=""
PANEL_SECRET=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) NAME="$2"; shift 2 ;;
    --location) LOCATION="$2"; shift 2 ;;
    --type) SERVER_TYPE="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --ssh-key) SSH_KEY_PATH="$2"; shift 2 ;;
    --repo) REPO_URL="$2"; shift 2 ;;
    --bot-token) BOT_TOKEN="$2"; shift 2 ;;
    --admin-username) ADMIN_USERNAME="$2"; shift 2 ;;
    --panel-token) PANEL_TOKEN="$2"; shift 2 ;;
    --panel-secret) PANEL_SECRET="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,70p' "$0"
      exit 0
      ;;
    *)
      echo "Невідомий аргумент: $1"
      exit 2
      ;;
  esac
done

if [[ -z "${HCLOUD_TOKEN:-}" ]]; then
  echo "Помилка: задай HCLOUD_TOKEN у середовищі."
  exit 1
fi
if [[ -z "$REPO_URL" || -z "$BOT_TOKEN" || -z "$PANEL_TOKEN" || -z "$PANEL_SECRET" ]]; then
  echo "Помилка: потрібно задати --repo --bot-token --panel-token --panel-secret"
  exit 1
fi
if [[ ! -f "$SSH_KEY_PATH" ]]; then
  echo "Помилка: SSH public key не знайдено: $SSH_KEY_PATH"
  exit 1
fi

command -v curl >/dev/null || { echo "Потрібен curl"; exit 1; }
command -v jq >/dev/null || { echo "Потрібен jq"; exit 1; }

API="https://api.hetzner.cloud/v1"
AUTH=(-H "Authorization: Bearer $HCLOUD_TOKEN" -H "Content-Type: application/json")

SSH_KEY_NAME="video-bot-key-$(date +%s)"
SSH_KEY_DATA="$(cat "$SSH_KEY_PATH")"

echo "[1/4] Додаю SSH key у Hetzner..."
KEY_RESP="$(curl -fsS -X POST "${AUTH[@]}" "$API/ssh_keys" -d "{\"name\":\"$SSH_KEY_NAME\",\"public_key\":\"$SSH_KEY_DATA\"}")"
SSH_KEY_ID="$(echo "$KEY_RESP" | jq -r '.ssh_key.id')"

if [[ "$SSH_KEY_ID" == "null" || -z "$SSH_KEY_ID" ]]; then
  echo "Не вдалося створити SSH key у Hetzner"
  exit 1
fi

echo "[2/4] Створюю VPS ($SERVER_TYPE, $LOCATION, $IMAGE)..."
CREATE_BODY=$(cat <<JSON
{
  "name": "$NAME",
  "server_type": "$SERVER_TYPE",
  "image": "$IMAGE",
  "location": "$LOCATION",
  "ssh_keys": [$SSH_KEY_ID]
}
JSON
)
SERVER_RESP="$(curl -fsS -X POST "${AUTH[@]}" "$API/servers" -d "$CREATE_BODY")"
SERVER_ID="$(echo "$SERVER_RESP" | jq -r '.server.id')"
SERVER_IP="$(echo "$SERVER_RESP" | jq -r '.server.public_net.ipv4.ip')"

if [[ "$SERVER_ID" == "null" || -z "$SERVER_ID" ]]; then
  echo "Не вдалося створити VPS"
  exit 1
fi

echo "[3/4] Чекаю готовність SSH на $SERVER_IP ..."
for _ in {1..40}; do
  if ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new root@"$SERVER_IP" 'echo ok' >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

echo "[4/4] Розгортаю video-bot на VPS..."
ssh -o StrictHostKeyChecking=accept-new root@"$SERVER_IP" bash <<REMOTE
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git
if [[ ! -d /opt/video-bot/.git ]]; then
  git clone "$REPO_URL" /opt/video-bot
else
  cd /opt/video-bot && git pull --rebase
fi
cd /opt/video-bot
sudo bash deploy/bootstrap_vps.sh "$BOT_TOKEN" "$ADMIN_USERNAME" "$PANEL_TOKEN" "$PANEL_SECRET"
REMOTE

echo
echo "✅ Готово. Твій VPS створено і сайт запущено."
echo "VPS IP: $SERVER_IP"
echo "Сайт: http://$SERVER_IP"
echo "SSH: ssh root@$SERVER_IP"
