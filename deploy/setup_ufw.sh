#!/usr/bin/env bash
set -euo pipefail

# Usage: sudo bash deploy/setup_ufw.sh <YOUR_IP>
ALLOW_IP="${1:-}"
if [[ -z "$ALLOW_IP" ]]; then
  echo "Вкажи свій IP: sudo bash deploy/setup_ufw.sh 1.2.3.4"
  exit 1
fi

ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow from "$ALLOW_IP" to any port 80 proto tcp
ufw --force enable
ufw status verbose
