# Video Bot — власний сервер + сайт керування (UA)

## Що вже готово
- Telegram-бот (24/7 через Docker).
- Сайт керування українською мовою.
- Режим `prod` з Nginx reverse-proxy (порт 80).
- Команда-обгортка `hostctl.sh` для всього керування.

## Де буде сайт
- Простий режим: `http://IP_СЕРВЕРА:8080`
- Прод-режим (через Nginx): `http://IP_СЕРВЕРА`

---


## 0) Автоматично (я все зробив за тебе)
На чистому Ubuntu VPS можна 1 командою поставити Docker, налаштувати .env і запустити сайт:
```bash
sudo bash deploy/bootstrap_vps.sh <BOT_TOKEN> dimagymenjuk <PANEL_TOKEN> <PANEL_SECRET>
```
Після завершення скрипт покаже URL твого сайту.

## 1) Підготовка VPS
Рекомендовано: Ubuntu 22.04+, 2 vCPU, 4GB RAM.

Встанови Docker + Compose plugin.

## 2) Перший запуск
```bash
git clone <YOUR_REPO_URL>
cd video-bot
./hostctl.sh init
```

Відкрий `.env` і заповни:
- `TOKEN=...`
- `ADMIN_USERNAME=dimagymenjuk`
- `ADMIN_PANEL_TOKEN=...`
- `ADMIN_PANEL_SECRET=...`

## 3) Запуск (звичайний)
```bash
./hostctl.sh start
./hostctl.sh status
```
Сайт: `http://IP_СЕРВЕРА:8080`

## 4) Запуск (потужний prod + Nginx)
```bash
./hostctl.sh start prod
./hostctl.sh status prod
```
Сайт: `http://IP_СЕРВЕРА`

## 5) Керування
```bash
./hostctl.sh logs
./hostctl.sh restart
./hostctl.sh stop

./hostctl.sh logs prod
./hostctl.sh restart prod
./hostctl.sh stop prod
```

## 6) Безпека (обовʼязково)
Обмежити доступ до сайту лише з твого IP:
```bash
sudo bash deploy/setup_ufw.sh <ТВІЙ_IP>
```

## 7) Telegram-адмін команди
- `/announce <текст>`
- `/subscribers`
- `/shutdown`

## Файли інфраструктури
- `docker-compose.yml` — базовий режим
- `docker-compose.prod.yml` — прод + nginx
- `deploy/nginx/video-bot.conf` — конфіг nginx
- `deploy/setup_ufw.sh` — firewall-скрипт
