from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from flask import Flask, redirect, render_template_string, request, session, url_for

BASE_DIR = Path(__file__).resolve().parent
STATS_FILE = BASE_DIR / "bot_stats.json"
SETTINGS_FILE = BASE_DIR / "bot_settings.json"
SUBSCRIBERS_FILE = BASE_DIR / "bot_subscribers.json"
HOSTCTL = BASE_DIR / "hostctl.sh"

PANEL_TOKEN = os.environ.get("ADMIN_PANEL_TOKEN", "change-me-now")
PANEL_PORT = int(os.environ.get("ADMIN_PANEL_PORT", "8080"))
SECRET_KEY = os.environ.get("ADMIN_PANEL_SECRET", "super-secret-key")

app = Flask(__name__)
app.secret_key = SECRET_KEY


def read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def run_hostctl(action: str) -> str:
    if action not in {"start", "stop", "restart", "status"}:
        return "Невідома команда"
    if not HOSTCTL.exists():
        return "hostctl.sh не знайдено"
    result = subprocess.run(["bash", str(HOSTCTL), action], cwd=BASE_DIR, capture_output=True, text=True)
    out = (result.stdout or "") + (result.stderr or "")
    return out.strip() or "OK"


HTML = """
<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Video Bot Control Center</title>
  <style>
    body{font-family:Arial,sans-serif;background:#0f172a;color:#e2e8f0;margin:0;padding:0}
    .wrap{max-width:1050px;margin:20px auto;padding:16px}
    .card{background:#111827;border:1px solid #334155;border-radius:14px;padding:16px;margin-bottom:14px}
    h1{margin:0 0 8px 0}.muted{color:#94a3b8}
    button{background:#2563eb;color:white;border:none;padding:10px 14px;border-radius:8px;cursor:pointer;margin:4px}
    button:hover{background:#1d4ed8}
    pre{background:#020617;color:#cbd5e1;padding:10px;border-radius:8px;overflow:auto;max-height:280px}
    input{padding:10px;border-radius:8px;border:1px solid #64748b;background:#0b1220;color:#e2e8f0;width:280px}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
  </style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <h1>🚀 Керування Video Bot</h1>
    <div class="muted">Потужна панель для запуску, зупинки та моніторингу бота</div>
  </div>

  {% if not logged_in %}
  <div class="card">
    <h3>Вхід в панель</h3>
    <form method="post" action="/login">
      <input type="password" name="token" placeholder="Введи ADMIN_PANEL_TOKEN" required />
      <button type="submit">Увійти</button>
    </form>
  </div>
  {% else %}
  <div class="card">
    <h3>Керування сервером</h3>
    <form method="post" action="/action"><button name="action" value="start">▶️ Запустити</button></form>
    <form method="post" action="/action"><button name="action" value="stop">⏹️ Зупинити</button></form>
    <form method="post" action="/action"><button name="action" value="restart">🔄 Перезапустити</button></form>
    <form method="post" action="/action"><button name="action" value="status">📊 Статус</button></form>
    <form method="get" action="/logout"><button>🚪 Вийти</button></form>
    <h4>Відповідь сервера</h4>
    <pre>{{ command_output }}</pre>
  </div>

  <div class="grid">
    <div class="card">
      <h3>Статистика бота</h3>
      <pre>{{ stats }}</pre>
    </div>
    <div class="card">
      <h3>Налаштування бота</h3>
      <pre>{{ settings }}</pre>
    </div>
  </div>

  <div class="card">
    <h3>Підписники</h3>
    <div>Кількість чатів: <b>{{ subscribers_count }}</b></div>
  </div>
  {% endif %}
</div>
</body>
</html>
"""


@app.get("/")
def index():
    logged_in = bool(session.get("ok"))
    stats = json.dumps(read_json(STATS_FILE, {}), ensure_ascii=False, indent=2)
    settings = json.dumps(read_json(SETTINGS_FILE, {}), ensure_ascii=False, indent=2)
    subscribers = read_json(SUBSCRIBERS_FILE, [])
    return render_template_string(
        HTML,
        logged_in=logged_in,
        stats=stats,
        settings=settings,
        subscribers_count=len(subscribers) if isinstance(subscribers, list) else 0,
        command_output=session.pop("last_out", "Готово до роботи ✅"),
    )


@app.post("/login")
def login():
    token = request.form.get("token", "")
    if token == PANEL_TOKEN:
        session["ok"] = True
    return redirect(url_for("index"))


@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))


@app.post("/action")
def action():
    if not session.get("ok"):
        return redirect(url_for("index"))
    act = request.form.get("action", "")
    session["last_out"] = run_hostctl(act)
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PANEL_PORT)
