from __future__ import annotations

# ─────────────────────────── Bootstrap ─────────────────────────
import importlib
import os
import site
import subprocess
import sys
from pathlib import Path

BASE_DIR_BOOT = Path(__file__).resolve().parent
PY_VER = f"python{sys.version_info.major}.{sys.version_info.minor}"

for _p in (
    BASE_DIR_BOOT / ".local" / "lib" / PY_VER / "site-packages",
    Path(site.getusersitepackages()),
):
    if _p.exists() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
        site.addsitedir(str(_p))


def _module_ok(name: str) -> bool:
    try:
        importlib.import_module(name)
        return True
    except Exception:
        return False


def _pip_install(package: str) -> None:
    print(f"[BOOT] Installing: {package}", flush=True)
    env = os.environ.copy()
    env["PIP_NO_CACHE_DIR"] = "1"
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--user", "--no-cache-dir", "-U", package],
        env=env,
    )
    user_site = Path(site.getusersitepackages())
    if user_site.exists() and str(user_site) not in sys.path:
        sys.path.insert(0, str(user_site))
        site.addsitedir(str(user_site))
    importlib.invalidate_caches()


def _ensure_deps() -> None:
    if os.environ.get("DISABLE_AUTO_INSTALL", "0") == "1":
        return

    deps = [
        ("requests", "requests>=2.31.0"),
        ("yt_dlp", "yt-dlp"),
        ("telegram.ext", "python-telegram-bot==22.7"),
    ]

    for module_name, package_name in deps:
        if not _module_ok(module_name):
            _pip_install(package_name)

    missing = [module_name for module_name, _ in deps if not _module_ok(module_name)]
    if missing:
        raise RuntimeError("Не вдалося встановити залежності: " + ", ".join(missing))


_ensure_deps()

# ─────────────────────────── Imports ───────────────────────────
import asyncio
import collections
import glob
import html
import ipaddress
import json
import logging
import re
import shutil
import socket
import time
from datetime import datetime
from functools import partial
from threading import Event
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
import yt_dlp
from telegram import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ─────────────────────────── Config ────────────────────────────
TOKEN = os.environ.get("TOKEN", "").strip()
if not TOKEN:
    raise ValueError("Не задано TOKEN в Environment Variables")

WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").rstrip("/") or None

ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "dimagymenjuk").replace("@", "").lower().strip()
OWNER_HANDLE = f"@{ADMIN_USERNAME}"

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

STATS_FILE = BASE_DIR / "bot_stats.json"
SETTINGS_FILE = BASE_DIR / "bot_settings.json"
USERS_FILE = BASE_DIR / "bot_users.json"
BANS_FILE = BASE_DIR / "bot_bans.json"
HISTORY_FILE = BASE_DIR / "bot_history.json"
OWNER_ID_FILE = BASE_DIR / "bot_owner_id.json"

MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(49 * 1024 * 1024)))
PROGRESS_THROTTLE = float(os.environ.get("PROGRESS_THROTTLE", "1.5"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
OLD_FILE_TTL = int(os.environ.get("OLD_FILE_TTL", str(60 * 60 * 3)))
MAX_LINKS_PER_MESSAGE = int(os.environ.get("MAX_LINKS_PER_MESSAGE", "3"))
PARALLEL_DOWNLOADS = max(1, int(os.environ.get("PARALLEL_DOWNLOADS", "2")))
RATE_LIMIT_N = int(os.environ.get("RATE_LIMIT_N", "5"))
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", "60"))
URL_CACHE_TTL = int(os.environ.get("URL_CACHE_TTL", "3600"))
MAX_HISTORY_PER_USER = int(os.environ.get("MAX_HISTORY_PER_USER", "10"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))

PARALLEL_LIMIT = asyncio.Semaphore(PARALLEL_DOWNLOADS)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("video-bot")

FFMPEG_PATH = os.environ.get("FFMPEG_PATH") or shutil.which("ffmpeg")
BOT_START_TIME = time.time()

URL_RE = re.compile(r"https?://[^\s<>\"]+", re.I)
DIRECT_VIDEO_RE = re.compile(
    r"https?://[^\s<>\"]+\.(?:mp4|mov|webm|m4v)(?:\?[^\s<>\"]*)?",
    re.I,
)

URL_PATTERNS: dict[str, re.Pattern[str]] = {
    "youtube": re.compile(r"(youtube\.com/(watch\?v=|shorts/|live/)|youtu\.be/|m\.youtube\.com/watch\?v=|music\.youtube\.com/watch)", re.I),
    "tiktok": re.compile(r"(tiktok\.com/|vt\.tiktok\.com/|vm\.tiktok\.com/)", re.I),
    "instagram": re.compile(r"instagram\.com/(reel|reels|p|tv|stories)/", re.I),
    "x": re.compile(r"(twitter\.com|x\.com)/\w+/status/\d+", re.I),
    "vimeo": re.compile(r"vimeo\.com/", re.I),
    "reddit": re.compile(r"reddit\.com/r/\w+/comments/", re.I),
    "facebook": re.compile(r"facebook\.com/(watch|reel|share|.+/videos)", re.I),
    "pinterest": re.compile(r"pinterest\.[a-z.]+/pin/\d+", re.I),
    "twitch": re.compile(r"twitch\.tv/(videos/\d+|clips/)", re.I),
    "dailymotion": re.compile(r"dailymotion\.com/video/", re.I),
    "rumble": re.compile(r"rumble\.com/v", re.I),
    "bilibili": re.compile(r"bilibili\.com/video/", re.I),
    "coub": re.compile(r"coub\.com/view/", re.I),
    "streamable": re.compile(r"streamable\.com/", re.I),
    "medal": re.compile(r"medal\.tv/", re.I),
}

PUBLIC_HELP = """🎥 Video Downloader Bot

Надішли посилання на відео — бот спробує завантажити його у Telegram.

Основні команди:
/video <url> — завантажити відео
/audio <url> — завантажити аудіо MP3
/quality — змінити якість
/settings — налаштування
/history — історія
/profile — мій профіль
/checkurl <url> — перевірити посилання
/tips — підказки
/about — інформація про бота

Додатково:
/thumb <url> — обкладинка відео
/sub <url> — субтитри
/clip <url> <старт> <кінець> — вирізати кліп
/info <url> — інформація про відео
/formats <url> — формати
/platforms — підтримувані платформи

Керування:
/cancel — скасувати завантаження
/queue — активне завантаження
/ping — перевірка відповіді

Порада: якщо файл завеликий — постав /quality mobile.
"""

ADMIN_HELP = """🔐 Адмін-панель

/admin — показати цю панель
/setupcommands — оновити меню команд Telegram
/users — список користувачів
/topusers — топ користувачів
/user <id> — інформація про користувача
/exportusers — експорт користувачів JSON
/broadcast <текст> — повідомлення всім
/ad <текст> — реклама всім
/ban <id> — заблокувати
/unban <id> — розблокувати
/userlimit <n|off> — ліміт користувачів
/adminstats — адмін-статистика
/resetstats — очистити статистику
/clearcache — очистити URL-кеш
/savecookies — інструкція для cookies.txt
/cookies — перевірити cookies.txt
/updateytdlp — оновити yt-dlp
/clean — очистити старі файли
/health — стан сервера
"""

PUBLIC_BOT_COMMANDS = [
    BotCommand("start", "допомога"),
    BotCommand("video", "завантажити відео"),
    BotCommand("audio", "завантажити аудіо MP3"),
    BotCommand("quality", "змінити якість"),
    BotCommand("settings", "налаштування"),
    BotCommand("history", "історія"),
    BotCommand("profile", "мій профіль"),
    BotCommand("checkurl", "перевірити посилання"),
    BotCommand("tips", "підказки"),
    BotCommand("about", "про бота"),
    BotCommand("ping", "перевірка бота"),
]

ADMIN_BOT_COMMANDS = PUBLIC_BOT_COMMANDS + [
    BotCommand("admin", "адмін-панель"),
    BotCommand("setupcommands", "оновити меню команд"),
    BotCommand("users", "список користувачів"),
    BotCommand("topusers", "топ користувачів"),
    BotCommand("broadcast", "повідомлення всім"),
    BotCommand("ad", "реклама всім"),
    BotCommand("ban", "заблокувати"),
    BotCommand("unban", "розблокувати"),
    BotCommand("adminstats", "адмін-статистика"),
    BotCommand("health", "стан сервера"),
    BotCommand("cookies", "перевірити cookies"),
    BotCommand("updateytdlp", "оновити yt-dlp"),
]

CANCEL_EVENTS: dict[int, Event] = {}
ACTIVE_TASKS: dict[int, dict[str, Any]] = {}
RATE_TRACKER: dict[int, collections.deque[float]] = {}
URL_CACHE: dict[str, tuple[str, str, float, bool]] = {}
ADMIN_IDS: set[int] = set()


class DownloadCancelled(Exception):
    pass


# ─────────────────────────── JSON I/O ──────────────────────────
def read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        log.exception("JSON read error: %s", path)
        return default


def write_json(path: Path, data: Any) -> None:
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        log.exception("JSON write error: %s", path)


STATS = read_json(
    STATS_FILE,
    {"success": 0, "errors": 0, "bytes": 0, "platforms": {}, "audio": 0, "video": 0},
)
SETTINGS = read_json(SETTINGS_FILE, {"quality": {}, "limits": {"max_users": 0}})
USERS = read_json(USERS_FILE, {})
BANNED_USERS: set[int] = set(int(x) for x in read_json(BANS_FILE, []))
HISTORY: dict[str, list[dict[str, Any]]] = read_json(HISTORY_FILE, {})
OWNER_ID: int | None = read_json(OWNER_ID_FILE, None)


def save_stats() -> None:
    write_json(STATS_FILE, STATS)


def save_settings() -> None:
    write_json(SETTINGS_FILE, SETTINGS)


def save_users() -> None:
    write_json(USERS_FILE, USERS)


def save_bans() -> None:
    write_json(BANS_FILE, sorted(BANNED_USERS))


def save_history() -> None:
    write_json(HISTORY_FILE, HISTORY)


def save_owner_id() -> None:
    write_json(OWNER_ID_FILE, OWNER_ID)


# ─────────────────────────── Helpers ───────────────────────────
def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def human_bytes(num: int | float | None) -> str:
    if not num:
        return "0 B"
    n = float(num)
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024 or unit == "GB":
            return f"{int(n)} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


def seconds_text(seconds: int | float | None) -> str:
    if not seconds:
        return "0с"
    s = int(seconds)
    h, r = divmod(s, 3600)
    m, sec = divmod(r, 60)
    if h:
        return f"{h}г {m}хв {sec}с"
    if m:
        return f"{m}хв {sec}с"
    return f"{sec}с"


def uptime_text() -> str:
    return seconds_text(time.time() - BOT_START_TIME)


def safe_text(value: Any, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "video")).strip()
    return (text or "video")[:limit]


def chat_id(update: Update) -> int:
    return int(update.effective_chat.id) if update.effective_chat else 0


def user_id(update: Update) -> int:
    return int(update.effective_user.id) if update.effective_user else 0


def quality_for(cid: int) -> str:
    return SETTINGS.get("quality", {}).get(str(cid), "fast")


def user_limit() -> int:
    try:
        return max(0, int(SETTINGS.get("limits", {}).get("max_users", 0)))
    except Exception:
        return 0


def user_allowed(uid: int) -> bool:
    if uid in ADMIN_IDS or uid == OWNER_ID:
        return True
    limit = user_limit()
    if limit <= 0:
        return True
    return str(uid) in USERS or len(USERS) < limit


def detect_platform(url: str) -> str | None:
    for name, pattern in URL_PATTERNS.items():
        if pattern.search(url):
            return name
    return None


def extract_urls(text: str) -> list[str]:
    found: list[str] = []
    for url in URL_RE.findall(text or ""):
        clean = url.strip().strip(".,;)\n\r\t ")
        if clean and clean not in found:
            found.append(clean)
    return found[:MAX_LINKS_PER_MESSAGE]


def safe_filename(prefix: str, url: str, ext: str = "mp4") -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug = url.split("?")[0].rstrip("/").split("/")[-1] or "video"
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", slug)[:40] or "video"
    return DOWNLOAD_DIR / f"{prefix}_{slug}_{ts}.{ext}"


def remove_file(path: str | Path | None) -> None:
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except OSError:
        pass


def clean_old_files(force: bool = False) -> int:
    now = time.time()
    deleted = 0
    for path in DOWNLOAD_DIR.glob("*"):
        try:
            if path.is_file() and (force or now - path.stat().st_mtime > OLD_FILE_TTL):
                path.unlink()
                deleted += 1
        except OSError:
            pass
    return deleted


def cookies_file() -> str | None:
    candidates = [
        Path("/etc/secrets/cookies.txt"),
        BASE_DIR / "cookies.txt",
        BASE_DIR / "cookies" / "cookies.txt",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            first = path.read_text(encoding="utf-8", errors="ignore").splitlines()[0].strip()
            if first in {"# Netscape HTTP Cookie File", "# HTTP Cookie File"}:
                return str(path)
        except Exception:
            pass
    return None


def url_is_safe(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        if not parsed.hostname:
            return False

        host = parsed.hostname.lower()
        if host in {"localhost", "0.0.0.0"}:
            return False

        try:
            ip = ipaddress.ip_address(host)
            return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved)
        except ValueError:
            pass

        try:
            infos = socket.getaddrinfo(host, None)
            for info in infos:
                ip_text = info[4][0]
                ip = ipaddress.ip_address(ip_text)
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                    return False
        except Exception:
            pass

        return True
    except Exception:
        return False


def progress_bar(pct: int, width: int = 14) -> str:
    pct = max(0, min(100, pct))
    filled = int(width * pct / 100)
    return f"[{'▓' * filled}{'░' * (width - filled)}] {pct}%"


def progress_text(prefix: str, done: int, total: int | None, start: float) -> str:
    elapsed = max(time.monotonic() - start, 0.1)
    speed = done / elapsed if done else 0

    if total:
        pct = int(done * 100 / total)
        eta = int((total - done) / speed) if speed else 0
        return (
            f"{prefix}\n"
            f"{progress_bar(pct)}\n"
            f"{human_bytes(done)} / {human_bytes(total)}\n"
            f"Швидкість: {human_bytes(speed)}/s\n"
            f"Залишилось: {seconds_text(eta)}"
        )

    return f"{prefix}\nЗавантажено: {human_bytes(done)}\nШвидкість: {human_bytes(speed)}/s"


# ─────────────────────────── Admin helpers ─────────────────────
def sync_owner_id(update: Update) -> None:
    global OWNER_ID

    user = update.effective_user
    if not user:
        return

    uid = int(user.id)
    username = (user.username or "").lower().strip()

    if username != ADMIN_USERNAME:
        return

    if OWNER_ID is None:
        OWNER_ID = uid
        save_owner_id()

    if uid == OWNER_ID:
        ADMIN_IDS.add(uid)


def is_admin_update(update: Update) -> bool:
    sync_owner_id(update)

    user = update.effective_user
    if not user:
        return False

    uid = int(user.id)
    username = (user.username or "").lower().strip()

    if username != ADMIN_USERNAME:
        return False

    if OWNER_ID is not None and uid != OWNER_ID:
        return False

    return uid in ADMIN_IDS or uid == OWNER_ID


def require_admin(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not is_admin_update(update):
            if update.effective_message:
                await update.effective_message.reply_text("🚫 Ця команда доступна тільки адміну.")
            return
        await func(update, context)

    wrapper.__name__ = func.__name__
    return wrapper


# ─────────────────────────── Users / stats ─────────────────────
def record_user(update: Update) -> None:
    user = update.effective_user
    if not user:
        return

    sync_owner_id(update)

    uid = str(user.id)
    now = utc_now()
    item = USERS.get(uid)
    changed = False

    if not item:
        USERS[uid] = {
            "id": int(user.id),
            "username": user.username or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "joined": now,
            "last_seen": now,
            "downloads": 0,
            "audio": 0,
            "video": 0,
            "messages": 1,
        }
        changed = True
    else:
        for key, value in {
            "username": user.username or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "last_seen": now,
        }.items():
            if item.get(key) != value:
                item[key] = value
                changed = True
        item["messages"] = int(item.get("messages", 0)) + 1
        changed = True

    if changed:
        save_users()


def inc_user_download(uid: int, audio: bool) -> None:
    key = str(uid)
    USERS.setdefault(key, {"id": uid, "joined": utc_now()})
    USERS[key]["downloads"] = int(USERS[key].get("downloads", 0)) + 1
    if audio:
        USERS[key]["audio"] = int(USERS[key].get("audio", 0)) + 1
    else:
        USERS[key]["video"] = int(USERS[key].get("video", 0)) + 1
    USERS[key]["last_seen"] = utc_now()
    save_users()


def record_history(uid: int, url: str, title: str, platform: str, audio: bool) -> None:
    key = str(uid)
    HISTORY.setdefault(key, [])
    HISTORY[key].insert(
        0,
        {
            "url": url,
            "title": safe_text(title, 90),
            "platform": platform,
            "type": "audio" if audio else "video",
            "ts": utc_now(),
        },
    )
    HISTORY[key] = HISTORY[key][:MAX_HISTORY_PER_USER]
    save_history()


def stats_ok(platform: str, size: int, audio: bool) -> None:
    STATS["success"] = int(STATS.get("success", 0)) + 1
    STATS["bytes"] = int(STATS.get("bytes", 0)) + int(size or 0)
    STATS["audio" if audio else "video"] = int(STATS.get("audio" if audio else "video", 0)) + 1
    STATS.setdefault("platforms", {})
    STATS["platforms"][platform] = int(STATS["platforms"].get(platform, 0)) + 1
    save_stats()


def stats_fail() -> None:
    STATS["errors"] = int(STATS.get("errors", 0)) + 1
    save_stats()


# ─────────────────────────── Rate limit ────────────────────────
def check_rate_limit(uid: int) -> bool:
    if uid in ADMIN_IDS or uid == OWNER_ID:
        return True

    now = time.time()
    dq = RATE_TRACKER.setdefault(uid, collections.deque())

    while dq and now - dq[0] > RATE_LIMIT_WINDOW:
        dq.popleft()

    if len(dq) >= RATE_LIMIT_N:
        return False

    dq.append(now)
    return True


def rate_reset_in(uid: int) -> int:
    dq = RATE_TRACKER.get(uid)
    if not dq:
        return 0
    return max(0, int(RATE_LIMIT_WINDOW - (time.time() - dq[0])))


# ─────────────────────────── URL cache ─────────────────────────
def cache_key(url: str, audio: bool) -> str:
    return f"{url}|{'audio' if audio else 'video'}"


def cache_get(url: str, audio: bool) -> tuple[str, str] | None:
    key = cache_key(url, audio)
    entry = URL_CACHE.get(key)

    if not entry:
        return None

    file_id, title, ts, _ = entry

    if time.time() - ts > URL_CACHE_TTL:
        URL_CACHE.pop(key, None)
        return None

    return file_id, title


def cache_set(url: str, audio: bool, file_id: str, title: str) -> None:
    URL_CACHE[cache_key(url, audio)] = (file_id, title, time.time(), audio)


def cache_cleanup() -> None:
    now = time.time()
    for key, value in list(URL_CACHE.items()):
        if now - value[2] > URL_CACHE_TTL:
            URL_CACHE.pop(key, None)


# ─────────────────────────── yt-dlp helpers ────────────────────
def friendly_error(platform: str | None, error: str) -> str:
    err = str(error or "")
    low = err.lower()

    if "exit code 137" in low or "killed" in low:
        return "⚠️ Серверу не вистачило ресурсів. Постав /quality mobile і спробуй ще раз."

    if platform == "youtube" and any(x in low for x in ["sign in to confirm", "not a bot", "use --cookies", "cookies"]):
        return (
            "🍪 YouTube просить cookies.txt.\n\n"
            "На Render free-серверах YouTube часто блокує завантаження без cookies.\n"
            "Перевір /cookies або додай cookies.txt у Secret Files."
        )

    if "requested format is not available" in low:
        return "⚠️ Ця якість недоступна. Спробуй /quality fast або /quality mobile."

    if "ffmpeg" in low and not FFMPEG_PATH:
        return "⚠️ Немає ffmpeg. Для аудіо MP3 і склеювання відео потрібен ffmpeg."

    if "unsupported url" in low:
        return "❌ Посилання не підтримується або платформа змінила захист."

    if any(x in low for x in ["private", "login required", "members-only"]):
        return "🔒 Відео приватне або потрібен вхід."

    if any(x in low for x in ["network", "connection", "timeout", "temporarily unavailable"]):
        return "🌐 Помилка мережі. Спробуй ще раз через хвилину."

    if "429" in low or "too many" in low:
        return "⏳ Платформа тимчасово обмежила запити. Зачекай 5–10 хвилин."

    if "not available in your country" in low or "geo" in low:
        return "🌍 Відео недоступне в регіоні сервера."

    return safe_text(err, 900)


def is_transient_error(error: str) -> bool:
    low = str(error).lower()
    return any(
        item in low
        for item in [
            "network",
            "connection",
            "timeout",
            "reset by peer",
            "read error",
            "http error 5",
            "503",
            "502",
            "429",
            "temporarily unavailable",
        ]
    )


def first_entry(info: dict[str, Any]) -> dict[str, Any]:
    entries = info.get("entries")
    if not entries:
        return info
    items = [item for item in entries if item]
    return items[0] if items else info


def find_file(info: dict[str, Any], ydl: yt_dlp.YoutubeDL) -> str | None:
    candidates: list[str] = []

    for item in info.get("requested_downloads") or []:
        if isinstance(item, dict):
            candidates += [item.get("filepath"), item.get("_filename")]

    candidates += [info.get("filepath"), info.get("_filename")]

    try:
        candidates.append(ydl.prepare_filename(info))
    except Exception:
        pass

    if info.get("id"):
        candidates += glob.glob(str(DOWNLOAD_DIR / f"*{info['id']}*"))

    existing = [str(Path(item)) for item in candidates if item and Path(item).exists()]
    existing.sort(key=lambda path: Path(path).stat().st_mtime, reverse=True)
    return existing[0] if existing else None


def format_selector(platform: str | None, audio: bool, quality: str) -> str:
    if audio:
        return "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best"

    if quality == "mobile":
        return "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best[height<=480]/best"

    if quality == "fast":
        return "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]/best"

    if platform == "youtube" and FFMPEG_PATH:
        return "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/best[ext=mp4]/best"

    return "best[ext=mp4]/best"


def ytdlp_opts(
    platform: str | None,
    audio: bool,
    quality: str,
    hook=None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36"

    if platform == "tiktok":
        user_agent = "com.zhiliaoapp.musically/2022600030 (Linux; U; Android 12; en_US; Pixel 6)"

    opts: dict[str, Any] = {
        "format": format_selector(platform, audio, quality),
        "outtmpl": str(DOWNLOAD_DIR / "%(extractor_key)s_%(id)s_%(title).80s.%(ext)s"),
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "restrictfilenames": True,
        "retries": 8,
        "fragment_retries": 8,
        "socket_timeout": 30,
        "continuedl": True,
        "concurrent_fragment_downloads": 3,
        "http_chunk_size": 6 * 1024 * 1024,
        "http_headers": {"User-Agent": user_agent},
        "progress_hooks": [hook] if hook else [],
    }

    if FFMPEG_PATH:
        opts["ffmpeg_location"] = FFMPEG_PATH
        if audio:
            opts["postprocessors"] = [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }
            ]
        else:
            opts["merge_output_format"] = "mp4"

    cookie_path = cookies_file()
    if cookie_path:
        opts["cookiefile"] = cookie_path

    if platform == "youtube":
        opts["extractor_args"] = {"youtube": {"player_client": ["android", "web"]}}

    if platform == "tiktok":
        opts["extractor_args"] = {"tiktok": {"app_version": "26.2.0", "manifest_app_version": "26.2.0"}}

    if extra:
        opts.update(extra)

    return opts


# ─────────────────────────── Downloaders ───────────────────────
def stream_download(
    url: str,
    filepath: Path,
    title: str,
    progress_cb=None,
    cancel_event: Event | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[str | None, str]:
    headers = headers or {"User-Agent": "Mozilla/5.0"}
    start = time.monotonic()
    done = 0

    try:
        with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=headers) as response:
            response.raise_for_status()
            total = int(response.headers.get("content-length") or 0)

            with open(filepath, "wb") as file:
                for chunk in response.iter_content(1024 * 256):
                    if cancel_event and cancel_event.is_set():
                        raise DownloadCancelled("Завантаження скасовано.")
                    if not chunk:
                        continue

                    file.write(chunk)
                    done += len(chunk)

                    if progress_cb:
                        progress_cb(progress_text("⏳ Завантажую файл", done, total, start))

        return str(filepath), title

    except DownloadCancelled as exc:
        remove_file(filepath)
        return None, str(exc)

    except Exception as exc:
        remove_file(filepath)
        return None, f"Помилка прямого завантаження: {exc}"


def tiktok_fallback_tikwm(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://tikwm.com/"}

    try:
        response = requests.get(
            "https://tikwm.com/api/",
            params={"url": url, "hd": "1"},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("code") != 0:
            return None, data.get("msg") or "TikTok fallback не спрацював."

        item = data.get("data") or {}
        video_url = item.get("hdplay") or item.get("play") or item.get("wmplay")

        if not video_url:
            return None, "TikTok fallback не повернув відео."

        video_url = urljoin("https://tikwm.com", video_url)

        return stream_download(
            video_url,
            safe_filename("tiktok", url),
            safe_text(item.get("title") or "TikTok video"),
            progress_cb,
            cancel_event,
            headers,
        )

    except Exception as exc:
        return None, f"TikTok fallback: {exc}"


def instagram_fallback(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    fixed = url.replace("www.instagram.com", "www.ddinstagram.com").replace("instagram.com", "ddinstagram.com")

    try:
        response = requests.get(fixed, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        video_url = None
        patterns = [
            r'<video[^>]+src="([^"]+)"',
            r'property="og:video"\s+content="([^"]+)"',
            r'property="og:video:secure_url"\s+content="([^"]+)"',
            r'"video_url":"([^"]+)"',
        ]

        for pattern in patterns:
            match = re.search(pattern, response.text)
            if match:
                video_url = match.group(1).replace("\\u0026", "&").replace("\\/", "/")
                break

        if not video_url:
            return None, "Instagram fallback не знайшов відео."

        return stream_download(
            video_url,
            safe_filename("instagram", url),
            "Instagram video",
            progress_cb,
            cancel_event,
            {"User-Agent": "Mozilla/5.0"},
        )

    except Exception as exc:
        return None, f"Instagram fallback: {exc}"


def download_direct(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    ext = url.split("?")[0].split(".")[-1].lower()
    if ext not in {"mp4", "mov", "webm", "m4v"}:
        ext = "mp4"

    return stream_download(
        url,
        safe_filename("direct", url, ext),
        "Пряме відео",
        progress_cb,
        cancel_event,
    )


def download_via_ytdlp(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
    extra_opts: dict[str, Any] | None = None,
) -> tuple[str | None, str]:
    start = time.monotonic()

    def hook(data: dict[str, Any]) -> None:
        if cancel_event and cancel_event.is_set():
            raise DownloadCancelled("Завантаження скасовано.")

        if not progress_cb:
            return

        if data.get("status") == "downloading":
            total = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
            done = data.get("downloaded_bytes") or 0
            progress_cb(progress_text("⏳ Завантажую", int(done), int(total), start))
        elif data.get("status") == "finished":
            progress_cb("🔧 Обробляю файл...")

    try:
        with yt_dlp.YoutubeDL(ytdlp_opts(platform, audio, quality, hook, extra_opts)) as ydl:
            info = first_entry(ydl.extract_info(url, download=True))
            path = find_file(info, ydl)

            if audio and path and FFMPEG_PATH:
                mp3_path = str(Path(path).with_suffix(".mp3"))
                if Path(mp3_path).exists():
                    path = mp3_path

            if not path or not Path(path).exists():
                return None, "Файл після завантаження не знайдено."

            return path, safe_text(info.get("title"), 180)

    except DownloadCancelled as exc:
        return None, str(exc)

    except Exception as exc:
        return None, friendly_error(platform, str(exc))


def download_media(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    if DIRECT_VIDEO_RE.search(url) and not audio:
        return download_direct(url, progress_cb, cancel_event)

    if platform == "tiktok" and not audio:
        if progress_cb:
            progress_cb("🔁 TikTok no-watermark fallback...")
        path, title = tiktok_fallback_tikwm(url, progress_cb, cancel_event)
        if path:
            return path, title
        if progress_cb:
            progress_cb("🔁 TikTok через yt-dlp...")

    last_error = ""

    for attempt in range(1, MAX_RETRIES + 1):
        if cancel_event and cancel_event.is_set():
            return None, "Завантаження скасовано."

        if attempt > 1:
            if progress_cb:
                progress_cb(f"🔁 Спроба {attempt}/{MAX_RETRIES}...")
            time.sleep(2 ** (attempt - 1))

        path, title_or_error = download_via_ytdlp(
            url,
            platform,
            audio,
            quality,
            progress_cb,
            cancel_event,
        )

        if path:
            return path, title_or_error

        last_error = title_or_error

        if not is_transient_error(title_or_error):
            break

    if platform == "instagram" and not audio:
        if progress_cb:
            progress_cb("🔁 Instagram fallback...")
        path, result = instagram_fallback(url, progress_cb, cancel_event)
        if path:
            return path, result
        return None, f"{last_error}\nInstagram fallback: {result}"

    return None, last_error or "Невідома помилка завантаження."


# ─────────────────────────── Info / formats / extras ───────────
def extract_info_text(url: str, platform: str | None, quality: str) -> str:
    opts = ytdlp_opts(platform, False, quality)
    opts["skip_download"] = True

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=False))

    lines = [
        f"ℹ️ {safe_text(info.get('title'), 180)}",
        f"Автор: {safe_text(info.get('uploader') or info.get('channel') or 'невідомо', 120)}",
        f"Тривалість: {seconds_text(info.get('duration'))}",
        f"Платформа: {platform or 'unknown'}",
    ]

    if info.get("view_count") is not None:
        lines.append(f"Перегляди: {int(info.get('view_count')):,}".replace(",", " "))

    if info.get("like_count") is not None:
        lines.append(f"Лайки: {int(info.get('like_count')):,}".replace(",", " "))

    if info.get("upload_date"):
        date = str(info["upload_date"])
        if len(date) == 8:
            lines.append(f"Дата: {date[:4]}-{date[4:6]}-{date[6:]}")

    if info.get("description"):
        lines += ["", safe_text(info["description"], 500)]

    lines += ["", f"URL: {info.get('webpage_url') or url}"]
    return "\n".join(lines)[:3900]


def extract_formats_text(url: str, platform: str | None, quality: str) -> str:
    opts = ytdlp_opts(platform, False, quality)
    opts["skip_download"] = True

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=False))

    lines = [f"🎞 Формати: {safe_text(info.get('title'), 100)}", ""]

    count = 0
    for fmt in info.get("formats") or []:
        if count >= 35:
            break

        height = fmt.get("height")
        fps = fmt.get("fps")
        size = fmt.get("filesize") or fmt.get("filesize_approx")

        label = f"{height}p" if height else (fmt.get("format_note") or fmt.get("resolution") or "audio")
        if fps:
            label += f"/{int(fps)}fps"

        media = "audio" if fmt.get("vcodec") == "none" else ("video-only" if fmt.get("acodec") == "none" else "video")
        lines.append(
            f"• {fmt.get('format_id', '?')}: {label} ({fmt.get('ext', '?')}, {media})"
            + (f" ~{human_bytes(size)}" if size else "")
        )
        count += 1

    if count == 0:
        lines.append("Формати не знайдено.")

    return "\n".join(lines)[:3900]


def download_subtitles(url: str, platform: str | None, quality: str) -> tuple[list[str], str]:
    opts = ytdlp_opts(platform, False, quality)
    opts.update(
        {
            "skip_download": True,
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitleslangs": ["uk", "en", "ru", "auto"],
            "subtitlesformat": "srt/best",
            "outtmpl": str(DOWNLOAD_DIR / "sub_%(id)s.%(ext)s"),
        }
    )

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=True))

    title = safe_text(info.get("title"), 180)
    vid_id = info.get("id", "")
    files = glob.glob(str(DOWNLOAD_DIR / f"sub_{vid_id}*"))
    return files, title


def download_thumbnail(url: str, platform: str | None, quality: str) -> tuple[str | None, str]:
    opts = ytdlp_opts(platform, False, quality)
    opts.update(
        {
            "skip_download": True,
            "writethumbnail": True,
            "outtmpl": str(DOWNLOAD_DIR / "thumb_%(id)s.%(ext)s"),
        }
    )

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=True))

    title = safe_text(info.get("title"), 180)
    vid_id = info.get("id", "")
    files = sorted(
        glob.glob(str(DOWNLOAD_DIR / f"thumb_{vid_id}*")),
        key=lambda path: Path(path).stat().st_mtime,
        reverse=True,
    )
    return (files[0] if files else None), title


def clip_video_ffmpeg(input_path: str, start: str, end: str, output_path: str) -> tuple[bool, str]:
    if not FFMPEG_PATH:
        return False, "ffmpeg не знайдено на сервері."

    try:
        result = subprocess.run(
            [FFMPEG_PATH, "-y", "-i", input_path, "-ss", start, "-to", end, "-c", "copy", output_path],
            capture_output=True,
            text=True,
            timeout=180,
        )

        if result.returncode != 0:
            return False, f"ffmpeg error:\n{result.stderr[-700:]}"

        return True, ""

    except subprocess.TimeoutExpired:
        return False, "ffmpeg timeout."

    except Exception as exc:
        return False, str(exc)


# ─────────────────────────── Telegram helpers ──────────────────
async def safe_edit(message, text: str) -> None:
    try:
        await message.edit_text(text[:3900])
    except RetryAfter as exc:
        await asyncio.sleep(float(exc.retry_after) + 0.2)
    except BadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            log.debug("edit error: %s", exc)
    except TelegramError as exc:
        log.debug("telegram edit error: %s", exc)


async def send_media(
    update: Update,
    filepath: str,
    title: str,
    is_audio: bool = False,
    progress_cb=None,
) -> tuple[int, str]:
    msg = update.effective_message
    if not msg:
        remove_file(filepath)
        return 0, ""

    try:
        path = Path(filepath)
        size = path.stat().st_size

        if size > MAX_UPLOAD_BYTES:
            await msg.reply_text(
                "❌ Файл більший за ліміт Telegram Bot API.\n"
                "Постав /quality mobile або виріж коротший фрагмент через /clip."
            )
            return 0, ""

        if progress_cb:
            progress_cb("📤 Надсилаю у Telegram...")

        with open(filepath, "rb") as file:
            if is_audio and path.suffix.lower() == ".mp3":
                sent = await msg.reply_audio(
                    audio=file,
                    title=title[:64],
                    caption=f"🎵 {title[:180]}",
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                    pool_timeout=60,
                )
                return size, str(sent.audio.file_id if sent.audio else "")

            if is_audio:
                sent = await msg.reply_document(
                    document=file,
                    caption=f"🎵 {title[:180]}\nФайл не конвертовано в MP3, бо немає ffmpeg.",
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                    pool_timeout=60,
                )
                return size, str(sent.document.file_id if sent.document else "")

            sent = await msg.reply_video(
                video=file,
                caption=f"✅ {title[:200]}",
                supports_streaming=True,
                read_timeout=180,
                write_timeout=180,
                connect_timeout=60,
                pool_timeout=60,
            )
            return size, str(sent.video.file_id if sent.video else "")

    except Exception:
        log.exception("send media error")
        await msg.reply_text("❌ Не вдалося надіслати файл у Telegram.")
        return 0, ""

    finally:
        remove_file(filepath)


async def send_cached(update: Update, file_id: str, title: str, is_audio: bool) -> None:
    msg = update.effective_message
    if not msg:
        return

    if is_audio:
        await msg.reply_audio(audio=file_id, title=title[:64], caption=f"🎵 {title[:180]} (з кешу)")
    else:
        await msg.reply_video(video=file_id, caption=f"✅ {title[:200]} (з кешу)")


def quality_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🏆 best", callback_data="quality:best"),
                InlineKeyboardButton("⚡ fast", callback_data="quality:fast"),
                InlineKeyboardButton("📱 mobile", callback_data="quality:mobile"),
            ]
        ]
    )


def settings_keyboard(cid: int) -> InlineKeyboardMarkup:
    q = quality_for(cid)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"Якість: {q}", callback_data="settings:quality")],
            [InlineKeyboardButton("Очистити мій кеш", callback_data="settings:clearcache")],
            [InlineKeyboardButton("Статистика", callback_data="settings:stats")],
        ]
    )


# ─────────────────────────── Core flow ─────────────────────────
async def download_and_send(
    update: Update,
    url: str,
    platform: str,
    audio: bool = False,
) -> None:
    msg = update.effective_message
    if not msg:
        return

    cid = chat_id(update)
    uid = user_id(update)
    quality = quality_for(cid)

    if uid in BANNED_USERS:
        await msg.reply_text("🚫 Ти заблокований у цьому боті.")
        return

    if not user_allowed(uid):
        await msg.reply_text("⛔ Бот тимчасово закритий для нових користувачів.")
        return

    if not check_rate_limit(uid):
        wait = rate_reset_in(uid)
        await msg.reply_text(
            f"⏳ Забагато завантажень. Ліміт: {RATE_LIMIT_N} за {RATE_LIMIT_WINDOW}с.\n"
            f"Спробуй ще раз приблизно через {wait}с."
        )
        return

    cached = cache_get(url, audio)
    if cached:
        file_id, title = cached
        status = await msg.reply_text("⚡ Знайдено в кеші. Надсилаю...")
        try:
            await send_cached(update, file_id, title, audio)
            await status.delete()
            return
        except Exception:
            URL_CACHE.pop(cache_key(url, audio), None)
            await safe_edit(status, "Кеш застарів. Завантажую заново...")

    cancel_event = Event()
    CANCEL_EVENTS[cid] = cancel_event
    ACTIVE_TASKS[cid] = {
        "url": url,
        "platform": platform,
        "audio": audio,
        "quality": quality,
        "started_at": time.time(),
        "user_id": uid,
    }

    async with PARALLEL_LIMIT:
        status = await msg.reply_text("🎵 Готую аудіо..." if audio else "⏳ Починаю завантаження...")
        loop = asyncio.get_running_loop()
        last_time = [0.0]
        last_text = [""]

        def progress_cb(text: str) -> None:
            now = time.monotonic()
            important = text.startswith(("🔧", "📤", "✅", "🔁", "❌", "⚡"))
            if text == last_text[0] or (now - last_time[0] < PROGRESS_THROTTLE and not important):
                return

            last_time[0] = now
            last_text[0] = text
            asyncio.run_coroutine_threadsafe(safe_edit(status, text), loop)

        try:
            job = partial(download_media, url, platform, audio, quality, progress_cb, cancel_event)
            path, title = await loop.run_in_executor(None, job)

            if not path:
                stats_fail()
                await safe_edit(status, f"❌ {title}")
                return

            await safe_edit(status, "✅ Завантажено. Надсилаю...")
            size, file_id = await send_media(update, path, title, audio, progress_cb)

            if size:
                stats_ok(platform, size, audio)
                inc_user_download(uid, audio)
                record_history(uid, url, title, platform, audio)

                if file_id:
                    cache_set(url, audio, file_id, title)

            try:
                await status.delete()
            except Exception:
                pass

        finally:
            CANCEL_EVENTS.pop(cid, None)
            ACTIVE_TASKS.pop(cid, None)
            clean_old_files(False)
            cache_cleanup()


# ─────────────────────────── Public commands ───────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    text = PUBLIC_HELP

    if is_admin_update(update):
        text += "\n" + ADMIN_HELP

    await msg.reply_text(text)


async def admin_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    if not is_admin_update(update):
        if update.effective_message:
            await update.effective_message.reply_text("🚫 Ця команда доступна тільки адміну.")
        return
    await update.effective_message.reply_text(ADMIN_HELP)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg or not msg.text:
        return

    urls = extract_urls(msg.text)

    if not urls:
        await msg.reply_text("❌ Надішли посилання на відео.")
        return

    if len(urls) > 1:
        await msg.reply_text(f"🔗 Знайдено {len(urls)} посилання. Оброблю по черзі.")

    for url in urls:
        if not url_is_safe(url):
            await msg.reply_text("❌ Небезпечне або невалідне посилання.")
            continue

        platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)

        if not platform:
            await msg.reply_text(f"❌ Платформа не підтримується:\n{url[:120]}")
            continue

        await download_and_send(update, url, platform, False)


async def video_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    url = context.args[0].strip() if context.args else None

    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await msg.reply_text("❌ Використання: /video <посилання>")
        return

    if not url_is_safe(url):
        await msg.reply_text("❌ Небезпечне або невалідне посилання.")
        return

    platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)

    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return

    await download_and_send(update, url, platform, False)


async def dl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await video_command(update, context)


async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    url = context.args[0].strip() if context.args else None

    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await msg.reply_text("❌ Використання: /audio <посилання>")
        return

    if not url_is_safe(url):
        await msg.reply_text("❌ Небезпечне або невалідне посилання.")
        return

    platform = detect_platform(url)

    if not platform:
        await msg.reply_text("❌ Платформа не підтримується для аудіо.")
        return

    await download_and_send(update, url, platform, True)


async def quality_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    cid = chat_id(update)

    if not context.args:
        await msg.reply_text(
            f"⚙️ Поточна якість: {quality_for(cid)}\n\nОбери нову:",
            reply_markup=quality_keyboard(),
        )
        return

    value = context.args[0].lower().strip()

    if value not in {"best", "fast", "mobile"}:
        await msg.reply_text("❌ Доступно: best, fast, mobile")
        return

    SETTINGS.setdefault("quality", {})[str(cid)] = value
    save_settings()
    await msg.reply_text(f"✅ Якість змінено на: {value}")


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    cid = chat_id(update)
    await msg.reply_text(
        f"⚙️ Налаштування\n\nЯкість: {quality_for(cid)}\nURL-кеш: {len(URL_CACHE)} записів",
        reply_markup=settings_keyboard(cid),
    )


async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    uid = user_id(update)
    user = USERS.get(str(uid), {})

    text = (
        "👤 Твій профіль\n\n"
        f"ID: {uid}\n"
        f"Username: @{user.get('username', '') or 'немає'}\n"
        f"Завантажень: {int(user.get('downloads', 0))}\n"
        f"Відео: {int(user.get('video', 0))}\n"
        f"Аудіо: {int(user.get('audio', 0))}\n"
        f"Якість: {quality_for(chat_id(update))}\n"
        f"Перший запуск: {user.get('joined', '-')}\n"
        f"Остання активність: {user.get('last_seen', '-')}"
    )
    await msg.reply_text(text)


async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if msg:
        await msg.reply_text(f"Твій ID: {user_id(update)}")


async def about_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if msg:
        await msg.reply_text(
            "🎥 Video Downloader Bot\n\n"
            "Функції: відео, аудіо MP3, обкладинки, субтитри, кліпи, історія, кеш, адмін-панель.\n"
            f"Власник: {OWNER_HANDLE}\n"
            f"Режим: {'webhook' if WEBHOOK_URL else 'polling'}"
        )


async def tips_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if msg:
        await msg.reply_text(
            "💡 Підказки\n\n"
            "1. Для великих файлів використовуй /quality mobile.\n"
            "2. Для YouTube на Render часто потрібен cookies.txt.\n"
            "3. Можна просто кинути посилання без команди.\n"
            "4. Для аудіо використовуй /audio <url>.\n"
            "5. Для короткого фрагмента використовуй /clip <url> 00:10 00:30."
        )


async def checkurl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    url = context.args[0].strip() if context.args else None

    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await msg.reply_text("❌ Використання: /checkurl <посилання>")
        return

    safe = url_is_safe(url)
    platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)

    await msg.reply_text(
        "🔎 Перевірка URL\n\n"
        f"Безпечне: {'так' if safe else 'ні'}\n"
        f"Платформа: {platform or 'не визначено'}\n"
        f"URL: {url[:300]}"
    )


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    uid = user_id(update)
    hist = HISTORY.get(str(uid), [])

    if not hist:
        await msg.reply_text("📋 Історія порожня.")
        return

    lines = [f"📋 Останні {len(hist)} завантажень:", ""]

    for index, item in enumerate(hist, 1):
        lines.append(
            f"{index}. [{item.get('platform', '?')}] {item.get('title', 'video')[:55]}\n"
            f"   Тип: {item.get('type', '-')}, дата: {str(item.get('ts', '-'))[:10]}"
        )

    await msg.reply_text("\n".join(lines)[:3900])


async def clearhistory_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    HISTORY[str(user_id(update))] = []
    save_history()
    await msg.reply_text("✅ Твою історію очищено.")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    platforms = STATS.get("platforms", {})

    lines = [
        "📊 Статистика",
        "",
        f"Успішних: {STATS.get('success', 0)}",
        f"Помилок: {STATS.get('errors', 0)}",
        f"Відео: {STATS.get('video', 0)}",
        f"Аудіо: {STATS.get('audio', 0)}",
        f"Відправлено: {human_bytes(STATS.get('bytes', 0))}",
        f"Користувачів: {len(USERS)}",
        f"URL-кеш: {len(URL_CACHE)}",
        f"Аптайм: {uptime_text()}",
    ]

    if platforms:
        lines += ["", "Платформи:"]
        for platform, count in sorted(platforms.items(), key=lambda item: -item[1]):
            lines.append(f"• {platform}: {count}")

    if update.effective_message:
        await update.effective_message.reply_text("\n".join(lines))


async def health_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    cookie = cookies_file()
    files = len(list(DOWNLOAD_DIR.glob("*")))
    disk = sum(path.stat().st_size for path in DOWNLOAD_DIR.glob("*") if path.is_file())

    await msg.reply_text(
        "🩺 Health check\n\n"
        f"Python: {sys.version.split()[0]}\n"
        f"yt-dlp: {getattr(getattr(yt_dlp, 'version', None), '__version__', '?')}\n"
        f"ffmpeg: {'✅ ' + str(FFMPEG_PATH) if FFMPEG_PATH else '❌ не знайдено'}\n"
        f"cookies.txt: {'✅ ' + cookie if cookie else '❌ не знайдено'}\n"
        f"Режим: {'webhook' if WEBHOOK_URL else 'polling'}\n"
        f"downloads/: {files} файлів ({human_bytes(disk)})\n"
        f"Активних: {len(ACTIVE_TASKS)}/{PARALLEL_DOWNLOADS}\n"
        f"Max upload: {human_bytes(MAX_UPLOAD_BYTES)}\n"
        f"Аптайм: {uptime_text()}"
    )


async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if msg:
        start = time.monotonic()
        sent = await msg.reply_text("🏓 Pong!")
        ms = int((time.monotonic() - start) * 1000)
        await sent.edit_text(f"🏓 Pong! {ms}ms")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    event = CANCEL_EVENTS.get(chat_id(update))

    if not event:
        await msg.reply_text("Немає активного завантаження.")
        return

    event.set()
    await msg.reply_text("🛑 Скасовую завантаження...")


async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    uid = user_id(update)
    admin = is_admin_update(update)

    visible_tasks = []
    for cid_key, task in ACTIVE_TASKS.items():
        if admin or int(task.get("user_id", 0)) == uid:
            visible_tasks.append((cid_key, task))

    if not visible_tasks:
        await msg.reply_text("Черга порожня.")
        return

    lines = [f"📋 Активних: {len(visible_tasks)}", ""]

    for _, task in visible_tasks:
        elapsed = seconds_text(time.time() - float(task.get("started_at", time.time())))
        lines.append(
            f"• {task.get('platform')} | "
            f"{'аудіо' if task.get('audio') else 'відео'} | "
            f"{task.get('quality')} | {elapsed}"
        )

    await msg.reply_text("\n".join(lines))


async def clean_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    if update.effective_message:
        deleted = clean_old_files(True)
        cache_cleanup()
        await update.effective_message.reply_text(f"🧹 Видалено файлів: {deleted}")


async def cookies_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    path = cookies_file()

    if not path:
        await msg.reply_text(
            "❌ cookies.txt не знайдено або формат неправильний.\n\n"
            "Перший рядок має бути:\n"
            "# Netscape HTTP Cookie File\n\n"
            "На Render краще додати cookies.txt у Secret Files."
        )
        return

    await msg.reply_text(
        f"✅ cookies.txt знайдено\n"
        f"Шлях: {path}\n"
        f"Розмір: {human_bytes(Path(path).stat().st_size)}"
    )


async def platforms_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    if update.effective_message:
        await update.effective_message.reply_text(
            "📡 Підтримувані платформи:\n\n" + "\n".join(f"• {name}" for name in URL_PATTERNS)
        )


async def thumb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    url = context.args[0].strip() if context.args else None

    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await msg.reply_text("❌ Використання: /thumb <посилання>")
        return

    if not url_is_safe(url):
        await msg.reply_text("❌ Небезпечне або невалідне посилання.")
        return

    platform = detect_platform(url)
    status = await msg.reply_text("🖼 Завантажую обкладинку...")

    try:
        loop = asyncio.get_running_loop()
        path, title = await loop.run_in_executor(
            None,
            partial(download_thumbnail, url, platform, quality_for(chat_id(update))),
        )

        if not path:
            await safe_edit(status, "❌ Обкладинку не знайдено.")
            return

        with open(path, "rb") as file:
            await msg.reply_photo(photo=file, caption=f"🖼 {title[:200]}")

        await status.delete()
        remove_file(path)

    except Exception as exc:
        await safe_edit(status, f"❌ {friendly_error(platform, str(exc))}")


async def sub_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    url = context.args[0].strip() if context.args else None

    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await msg.reply_text("❌ Використання: /sub <посилання>")
        return

    if not url_is_safe(url):
        await msg.reply_text("❌ Небезпечне або невалідне посилання.")
        return

    platform = detect_platform(url)
    status = await msg.reply_text("📝 Шукаю субтитри...")

    try:
        loop = asyncio.get_running_loop()
        files, title = await loop.run_in_executor(
            None,
            partial(download_subtitles, url, platform, quality_for(chat_id(update))),
        )

        if not files:
            await safe_edit(status, "❌ Субтитри не знайдено.")
            return

        await safe_edit(status, f"📝 Знайдено {len(files)} файл(и):\n{title[:160]}")

        for path in files:
            with open(path, "rb") as file:
                await msg.reply_document(document=file, caption=f"📝 {Path(path).name}")
            remove_file(path)

    except Exception as exc:
        await safe_edit(status, f"❌ {friendly_error(platform, str(exc))}")


async def info_or_formats(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    url = context.args[0].strip() if context.args else None

    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await msg.reply_text(f"❌ Використання: /{mode} <посилання>")
        return

    if not url_is_safe(url):
        await msg.reply_text("❌ Небезпечне або невалідне посилання.")
        return

    if DIRECT_VIDEO_RE.search(url):
        await msg.reply_text("ℹ️ Це пряме посилання на файл.")
        return

    platform = detect_platform(url)

    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return

    status = await msg.reply_text("🔎 Отримую дані...")

    try:
        func = extract_info_text if mode == "info" else extract_formats_text
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            partial(func, url, platform, quality_for(chat_id(update))),
        )
        await safe_edit(status, result)

    except Exception as exc:
        await safe_edit(status, f"❌ {friendly_error(platform, str(exc))}")


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await info_or_formats(update, context, "info")


async def formats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await info_or_formats(update, context, "formats")


async def clip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return

    if not FFMPEG_PATH:
        await msg.reply_text("❌ /clip потребує ffmpeg, якого немає на сервері.")
        return

    args = context.args or []
    url = None
    start_time = None
    end_time = None

    if len(args) >= 3:
        url, start_time, end_time = args[0], args[1], args[2]
    elif len(args) == 2 and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        if found:
            url, start_time, end_time = found[0], args[0], args[1]

    if not url or not start_time or not end_time:
        await msg.reply_text(
            "❌ Використання:\n"
            "/clip <url> <старт> <кінець>\n\n"
            "Приклад:\n"
            "/clip https://youtu.be/xxx 00:01:30 00:02:00"
        )
        return

    time_re = re.compile(r"^\d{1,2}:\d{2}(:\d{2})?$")
    if not time_re.match(start_time) or not time_re.match(end_time):
        await msg.reply_text("❌ Час має бути у форматі MM:SS або HH:MM:SS")
        return

    if not url_is_safe(url):
        await msg.reply_text("❌ Небезпечне або невалідне посилання.")
        return

    platform = detect_platform(url)
    cid = chat_id(update)
    cancel_event = Event()
    CANCEL_EVENTS[cid] = cancel_event

    status = await msg.reply_text("⏳ Завантажую відео для нарізки...")
    loop = asyncio.get_running_loop()
    last_time = [0.0]
    last_text = [""]

    def progress_cb(text: str) -> None:
        now = time.monotonic()
        if text == last_text[0] or now - last_time[0] < PROGRESS_THROTTLE:
            return
        last_time[0] = now
        last_text[0] = text
        asyncio.run_coroutine_threadsafe(safe_edit(status, text), loop)

    try:
        path, title = await loop.run_in_executor(
            None,
            partial(download_media, url, platform, False, quality_for(cid), progress_cb, cancel_event),
        )

        if not path:
            await safe_edit(status, f"❌ {title}")
            return

        await safe_edit(status, "✂️ Нарізаю кліп...")

        clip_path = str(safe_filename("clip", url, "mp4"))
        ok, error = await loop.run_in_executor(
            None,
            partial(clip_video_ffmpeg, path, start_time, end_time, clip_path),
        )

        remove_file(path)

        if not ok:
            await safe_edit(status, f"❌ {error}")
            return

        if Path(clip_path).stat().st_size > MAX_UPLOAD_BYTES:
            remove_file(clip_path)
            await safe_edit(status, "❌ Кліп завеликий для Telegram. Зменш інтервал.")
            return

        await safe_edit(status, "📤 Надсилаю кліп...")

        with open(clip_path, "rb") as file:
            await msg.reply_video(
                video=file,
                caption=f"✂️ {title[:160]}\n⏱ {start_time} → {end_time}",
                supports_streaming=True,
                read_timeout=180,
                write_timeout=180,
                connect_timeout=60,
                pool_timeout=60,
            )

        remove_file(clip_path)

        try:
            await status.delete()
        except Exception:
            pass

    except Exception as exc:
        await safe_edit(status, f"❌ {friendly_error(platform, str(exc))}")

    finally:
        CANCEL_EVENTS.pop(cid, None)
        ACTIVE_TASKS.pop(cid, None)
        clean_old_files(False)


# ─────────────────────────── Callbacks ─────────────────────────
async def quality_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    await query.answer()

    value = query.data.split(":", 1)[1] if ":" in query.data else ""

    if value not in {"best", "fast", "mobile"}:
        return

    cid = int(query.message.chat.id)
    SETTINGS.setdefault("quality", {})[str(cid)] = value
    save_settings()

    await query.edit_message_text(f"✅ Якість змінено на: {value}")


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    await query.answer()

    action = query.data.split(":", 1)[1] if ":" in query.data else ""
    cid = int(query.message.chat.id)

    if action == "quality":
        await query.edit_message_text(
            f"Поточна якість: {quality_for(cid)}\nОбери:",
            reply_markup=quality_keyboard(),
        )
        return

    if action == "clearcache":
        URL_CACHE.clear()
        await query.edit_message_text("✅ URL-кеш очищено.")
        return

    if action == "stats":
        await query.answer(
            f"Успішних: {STATS.get('success', 0)}\n"
            f"Помилок: {STATS.get('errors', 0)}\n"
            f"Відправлено: {human_bytes(STATS.get('bytes', 0))}",
            show_alert=True,
        )


# ─────────────────────────── Admin commands ────────────────────
@require_admin
async def setup_commands_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    await context.bot.set_my_commands(PUBLIC_BOT_COMMANDS, scope=BotCommandScopeDefault())
    await context.bot.set_my_commands(
        ADMIN_BOT_COMMANDS,
        scope=BotCommandScopeChat(chat_id=chat_id(update)),
    )

    await msg.reply_text("✅ Меню команд оновлено. Звичайні користувачі не бачитимуть адмін-команди.")


@require_admin
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    text = " ".join(context.args).strip()

    if not text:
        await msg.reply_text("❌ Використання: /broadcast <текст>")
        return

    sent = 0
    failed = 0
    status = await msg.reply_text("📣 Починаю розсилку...")

    for uid in list(USERS.keys()):
        try:
            await context.bot.send_message(chat_id=int(uid), text=text[:3900])
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await safe_edit(status, f"✅ Розсилка завершена.\nНадіслано: {sent}\nПомилок: {failed}")


@require_admin
async def ad_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    text = " ".join(context.args).strip()

    if not text:
        await msg.reply_text("❌ Використання: /ad <текст>")
        return

    sent = 0
    failed = 0
    status = await msg.reply_text("📢 Надсилаю рекламу...")

    ad_text = f"📢 Реклама\n\n{text[:3500]}"

    for uid in list(USERS.keys()):
        try:
            await context.bot.send_message(chat_id=int(uid), text=ad_text)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await safe_edit(status, f"✅ Рекламу надіслано.\nНадіслано: {sent}\nПомилок: {failed}")


@require_admin
async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    if not USERS:
        await msg.reply_text("Користувачів поки немає.")
        return

    lines = [f"👥 Користувачі: {len(USERS)}", ""]

    for uid, item in list(USERS.items())[-40:]:
        username = item.get("username") or ""
        name = " ".join(x for x in [item.get("first_name", ""), item.get("last_name", "")] if x).strip()
        downloads = int(item.get("downloads", 0))
        banned = " 🚫" if int(uid) in BANNED_USERS else ""
        lines.append(f"• {uid}{banned} @{username or '-'} | {name or '-'} | {downloads} завантажень")

    await msg.reply_text("\n".join(lines)[:3900])


@require_admin
async def topusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    items = sorted(
        USERS.items(),
        key=lambda pair: int(pair[1].get("downloads", 0)),
        reverse=True,
    )[:15]

    if not items:
        await msg.reply_text("Немає даних.")
        return

    lines = ["🏆 Топ користувачів", ""]

    for index, (uid, item) in enumerate(items, 1):
        username = item.get("username") or "-"
        downloads = int(item.get("downloads", 0))
        lines.append(f"{index}. {uid} @{username} — {downloads}")

    await msg.reply_text("\n".join(lines))


@require_admin
async def user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    if not context.args:
        await msg.reply_text("❌ Використання: /user <id>")
        return

    uid = context.args[0].strip()
    item = USERS.get(uid)

    if not item:
        await msg.reply_text("❌ Користувача не знайдено.")
        return

    await msg.reply_text(
        "👤 Користувач\n\n"
        f"ID: {uid}\n"
        f"Username: @{item.get('username', '') or 'немає'}\n"
        f"Імʼя: {item.get('first_name', '')} {item.get('last_name', '')}\n"
        f"Joined: {item.get('joined', '-')}\n"
        f"Last seen: {item.get('last_seen', '-')}\n"
        f"Downloads: {int(item.get('downloads', 0))}\n"
        f"Video: {int(item.get('video', 0))}\n"
        f"Audio: {int(item.get('audio', 0))}\n"
        f"Messages: {int(item.get('messages', 0))}\n"
        f"Banned: {'так' if int(uid) in BANNED_USERS else 'ні'}"
    )


@require_admin
async def exportusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    export_path = BASE_DIR / "users_export.json"
    write_json(export_path, USERS)

    with open(export_path, "rb") as file:
        await msg.reply_document(document=file, caption="👥 users_export.json")

    remove_file(export_path)


@require_admin
async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    if not context.args or not context.args[0].isdigit():
        await msg.reply_text("❌ Використання: /ban <id>")
        return

    uid = int(context.args[0])

    if uid == OWNER_ID:
        await msg.reply_text("❌ Не можна заблокувати власника.")
        return

    BANNED_USERS.add(uid)
    save_bans()
    await msg.reply_text(f"✅ Користувача {uid} заблоковано.")


@require_admin
async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    if not context.args or not context.args[0].isdigit():
        await msg.reply_text("❌ Використання: /unban <id>")
        return

    uid = int(context.args[0])
    BANNED_USERS.discard(uid)
    save_bans()
    await msg.reply_text(f"✅ Користувача {uid} розблоковано.")


@require_admin
async def reset_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    STATS.clear()
    STATS.update({"success": 0, "errors": 0, "bytes": 0, "platforms": {}, "audio": 0, "video": 0})
    save_stats()

    if update.effective_message:
        await update.effective_message.reply_text("✅ Статистику очищено.")


@require_admin
async def adminstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    total_downloads = sum(int(item.get("downloads", 0)) for item in USERS.values())
    banned = len(BANNED_USERS)
    active_tasks = len(ACTIVE_TASKS)

    await msg.reply_text(
        "📊 Адмін-статистика\n\n"
        f"Користувачів: {len(USERS)}\n"
        f"Заблоковано: {banned}\n"
        f"Завантажень користувачів: {total_downloads}\n"
        f"Успішних за stats: {STATS.get('success', 0)}\n"
        f"Помилок: {STATS.get('errors', 0)}\n"
        f"Активних задач: {active_tasks}\n"
        f"Кеш: {len(URL_CACHE)}\n"
        f"Ліміт користувачів: {user_limit() or 'off'}\n"
        f"Owner ID: {OWNER_ID or 'ще не зафіксовано'}"
    )


@require_admin
async def userlimit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    if not context.args:
        await msg.reply_text(f"Поточний ліміт: {user_limit() or 'off'}\nВикористання: /userlimit <n|off>")
        return

    raw = context.args[0].strip().lower()

    if raw == "off":
        SETTINGS.setdefault("limits", {})["max_users"] = 0
        save_settings()
        await msg.reply_text("✅ Ліміт користувачів вимкнено.")
        return

    if not raw.isdigit():
        await msg.reply_text("❌ Використання: /userlimit <n|off>")
        return

    value = int(raw)
    SETTINGS.setdefault("limits", {})["max_users"] = value
    save_settings()
    await msg.reply_text(f"✅ Ліміт користувачів встановлено: {value}")


@require_admin
async def clearcache_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    URL_CACHE.clear()
    if update.effective_message:
        await update.effective_message.reply_text("✅ URL-кеш очищено.")


@require_admin
async def savecookies_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(
            "🍪 Як додати cookies.txt на Render\n\n"
            "1. Зайди в Render → твій сервіс.\n"
            "2. Відкрий Environment → Secret Files.\n"
            "3. Створи файл cookies.txt.\n"
            "4. Встав cookies у Netscape форматі.\n"
            "5. Перший рядок має бути:\n"
            "# Netscape HTTP Cookie File\n\n"
            "Після цього зроби redeploy і перевір /cookies."
        )


@require_admin
async def update_ytdlp_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    status = await msg.reply_text("🔄 Оновлюю yt-dlp...")

    def job() -> str:
        env = os.environ.copy()
        env["PIP_NO_CACHE_DIR"] = "1"
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--user", "--no-cache-dir", "-U", "yt-dlp"],
            env=env,
        )
        importlib.invalidate_caches()
        return "ok"

    try:
        await asyncio.get_running_loop().run_in_executor(None, job)
        version = getattr(getattr(yt_dlp, "version", None), "__version__", "?")
        await safe_edit(status, f"✅ yt-dlp оновлено. Версія: {version}")
    except Exception as exc:
        await safe_edit(status, f"❌ Не вдалося оновити yt-dlp:\n{safe_text(exc, 700)}")


# ─────────────────────────── Error handler ─────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Unhandled exception", exc_info=context.error)

    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("❌ Виникла внутрішня помилка. Спробуй ще раз.")
    except Exception:
        pass


# ─────────────────────────── App setup ─────────────────────────
async def setup_command_menu(app: Application) -> None:
    try:
        await app.bot.set_my_commands(PUBLIC_BOT_COMMANDS, scope=BotCommandScopeDefault())
        log.info("Public command menu installed")
    except Exception:
        log.exception("Could not set command menu")


def add_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", start_command))
    app.add_handler(CommandHandler("video", video_command))
    app.add_handler(CommandHandler("dl", dl_command))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("quality", quality_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("clearhistory", clearhistory_command))
    app.add_handler(CommandHandler("profile", profile_command))
    app.add_handler(CommandHandler("myid", myid_command))
    app.add_handler(CommandHandler("about", about_command))
    app.add_handler(CommandHandler("tips", tips_command))
    app.add_handler(CommandHandler("checkurl", checkurl_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("health", health_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("queue", queue_command))
    app.add_handler(CommandHandler("clean", clean_command))
    app.add_handler(CommandHandler("cookies", cookies_command))
    app.add_handler(CommandHandler("platforms", platforms_command))
    app.add_handler(CommandHandler("thumb", thumb_command))
    app.add_handler(CommandHandler("sub", sub_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("formats", formats_command))
    app.add_handler(CommandHandler("clip", clip_command))

    app.add_handler(CommandHandler("admin", admin_help_command))
    app.add_handler(CommandHandler("setupcommands", setup_commands_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("ad", ad_command))
    app.add_handler(CommandHandler("users", users_command))
    app.add_handler(CommandHandler("topusers", topusers_command))
    app.add_handler(CommandHandler("user", user_command))
    app.add_handler(CommandHandler("exportusers", exportusers_command))
    app.add_handler(CommandHandler("ban", ban_command))
    app.add_handler(CommandHandler("unban", unban_command))
    app.add_handler(CommandHandler("resetstats", reset_stats_command))
    app.add_handler(CommandHandler("adminstats", adminstats_command))
    app.add_handler(CommandHandler("userlimit", userlimit_command))
    app.add_handler(CommandHandler("clearcache", clearcache_command))
    app.add_handler(CommandHandler("savecookies", savecookies_command))
    app.add_handler(CommandHandler("updateytdlp", update_ytdlp_command))

    app.add_handler(CallbackQueryHandler(quality_callback, pattern=r"^quality:"))
    app.add_handler(CallbackQueryHandler(settings_callback, pattern=r"^settings:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)


def main() -> None:
    clean_old_files(False)

    app = Application.builder().token(TOKEN).post_init(setup_command_menu).build()
    add_handlers(app)

    if WEBHOOK_URL:
        port = int(os.environ.get("PORT", "10000"))
        url_path = TOKEN
        webhook_url = f"{WEBHOOK_URL}/{TOKEN}"

        log.info("Starting webhook on port %s", port)
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=url_path,
            webhook_url=webhook_url,
            drop_pending_updates=True,
        )
    else:
        log.info("Starting polling")
        app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
