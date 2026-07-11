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

_IN_VENV = bool(
    getattr(sys, "real_prefix", False)
    or (getattr(sys, "base_prefix", None) and sys.base_prefix != sys.prefix)
    or os.environ.get("VIRTUAL_ENV")
)

if not _IN_VENV:
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
    cmd = [sys.executable, "-m", "pip", "install", "--no-cache-dir", "-U"]
    if not _IN_VENV:
        cmd.append("--user")
    cmd.append(package)
    subprocess.check_call(cmd, env=env)
    if not _IN_VENV:
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
        ("telegram.ext", "python-telegram-bot[webhooks,job-queue]>=21.0,<22.0"),
    ]
    for module_name, package_name in deps:
        if not _module_ok(module_name):
            _pip_install(package_name)

    # yt-dlp релізиться дуже часто (буває й по кілька разів на тиждень) саме
    # тому, що YouTube/TikTok/Instagram постійно змінюють захист. Якщо він
    # вже стоїть, але застарілий — бот тихо почне все гірше і гірше качати
    # з часом, і це буде виглядати як "бот зламався" без жодної причини в
    # коді. Тому оновлюємо його при КОЖНОМУ старті (можна вимкнути прапорцем
    # SKIP_YTDLP_UPDATE=1, якщо хочеш пришвидшити рестарти).
    if not _module_ok("yt_dlp"):
        _pip_install("yt-dlp")
    elif os.environ.get("SKIP_YTDLP_UPDATE", "0") != "1":
        try:
            _pip_install("yt-dlp")
        except Exception as exc:
            print(f"[BOOT] Не вдалося оновити yt-dlp, працюю зі старою версією: {exc}", flush=True)

    all_deps = deps + [("yt_dlp", "yt-dlp")]
    bad = [module_name for module_name, _ in all_deps if not _module_ok(module_name)]
    if bad:
        raise RuntimeError("Не вдалося встановити залежності: " + ", ".join(bad))


_ensure_deps()

# ─────────────────────────── Imports ───────────────────────────

import asyncio
import glob
import itertools
import json
import logging
import multiprocessing as mp
import re
import shutil
import time
from datetime import datetime
from functools import partial
from threading import Event
from typing import Any
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

import requests
import yt_dlp
from telegram import (
    BotCommand,
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

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано TOKEN у Environment Variables")

WEBHOOK_URL: str | None = os.environ.get("WEBHOOK_URL", "").rstrip("/") or None

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

SETTINGS_FILE = BASE_DIR / "bot_settings.json"
HISTORY_FILE = BASE_DIR / "bot_history.json"

MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(49 * 1024 * 1024)))
PROGRESS_THROTTLE = float(os.environ.get("PROGRESS_THROTTLE", "1.5"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
OLD_FILE_TTL = int(os.environ.get("OLD_FILE_TTL", str(60 * 60 * 3)))
MAX_LINKS_PER_MSG = int(os.environ.get("MAX_LINKS_PER_MESSAGE", "3"))
PARALLEL_DOWNLOADS = max(1, int(os.environ.get("PARALLEL_DOWNLOADS", "2")))
RATE_LIMIT_N = int(os.environ.get("RATE_LIMIT_N", "5"))
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", "60"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))
URL_CACHE_TTL = int(os.environ.get("URL_CACHE_TTL", "3600"))
MAX_HISTORY_PER_USER = int(os.environ.get("MAX_HISTORY_PER_USER", "10"))
DOWNLOAD_TIMEOUT = int(os.environ.get("DOWNLOAD_TIMEOUT", "300"))

PARALLEL_LIMIT = asyncio.Semaphore(PARALLEL_DOWNLOADS)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("video-bot")

FFMPEG_PATH = os.environ.get("FFMPEG_PATH") or shutil.which("ffmpeg")

# З листопада 2025 yt-dlp офіційно вимагає зовнішній JS-рантайм (Deno,
# Node, Bun або QuickJS) для повноцінного витягування YouTube-форматів —
# без нього частина форматів (а часом і всі) буде "missing". Це НЕ баг
# бота, а вимога самого yt-dlp через ускладнення JS-захисту YouTube.
# Дет.: https://github.com/yt-dlp/yt-dlp/issues/15012
JS_RUNTIME = next(
    (p for p in (shutil.which(x) for x in ("deno", "node", "bun", "quickjs")) if p),
    None,
)

# Публічний api.cobalt.tools захищений bot-protection (Turnstile) і
# офіційно "не призначений для сторонніх проєктів без дозволу власника".
# Якщо є власний інстанс cobalt або дозвіл/ключ — вкажи їх тут, інакше
# цей фолбек просто не спрацює на публічному сервері (це очікувано).
# Дет.: https://github.com/imputnet/cobalt/blob/main/docs/api.md
COBALT_API_URL = os.environ.get("COBALT_API_URL", "https://api.cobalt.tools/").rstrip("/") + "/"
COBALT_API_KEY = os.environ.get("COBALT_API_KEY")

URL_RE = re.compile(r"https?://[^\s<>\"]+", re.I)
DIRECT_VIDEO_RE = re.compile(
    r"https?://[^\s<>\"]+\.(?:mp4|mov|webm|m4v|mkv)(?:\?[^\s<>\"]*)?",
    re.I,
)

URL_PATTERNS: dict[str, re.Pattern[str]] = {
    "youtube": re.compile(
        r"(?:youtube\.com/(?:watch\?v=|shorts/|live/|embed/)|youtu\.be/|m\.youtube\.com/watch\?v=|music\.youtube\.com/watch)",
        re.I,
    ),
    "tiktok": re.compile(
        r"tiktok\.com/|vt\.tiktok\.com/|vm\.tiktok\.com/",
        re.I,
    ),
    "instagram": re.compile(
        r"(?:instagram\.com/(?:reel|reels|p|tv|stories|share)/|instagr\.am/|ddinstagram\.com/|www\.instagram\.com/)",
        re.I,
    ),
    "twitter": re.compile(
        r"(?:twitter\.com|x\.com)/(?:\w+/status|i/status|i/web/status)/\d+",
        re.I,
    ),
    "vimeo": re.compile(
        r"vimeo\.com/(?:\d+|channels/[^/]+/\d+|video/\d+)",
        re.I,
    ),
    "reddit": re.compile(
        r"reddit\.com/r/\w+/(?:comments|s)/|v\.redd\.it/",
        re.I,
    ),
    "facebook": re.compile(
        r"facebook\.com/(?:watch/\?v=|watch\?v=|reel/|share/r/|share/v/|[\w.]+/videos/|video\.php)|fb\.watch/",
        re.I,
    ),
    "likee": re.compile(r"likee\.video/|likee\.com/", re.I),
    "snapchat": re.compile(r"snapchat\.com/(?:spotlight|add)/", re.I),
    "pinterest": re.compile(r"pinterest\.[a-z.]+/pin/\d+|pin\.it/", re.I),
    "twitch": re.compile(r"twitch\.tv/(?:videos/\d+|clips/)", re.I),
    "dailymotion": re.compile(r"dailymotion\.com/video/", re.I),
    "rumble": re.compile(r"rumble\.com/v", re.I),
    "odysee": re.compile(r"odysee\.com/@", re.I),
    "bilibili": re.compile(r"bilibili\.com/video/", re.I),
    "coub": re.compile(r"coub\.com/view/", re.I),
    "streamable": re.compile(r"streamable\.com/", re.I),
    "medal": re.compile(r"medal\.tv/", re.I),
}

COBALT_SUPPORTED = {
    "youtube", "instagram", "tiktok", "twitter",
    "facebook", "reddit", "pinterest", "vimeo",
    "dailymotion", "twitch",
}

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "igshid", "si", "feature", "share_id", "ref", "h",
}

USER_HELP_TEXT = """🎥 *Video Downloader Bot*

Кинь посилання — бот завантажить відео або аудіо.

*📥 Команди:*

/video `<url>` — відео
/audio `<url>` — аудіо MP3
/cancel — скасувати активне завантаження
/queue — твої активні завантаження
/history — історія
/settings — налаштування
/quality — якість: best / fast / mobile
/platforms — список платформ
/ping — перевірка бота

*💡 Поради:*
• Instagram без cookies — працює через проксі (ddinstagram).
• YouTube без cookies — може блокувати, додай cookies.txt.
• Якщо відео велике — постав /quality mobile.
• Для скасування надішли /cancel під час завантаження.
"""

USER_BOT_COMMANDS = [
    BotCommand("start", "Запустити бота"),
    BotCommand("help", "Допомога"),
    BotCommand("video", "Завантажити відео"),
    BotCommand("audio", "Завантажити аудіо"),
    BotCommand("cancel", "Скасувати завантаження"),
    BotCommand("queue", "Активні завантаження"),
    BotCommand("history", "Історія"),
    BotCommand("settings", "Налаштування"),
    BotCommand("quality", "Якість відео"),
    BotCommand("platforms", "Платформи"),
    BotCommand("ping", "Перевірка"),
]

# ─────────────────────────── State ─────────────────────────────

# КРИТИЧНО: ключ - унікальний task_id, а НЕ chat_id. Раніше було по
# chat_id, і якщо в одному чаті одночасно активні два завантаження
# (два різні юзери, або один юзер встиг кинути другий лінк поки перший
# ще качається - PARALLEL_DOWNLOADS це якраз і дозволяє), записи
# перезаписували одне одного: /cancel міг скасувати чуже завантаження,
# а finally-блок того, хто фінішував першим, зносив запис ще активного.
_TASK_ID_COUNTER = itertools.count(1)
CANCEL_EVENTS: dict[int, Event] = {}
ACTIVE_TASKS: dict[int, dict[str, Any]] = {}
URL_CACHE: dict[str, tuple[str, str, float, bool]] = {}
RATE_BUCKETS: dict[int, list[float]] = {}


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
        tmp.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(path)
    except Exception:
        log.exception("JSON write error: %s", path)


SETTINGS = read_json(SETTINGS_FILE, {"quality": {}})
HISTORY: dict[str, list[dict[str, Any]]] = read_json(HISTORY_FILE, {})


def save_settings() -> None:
    write_json(SETTINGS_FILE, SETTINGS)


def save_history() -> None:
    write_json(HISTORY_FILE, HISTORY)


def record_history(uid: int, url: str, title: str, platform: str) -> None:
    key = str(uid)
    HISTORY.setdefault(key, [])
    HISTORY[key].insert(
        0,
        {
            "url": url,
            "title": safe_text(title, 80),
            "platform": platform,
            "ts": datetime.utcnow().isoformat(),
        },
    )
    HISTORY[key] = HISTORY[key][:MAX_HISTORY_PER_USER]
    save_history()


# ─────────────────────────── Helpers ───────────────────────────

def human_bytes(num: int | float | None) -> str:
    if not num:
        return "0 B"
    n = float(num)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(n)} B"
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def seconds_text(seconds: int | float | None) -> str:
    if not seconds:
        return "0с"
    s = max(0, int(seconds))
    h, r = divmod(s, 3600)
    m, sec = divmod(r, 60)
    if h:
        return f"{h}г {m}хв {sec}с"
    if m:
        return f"{m}хв {sec}с"
    return f"{sec}с"


def safe_text(value: Any, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "video")).strip()
    if not text:
        text = "video"
    return text[:limit]


def chat_id(update: Update) -> int:
    return int(update.effective_chat.id) if update.effective_chat else 0


def user_id(update: Update) -> int:
    return int(update.effective_user.id) if update.effective_user else 0


def quality_for(cid: int) -> str:
    return SETTINGS.get("quality", {}).get(str(cid), "fast")


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
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            if not lines:
                continue
            first = lines[0].strip()
            if first in {"# Netscape HTTP Cookie File", "# HTTP Cookie File"}:
                return str(path)
        except Exception:
            continue
    return None


def extract_urls(text: str) -> list[str]:
    found: list[str] = []
    for url in URL_RE.findall(text or ""):
        clean = url.strip().strip(".,;)\n\r\t <>")
        if clean and clean not in found:
            found.append(clean)
    return found[:MAX_LINKS_PER_MSG]


def detect_platform(url: str) -> str | None:
    for name, pattern in URL_PATTERNS.items():
        if pattern.search(url):
            return name
    return None


def platform_for_url(url: str) -> str | None:
    if DIRECT_VIDEO_RE.search(url):
        return "direct"
    return detect_platform(url)


def normalize_url(url: str) -> str:
    """Очистка URL від трекінг-параметрів та нормалізація."""
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        clean_qs = {k: v for k, v in qs.items() if k not in TRACKING_PARAMS}
        new_query = urlencode(clean_qs, doseq=True) if clean_qs else ""
        normalized = parsed._replace(query=new_query).geturl()
        normalized = normalized.replace("m.youtube.com", "www.youtube.com")
        normalized = normalized.replace("m.instagram.com", "www.instagram.com")
        normalized = normalized.replace("m.facebook.com", "www.facebook.com")
        return normalized
    except Exception:
        return url


def to_ddinstagram(url: str) -> str:
    """
    Конвертує instagram.com URL у ddinstagram.com проксі.

    БАГ, який тут був: ланцюжок двох .replace() ламав URL для
    найпоширенішого формату www.instagram.com/... — після першої заміни
    рядок містив "ddinstagram.com", а всередині нього є підрядок
    "instagram.com" (з позиції 2), тож друга заміна спрацьовувала ЩЕ РАЗ
    і давала "ddddinstagram.com" — домен, якого не існує. Один regex
    без повторного проходу вирішує це раз і назавжди.
    """
    return re.sub(r"(?:www\.|m\.)?instagram\.com", "ddinstagram.com", url)


def safe_filename(prefix: str, url: str, ext: str = "mp4") -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug = url.split("?")[0].rstrip("/").split("/")[-1] or "video"
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", slug)[:35] or "video"
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
    count = 0
    for path in DOWNLOAD_DIR.glob("*"):
        try:
            if path.is_file() and (force or now - path.stat().st_mtime > OLD_FILE_TTL):
                path.unlink()
                count += 1
        except OSError:
            pass
    return count


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


def rate_limited(uid: int) -> bool:
    if RATE_LIMIT_N <= 0:
        return False
    now = time.time()
    bucket = RATE_BUCKETS.setdefault(uid, [])
    bucket[:] = [ts for ts in bucket if now - ts <= RATE_LIMIT_WINDOW]
    if len(bucket) >= RATE_LIMIT_N:
        return True
    bucket.append(now)
    return False


def retry_after_seconds(uid: int) -> int:
    bucket = RATE_BUCKETS.get(uid, [])
    if not bucket:
        return RATE_LIMIT_WINDOW
    oldest = min(bucket)
    wait = RATE_LIMIT_WINDOW - int(time.time() - oldest)
    return max(1, wait)


def get_url_from_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str | None:
    msg = update.effective_message
    if context.args:
        return context.args[0].strip()
    if msg and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        if found:
            return found[0]
    return None


# ─────────────────────────── URL Cache ─────────────────────────

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
    URL_CACHE[cache_key(url, audio)] = (
        file_id,
        safe_text(title, 180),
        time.time(),
        audio,
    )


def cache_cleanup() -> None:
    now = time.time()
    dead = [key for key, value in URL_CACHE.items() if now - value[2] > URL_CACHE_TTL]
    for key in dead:
        URL_CACHE.pop(key, None)


# ─────────────────────────── Progress ──────────────────────────

def progress_bar(pct: int, width: int = 14) -> str:
    pct = max(0, min(100, int(pct)))
    filled = int(width * pct / 100)
    return f"[{'▓' * filled}{'░' * (width - filled)}] {pct}%"


def progress_text(prefix: str, done: int, total: int | None, start: float) -> str:
    elapsed = max(time.monotonic() - start, 0.1)
    speed = done / elapsed if done else 0
    if total:
        pct = max(0, min(100, int(done * 100 / total)))
        eta = int((total - done) / speed) if speed else 0
        return (
            f"{prefix}\n"
            f"{progress_bar(pct)}\n"
            f"{human_bytes(done)} / {human_bytes(total)}\n"
            f"⚡ {human_bytes(speed)}/s • ETA {seconds_text(eta)}"
        )
    return (
        f"{prefix}\n"
        f"Завантажено: {human_bytes(done)}\n"
        f"⚡ {human_bytes(speed)}/s"
    )


# ─────────────────────────── Error text ────────────────────────

def friendly_error(platform: str | None, error: str) -> str:
    err = str(error or "")
    low = err.lower()

    if "exit code 137" in low or "killed" in low:
        return "⚠️ Серверу не вистачило ресурсів. Постав /quality mobile і спробуй ще раз."

    if any(x in low for x in ["max-filesize", "max_filesize", "larger than max", "exceeds max"]):
        return (
            "⚠️ Джерело більше за ліміт Telegram (50MB) — завантаження зупинено ще до старту.\n"
            "Постав /quality mobile: там формати менші, це має допомогти."
        )

    if platform == "youtube" and "javascript runtime" in low:
        return (
            "⚠️ На сервері немає JS-рантайму (Deno/Node), а YouTube з листопада 2025 "
            "вимагає його для повноцінного витягування форматів.\n\n"
            "Постав Deno на сервері (офіційний спосіб — команда з "
            "https://github.com/yt-dlp/yt-dlp/wiki/EJS), і частина відео, "
            "які зараз не качаються (або якими бракує форматів), запрацюють."
        )

    if platform == "youtube" and any(
        x in low
        for x in [
            "sign in to confirm",
            "not a bot",
            "use --cookies",
            "confirm you're not a bot",
            "po token",
        ]
    ):
        return (
            "🍪 YouTube просить авторизацію.\n\n"
            "Рішення:\n"
            "1. Поклади cookies.txt поруч із bot.py\n"
            "2. Перший рядок: # Netscape HTTP Cookie File\n"
            "3. Експортуй cookies з браузера (розширення Get cookies.txt)\n\n"
            "Без cookies YouTube блокує free-сервери."
        )

    if platform == "instagram" and any(
        x in low
        for x in [
            "login", "log in", "sign in", "private",
            "restricted", "unauthorized", "please wait",
        ]
    ):
        has_cookies = bool(cookies_file())
        if has_cookies:
            return (
                "🔒 Instagram заблокував запит.\n\n"
                "Можливі причини:\n"
                "• cookies.txt застарів або неправильний\n"
                "• акаунт забанений\n"
                "• IP тимчасово обмежений\n\n"
                "Спробуй оновити cookies.txt і почекати 10-15 хв."
            )
        return (
            "🔒 Instagram заблокував запит.\n\n"
            "Бот вже пробував проксі (ddinstagram.com), але не вийшло.\n\n"
            "Для надійності додай cookies.txt:\n"
            "1. Увійди в Instagram у Chrome\n"
            "2. Встанови розширення Get cookies.txt LOCALLY\n"
            "3. Експортуй cookies для instagram.com\n"
            "4. Поклади файл поруч із bot.py\n"
            "5. Перший рядок: # Netscape HTTP Cookie File"
        )

    if "requested format is not available" in low:
        return "⚠️ Ця якість недоступна. Спробуй /quality fast або /quality mobile."

    if "ffmpeg" in low and not FFMPEG_PATH:
        return "⚠️ Немає ffmpeg на сервері. Для аудіо потрібен ffmpeg. Спробуй /quality fast для відео."

    if "unsupported url" in low:
        return "❌ Посилання не підтримується або платформа змінила захист."

    if "private" in low or "login" in low:
        return "🔒 Відео приватне або потрібен вхід. Додай cookies.txt."

    if any(x in low for x in ["network", "connection", "timeout", "reset by peer", "read error"]):
        return "🌐 Помилка мережі. Спробуй ще раз через хвилину."

    if any(x in low for x in ["429", "too many", "rate limit"]):
        return "⏳ Платформа тимчасово блокує. Зачекай 5-10 хвилин."

    if "geo" in low or "not available in your country" in low:
        return "🌍 Відео недоступне в регіоні сервера."

    if "name resolution" in low or "resolve" in low:
        return "🌐 DNS-помилка. Спробуй ще раз."

    if "video unavailable" in low and platform == "youtube":
        return "❌ Відео недоступне (видалено, приватне або обмежене віком)."

    return safe_text(err, 900)


def is_transient_error(error: str) -> bool:
    low = str(error or "").lower()
    return any(
        x in low
        for x in [
            "network", "connection", "timeout", "reset by peer",
            "read error", "http error 5", "503", "502", "429",
            "name resolution", "eof occurred",
        ]
    )


# ─────────────────────────── yt-dlp ────────────────────────────

def first_entry(info: dict[str, Any]) -> dict[str, Any]:
    entries = info.get("entries")
    if not entries:
        return info
    valid_entries = [item for item in entries if item]
    return valid_entries[0] if valid_entries else info


def find_file(info: dict[str, Any], ydl: yt_dlp.YoutubeDL) -> str | None:
    candidates: list[str] = []
    for item in info.get("requested_downloads") or []:
        if isinstance(item, dict):
            for key in ("filepath", "_filename", "filename"):
                value = item.get(key)
                if value:
                    candidates.append(str(value))
    for key in ("filepath", "_filename", "filename"):
        value = info.get(key)
        if value:
            candidates.append(str(value))
    try:
        candidates.append(str(ydl.prepare_filename(info)))
    except Exception:
        pass
    if info.get("id"):
        candidates.extend(glob.glob(str(DOWNLOAD_DIR / f"*{info['id']}*")))
    existing = [str(Path(p)) for p in candidates if p and Path(p).exists()]
    existing.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
    return existing[0] if existing else None


def format_selector(
    platform: str | None,
    audio: bool,
    quality: str,
    has_ffmpeg: bool = True,
) -> str:
    """
    Формат-селектор для yt-dlp.

    КРИТИЧНО: для YouTube audio НЕМАЄ окремих форматів без DASH.
    Тому для аудіо ми завжди просимо bestaudio — але DASH маніфести
    мають бути доступні (не пропускатися в extractor_args).
    """
    if audio:
        if has_ffmpeg:
            # ffmpeg є — беремо будь-яке аудіо, конвертуємо в mp3
            return "bestaudio/best"
        else:
            # ffmpeg немає — пробуємо взяти m4a/webm без конвертації
            return (
                "bestaudio[ext=m4a]/bestaudio[ext=webm]/"
                "bestaudio[ext=opus]/bestaudio/best"
            )

    # --- Відео ---
    if not has_ffmpeg:
        # Без ffmpeg — тільки вже злиті формати
        if quality == "mobile":
            return "best[height<=480][ext=mp4]/best[height<=480]/best"
        if quality == "best":
            return "best[ext=mp4]/best"
        return "best[height<=720][ext=mp4]/best[height<=720]/best"

    # З ffmpeg — можемо мержити video + audio
    if quality == "mobile":
        return (
            "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/"
            "best[height<=480][ext=mp4]/best[height<=480]/best"
        )
    if quality == "fast":
        return (
            "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/"
            "best[height<=720][ext=mp4]/best[height<=720]/best"
        )
    # best
    if platform == "youtube":
        return "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/b[ext=mp4]/best"
    return "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"


def _build_user_agent(platform: str | None) -> str:
    if platform == "tiktok":
        return (
            "com.zhiliaoapp.musically/2022600030 "
            "(Linux; U; Android 12; en_US; Pixel 6; Build/SP1A.210812.016)"
        )
    if platform == "instagram":
        return (
            "Mozilla/5.0 (Linux; Android 14; SM-S918B) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/151.0.0.0 Mobile Safari/537.36"
        )
    return (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    )


def ytdlp_opts(
    platform: str | None,
    audio: bool,
    quality: str,
    hook=None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    has_ffmpeg = bool(FFMPEG_PATH)

    opts: dict[str, Any] = {
        "format": format_selector(platform, audio, quality, has_ffmpeg),
        "outtmpl": str(
            DOWNLOAD_DIR / "%(extractor_key)s_%(id)s_%(title).80s.%(ext)s"
        ),
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "restrictfilenames": True,
        "retries": 5,
        "fragment_retries": 5,
        "socket_timeout": 30,
        "continuedl": False,
        "concurrent_fragment_downloads": 3,
        "http_chunk_size": 4 * 1024 * 1024,
        # Немає сенсу качати те, що все одно не влізе в Telegram (50MB) —
        # яkщо джерело віддає розмір заздалегідь, yt-dlp відсіє формат
        # ще ДО завантаження замість того, щоб качати файл повністю
        # і відкидати його аж на етапі відправки.
        "max_filesize": MAX_UPLOAD_BYTES,
        "http_headers": {
            "User-Agent": _build_user_agent(platform),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        "progress_hooks": [hook] if hook else [],
    }

    if has_ffmpeg:
        opts["ffmpeg_location"] = FFMPEG_PATH
        if not audio:
            opts["merge_output_format"] = "mp4"
        if audio:
            opts["postprocessors"] = [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }
            ]

    # --- Cookies ---
    ck = cookies_file()
    if ck:
        opts["cookiefile"] = ck

    # --- Platform-specific extractor args ---
    if platform == "youtube":
        # КРИТИЧНИЙ ФІКС: для аудіо НЕ пропускаємо dash_manifests,
        # бо audio-only формати живуть тільки в DASH
        skip_list = ["translated_subs"]
        if not audio:
            skip_list.append("dash_manifests")
        # player_client свідомо НЕ фіксуємо списком. YouTube і yt-dlp
        # постійно міняють правила гри (po_token, дозволені клієнти,
        # SABR-стрімінг) — yt-dlp-мейнтейнери підтримують дефолт
        # (зараз це tv,ios,web, або tv,web з cookies) саме під ці зміни
        # і оновлюють його з кожним релізом. Захардкоджений список тут
        # гарантовано застаріє за кілька місяців і почне мовчки різати
        # доступні формати. Раз yt-dlp тепер апдейтиться при кожному
        # старті бота (див. _ensure_deps) — має сенс довіряти його дефолту.
        opts["extractor_args"] = {
            "youtube": {
                "skip": skip_list,
            }
        }

    elif platform == "tiktok":
        opts["extractor_args"] = {
            "tiktok": {
                "app_version": "26.2.0",
                "manifest_app_version": "26.2.0",
            }
        }

    elif platform == "instagram":
        # КРИТИЧНИЙ ФІКС: НЕ встановлюємо api_version!
        # yt-dlp сам обирає правильний API (GraphQL).
        # api_version: "v1" примусово вмикає мертвий REST API.
        pass

    if extra:
        opts.update(extra)
    return opts


# ─────────────────────────── Stream download ───────────────────

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
        with requests.get(
            url,
            stream=True,
            timeout=REQUEST_TIMEOUT,
            headers=headers,
        ) as response:
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
                        progress_cb(
                            progress_text(
                                "⏳ Завантажую файл", done, total, start
                            )
                        )
        return str(filepath), safe_text(title, 180)
    except DownloadCancelled as e:
        remove_file(filepath)
        return None, str(e)
    except Exception as e:
        remove_file(filepath)
        return None, f"Помилка прямого завантаження: {e}"


# ─────────────────────────── TikTok fallbacks ──────────────────

def tiktok_fallback_tikwm(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://tikwm.com/",
    }
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
            return None, data.get("msg") or "tikwm.com помилка."
        item = data.get("data") or {}
        video_url = item.get("hdplay") or item.get("play") or item.get("wmplay")
        if not video_url:
            return None, "tikwm.com не повернув відео."
        video_url = urljoin("https://tikwm.com", video_url)
        return stream_download(
            video_url,
            safe_filename("tiktok", url),
            safe_text(item.get("title") or "TikTok video", 180),
            progress_cb,
            cancel_event,
            headers,
        )
    except Exception as e:
        return None, f"tikwm.com: {e}"


def tiktok_fallback_snaptik(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        response = requests.post(
            "https://snaptik.app/abc2.php",
            data={"url": url},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        match = re.search(r'href="(https://[^"]+\.mp4[^"]*)"', response.text)
        if not match:
            return None, "SnapTik не знайшов відео."
        return stream_download(
            match.group(1),
            safe_filename("tiktok_snap", url),
            "TikTok video",
            progress_cb,
            cancel_event,
        )
    except Exception as e:
        return None, f"SnapTik: {e}"


# ─────────────────────────── Cobalt.tools fallback ─────────────

def download_via_cobalt(
    url: str,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    """
    Універсальний фолбек через cobalt API (актуальна схема, docs/api.md
    у imputnet/cobalt). Стара схема (isAudioOnly, статус "stream") більше
    не підтримується — cobalt зробив breaking change: тепер це
    downloadMode ("auto"/"audio"/"mute"), а валідні статуси відповіді —
    tunnel / redirect / picker / local-processing / error.

    ВАЖЛИВО: публічний api.cobalt.tools захищений bot-protection
    (Cloudflare Turnstile) і офіційно "не призначений для використання в
    сторонніх проєктах без явного дозволу власника інстансу". Без свого
    інстансу чи API-ключа (COBALT_API_KEY) цей фолбек, найімовірніше,
    впаде з помилкою авторизації — це очікувана поведінка публічного
    сервісу, а не баг бота.
    """
    quality_map = {"best": "1080", "fast": "720", "mobile": "360"}
    try:
        body: dict[str, Any] = {
            "url": url,
            "videoQuality": quality_map.get(quality, "720"),
            "audioFormat": "mp3",
            "downloadMode": "audio" if audio else "auto",
            "disableMetadata": True,
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "VideoDownloaderBot/1.0",
        }
        if COBALT_API_KEY:
            headers["Authorization"] = f"Api-Key {COBALT_API_KEY}"

        if progress_cb:
            progress_cb("🔄 Пробую альтернативний сервіс...")

        resp = requests.post(COBALT_API_URL, json=body, headers=headers, timeout=45)

        try:
            data = resp.json()
        except ValueError:
            data = {}

        if not resp.ok or data.get("status") == "error":
            err = data.get("error") or {}
            code = str(err.get("code") or f"http_{resp.status_code}")
            if "auth" in code:
                return None, (
                    "Cobalt: публічний інстанс вимагає авторизації (bot-protection). "
                    "Без власного інстансу/ключа (COBALT_API_KEY) це очікувано."
                )
            if resp.status_code == 429 or "rate" in code:
                return None, "Cobalt: rate limit. Спробуй пізніше."
            return None, f"Cobalt: {code}"

        status = data.get("status")
        download_url = None
        if status in ("tunnel", "redirect"):
            download_url = data.get("url")
        elif status == "picker":
            items = data.get("picker") or []
            if items:
                download_url = items[0].get("url")

        if not download_url:
            return None, f"Cobalt: непідтримувана відповідь ('{status}')"

        ext = "mp3" if audio else "mp4"
        label = "Audio" if audio else "Video"
        return stream_download(
            download_url,
            safe_filename("cobalt", url, ext),
            label,
            progress_cb,
            cancel_event,
        )
    except Exception as e:
        return None, f"Cobalt: {e}"


# ─────────────────────────── Direct download ───────────────────

def download_direct(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    ext = url.split("?")[0].split(".")[-1].lower()
    if ext not in {"mp4", "mov", "webm", "m4v", "mkv"}:
        ext = "mp4"
    return stream_download(
        url,
        safe_filename("direct", url, ext),
        "Пряме відео",
        progress_cb,
        cancel_event,
    )


# ─────────────────────────── yt-dlp process worker ─────────────

def _ytdlp_worker(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    extra_opts: dict[str, Any] | None,
    progress_queue: "mp.Queue",
    result_queue: "mp.Queue",
    cancel_event: "mp.Event",
) -> None:
    """Запускається в окремому процесі для справжнього скасування."""
    start = time.monotonic()

    def hook(data: dict[str, Any]) -> None:
        if cancel_event.is_set():
            raise DownloadCancelled("Завантаження скасовано.")
        if not progress_queue:
            return
        status = data.get("status")
        if status == "downloading":
            total = (
                data.get("total_bytes")
                or data.get("total_bytes_estimate")
                or 0
            )
            done = data.get("downloaded_bytes") or 0
            try:
                progress_queue.put_nowait(
                    progress_text(
                        "⏳ Завантажую", int(done or 0), int(total or 0), start
                    )
                )
            except Exception:
                pass
        elif status == "finished":
            try:
                progress_queue.put_nowait("🔧 Обробляю файл...")
            except Exception:
                pass
        elif status == "processing":
            try:
                progress_queue.put_nowait("🔧 Обробляю файл...")
            except Exception:
                pass

    try:
        opts = ytdlp_opts(platform, audio, quality, hook, extra_opts)
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = first_entry(ydl.extract_info(url, download=True))
            path = find_file(info, ydl)

            # Для аудіо з ffmpeg — шукаємо .mp3 файл
            if audio and path and FFMPEG_PATH:
                mp3_path = str(Path(path).with_suffix(".mp3"))
                if Path(mp3_path).exists():
                    path = mp3_path
                else:
                    # ffmpeg міг створити файл з іншою назвою
                    mp3_candidates = list(DOWNLOAD_DIR.glob(f"*{info.get('id', '')}*.mp3"))
                    if mp3_candidates:
                        mp3_candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                        path = str(mp3_candidates[0])

            if not path or not Path(path).exists():
                result_queue.put(
                    ("error", None, "Файл після завантаження не знайдено.")
                )
                return

            # Перевірка розміру
            size = Path(path).stat().st_size
            if size == 0:
                remove_file(path)
                result_queue.put(
                    ("error", None, "Завантажений файл порожній (0 байт).")
                )
                return

            result_queue.put(("ok", path, safe_text(info.get("title"), 180)))
    except DownloadCancelled as e:
        result_queue.put(("cancelled", None, str(e)))
    except Exception as e:
        result_queue.put(("error", None, friendly_error(platform, str(e))))


def download_via_ytdlp(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
    extra_opts: dict[str, Any] | None = None,
) -> tuple[str | None, str]:
    """yt-dlp в окремому процесі — /cancel вбиває процес."""
    ctx = mp.get_context("spawn")
    progress_queue: "mp.Queue" = ctx.Queue()
    result_queue: "mp.Queue" = ctx.Queue()
    mp_cancel_event: "mp.Event" = ctx.Event()

    p = ctx.Process(
        target=_ytdlp_worker,
        args=(
            url,
            platform,
            audio,
            quality,
            extra_opts,
            progress_queue,
            result_queue,
            mp_cancel_event,
        ),
    )
    p.start()
    deadline = time.monotonic() + DOWNLOAD_TIMEOUT

    def _drain_progress() -> None:
        while not progress_queue.empty():
            try:
                msg = progress_queue.get_nowait()
                if progress_cb:
                    progress_cb(msg)
            except Exception:
                break

    try:
        while p.is_alive():
            if cancel_event and cancel_event.is_set():
                mp_cancel_event.set()
                p.terminate()
                p.join(timeout=5)
                if p.is_alive():
                    p.kill()
                    p.join(timeout=5)
                return None, "Завантаження скасовано."

            if time.monotonic() > deadline:
                # DOWNLOAD_TIMEOUT раніше читався з env і логувався, але
                # ніде не перевірявся - завислий процес міг тримати слот
                # PARALLEL_DOWNLOADS вічно. Тепер реально обмежуємо.
                mp_cancel_event.set()
                p.terminate()
                p.join(timeout=5)
                if p.is_alive():
                    p.kill()
                    p.join(timeout=5)
                return None, (
                    f"⏰ Перевищено ліміт часу завантаження ({DOWNLOAD_TIMEOUT}с). "
                    f"Спробуй /quality mobile."
                )

            _drain_progress()
            time.sleep(0.2)

        # Таймаут на випадок якщо процес завис
        p.join(timeout=10)
        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():
                p.kill()
                p.join(timeout=5)
            return None, "⏰ Завантаження занадто довге (таймаут). Спробуй /quality mobile."

        _drain_progress()

        try:
            result = result_queue.get(timeout=5)
        except Exception:
            return None, "Помилка отримання результату від завантажувача."

        status, path, message = result
        if status == "ok":
            return path, message
        if status == "cancelled":
            return None, message
        return None, message
    except Exception as e:
        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():
                p.kill()
                p.join(timeout=5)
        return None, f"Помилка завантажувача: {e}"


# ─────────────────────────── Main download orchestrator ────────

def download_media(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    """
    Головний оркестратор завантажень.
    Реалізує стратегію фолбеків для кожної платформи.
    """
    url = normalize_url(url)

    # 1. Прямі посилання на відео
    if DIRECT_VIDEO_RE.search(url) and not audio:
        return download_direct(url, progress_cb, cancel_event)

    # 2. ─── INSTAGRAM: спеціальна стратегія ───
    if platform == "instagram":
        has_cookies = bool(cookies_file())

        # 2a. Без cookies — спочатку пробуємо ddinstagram проксі
        if not has_cookies:
            if progress_cb:
                progress_cb("🔄 Instagram через проксі (ddinstagram)...")
            ddurl = to_ddinstagram(url)
            path, result = download_via_ytdlp(
                ddurl, "instagram", audio, quality, progress_cb, cancel_event
            )
            if path:
                return path, result
            if cancel_event and cancel_event.is_set():
                return None, "Завантаження скасовано."

        # 2b. Пробуємо прямий yt-dlp (з cookies або без)
        if progress_cb:
            progress_cb("🔄 Instagram через yt-dlp...")
        path, result = download_via_ytdlp(
            url, "instagram", audio, quality, progress_cb, cancel_event
        )
        if path:
            return path, result
        if cancel_event and cancel_event.is_set():
            return None, "Завантаження скасовано."

        # 2c. Фолбек на cobalt.tools
        if progress_cb:
            progress_cb("🔄 Instagram через cobalt...")
        path, result = download_via_cobalt(
            url, audio, quality, progress_cb, cancel_event
        )
        if path:
            return path, result

        # 2d. Усі спроби провалились
        return None, result or "Instagram: не вдалося завантажити."

    # 3. ─── TIKTOK: tikwm → yt-dlp ───
    if platform == "tiktok" and not audio:
        if progress_cb:
            progress_cb("🔄 TikTok no-watermark (tikwm)...")
        path, result = tiktok_fallback_tikwm(url, progress_cb, cancel_event)
        if path:
            return path, result
        if cancel_event and cancel_event.is_set():
            return None, "Завантаження скасовано."

        if progress_cb:
            progress_cb("🔄 TikTok yt-dlp...")
        path, result = download_via_ytdlp(
            url, "tiktok", audio, quality, progress_cb, cancel_event
        )
        if path:
            return path, result
        return None, result or "TikTok: не вдалося завантажити."

    # 4. ─── TIKTOK AUDIO: yt-dlp → cobalt ───
    if platform == "tiktok" and audio:
        path, result = download_via_ytdlp(
            url, "tiktok", audio, quality, progress_cb, cancel_event
        )
        if path:
            return path, result
        if cancel_event and cancel_event.is_set():
            return None, "Завантаження скасовано."
        path, result = download_via_cobalt(
            url, audio, quality, progress_cb, cancel_event
        )
        if path:
            return path, result
        return None, result or "TikTok audio: не вдалося."

    # 5. ─── YOUTUBE: yt-dlp з ретраями → cobalt ───
    if platform == "youtube":
        last_error = ""
        for attempt in range(1, MAX_RETRIES + 1):
            if cancel_event and cancel_event.is_set():
                return None, "Завантаження скасовано."
            if attempt > 1:
                wait = min(2 ** (attempt - 1), 10)
                if progress_cb:
                    progress_cb(f"🔁 Спроба {attempt}/{MAX_RETRIES} (за {wait}с)...")
                time.sleep(wait)

            path, result = download_via_ytdlp(
                url, "youtube", audio, quality, progress_cb, cancel_event
            )
            if path:
                return path, result
            last_error = result
            if not is_transient_error(result):
                break

        # Фолбек на cobalt
        if progress_cb:
            progress_cb("🔄 YouTube через cobalt...")
        path, result = download_via_cobalt(
            url, audio, quality, progress_cb, cancel_event
        )
        if path:
            return path, result

        return None, last_error or "YouTube: не вдалося завантажити."

    # 6. ─── ІНШІ ПЛАТФОРМИ: yt-dlp з ретраями → cobalt ───
    last_error = ""
    for attempt in range(1, MAX_RETRIES + 1):
        if cancel_event and cancel_event.is_set():
            return None, "Завантаження скасовано."
        if attempt > 1:
            wait = min(2 ** (attempt - 1), 10)
            if progress_cb:
                progress_cb(f"🔁 Спроба {attempt}/{MAX_RETRIES} (за {wait}с)...")
            time.sleep(wait)

        path, result = download_via_ytdlp(
            url, platform, audio, quality, progress_cb, cancel_event
        )
        if path:
            return path, result
        last_error = result
        if not is_transient_error(result):
            break

    # Фолбек на cobalt для підтримуваних платформ
    if platform in COBALT_SUPPORTED:
        if progress_cb:
            progress_cb(f"🔄 {platform} через cobalt...")
        path, result = download_via_cobalt(
            url, audio, quality, progress_cb, cancel_event
        )
        if path:
            return path, result

    return None, last_error or "Не вдалося завантажити."


# ─────────────────────────── Telegram helpers ──────────────────

async def safe_edit(message, text: str) -> None:
    text = str(text or "")[:3900]
    try:
        await message.edit_text(text, parse_mode="Markdown")
        return
    except RetryAfter as e:
        await asyncio.sleep(float(e.retry_after) + 0.2)
        try:
            await message.edit_text(text, parse_mode="Markdown")
            return
        except Exception:
            pass
    except BadRequest as e:
        err = str(e).lower()
        if "message is not modified" in err:
            return
        if "can't parse" in err or "can't find end" in err:
            try:
                await message.edit_text(text)
                return
            except Exception:
                return
        log.debug("edit error: %s", e)
    except TelegramError as e:
        log.debug("telegram error: %s", e)


async def safe_delete(message) -> None:
    try:
        await message.delete()
    except Exception:
        pass


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
                "❌ Файл більший за ліміт Telegram Bot API (50MB).\n"
                "Постав /quality mobile і спробуй ще раз."
            )
            return 0, ""
        if progress_cb:
            progress_cb("📤 Надсилаю у Telegram...")
        title_clean = safe_text(title, 180)
        with open(path, "rb") as file:
            if is_audio:
                ext = path.suffix.lower().lstrip(".")
                # Telegram підтримує: mp3, m4a, ogg, wav, flac, aac
                audio_exts = {"mp3", "m4a", "ogg", "wav", "flac", "aac", "opus", "webm"}
                if ext in audio_exts:
                    sent = await msg.reply_audio(
                        audio=file,
                        title=title_clean[:64],
                        caption=f"🎵 {title_clean}",
                        read_timeout=180,
                        write_timeout=180,
                        connect_timeout=60,
                        pool_timeout=60,
                    )
                    return (
                        size,
                        str(sent.audio.file_id if sent.audio else ""),
                    )
                else:
                    # Невідомий формат — як документ
                    sent = await msg.reply_document(
                        document=file,
                        caption=(
                            f"🎵 {title_clean}\n"
                            f"Формат .{ext} — Telegram може не відтворити.\n"
                            f"Для MP3 конвертації потрібен ffmpeg на сервері."
                        ),
                        read_timeout=180,
                        write_timeout=180,
                        connect_timeout=60,
                        pool_timeout=60,
                    )
                    return (
                        size,
                        str(sent.document.file_id if sent.document else ""),
                    )

            sent = await msg.reply_video(
                video=file,
                caption=f"✅ {title_clean}",
                supports_streaming=True,
                read_timeout=180,
                write_timeout=180,
                connect_timeout=60,
                pool_timeout=60,
            )
            return size, str(sent.video.file_id if sent.video else "")
    except Exception:
        log.exception("send error")
        await msg.reply_text("❌ Не вдалося надіслати файл у Telegram.")
        return 0, ""
    finally:
        remove_file(filepath)


async def send_cached(
    update: Update, file_id: str, title: str, is_audio: bool
) -> None:
    msg = update.effective_message
    if not msg:
        return
    title_clean = safe_text(title, 180)
    try:
        if is_audio:
            await msg.reply_audio(
                audio=file_id,
                title=title_clean[:64],
                caption=f"🎵 {title_clean} (з кешу)",
            )
        else:
            await msg.reply_video(
                video=file_id,
                caption=f"✅ {title_clean} (з кешу)",
            )
    except Exception:
        raise  # Нехай caller обробляє stale cache


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
    # Нормалізуємо ОДИН раз і використовуємо цей самий варіант далі
    # всюди: у кеші, історії та самому завантаженні. Раніше кеш/історія
    # писались по "сирому" лінку, а download_media нормалізував його
    # вже всередині - той самий контент з різним utm/трекінгом чи
    # m.-піддоменом не потрапляв у кеш, хоча міг би.
    url = normalize_url(url)
    cid = chat_id(update)
    uid = user_id(update)
    quality = quality_for(cid)

    if rate_limited(uid or cid):
        wait = retry_after_seconds(uid or cid)
        await msg.reply_text(
            f"⏳ Забагато запитів. Спробуй ще раз через {wait}с."
        )
        return

    # Кеш
    cached = cache_get(url, audio)
    if cached:
        file_id, title = cached
        status = await msg.reply_text("⚡ Знайдено в кеші, надсилаю...")
        try:
            await send_cached(update, file_id, title, audio)
            await safe_delete(status)
            return
        except Exception:
            await safe_edit(status, "🔄 Кеш застарів. Завантажую заново...")
            URL_CACHE.pop(cache_key(url, audio), None)

    task_id = next(_TASK_ID_COUNTER)
    cancel_event = Event()
    CANCEL_EVENTS[task_id] = cancel_event
    ACTIVE_TASKS[task_id] = {
        "url": url,
        "platform": platform,
        "audio": audio,
        "quality": quality,
        "started_at": time.time(),
        "user_id": uid,
        "chat_id": cid,
    }

    # Миттєве підтвердження ДО входу в семафор PARALLEL_DOWNLOADS: якщо
    # всі слоти зайняті, юзер раніше не бачив узагалі нічого, поки
    # чекав на слот - виглядало так, ніби бот завис/не відповідає.
    status = await msg.reply_text("🔗 Прийняв, стаю в чергу...")
    try:
        async with PARALLEL_LIMIT:
            await safe_edit(
                status,
                "🎵 Готую аудіо..." if audio else "⏳ Починаю завантаження...",
            )
            loop = asyncio.get_running_loop()
            last_time = [0.0]
            last_text = [""]

            def progress_cb(text: str) -> None:
                now = time.monotonic()
                important = text.startswith(
                    ("🔧", "📤", "✅", "🔁", "❌", "⚡", "🎵", "🔄", "⏰")
                )
                if text == last_text[0]:
                    return
                if now - last_time[0] < PROGRESS_THROTTLE and not important:
                    return
                last_time[0] = now
                last_text[0] = text
                asyncio.run_coroutine_threadsafe(safe_edit(status, text), loop)

            job = partial(
                download_media,
                url,
                platform,
                audio,
                quality,
                progress_cb,
                cancel_event,
            )
            path, title = await loop.run_in_executor(None, job)
            if not path:
                await safe_edit(status, f"❌ {title}")
                return
            await safe_edit(status, "✅ Завантажено. Надсилаю...")
            size, file_id = await send_media(
                update, path, title, audio, progress_cb
            )
            if size:
                record_history(uid, url, title, platform)
                if file_id:
                    cache_set(url, audio, file_id, title)
            await safe_delete(status)
    finally:
        CANCEL_EVENTS.pop(task_id, None)
        ACTIVE_TASKS.pop(task_id, None)
        clean_old_files(False)
        cache_cleanup()


# ─────────────────────────── Handlers ──────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or not msg.text:
        return
    urls = extract_urls(msg.text)
    if not urls:
        return
    if len(urls) > 1:
        await msg.reply_text(f"🔗 Знайдено {len(urls)} посилання. Оброблю по черзі.")
    for url in urls:
        platform = platform_for_url(url)
        if not platform:
            await msg.reply_text(f"❌ Платформа не підтримується:\n{url[:100]}")
            continue
        await download_and_send(update, url, platform, False)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(
            USER_HELP_TEXT, parse_mode="Markdown"
        )


async def dl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await video_command(update, context)


async def video_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    url = get_url_from_command(update, context)
    if not url:
        await msg.reply_text("❌ Використання: /video <посилання>")
        return
    platform = platform_for_url(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return
    await download_and_send(update, url, platform, False)


async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    url = get_url_from_command(update, context)
    if not url:
        await msg.reply_text("❌ Використання: /audio <посилання>")
        return
    platform = detect_platform(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується для аудіо.")
        return
    await download_and_send(update, url, platform, True)


async def quality_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
    msg = update.effective_message
    if not msg:
        return
    cid = chat_id(update)
    q = quality_for(cid)
    has_ff = "✅" if FFMPEG_PATH else "❌"
    has_ck = "✅" if cookies_file() else "❌"
    has_js = f"✅ ({Path(JS_RUNTIME).name})" if JS_RUNTIME else "❌ (YouTube буде гірше качати)"
    await msg.reply_text(
        f"⚙️ Налаштування\n\n"
        f"Якість: {q}\n"
        f"ffmpeg: {has_ff}\n"
        f"cookies.txt: {has_ck}\n"
        f"JS-рантайм (Deno/Node) для YouTube: {has_js}\n\n"
        f"Щоб змінити якість — /quality"
    )


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    uid = user_id(update)
    hist = HISTORY.get(str(uid), [])
    if not hist:
        await msg.reply_text("📋 Немає завантажень.")
        return
    lines = [f"📋 Останні {len(hist)} завантажень:", ""]
    for index, item in enumerate(hist, 1):
        ts = str(item.get("ts", ""))[:10]
        platform = safe_text(item.get("platform", "unknown"), 30)
        title = safe_text(item.get("title", "video"), 50)
        lines.append(f"{index}. [{platform}] {title}\n   {ts}")
    await msg.reply_text("\n".join(lines))


async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return
    start = time.monotonic()
    message = await update.effective_message.reply_text("🏓 Pong!")
    ms = int((time.monotonic() - start) * 1000)
    await message.edit_text(f"🏓 Pong! {ms}ms")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    cid = chat_id(update)
    uid = user_id(update)
    # Скасовуємо тільки завдання САМЕ ЦЬОГО юзера в ЦЬОМУ чаті. Раніше
    # /cancel шукав "хоч якесь" активне завдання в чаті по chat_id - у
    # груповому чаті будь-хто міг випадково (чи навмисно) скасувати
    # завантаження іншої людини.
    matches = [
        (tid, task)
        for tid, task in ACTIVE_TASKS.items()
        if task.get("chat_id") == cid and task.get("user_id") == uid
    ]
    if not matches:
        await msg.reply_text("Немає активного завантаження, яке я можу тут для тебе скасувати.")
        return
    for tid, _ in matches:
        event = CANCEL_EVENTS.get(tid)
        if event:
            event.set()
    word = "завантаження" if len(matches) == 1 else f"{len(matches)} завантаження"
    await msg.reply_text(f"🛑 Скасовую {word}...")


async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    uid = user_id(update)
    tasks = [task for task in ACTIVE_TASKS.values() if task.get("user_id") == uid]
    if not tasks:
        await msg.reply_text("У тебе немає активних завантажень.")
        return
    lines = [f"📋 Твоїх активних: {len(tasks)}", ""]
    for task in tasks:
        elapsed = seconds_text(time.time() - float(task["started_at"]))
        lines.append(
            f"• {task['platform']} | "
            f"{'🎵' if task['audio'] else '🎬'} | "
            f"{task['quality']} | ⏱ {elapsed}"
        )
    await msg.reply_text("\n".join(lines))


async def platforms_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return
    text = "📡 Підтримувані платформи:\n" + "\n".join(
        f"• {p}" for p in URL_PATTERNS
    )
    await update.effective_message.reply_text(text)


# ─────────────────────────── Callbacks ─────────────────────────

async def quality_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    value = (
        query.data.split(":")[1] if query.data and ":" in query.data else ""
    )
    if value not in {"best", "fast", "mobile"}:
        return
    message = query.message
    if not message:
        return
    cid = int(message.chat.id)
    SETTINGS.setdefault("quality", {})[str(cid)] = value
    save_settings()
    await query.edit_message_text(f"✅ Якість: {value}")


# ─────────────────────────── Error handler ─────────────────────

async def error_handler(
    update: object, context: ContextTypes.DEFAULT_TYPE
) -> None:
    log.error("Unhandled error:", exc_info=context.error)
    # Раніше юзер при непередбаченій помилці не бачив НІЧОГО - виглядало,
    # ніби бот просто завис/помер. Тепер хоч коротке повідомлення є.
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ Сталася неочікувана помилка. Спробуй ще раз трохи пізніше."
            )
        except Exception:
            pass


# ─────────────────────────── Scheduled tasks ───────────────────

async def scheduled_cleanup(context: ContextTypes.DEFAULT_TYPE) -> None:
    removed = clean_old_files(False)
    cache_cleanup()
    if removed:
        log.info("Scheduled cleanup: removed %d files", removed)


# ─────────────────────────── Bot menu setup ────────────────────

async def setup_bot_commands(app: Application) -> None:
    try:
        await app.bot.set_my_commands(
            USER_BOT_COMMANDS,
            scope=BotCommandScopeDefault(),
        )
        log.info("Default user command menu set")
    except Exception:
        log.exception("Failed to set user commands")


# ─────────────────────────── Main ──────────────────────────────

def build_application() -> Application:
    app = (
        Application.builder()
        .token(TOKEN)
        .post_init(setup_bot_commands)
        .build()
    )

    if app.job_queue:
        app.job_queue.run_repeating(scheduled_cleanup, interval=7200, first=60)
    else:
        log.warning("JobQueue недоступний.")

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", start_command))
    app.add_handler(CommandHandler("dl", dl_command))
    app.add_handler(CommandHandler("video", video_command))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("platforms", platforms_command))
    app.add_handler(CommandHandler("quality", quality_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("queue", queue_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(
        CallbackQueryHandler(quality_callback, pattern=r"^quality:")
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)
    return app


def main() -> None:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    clean_old_files(False)

    log.info("ffmpeg=%s", FFMPEG_PATH or "не знайдено")
    log.info("cookies=%s", cookies_file() or "не знайдено")
    log.info("js_runtime=%s", JS_RUNTIME or "не знайдено (YouTube без нього деградує - див. /settings)")
    log.info("max_upload=%s", human_bytes(MAX_UPLOAD_BYTES))
    log.info("download_timeout=%ds", DOWNLOAD_TIMEOUT)
    log.info("parallel_downloads=%d", PARALLEL_DOWNLOADS)

    app = build_application()

    if WEBHOOK_URL:
        log.info("Starting with webhook: %s", WEBHOOK_URL)
        app.run_webhook(
            listen="0.0.0.0",
            port=int(os.environ.get("PORT", "8080")),
            url_path=TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{TOKEN}",
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        log.info("Starting with polling")
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
        )


if __name__ == "__main__":
    main()
