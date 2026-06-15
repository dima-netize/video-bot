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
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--user",
            "--no-cache-dir",
            "-U",
            package,
        ],
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
        ("telegram.ext", "python-telegram-bot[webhooks,job-queue]==20.7"),
    ]
    for module_name, package_name in deps:
        if not _module_ok(module_name):
            _pip_install(package_name)
    if not _module_ok("tornado") or not _module_ok("apscheduler"):
        _pip_install("python-telegram-bot[webhooks,job-queue]==20.7")
    bad = [module_name for module_name, _ in deps if not _module_ok(module_name)]
    if bad:
        raise RuntimeError("Не вдалося встановити залежності: " + ", ".join(bad))


_ensure_deps()

# ─────────────────────────── Imports ───────────────────────────

import asyncio
import glob
import json
import logging
import multiprocessing as mp
import re
import shutil
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from threading import Event
from typing import Any
from urllib.parse import urljoin

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
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

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
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
URL_CACHE_TTL = int(os.environ.get("URL_CACHE_TTL", "3600"))
MAX_HISTORY_PER_USER = int(os.environ.get("MAX_HISTORY_PER_USER", "10"))
WHISPER_MAX_BYTES = 25 * 1024 * 1024

PARALLEL_LIMIT = asyncio.Semaphore(PARALLEL_DOWNLOADS)

logging.basicConfig(
    level=logging.INFO,
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
    "youtube": re.compile(
        r"(?:youtube\.com/(?:watch\?v=|shorts/|live/)|youtu\.be/|m\.youtube\.com/watch\?v=)",
        re.I,
    ),
    "tiktok": re.compile(
        r"(?:tiktok\.com/@[\w.-]+/video/\d+|tiktok\.com/t/|vt\.tiktok\.com/|vm\.tiktok\.com/|www\.tiktok\.com/)",
        re.I,
    ),
    "instagram": re.compile(
        r"instagram\.com/(?:reel|reels|p|tv|stories)/",
        re.I,
    ),
    "twitter": re.compile(
        r"(?:twitter\.com|x\.com)/\w+/status/\d+",
        re.I,
    ),
    "vimeo": re.compile(
        r"vimeo\.com/(?:\d+|channels/[^/]+/\d+)",
        re.I,
    ),
    "reddit": re.compile(
        r"reddit\.com/r/\w+/comments/",
        re.I,
    ),
    "facebook": re.compile(
        r"facebook\.com/(?:watch/\?v=|watch\?v=|reel/|share/r/|[\w.]+/videos/)",
        re.I,
    ),
    "likee": re.compile(r"likee\.video/|likee\.com/", re.I),
    "snapchat": re.compile(r"snapchat\.com/(?:spotlight|add)/", re.I),
    "pinterest": re.compile(r"pinterest\.[a-z.]+/pin/\d+", re.I),
    "twitch": re.compile(r"twitch\.tv/(?:videos/\d+|clips/)", re.I),
    "dailymotion": re.compile(r"dailymotion\.com/video/", re.I),
    "rumble": re.compile(r"rumble\.com/v", re.I),
    "odysee": re.compile(r"odysee\.com/@", re.I),
    "bilibili": re.compile(r"bilibili\.com/video/", re.I),
    "coub": re.compile(r"coub\.com/view/", re.I),
    "streamable": re.compile(r"streamable\.com/", re.I),
    "medal": re.compile(r"medal\.tv/", re.I),
    "youtube_music": re.compile(r"music\.youtube\.com/watch", re.I),
}

USER_HELP_TEXT = """🎥 *Fast Video Downloader Bot*

Просто кинь посилання — бот завантажить відео.

*📥 Завантаження:*

/dl `<url>` — завантажити відео
/video `<url>` — завантажити відео
/audio `<url>` — завантажити аудіо MP3
/thumb `<url>` — обкладинка відео
/sub `<url>` — субтитри
/clip `<url> <старт> <кінець>` — вирізати кліп

*ℹ️ Інформація:*

/info `<url>` — деталі про відео
/formats `<url>` — список форматів
/platforms — підтримувані платформи

*⚙️ Налаштування:*

/settings — налаштування
/quality — якість: best / fast / mobile

*📋 Керування:*

/cancel — скасувати завантаження
/queue — твої активні завантаження
/history — останні 10 завантажень
/ping — перевірка відповіді

*🎤 Транскрипція:*

/transcribe `<url>` — розпізнати мову у відео

*💡 Якщо YouTube не качає, серверу можуть бути потрібні cookies.*
"""

USER_BOT_COMMANDS = [
    BotCommand("start", "Запустити бота"),
    BotCommand("help", "Допомога"),
    BotCommand("video", "Завантажити відео"),
    BotCommand("audio", "Завантажити аудіо"),
    BotCommand("thumb", "Обкладинка відео"),
    BotCommand("sub", "Субтитри"),
    BotCommand("clip", "Вирізати кліп"),
    BotCommand("info", "Інформація про відео"),
    BotCommand("formats", "Формати відео"),
    BotCommand("transcribe", "Транскрипція з відео"),
    BotCommand("quality", "Якість завантаження"),
    BotCommand("settings", "Налаштування"),
    BotCommand("history", "Історія"),
    BotCommand("cancel", "Скасувати"),
    BotCommand("queue", "Черга"),
    BotCommand("platforms", "Платформи"),
    BotCommand("ping", "Перевірка бота"),
]

# ─────────────────────────── State ─────────────────────────────

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
        clean = url.strip().strip(".,;)\n\r\t ")
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


def settings_keyboard(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"Якість: {quality_for(cid)} ↕️",
                    callback_data="settings:quality",
                )
            ]
        ]
    )


def parse_time_to_seconds(value: str) -> int | None:
    parts = value.strip().split(":")
    if len(parts) not in {2, 3}:
        return None
    try:
        nums = [int(x) for x in parts]
    except ValueError:
        return None
    if any(x < 0 for x in nums):
        return None
    if len(nums) == 2:
        minutes, seconds = nums
        if seconds >= 60:
            return None
        return minutes * 60 + seconds
    hours, minutes, seconds = nums
    if minutes >= 60 or seconds >= 60:
        return None
    return hours * 3600 + minutes * 60 + seconds


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
    if platform == "youtube" and any(
        x in low
        for x in [
            "sign in to confirm",
            "not a bot",
            "use --cookies",
            "cookies",
        ]
    ):
        return (
            "🍪 YouTube просить cookies.txt.\n\n"
            "Поклади правильний cookies.txt поруч із bot.py.\n"
            "Перший рядок має бути:\n"
            "# Netscape HTTP Cookie File\n\n"
            "Без cookies YouTube часто блокує free-сервери."
        )
    if "requested format is not available" in low:
        return "⚠️ Ця якість недоступна. Спробуй /quality fast або /quality mobile."
    if "ffmpeg" in low and not FFMPEG_PATH:
        return "⚠️ Немає ffmpeg. Постав /quality fast або /quality mobile."
    if "unsupported url" in low:
        return "❌ Посилання не підтримується або платформа змінила захист."
    if "private" in low or "login" in low:
        return "🔒 Відео приватне або потрібен вхід. Потрібен cookies.txt."
    if "network" in low or "connection" in low or "timeout" in low:
        return "🌐 Помилка мережі. Спробуй ще раз через хвилину."
    if "429" in low or "too many" in low:
        return "⏳ Платформа тимчасово блокує завантаження. Зачекай 5-10 хвилин."
    if "geo" in low or "not available in your country" in low:
        return "🌍 Відео недоступне в регіоні сервера."
    return safe_text(err, 900)


def is_transient_error(error: str) -> bool:
    low = str(error or "").lower()
    return any(
        x in low
        for x in [
            "network",
            "connection",
            "timeout",
            "reset by peer",
            "read error",
            "http error 5",
            "503",
            "502",
            "429",
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
            for key in ["filepath", "_filename", "filename"]:
                value = item.get(key)
                if value:
                    candidates.append(str(value))
    for key in ["filepath", "_filename", "filename"]:
        value = info.get(key)
        if value:
            candidates.append(str(value))
    try:
        candidates.append(str(ydl.prepare_filename(info)))
    except Exception:
        pass
    if info.get("id"):
        candidates.extend(glob.glob(str(DOWNLOAD_DIR / f"*{info['id']}*")))
    existing = [str(Path(path)) for path in candidates if path and Path(path).exists()]
    existing.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
    return existing[0] if existing else None


def format_selector(platform: str | None, audio: bool, quality: str) -> str:
    if audio:
        return "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best"
    if not FFMPEG_PATH:
        if quality == "mobile":
            return "best[height<=480][ext=mp4]/best[height<=480]/best"
        if quality == "best":
            return "best[ext=mp4]/best"
        return "best[height<=720][ext=mp4]/best[height<=720]/best"
    if quality == "mobile":
        return (
            "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/"
            "best[height<=480][ext=mp4]/"
            "best[height<=480]/"
            "best"
        )
    if quality == "fast":
        return (
            "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/"
            "best[height<=720][ext=mp4]/"
            "best[height<=720]/"
            "best"
        )
    if platform == "youtube":
        return "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/best[ext=mp4]/best"
    return "best[ext=mp4]/best"


def ytdlp_opts(
    platform: str | None,
    audio: bool,
    quality: str,
    hook=None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    )
    if platform == "tiktok":
        user_agent = (
            "com.zhiliaoapp.musically/2022600030 "
            "(Linux; U; Android 12; en_US; Pixel 6; Build/SP1A.210812.016)"
        )
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
    ck = cookies_file()
    if ck:
        opts["cookiefile"] = ck
    if platform == "youtube":
        opts["extractor_args"] = {
            "youtube": {
                "player_client": ["android", "web"],
            }
        }
    if platform == "tiktok":
        opts["extractor_args"] = {
            "tiktok": {
                "app_version": "26.2.0",
                "manifest_app_version": "26.2.0",
            }
        }
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
                                "⏳ Завантажую файл",
                                done,
                                total,
                                start,
                            )
                        )
        return str(filepath), safe_text(title, 180)
    except DownloadCancelled as e:
        remove_file(filepath)
        return None, str(e)
    except Exception as e:
        remove_file(filepath)
        return None, f"Помилка прямого завантаження: {e}"


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


def instagram_fallback(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
) -> tuple[str | None, str]:
    fixed = re.sub(
        r"https?://(?:www\.)?instagram\.com",
        "https://www.ddinstagram.com",
        url,
        count=1,
        flags=re.I,
    )
    try:
        response = requests.get(
            fixed,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        video_url = None
        for pattern in [
            r'<video[^>]+src="([^"]+)"',
            r'property="og:video"\s+content="([^"]+)"',
            r'property="og:video:secure_url"\s+content="([^"]+)"',
            r'"video_url":"([^"]+)"',
        ]:
            match = re.search(pattern, response.text)
            if match:
                video_url = (
                    match.group(1)
                    .replace("\\u0026", "&")
                    .replace("\\/", "/")
                    .replace("&amp;", "&")
                )
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
    except Exception as e:
        return None, f"Instagram fallback: {e}"


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
    """Запускається в окремому процесі для підтримки справжнього скасування."""
    start = time.monotonic()

    def hook(data: dict[str, Any]) -> None:
        if cancel_event.is_set():
            raise DownloadCancelled("Завантаження скасовано.")
        if not progress_queue:
            return
        status = data.get("status")
        if status == "downloading":
            total = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
            done = data.get("downloaded_bytes") or 0
            try:
                progress_queue.put_nowait(
                    progress_text(
                        "⏳ Завантажую",
                        int(done or 0),
                        int(total or 0),
                        start,
                    )
                )
            except Exception:
                pass
        elif status == "finished":
            try:
                progress_queue.put_nowait("🔧 Обробляю файл...")
            except Exception:
                pass

    try:
        with yt_dlp.YoutubeDL(
            ytdlp_opts(platform, audio, quality, hook, extra_opts)
        ) as ydl:
            info = first_entry(ydl.extract_info(url, download=True))
            path = find_file(info, ydl)
            if audio and path and FFMPEG_PATH:
                mp3_path = str(Path(path).with_suffix(".mp3"))
                if Path(mp3_path).exists():
                    path = mp3_path
            if not path or not Path(path).exists():
                result_queue.put(("error", None, "Файл після завантаження не знайдено."))
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
    """yt-dlp тепер працює в окремому процесі — /cancel вбиває процес."""
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

            _drain_progress()
            time.sleep(0.2)

        p.join()
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
            progress_cb("🔁 TikTok no-watermark...")
        path, result = tiktok_fallback_tikwm(url, progress_cb, cancel_event)
        if path:
            return path, result
        if progress_cb:
            progress_cb("🔁 TikTok fallback #2...")
        path, result = tiktok_fallback_snaptik(url, progress_cb, cancel_event)
        if path:
            return path, result
        if progress_cb:
            progress_cb("🔁 TikTok yt-dlp...")

    last_error = ""
    for attempt in range(1, MAX_RETRIES + 1):
        if cancel_event and cancel_event.is_set():
            return None, "Завантаження скасовано."
        if attempt > 1:
            if progress_cb:
                progress_cb(f"🔁 Спроба {attempt}/{MAX_RETRIES}...")
            time.sleep(2 ** (attempt - 1))

        path, result = download_via_ytdlp(
            url,
            platform,
            audio,
            quality,
            progress_cb,
            cancel_event,
        )
        if path:
            return path, result
        last_error = result
        if not is_transient_error(result):
            break

    if not audio and platform == "instagram":
        if progress_cb:
            progress_cb("🔁 Instagram fallback...")
        path, result = instagram_fallback(url, progress_cb, cancel_event)
        if path:
            return path, result
        return None, f"{last_error}\nInstagram fallback: {result}"

    return None, last_error or "Не вдалося завантажити відео."


# ─────────────────────────── Info / Formats / Sub / Thumb ──────

def extract_info_text(url: str, platform: str | None, quality: str) -> str:
    opts = ytdlp_opts(platform, False, quality)
    opts["skip_download"] = True
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=False))
    lines = [
        f"ℹ️ {safe_text(info.get('title'), 180)}",
        f"👤 Автор: {safe_text(info.get('uploader') or info.get('channel') or 'невідомо', 120)}",
        f"⏱ Тривалість: {seconds_text(info.get('duration'))}",
        f"📡 Платформа: {platform or 'unknown'}",
    ]
    if info.get("view_count") is not None:
        lines.append(f"👁 Перегляди: {info.get('view_count'):,}".replace(",", " "))
    if info.get("like_count") is not None:
        lines.append(f"👍 Лайки: {info.get('like_count'):,}".replace(",", " "))
    if info.get("upload_date"):
        date = str(info["upload_date"])
        lines.append(f"📅 Дата: {date[:4]}-{date[4:6]}-{date[6:]}")
    if info.get("description"):
        lines.extend(["", f"📝 {safe_text(info['description'], 300)}"])
    lines.extend(["", f"🔗 {info.get('webpage_url') or url}"])
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
        if fmt.get("vcodec") == "none":
            media = "audio"
        elif fmt.get("acodec") == "none":
            media = "video-only"
        else:
            media = "video"
        line = f"• {fmt.get('format_id', '?')}: {label} ({fmt.get('ext', '?')}, {media})"
        if size:
            line += f" ~{human_bytes(size)}"
        lines.append(line)
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
    video_id = info.get("id", "")
    files = glob.glob(str(DOWNLOAD_DIR / f"sub_{video_id}*"))
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
    video_id = info.get("id", "")
    files = sorted(
        glob.glob(str(DOWNLOAD_DIR / f"thumb_{video_id}*")),
        key=lambda x: Path(x).stat().st_mtime,
        reverse=True,
    )
    return (files[0] if files else None), title


def clip_video_ffmpeg(
    input_path: str,
    start: str,
    end: str,
    output_path: str,
) -> tuple[bool, str]:
    if not FFMPEG_PATH:
        return False, "ffmpeg не знайдено на сервері."
    try:
        result = subprocess.run(
            [
                FFMPEG_PATH,
                "-y",
                "-ss",
                start,
                "-to",
                end,
                "-i",
                input_path,
                "-c",
                "copy",
                output_path,
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode != 0:
            return False, f"ffmpeg error:\n{result.stderr[-700:]}"
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "ffmpeg timeout >180с."
    except Exception as e:
        return False, str(e)


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
        if (
            "can't parse entities" in err
            or "can't find end" in err
            or "can't find end of the entity" in err
        ):
            try:
                await message.edit_text(text)
                return
            except Exception:
                log.debug("edit fallback error", exc_info=True)
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
                "❌ Файл більший за ліміт Telegram Bot API 50MB.\n"
                "Постав /quality mobile і спробуй ще раз."
            )
            return 0, ""
        if progress_cb:
            progress_cb("📤 Надсилаю у Telegram...")
        title_clean = safe_text(title, 180)
        with open(path, "rb") as file:
            if is_audio and path.suffix.lower() == ".mp3":
                sent = await msg.reply_audio(
                    audio=file,
                    title=title_clean[:64],
                    caption=f"🎵 {title_clean}",
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                    pool_timeout=60,
                )
                return size, str(sent.audio.file_id if sent.audio else "")
            if is_audio:
                sent = await msg.reply_document(
                    document=file,
                    caption=f"🎵 {title_clean}\nФайл не MP3, бо ffmpeg недоступний.",
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                    pool_timeout=60,
                )
                return size, str(sent.document.file_id if sent.document else "")
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


async def send_cached(update: Update, file_id: str, title: str, is_audio: bool) -> None:
    msg = update.effective_message
    if not msg:
        return
    title_clean = safe_text(title, 180)
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

    if rate_limited(uid or cid):
        wait = retry_after_seconds(uid or cid)
        await msg.reply_text(f"⏳ Забагато запитів. Спробуй ще раз через {wait}с.")
        return

    cached = cache_get(url, audio)
    if cached:
        file_id, title = cached
        status = await msg.reply_text("⚡ Знайдено в кеші, надсилаю...")
        try:
            await send_cached(update, file_id, title, audio)
            await safe_delete(status)
            return
        except Exception:
            await safe_edit(status, "❌ Кеш застарів. Завантажую заново...")
            URL_CACHE.pop(cache_key(url, audio), None)

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
        status = await msg.reply_text(
            "🎵 Готую аудіо..." if audio else "⏳ Починаю завантаження..."
        )
        loop = asyncio.get_running_loop()
        last_time = [0.0]
        last_text = [""]

        def progress_cb(text: str) -> None:
            now = time.monotonic()
            important = text.startswith(("🔧", "📤", "✅", "🔁", "❌", "⚡", "🎵"))
            if text == last_text[0]:
                return
            if now - last_time[0] < PROGRESS_THROTTLE and not important:
                return
            last_time[0] = now
            last_text[0] = text
            asyncio.run_coroutine_threadsafe(safe_edit(status, text), loop)

        try:
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
                update,
                path,
                title,
                audio,
                progress_cb,
            )
            if size:
                record_history(uid, url, title, platform)
                if file_id:
                    cache_set(url, audio, file_id, title)
            await safe_delete(status)
        finally:
            CANCEL_EVENTS.pop(cid, None)
            ACTIVE_TASKS.pop(cid, None)
            clean_old_files(False)
            cache_cleanup()


# ─────────────────────────── Handlers ──────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
        platform = platform_for_url(url)
        if not platform:
            await msg.reply_text(f"❌ Платформа не підтримується:\n{url[:100]}")
            continue
        await download_and_send(update, url, platform, False)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(USER_HELP_TEXT, parse_mode="Markdown")


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


async def thumb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    url = get_url_from_command(update, context)
    if not url:
        await msg.reply_text("❌ Використання: /thumb <посилання>")
        return
    platform = detect_platform(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return
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
            await msg.reply_photo(photo=file, caption=f"🖼 {safe_text(title, 200)}")
        await safe_delete(status)
        remove_file(path)
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")


async def sub_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    url = get_url_from_command(update, context)
    if not url:
        await msg.reply_text("❌ Використання: /sub <посилання>")
        return
    platform = detect_platform(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return
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
        await safe_edit(status, f"📝 Знайдено {len(files)} файл(и) для:\n{safe_text(title, 160)}")
        for file_path in files:
            with open(file_path, "rb") as file:
                await msg.reply_document(
                    document=file,
                    caption=f"📝 {Path(file_path).name}",
                )
            remove_file(file_path)
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")


async def clip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not FFMPEG_PATH:
        await msg.reply_text("❌ /clip потребує ffmpeg, якого немає на сервері.")
        return
    args = context.args or []
    url = None
    start_t = None
    end_t = None
    if len(args) >= 3:
        url, start_t, end_t = args[0], args[1], args[2]
    elif len(args) == 2 and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        if found:
            url, start_t, end_t = found[0], args[0], args[1]
    if not url or not start_t or not end_t:
        await msg.reply_text(
            "❌ Використання:\n"
            "/clip <url> <старт> <кінець>\n\n"
            "Приклад:\n"
            "/clip https://youtu.be/xxx 00:01:30 00:02:00"
        )
        return
    start_sec = parse_time_to_seconds(start_t)
    end_sec = parse_time_to_seconds(end_t)
    if start_sec is None or end_sec is None:
        await msg.reply_text("❌ Час має бути у форматі MM:SS або HH:MM:SS.")
        return
    if start_sec >= end_sec:
        await msg.reply_text("❌ Кінець кліпу має бути пізніше за старт.")
        return
    if end_sec - start_sec > 10 * 60:
        await msg.reply_text("❌ Кліп занадто довгий. Максимум 10 хвилин.")
        return
    platform = platform_for_url(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return
    cid = chat_id(update)
    uid = user_id(update)
    if rate_limited(uid or cid):
        wait = retry_after_seconds(uid or cid)
        await msg.reply_text(f"⏳ Забагато запитів. Спробуй ще раз через {wait}с.")
        return
    cancel_event = Event()
    CANCEL_EVENTS[cid] = cancel_event
    ACTIVE_TASKS[cid] = {
        "url": url,
        "platform": platform,
        "audio": False,
        "quality": quality_for(cid),
        "started_at": time.time(),
        "user_id": uid,
    }
    status = await msg.reply_text("⏳ Завантажую відео для нарізки...")
    loop = asyncio.get_running_loop()
    last_time = [0.0]
    last_text = [""]

    def progress_cb(text: str) -> None:
        now = time.monotonic()
        if text == last_text[0]:
            return
        if now - last_time[0] < PROGRESS_THROTTLE:
            return
        last_time[0] = now
        last_text[0] = text
        asyncio.run_coroutine_threadsafe(safe_edit(status, text), loop)

    try:
        path, title = await loop.run_in_executor(
            None,
            partial(
                download_media,
                url,
                platform,
                False,
                quality_for(cid),
                progress_cb,
                cancel_event,
            ),
        )
        if not path:
            await safe_edit(status, f"❌ {title}")
            return
        await safe_edit(status, "✂️ Нарізаю кліп...")
        clip_path = str(safe_filename("clip", url, "mp4"))
        ok, err = await loop.run_in_executor(
            None,
            partial(clip_video_ffmpeg, path, start_t, end_t, clip_path),
        )
        remove_file(path)
        if not ok:
            await safe_edit(status, f"❌ {err}")
            return
        if Path(clip_path).stat().st_size > MAX_UPLOAD_BYTES:
            remove_file(clip_path)
            await safe_edit(status, "❌ Кліп завеликий для Telegram. Зменш інтервал.")
            return
        await safe_edit(status, "📤 Надсилаю кліп...")
        with open(clip_path, "rb") as file:
            await msg.reply_video(
                video=file,
                caption=f"✂️ {safe_text(title, 160)}\n⏱ {start_t} → {end_t}",
                supports_streaming=True,
                read_timeout=180,
                write_timeout=180,
                connect_timeout=60,
                pool_timeout=60,
            )
        remove_file(clip_path)
        await safe_delete(status)
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")
    finally:
        CANCEL_EVENTS.pop(cid, None)
        ACTIVE_TASKS.pop(cid, None)
        clean_old_files(False)


async def info_or_formats(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    mode: str,
) -> None:
    msg = update.effective_message
    if not msg:
        return
    url = get_url_from_command(update, context)
    if not url:
        await msg.reply_text(f"❌ Використання: /{mode} <посилання>")
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
        fn = extract_info_text if mode == "info" else extract_formats_text
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            partial(fn, url, platform, quality_for(chat_id(update))),
        )
        await safe_edit(status, result)
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await info_or_formats(update, context, "info")


async def formats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await info_or_formats(update, context, "formats")


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
    await msg.reply_text(
        f"⚙️ Налаштування\n\nЯкість: {quality_for(cid)}",
        reply_markup=settings_keyboard(cid),
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
    event = CANCEL_EVENTS.get(chat_id(update))
    if not event:
        await msg.reply_text("Немає активного завантаження.")
        return
    event.set()
    await msg.reply_text("🛑 Скасовую завантаження...")


async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    uid = user_id(update)
    tasks = [
        (cid_key, task)
        for cid_key, task in ACTIVE_TASKS.items()
        if task.get("user_id") == uid
    ]
    if not tasks:
        await msg.reply_text("У тебе немає активних завантажень.")
        return
    lines = [f"📋 Твоїх активних: {len(tasks)}", ""]
    for _, task in tasks:
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
        f"• {platform}" for platform in URL_PATTERNS
    )
    await update.effective_message.reply_text(text)


# ─────────────────────────── Transcribe ────────────────────────

async def transcribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not OPENAI_API_KEY:
        await msg.reply_text("❌ Не задано OPENAI_API_KEY. Транскрипція недоступна.")
        return
    if not FFMPEG_PATH:
        await msg.reply_text("❌ Для транскрипції потрібен ffmpeg, щоб отримати правильний аудіофайл.")
        return
    url = get_url_from_command(update, context)
    if not url:
        await msg.reply_text("❌ Використання: /transcribe <посилання>")
        return
    platform = detect_platform(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується для транскрипції.")
        return
    cid = chat_id(update)
    uid = user_id(update)
    if rate_limited(uid or cid):
        wait = retry_after_seconds(uid or cid)
        await msg.reply_text(f"⏳ Забагато запитів. Спробуй ще раз через {wait}с.")
        return
    status = await msg.reply_text("🎤 Завантажую аудіо для транскрипції...")
    try:
        loop = asyncio.get_running_loop()
        audio_path, title = await loop.run_in_executor(
            None,
            partial(
                download_via_ytdlp,
                url,
                platform,
                True,
                quality_for(cid),
            ),
        )
        if not audio_path:
            await safe_edit(status, f"❌ Не вдалося отримати аудіо: {title}")
            return
        audio_file = Path(audio_path)
        if not audio_file.exists():
            await safe_edit(status, "❌ Аудіофайл після завантаження не знайдено.")
            return
        size = audio_file.stat().st_size
        if size > WHISPER_MAX_BYTES:
            remove_file(audio_path)
            await safe_edit(status, "❌ Аудіо перевищує 25MB — це ліміт Whisper API.")
            return
        await safe_edit(status, "🎤 Надсилаю аудіо на розпізнавання...")
        with open(audio_path, "rb") as file:
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    files={"file": (audio_file.name, file, "audio/mpeg")},
                    data={"model": "whisper-1"},
                    timeout=120,
                ),
            )
        remove_file(audio_path)
        if response.status_code != 200:
            await safe_edit(status, f"❌ Помилка OpenAI API:\n{response.text[:500]}")
            return
        data = response.json()
        text = str(data.get("text", "")).strip()
        if not text:
            await safe_edit(status, "❌ Транскрипція порожня.")
            return
        title_clean = safe_text(title, 160)
        parts = [text[i:i + 3900] for i in range(0, len(text), 3900)]
        for index, part in enumerate(parts, 1):
            header = f"🎤 {title_clean}"
            if len(parts) > 1:
                header += f"\nЧастина {index}/{len(parts)}"
            await msg.reply_text(f"{header}\n\n{part}")
        await safe_delete(status)
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")


# ─────────────────────────── Callbacks ─────────────────────────

async def quality_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    value = query.data.split(":")[1] if query.data and ":" in query.data else ""
    if value not in {"best", "fast", "mobile"}:
        return
    message = query.message
    if not message:
        return
    cid = int(message.chat.id)
    SETTINGS.setdefault("quality", {})[str(cid)] = value
    save_settings()
    await query.edit_message_text(f"✅ Якість: {value}")


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    action = query.data.split(":")[1] if query.data and ":" in query.data else ""
    if action != "quality":
        return
    await query.answer()
    message = query.message
    if not message:
        return
    cid = int(message.chat.id)
    await query.edit_message_text(
        f"Поточна якість: {quality_for(cid)}\nОбери:",
        reply_markup=quality_keyboard(),
    )


# ─────────────────────────── Error handler ─────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Unhandled error:", exc_info=context.error)


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

_THREAD_POOL: ThreadPoolExecutor | None = None


def get_thread_pool() -> ThreadPoolExecutor:
    global _THREAD_POOL
    if _THREAD_POOL is None:
        _THREAD_POOL = ThreadPoolExecutor(max_workers=PARALLEL_DOWNLOADS)
    return _THREAD_POOL


def build_application() -> Application:
    app = Application.builder().token(TOKEN).post_init(setup_bot_commands).build()
    if app.job_queue:
        app.job_queue.run_repeating(
            scheduled_cleanup,
            interval=7200,
            first=60,
        )
    else:
        log.warning(
            "JobQueue недоступний. Перевір requirements: "
            "python-telegram-bot[webhooks,job-queue]==20.7"
        )
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", start_command))
    app.add_handler(CommandHandler("dl", dl_command))
    app.add_handler(CommandHandler("video", video_command))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("thumb", thumb_command))
    app.add_handler(CommandHandler("sub", sub_command))
    app.add_handler(CommandHandler("clip", clip_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("formats", formats_command))
    app.add_handler(CommandHandler("transcribe", transcribe_command))
    app.add_handler(CommandHandler("platforms", platforms_command))
    app.add_handler(CommandHandler("quality", quality_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("queue", queue_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CallbackQueryHandler(quality_callback, pattern=r"^quality:"))
    app.add_handler(CallbackQueryHandler(settings_callback, pattern=r"^settings:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)
    return app


def main() -> None:
    clean_old_files(False)
    app = build_application()
    log.info("ffmpeg=%s", FFMPEG_PATH or "не знайдено")
    log.info("cookies=%s", cookies_file() or "не знайдено")
    log.info("webhook_url=%s", WEBHOOK_URL or "не задано polling режим")
    if WEBHOOK_URL:
        port = int(os.environ.get("PORT", "8443"))
        log.info("Запускаю webhook на порту %d", port)
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{TOKEN}",
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        log.info("Запускаю polling")
        try:
            requests.get(
                f"https://api.telegram.org/bot{TOKEN}/deleteWebhook",
                params={"drop_pending_updates": "true"},
                timeout=10,
            ).raise_for_status()
        except Exception as e:
            log.warning("Не вдалося видалити webhook перед polling: %s", e)
        app.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
