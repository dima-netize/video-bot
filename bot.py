from __future__ import annotations

# ─────────────────────────── Bootstrap ────────────────────────
import importlib, os, site, subprocess, sys
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
    _user = Path(site.getusersitepackages())
    if _user.exists() and str(_user) not in sys.path:
        sys.path.insert(0, str(_user))
        site.addsitedir(str(_user))
    importlib.invalidate_caches()


def _ensure_deps() -> None:
    if os.environ.get("DISABLE_AUTO_INSTALL", "0") == "1":
        return
    deps = [
        ("requests",   "requests>=2.31.0"),
        ("yt_dlp",     "yt-dlp"),
        ("telegram.ext", "python-telegram-bot==20.7"),
    ]
    for mod, pkg in deps:
        if not _module_ok(mod):
            _pip_install(pkg)
    bad = [m for m, _ in deps if not _module_ok(m)]
    if bad:
        raise RuntimeError("Не вдалося встановити: " + ", ".join(bad))


_ensure_deps()

# ─────────────────────────── Imports ──────────────────────────
import asyncio
import collections
import glob
import json
import logging
import re
import shutil
import time
import traceback
from datetime import datetime, timedelta
from functools import partial
from threading import Event
from typing import Any
from urllib.parse import urljoin

import requests
import yt_dlp
from telegram import (
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

# ─────────────────────────── Config ───────────────────────────
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано TOKEN у Environment Variables")

# WEBHOOK_URL — задай в Render як: https://video-bot-p2zt.onrender.com
# Якщо не задано — бот запускається в polling режимі (для локальної розробки)
WEBHOOK_URL: str | None = os.environ.get("WEBHOOK_URL", "").rstrip("/") or None

ADMIN_USERNAME = "dimagymenjuk"
OWNER_HANDLE = f"@{ADMIN_USERNAME}"
ADMIN_IDS: set[int] = set()  # Динамічно фіксуємо ID власника за username

BASE_DIR     = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

STATS_FILE    = BASE_DIR / "bot_stats.json"
SETTINGS_FILE = BASE_DIR / "bot_settings.json"
USERS_FILE    = BASE_DIR / "bot_users.json"
BANS_FILE     = BASE_DIR / "bot_bans.json"
HISTORY_FILE  = BASE_DIR / "bot_history.json"
OWNER_ID_FILE = BASE_DIR / "bot_owner_id.json"

MAX_UPLOAD_BYTES    = int(os.environ.get("MAX_UPLOAD_BYTES",     str(49 * 1024 * 1024)))
PROGRESS_THROTTLE   = float(os.environ.get("PROGRESS_THROTTLE",  "1.5"))
REQUEST_TIMEOUT     = int(os.environ.get("REQUEST_TIMEOUT",      "30"))
OLD_FILE_TTL        = int(os.environ.get("OLD_FILE_TTL",         str(60 * 60 * 3)))
MAX_LINKS_PER_MSG   = int(os.environ.get("MAX_LINKS_PER_MESSAGE","3"))
PARALLEL_DOWNLOADS  = max(1, int(os.environ.get("PARALLEL_DOWNLOADS", "2")))
PARALLEL_LIMIT      = asyncio.Semaphore(PARALLEL_DOWNLOADS)
RATE_LIMIT_N        = int(os.environ.get("RATE_LIMIT_N",         "5"))
RATE_LIMIT_WINDOW   = int(os.environ.get("RATE_LIMIT_WINDOW",    "60"))
MAX_RETRIES         = 3
URL_CACHE_TTL       = int(os.environ.get("URL_CACHE_TTL",        "3600"))   # 1 год
MAX_HISTORY_PER_USER = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("video-bot")

FFMPEG_PATH  = os.environ.get("FFMPEG_PATH") or shutil.which("ffmpeg")
BOT_START_TIME = time.time()

URL_RE          = re.compile(r"https?://[^\s<>\"]+", re.I)
DIRECT_VIDEO_RE = re.compile(
    r"https?://[^\s<>\"]+\.(?:mp4|mov|webm|m4v)(?:\?[^\s<>\"]*)?", re.I
)

URL_PATTERNS: dict[str, re.Pattern[str]] = {
    "youtube":     re.compile(r"(?:youtube\.com/(?:watch\?v=|shorts/|live/)|youtu\.be/|m\.youtube\.com/watch\?v=)", re.I),
    "tiktok":      re.compile(r"(?:tiktok\.com/@[\w.-]+/video/\d+|tiktok\.com/t/|vt\.tiktok\.com/|vm\.tiktok\.com/|www\.tiktok\.com/)", re.I),
    "instagram":   re.compile(r"instagram\.com/(?:reel|reels|p|tv|stories)/", re.I),
    "twitter":     re.compile(r"(?:twitter\.com|x\.com)/\w+/status/\d+", re.I),
    "vimeo":       re.compile(r"vimeo\.com/(?:\d+|channels/[^/]+/\d+)", re.I),
    "reddit":      re.compile(r"reddit\.com/r/\w+/comments/", re.I),
    "facebook":    re.compile(r"facebook\.com/(?:watch/\?v=|watch\?v=|reel/|share/r/|[\w.]+/videos/)", re.I),
    "likee":       re.compile(r"likee\.video/|likee\.com/", re.I),
    "snapchat":    re.compile(r"snapchat\.com/(?:spotlight|add)/", re.I),
    "pinterest":   re.compile(r"pinterest\.[a-z.]+/pin/\d+", re.I),
    "twitch":      re.compile(r"twitch\.tv/(?:videos/\d+|clips/)", re.I),
    "dailymotion": re.compile(r"dailymotion\.com/video/", re.I),
    "rumble":      re.compile(r"rumble\.com/v", re.I),
    "odysee":      re.compile(r"odysee\.com/@", re.I),
    "bilibili":    re.compile(r"bilibili\.com/video/", re.I),
    "coub":        re.compile(r"coub\.com/view/", re.I),
    "streamable":  re.compile(r"streamable\.com/", re.I),
    "medal":       re.compile(r"medal\.tv/", re.I),
    "youtube_music": re.compile(r"music\.youtube\.com/watch", re.I),
}

HELP_TEXT = """🎥 *Fast Video Downloader Bot*

Просто кинь посилання — бот завантажить відео.

*📥 Завантаження:*
/dl `<url>` — завантажити відео (аліас /video)
/video `<url>` — завантажити відео
/audio `<url>` — завантажити аудіо (MP3)
/thumb `<url>` — обкладинка відео
/sub `<url>` — субтитри
/clip `<url> <старт> <кінець>` — вирізати кліп
    _Приклад:_ `/clip https://... 00:01:30 00:02:00`

*ℹ️ Інформація:*
/info `<url>` — деталі про відео
/formats `<url>` — список доступних форматів
/platforms — підтримувані платформи

*⚙️ Налаштування:*
/settings — панель налаштувань
/quality — переглянути/змінити якість
  • `best` — максимальна якість
  • `fast` — до 720p (швидше)
  • `mobile` — до 480p (малий файл)

*📋 Керування:*
/cancel — скасувати поточне завантаження
/queue — активні завантаження
/history — останні 10 завантажень
/clean — видалити старі файли

*📊 Статус:*
/stats — статистика
/health — стан бота
/ping — перевірка відповіді
/cookies — перевірити cookies.txt
/updateytdlp — оновити yt-dlp

*🔐 Адмін:*
/broadcast `<текст>` — надіслати всім
/ad `<текст>` — реклама для всіх
/users — список юзерів
/ban `<id>` — заблокувати
/unban `<id>` — розблокувати
/resetstats — очистити статистику
/adminstats — аналітика користувачів
/userlimit <n|off> — ліміт юзерів

_💡 Підказка: YouTube часто потребує cookies.txt на free-серверах._
"""

# ─────────────────────────── State ────────────────────────────
CANCEL_EVENTS: dict[int, Event] = {}
ACTIVE_TASKS:  dict[int, dict[str, Any]] = {}
RATE_TRACKER:  dict[int, collections.deque] = {}

# URL-кеш: {url → (file_id, title, timestamp, is_audio)}
URL_CACHE: dict[str, tuple[str, str, float, bool]] = {}


class DownloadCancelled(Exception):
    pass


# ─────────────────────────── JSON I/O ─────────────────────────
def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default
    except Exception:
        return default


def write_json(path: Path, data: Any) -> None:
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        log.exception("JSON write error: %s", path)


STATS    = read_json(STATS_FILE,    {"success": 0, "errors": 0, "bytes": 0, "platforms": {}})
SETTINGS = read_json(SETTINGS_FILE, {"quality": {}})
USERS    = read_json(USERS_FILE,    {})
BANNED_USERS: set[int] = set(read_json(BANS_FILE, []))
HISTORY: dict[str, list[dict]] = read_json(HISTORY_FILE, {})  # {uid_str: [{url, title, ts, platform}]}
OWNER_ID: int | None = read_json(OWNER_ID_FILE, None)


def save_stats()    -> None: write_json(STATS_FILE,    STATS)
def save_settings() -> None: write_json(SETTINGS_FILE, SETTINGS)
def save_users()    -> None: write_json(USERS_FILE,    USERS)
def save_bans()     -> None: write_json(BANS_FILE,     list(BANNED_USERS))
def save_history()  -> None: write_json(HISTORY_FILE,  HISTORY)
def save_owner_id() -> None: write_json(OWNER_ID_FILE, OWNER_ID)


def record_user(update: Update) -> None:
    user = update.effective_user
    if not user:
        return
    uid = str(user.id)
    changed = uid not in USERS
    USERS.setdefault(uid, {
        "username":   user.username or "",
        "first_name": user.first_name or "",
        "joined":     datetime.utcnow().isoformat(),
    })
    if user.username and USERS[uid].get("username") != user.username:
        USERS[uid]["username"] = user.username
        changed = True
    if changed:
        save_users()


def record_history(uid: int, url: str, title: str, platform: str) -> None:
    key = str(uid)
    HISTORY.setdefault(key, [])
    HISTORY[key].insert(0, {
        "url":      url,
        "title":    title[:80],
        "platform": platform,
        "ts":       datetime.utcnow().isoformat(),
    })
    HISTORY[key] = HISTORY[key][:MAX_HISTORY_PER_USER]
    save_history()


def stats_ok(platform: str, size: int) -> None:
    STATS["success"] = int(STATS.get("success", 0)) + 1
    STATS["bytes"]   = int(STATS.get("bytes", 0)) + int(size or 0)
    STATS.setdefault("platforms", {})
    STATS["platforms"][platform] = int(STATS["platforms"].get(platform, 0)) + 1
    save_stats()


def stats_fail() -> None:
    STATS["errors"] = int(STATS.get("errors", 0)) + 1
    save_stats()


# ─────────────────────────── URL Cache ────────────────────────
def cache_get(url: str, audio: bool) -> tuple[str, str] | None:
    """Повертає (file_id, title) якщо є в кеші та не протух."""
    key = f"{url}|{'audio' if audio else 'video'}"
    entry = URL_CACHE.get(key)
    if not entry:
        return None
    file_id, title, ts, _ = entry
    if time.time() - ts > URL_CACHE_TTL:
        del URL_CACHE[key]
        return None
    return file_id, title


def cache_set(url: str, audio: bool, file_id: str, title: str) -> None:
    key = f"{url}|{'audio' if audio else 'video'}"
    URL_CACHE[key] = (file_id, title, time.time(), audio)


def cache_cleanup() -> None:
    now = time.time()
    dead = [k for k, v in URL_CACHE.items() if now - v[2] > URL_CACHE_TTL]
    for k in dead:
        del URL_CACHE[k]


# ─────────────────────────── Helpers ──────────────────────────
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
    return f"{m}хв {sec}с" if m else f"{sec}с"


def uptime_text() -> str:
    return seconds_text(time.time() - BOT_START_TIME)


def safe_text(value: Any, limit: int = 220) -> str:
    return (re.sub(r"\s+", " ", str(value or "video")).strip() or "video")[:limit]


def chat_id(update: Update) -> int:
    return int(update.effective_chat.id) if update.effective_chat else 0


def user_id(update: Update) -> int:
    return int(update.effective_user.id) if update.effective_user else 0


def quality_for(cid: int) -> str:
    return SETTINGS.get("quality", {}).get(str(cid), "fast")


def cookies_file() -> str | None:
    for p in [
        Path("/etc/secrets/cookies.txt"),
        BASE_DIR / "cookies.txt",
        BASE_DIR / "cookies" / "cookies.txt",
    ]:
        if not p.exists():
            continue
        try:
            first = p.read_text(encoding="utf-8", errors="ignore").splitlines()[0].strip()
            if first in {"# Netscape HTTP Cookie File", "# HTTP Cookie File"}:
                return str(p)
        except Exception:
            pass
    return None


def extract_urls(text: str) -> list[str]:
    out: list[str] = []
    for url in URL_RE.findall(text or ""):
        url = url.strip().strip(".,;)\n\r\t ")
        if url and url not in out:
            out.append(url)
    return out[:MAX_LINKS_PER_MSG]


def detect_platform(url: str) -> str | None:
    for name, pat in URL_PATTERNS.items():
        if pat.search(url):
            return name
    return None


def safe_filename(prefix: str, url: str, ext: str = "mp4") -> Path:
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug = url.split("?")[0].rstrip("/").split("/")[-1] or "video"
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", slug)[:35] or "video"
    return DOWNLOAD_DIR / f"{prefix}_{slug}_{ts}.{ext}"


def remove_file(path: str | Path | None) -> None:
    try:
        if path:
            Path(path).unlink(missing_ok=True)
    except OSError:
        pass


def clean_old_files(force: bool = False) -> int:
    now, count = time.time(), 0
    for p in DOWNLOAD_DIR.glob("*"):
        try:
            if p.is_file() and (force or now - p.stat().st_mtime > OLD_FILE_TTL):
                p.unlink()
                count += 1
        except OSError:
            pass
    return count


# ─────────────────────────── Progress ─────────────────────────
def progress_bar(pct: int, width: int = 14) -> str:
    filled = int(width * pct / 100)
    return f"[{'▓' * filled}{'░' * (width - filled)}] {pct}%"


def progress_text(prefix: str, done: int, total: int | None, start: float) -> str:
    elapsed = max(time.monotonic() - start, 0.1)
    speed   = done / elapsed if done else 0
    if total:
        pct = max(0, min(100, int(done * 100 / total)))
        eta = int((total - done) / speed) if speed else 0
        return (
            f"{prefix}\n"
            f"{progress_bar(pct)}\n"
            f"{human_bytes(done)} / {human_bytes(total)}\n"
            f"⚡ {human_bytes(speed)}/s  •  ETA {seconds_text(eta)}"
        )
    return (
        f"{prefix}\n"
        f"Завантажено: {human_bytes(done)}\n"
        f"⚡ {human_bytes(speed)}/s"
    )


# ─────────────────────────── Rate limit ───────────────────────
def check_rate_limit(uid: int) -> bool:
    if uid in ADMIN_IDS:
        return True
    now = time.time()
    dq  = RATE_TRACKER.setdefault(uid, collections.deque())
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


# ─────────────────────────── Error text ───────────────────────
def friendly_error(platform: str | None, error: str) -> str:
    err = str(error or "")
    low = err.lower()
    if "exit code 137" in low or "killed" in low:
        return "⚠️ Серверу не вистачило ресурсів. Постав /quality mobile і спробуй ще раз."
    if platform == "youtube" and any(x in low for x in ["sign in to confirm", "not a bot", "use --cookies", "cookies"]):
        return (
            "🍪 YouTube просить cookies.txt.\n\n"
            "Поклади правильний cookies.txt поруч із bot.py.\n"
            "Перший рядок: `# Netscape HTTP Cookie File`\n\n"
            "_Без cookies YouTube часто блокує free-сервери._"
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
        return "🌐 Помилка мережі. Зачекай хвилину і спробуй ще раз."
    if "429" in low or "too many" in low:
        return "⏳ Платформа тимчасово блокує завантаження. Зачекай 5-10 хвилин."
    if "geo" in low or "not available in your country" in low:
        return "🌍 Відео недоступне в регіоні сервера."
    return safe_text(err, 900)


def is_transient_error(error: str) -> bool:
    low = error.lower()
    return any(x in low for x in [
        "network", "connection", "timeout", "reset by peer",
        "read error", "http error 5", "503", "502", "429",
    ])


# ─────────────────────────── yt-dlp ───────────────────────────
def first_entry(info: dict[str, Any]) -> dict[str, Any]:
    entries = info.get("entries")
    if not entries:
        return info
    entries = [x for x in entries if x]
    return entries[0] if entries else info


def find_file(info: dict[str, Any], ydl) -> str | None:
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
    existing = [str(Path(c)) for c in candidates if c and Path(c).exists()]
    existing.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
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
    extra: dict | None = None,
) -> dict[str, Any]:
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    if platform == "tiktok":
        ua = "com.zhiliaoapp.musically/2022600030 (Linux; U; Android 12; en_US; Pixel 6; Build/SP1A.210812.016)"

    opts: dict[str, Any] = {
        "format":                        format_selector(platform, audio, quality),
        "outtmpl":                       str(DOWNLOAD_DIR / "%(extractor_key)s_%(id)s_%(title).80s.%(ext)s"),
        "quiet":                         True,
        "no_warnings":                   True,
        "noplaylist":                    True,
        "restrictfilenames":             True,
        "retries":                       8,
        "fragment_retries":              8,
        "socket_timeout":                30,
        "continuedl":                    True,
        "concurrent_fragment_downloads": 3,
        "http_chunk_size":               6 * 1024 * 1024,
        "http_headers":                  {"User-Agent": ua},
        "progress_hooks":                [hook] if hook else [],
    }

    if FFMPEG_PATH:
        opts["ffmpeg_location"] = FFMPEG_PATH
        if not audio:
            opts["merge_output_format"] = "mp4"
        if audio:
            opts["postprocessors"] = [{
                "key":              "FFmpegExtractAudio",
                "preferredcodec":   "mp3",
                "preferredquality": "192",
            }]

    ck = cookies_file()
    if ck:
        opts["cookiefile"] = ck

    if platform == "youtube":
        opts["extractor_args"] = {"youtube": {"player_client": ["android", "web"]}}
    if platform == "tiktok":
        opts["extractor_args"] = {"tiktok": {"app_version": "26.2.0", "manifest_app_version": "26.2.0"}}

    if extra:
        opts.update(extra)
    return opts


# ─────────────────────────── Downloaders ──────────────────────
def stream_download(
    url: str,
    filepath: Path,
    title: str,
    progress_cb=None,
    cancel_event: Event | None = None,
    headers: dict[str, str] | None = None,
):
    headers = headers or {"User-Agent": "Mozilla/5.0"}
    start, done = time.monotonic(), 0
    try:
        with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=headers) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length") or 0)
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(1024 * 256):
                    if cancel_event and cancel_event.is_set():
                        raise DownloadCancelled("Завантаження скасовано.")
                    if not chunk:
                        continue
                    f.write(chunk)
                    done += len(chunk)
                    if progress_cb:
                        progress_cb(progress_text("⏳ Завантажую файл", done, total, start))
        return str(filepath), title
    except DownloadCancelled as e:
        remove_file(filepath)
        return None, str(e)
    except Exception as e:
        remove_file(filepath)
        return None, f"Помилка прямого завантаження: {e}"


def tiktok_fallback_tikwm(url: str, progress_cb=None, cancel_event: Event | None = None):
    """TikTok без watermark через tikwm.com."""
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://tikwm.com/"}
    try:
        r = requests.get(
            "https://tikwm.com/api/",
            params={"url": url, "hd": "1"},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            return None, data.get("msg") or "tikwm.com помилка."
        item      = data.get("data") or {}
        video_url = item.get("hdplay") or item.get("play") or item.get("wmplay")
        if not video_url:
            return None, "tikwm.com не повернув відео."
        video_url = urljoin("https://tikwm.com", video_url)
        return stream_download(
            video_url,
            safe_filename("tiktok", url),
            safe_text(item.get("title") or "TikTok video"),
            progress_cb, cancel_event, headers,
        )
    except Exception as e:
        return None, f"tikwm.com: {e}"


def tiktok_fallback_snaptik(url: str, progress_cb=None, cancel_event: Event | None = None):
    """TikTok резервний fallback через SnapTik API."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        r = requests.post(
            "https://snaptik.app/abc2.php",
            data={"url": url},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        m = re.search(r'href="(https://[^"]+\.mp4[^"]*)"', r.text)
        if not m:
            return None, "SnapTik не знайшов відео."
        return stream_download(
            m.group(1),
            safe_filename("tiktok_snap", url),
            "TikTok video",
            progress_cb, cancel_event,
        )
    except Exception as e:
        return None, f"SnapTik: {e}"


def instagram_fallback(url: str, progress_cb=None, cancel_event: Event | None = None):
    fixed = url.replace("www.instagram.com", "www.ddinstagram.com").replace("instagram.com", "ddinstagram.com")
    try:
        r = requests.get(fixed, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        video_url = None
        for pat in [
            r'<video[^>]+src="([^"]+)"',
            r'property="og:video"\s+content="([^"]+)"',
            r'property="og:video:secure_url"\s+content="([^"]+)"',
            r'"video_url":"([^"]+)"',
        ]:
            m = re.search(pat, r.text)
            if m:
                video_url = m.group(1).replace("\\u0026", "&").replace("\\/", "/")
                break
        if not video_url:
            return None, "Instagram fallback не знайшов відео."
        return stream_download(
            video_url,
            safe_filename("instagram", url),
            "Instagram video",
            progress_cb, cancel_event,
            {"User-Agent": "Mozilla/5.0"},
        )
    except Exception as e:
        return None, f"Instagram fallback: {e}"


def download_direct(url: str, progress_cb=None, cancel_event: Event | None = None):
    ext = url.split("?")[0].split(".")[-1].lower()
    if ext not in {"mp4", "mov", "webm", "m4v"}:
        ext = "mp4"
    return stream_download(url, safe_filename("direct", url, ext), "Пряме відео", progress_cb, cancel_event)


def download_via_ytdlp(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
    extra_opts: dict | None = None,
):
    start = time.monotonic()

    def hook(d: dict[str, Any]) -> None:
        if cancel_event and cancel_event.is_set():
            raise DownloadCancelled("Завантаження скасовано.")
        if not progress_cb:
            return
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done  = d.get("downloaded_bytes") or 0
            progress_cb(progress_text("⏳ Завантажую", int(done or 0), int(total or 0), start))
        elif d.get("status") == "finished":
            progress_cb("🔧 Обробляю файл...")

    try:
        with yt_dlp.YoutubeDL(ytdlp_opts(platform, audio, quality, hook, extra_opts)) as ydl:
            info = first_entry(ydl.extract_info(url, download=True))
            path = find_file(info, ydl)
            if audio and path and FFMPEG_PATH:
                mp3 = str(Path(path).with_suffix(".mp3"))
                if Path(mp3).exists():
                    path = mp3
            if not path or not Path(path).exists():
                return None, "Файл після завантаження не знайдено."
            return path, safe_text(info.get("title"), 180)
    except DownloadCancelled as e:
        return None, str(e)
    except Exception as e:
        return None, friendly_error(platform, str(e))


def download_media(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
):
    if DIRECT_VIDEO_RE.search(url) and not audio:
        return download_direct(url, progress_cb, cancel_event)

    if platform == "tiktok" and not audio:
        if progress_cb:
            progress_cb("🔁 TikTok (no-watermark)...")
        p, r = tiktok_fallback_tikwm(url, progress_cb, cancel_event)
        if p:
            return p, r
        if progress_cb:
            progress_cb("🔁 TikTok fallback #2...")
        p, r = tiktok_fallback_snaptik(url, progress_cb, cancel_event)
        if p:
            return p, r
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
        p, r = download_via_ytdlp(url, platform, audio, quality, progress_cb, cancel_event)
        if p:
            return p, r
        last_error = r
        if not is_transient_error(r):
            break

    if not audio and platform == "instagram":
        if progress_cb:
            progress_cb("🔁 Instagram fallback...")
        p2, r2 = instagram_fallback(url, progress_cb, cancel_event)
        if p2:
            return p2, r2
        return None, f"{last_error}\nInstagram fallback: {r2}"

    return None, last_error


# ─────────────────────────── Info/Formats/Sub/Thumb ───────────
def extract_info_text(url: str, platform: str | None, quality: str) -> str:
    opts = ytdlp_opts(platform, False, quality)
    opts["skip_download"] = True
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=False))
    lines = [
        f"ℹ️ *{safe_text(info.get('title'), 180)}*",
        f"👤 Автор: {safe_text(info.get('uploader') or info.get('channel') or 'невідомо', 120)}",
        f"⏱ Тривалість: {seconds_text(info.get('duration'))}",
        f"📡 Платформа: {platform or 'unknown'}",
    ]
    if info.get("view_count") is not None:
        lines.append(f"👁 Перегляди: {info.get('view_count'):,}".replace(",", " "))
    if info.get("like_count") is not None:
        lines.append(f"👍 Лайки: {info.get('like_count'):,}".replace(",", " "))
    if info.get("upload_date"):
        d = info["upload_date"]
        lines.append(f"📅 Дата: {d[:4]}-{d[4:6]}-{d[6:]}")
    if info.get("description"):
        lines += ["", f"📝 {safe_text(info['description'], 300)}"]
    lines += ["", f"🔗 {info.get('webpage_url') or url}"]
    return "\n".join(lines)[:3900]


def extract_formats_text(url: str, platform: str | None, quality: str) -> str:
    opts = ytdlp_opts(platform, False, quality)
    opts["skip_download"] = True
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=False))
    lines = [f"🎞 Формати: *{safe_text(info.get('title'), 100)}*", ""]
    count = 0
    for fmt in info.get("formats") or []:
        if count >= 35:
            break
        h    = fmt.get("height")
        fps  = fmt.get("fps")
        size = fmt.get("filesize") or fmt.get("filesize_approx")
        label = f"{h}p" if h else (fmt.get("format_note") or fmt.get("resolution") or "audio")
        if fps:
            label += f"/{int(fps)}fps"
        media = "audio" if fmt.get("vcodec") == "none" else ("video-only" if fmt.get("acodec") == "none" else "video")
        lines.append(
            f"• `{fmt.get('format_id', '?')}`: {label} ({fmt.get('ext', '?')}, {media})"
            + (f" ~{human_bytes(size)}" if size else "")
        )
        count += 1
    if count == 0:
        lines.append("Формати не знайдено.")
    return "\n".join(lines)[:3900]


def download_subtitles(url: str, platform: str | None, quality: str) -> tuple[list[str], str]:
    opts = ytdlp_opts(platform, False, quality)
    opts.update({
        "skip_download":     True,
        "writesubtitles":    True,
        "writeautomaticsub": True,
        "subtitleslangs":    ["uk", "en", "ru", "auto"],
        "subtitlesformat":   "srt/best",
        "outtmpl":           str(DOWNLOAD_DIR / "sub_%(id)s.%(ext)s"),
    })
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=True))
    title  = safe_text(info.get("title"), 180)
    vid_id = info.get("id", "")
    files  = glob.glob(str(DOWNLOAD_DIR / f"sub_{vid_id}*"))
    return files, title


def download_thumbnail(url: str, platform: str | None, quality: str) -> tuple[str | None, str]:
    opts = ytdlp_opts(platform, False, quality)
    opts.update({
        "skip_download":  True,
        "writethumbnail": True,
        "outtmpl":        str(DOWNLOAD_DIR / "thumb_%(id)s.%(ext)s"),
    })
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=True))
    title  = safe_text(info.get("title"), 180)
    vid_id = info.get("id", "")
    files  = sorted(
        glob.glob(str(DOWNLOAD_DIR / f"thumb_{vid_id}*")),
        key=lambda x: Path(x).stat().st_mtime,
        reverse=True,
    )
    return (files[0] if files else None), title


def clip_video_ffmpeg(input_path: str, start: str, end: str, output_path: str) -> tuple[bool, str]:
    if not FFMPEG_PATH:
        return False, "ffmpeg не знайдено на сервері."
    try:
        result = subprocess.run(
            [FFMPEG_PATH, "-y", "-i", input_path, "-ss", start, "-to", end, "-c", "copy", output_path],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            return False, f"ffmpeg error:\n{result.stderr[-600:]}"
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "ffmpeg timeout (>120с)."
    except Exception as e:
        return False, str(e)


# ─────────────────────────── Telegram helpers ─────────────────
async def safe_edit(message, text: str) -> None:
    try:
        await message.edit_text(text[:3900], parse_mode="Markdown")
    except RetryAfter as e:
        await asyncio.sleep(float(e.retry_after) + 0.2)
    except BadRequest as e:
        if "message is not modified" not in str(e).lower():
            log.debug("edit error: %s", e)
    except TelegramError as e:
        log.debug("telegram error: %s", e)


async def send_media(
    update: Update,
    filepath: str,
    title: str,
    is_audio: bool = False,
    progress_cb=None,
) -> tuple[int, str]:
    """Повертає (size, file_id) або (0, '')."""
    msg = update.effective_message
    if not msg:
        remove_file(filepath)
        return 0, ""
    try:
        size = Path(filepath).stat().st_size
        if size > MAX_UPLOAD_BYTES:
            await msg.reply_text(
                "❌ Файл більший за ліміт Telegram Bot API (50MB).\n"
                "Постав /quality mobile і спробуй ще раз."
            )
            return 0, ""
        if progress_cb:
            progress_cb("📤 Надсилаю у Telegram...")
        with open(filepath, "rb") as f:
            if is_audio and Path(filepath).suffix.lower() == ".mp3":
                sent = await msg.reply_audio(
                    audio=f, title=title[:64],
                    caption=f"🎵 {title[:180]}",
                    read_timeout=180, write_timeout=180, connect_timeout=60, pool_timeout=60,
                )
                return size, str(sent.audio.file_id if sent.audio else "")
            elif is_audio:
                sent = await msg.reply_document(
                    document=f,
                    caption=f"🎵 {title[:180]}\n_Файл без MP3 (немає ffmpeg)._",
                    parse_mode="Markdown",
                    read_timeout=180, write_timeout=180, connect_timeout=60, pool_timeout=60,
                )
                return size, str(sent.document.file_id if sent.document else "")
            else:
                sent = await msg.reply_video(
                    video=f, caption=f"✅ {title[:200]}",
                    supports_streaming=True,
                    read_timeout=180, write_timeout=180, connect_timeout=60, pool_timeout=60,
                )
                return size, str(sent.video.file_id if sent.video else "")
    except Exception:
        log.exception("send error")
        await msg.reply_text("❌ Не вдалося надіслати файл у Telegram.")
        return 0, ""
    finally:
        remove_file(filepath)


async def send_cached(update: Update, file_id: str, title: str, is_audio: bool) -> None:
    """Надіслати відео/аудіо з кешу Telegram за file_id."""
    msg = update.effective_message
    if not msg:
        return
    if is_audio:
        await msg.reply_audio(audio=file_id, title=title[:64], caption=f"🎵 {title[:180]} _(з кешу)_", parse_mode="Markdown")
    else:
        await msg.reply_video(video=file_id, caption=f"✅ {title[:200]} _(з кешу)_", parse_mode="Markdown")


def quality_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🏆 best",   callback_data="quality:best"),
        InlineKeyboardButton("⚡ fast",   callback_data="quality:fast"),
        InlineKeyboardButton("📱 mobile", callback_data="quality:mobile"),
    ]])


def settings_keyboard(cid: int) -> InlineKeyboardMarkup:
    q = quality_for(cid)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Якість: {q} ↕️", callback_data="settings:quality")],
        [InlineKeyboardButton("🧹 Очистити кеш URL", callback_data="settings:clearcache")],
        [InlineKeyboardButton("📊 Статистика",       callback_data="settings:stats")],
    ])


def user_limit() -> int:
    raw = SETTINGS.get("limits", {}).get("max_users", 0)
    try:
        return max(0, int(raw))
    except Exception:
        return 0


def user_allowed(uid: int) -> bool:
    if uid in ADMIN_IDS:
        return True
    limit = user_limit()
    if not limit:
        return True
    return str(uid) in USERS or len(USERS) < limit


# ─────────────────────────── Core flow ────────────────────────
async def download_and_send(
    update: Update,
    url: str,
    platform: str,
    audio: bool = False,
) -> None:
    msg = update.effective_message
    if not msg:
        return

    cid     = chat_id(update)
    uid     = user_id(update)
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
            f"⏳ Забагато завантажень! Максимум {RATE_LIMIT_N} за {RATE_LIMIT_WINDOW}с.\n"
            f"Зачекай ще ~{wait}с."
        )
        return

    # Перевірка URL-кешу
    cached = cache_get(url, audio)
    if cached:
        file_id, title = cached
        status = await msg.reply_text("⚡ Знайдено в кеші, надсилаю...")
        try:
            await send_cached(update, file_id, title, audio)
            await status.delete()
        except Exception:
            await safe_edit(status, "❌ Кеш застарів. Завантажую заново...")
            # якщо кеш не спрацював — видаляємо і йдемо далі
            key = f"{url}|{'audio' if audio else 'video'}"
            URL_CACHE.pop(key, None)
        else:
            return

    cancel_event = Event()
    CANCEL_EVENTS[cid] = cancel_event
    ACTIVE_TASKS[cid]  = {
        "url":        url,
        "platform":   platform,
        "audio":      audio,
        "quality":    quality,
        "started_at": time.time(),
        "user_id":    uid,
    }

    async with PARALLEL_LIMIT:
        status = await msg.reply_text(
            "🎵 Готую аудіо..." if audio else "⏳ Починаю завантаження..."
        )
        loop = asyncio.get_running_loop()
        last_time, last_text = [0.0], [""]

        def progress_cb(text: str) -> None:
            now       = time.monotonic()
            important = text.startswith(("🔧", "📤", "✅", "🔁", "❌", "⚡"))
            if text == last_text[0] or (now - last_time[0] < PROGRESS_THROTTLE and not important):
                return
            last_time[0], last_text[0] = now, text
            asyncio.run_coroutine_threadsafe(safe_edit(status, text), loop)

        try:
            job  = partial(download_media, url, platform, audio, quality, progress_cb, cancel_event)
            path, title = await loop.run_in_executor(None, job)

            if not path:
                stats_fail()
                await safe_edit(status, f"❌ {title}")
                return

            await safe_edit(status, "✅ Завантажено. Надсилаю...")
            size, file_id = await send_media(update, path, title, audio, progress_cb)
            if size:
                stats_ok(platform, size)
                record_history(uid, url, title, platform)
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


# ─────────────────────────── Handlers ─────────────────────────
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
        platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)
        if not platform:
            await msg.reply_text(f"❌ Платформа не підтримується:\n`{url[:100]}`", parse_mode="Markdown")
            continue
        await download_and_send(update, url, platform, False)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    if update.effective_message:
        await update.effective_message.reply_text(HELP_TEXT, parse_mode="Markdown")


async def dl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Аліас для /video."""
    await video_command(update, context)


async def video_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    url = context.args[0].strip() if context.args else None
    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url   = found[0] if found else None
    if not url:
        await msg.reply_text("❌ Використання: /video <посилання>")
        return
    platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return
    await download_and_send(update, url, platform, False)


async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    url = context.args[0].strip() if context.args else None
    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url   = found[0] if found else None
    if not url:
        await msg.reply_text("❌ Використання: /audio <посилання>")
        return
    platform = detect_platform(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується для аудіо.")
        return
    await download_and_send(update, url, platform, True)


async def thumb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    url = context.args[0].strip() if context.args else None
    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url   = found[0] if found else None
    if not url:
        await msg.reply_text("❌ Використання: /thumb <посилання>")
        return
    platform = detect_platform(url)
    status   = await msg.reply_text("🖼 Завантажую обкладинку...")
    try:
        loop = asyncio.get_running_loop()
        path, title = await loop.run_in_executor(
            None,
            partial(download_thumbnail, url, platform, quality_for(chat_id(update))),
        )
        if not path:
            await safe_edit(status, "❌ Обкладинку не знайдено.")
            return
        with open(path, "rb") as f:
            await msg.reply_photo(photo=f, caption=f"🖼 {title[:200]}")
        await status.delete()
        remove_file(path)
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")


async def sub_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    url = context.args[0].strip() if context.args else None
    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url   = found[0] if found else None
    if not url:
        await msg.reply_text("❌ Використання: /sub <посилання>")
        return
    platform = detect_platform(url)
    status   = await msg.reply_text("📝 Шукаю субтитри...")
    try:
        loop = asyncio.get_running_loop()
        files, title = await loop.run_in_executor(
            None,
            partial(download_subtitles, url, platform, quality_for(chat_id(update))),
        )
        if not files:
            await safe_edit(status, "❌ Субтитри не знайдено.")
            return
        await safe_edit(status, f"📝 {len(files)} файл(и) для:\n*{title[:160]}*")
        for fpath in files:
            with open(fpath, "rb") as f:
                await msg.reply_document(document=f, caption=f"📝 {Path(fpath).name}")
            remove_file(fpath)
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")


async def clip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    if not FFMPEG_PATH:
        await msg.reply_text("❌ /clip потребує ffmpeg, якого немає на сервері.")
        return
    args = context.args or []
    url, start_t, end_t = None, None, None
    if len(args) >= 3:
        url, start_t, end_t = args[0], args[1], args[2]
    elif len(args) == 2 and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        if found:
            url, start_t, end_t = found[0], args[0], args[1]
    if not url or not start_t or not end_t:
        await msg.reply_text(
            "❌ Використання: `/clip <url> <старт> <кінець>`\n"
            "Наприклад: `/clip https://youtu.be/xxx 00:01:30 00:02:00`",
            parse_mode="Markdown",
        )
        return
    time_re = re.compile(r"^\d{1,2}:\d{2}(:\d{2})?$")
    if not time_re.match(start_t) or not time_re.match(end_t):
        await msg.reply_text("❌ Час у форматі MM:SS або HH:MM:SS")
        return
    platform = detect_platform(url)
    cid      = chat_id(update)
    cancel_event = Event()
    CANCEL_EVENTS[cid] = cancel_event
    status = await msg.reply_text("⏳ Завантажую відео для нарізки...")
    loop = asyncio.get_running_loop()
    last_time, last_text = [0.0], [""]

    def progress_cb(text: str) -> None:
        now = time.monotonic()
        if text == last_text[0] or now - last_time[0] < PROGRESS_THROTTLE:
            return
        last_time[0], last_text[0] = now, text
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
        ok, err   = await loop.run_in_executor(None, partial(clip_video_ffmpeg, path, start_t, end_t, clip_path))
        remove_file(path)
        if not ok:
            await safe_edit(status, f"❌ {err}")
            return
        if Path(clip_path).stat().st_size > MAX_UPLOAD_BYTES:
            remove_file(clip_path)
            await safe_edit(status, "❌ Кліп завеликий для Telegram. Зменш інтервал.")
            return
        await safe_edit(status, "📤 Надсилаю кліп...")
        with open(clip_path, "rb") as f:
            await msg.reply_video(
                video=f,
                caption=f"✂️ {title[:160]}\n⏱ {start_t} → {end_t}",
                supports_streaming=True,
                read_timeout=180, write_timeout=180, connect_timeout=60, pool_timeout=60,
            )
        try:
            await status.delete()
        except Exception:
            pass
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")
    finally:
        CANCEL_EVENTS.pop(cid, None)
        ACTIVE_TASKS.pop(cid, None)
        clean_old_files(False)


async def info_or_formats(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    url = context.args[0].strip() if context.args else None
    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url   = found[0] if found else None
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
        fn     = extract_info_text if mode == "info" else extract_formats_text
        result = await asyncio.get_running_loop().run_in_executor(
            None, partial(fn, url, platform, quality_for(chat_id(update)))
        )
        await safe_edit(status, result)
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await info_or_formats(update, context, "info")


async def formats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await info_or_formats(update, context, "formats")


async def quality_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    cid = chat_id(update)
    if not context.args:
        await msg.reply_text(
            f"⚙️ Поточна якість: *{quality_for(cid)}*\n\nОбери нову:",
            parse_mode="Markdown",
            reply_markup=quality_keyboard(),
        )
        return
    value = context.args[0].lower().strip()
    if value not in {"best", "fast", "mobile"}:
        await msg.reply_text("❌ Доступно: best, fast, mobile")
        return
    SETTINGS.setdefault("quality", {})[str(cid)] = value
    save_settings()
    await msg.reply_text(f"✅ Якість змінено на: *{value}*", parse_mode="Markdown")


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    cid = chat_id(update)
    await msg.reply_text(
        f"⚙️ *Налаштування*\n\nЯкість: `{quality_for(cid)}`\nКеш URL: {len(URL_CACHE)} записів",
        parse_mode="Markdown",
        reply_markup=settings_keyboard(cid),
    )


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    uid  = user_id(update)
    hist = HISTORY.get(str(uid), [])
    if not hist:
        await msg.reply_text("📋 Немає завантажень.")
        return
    lines = [f"📋 *Останні {len(hist)} завантажень:*", ""]
    for i, item in enumerate(hist, 1):
        ts = item.get("ts", "")[:10]
        lines.append(f"{i}. [{item['platform']}] {item['title'][:50]}\n   _{ts}_")
    await msg.reply_text("\n".join(lines), parse_mode="Markdown")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    platforms = STATS.get("platforms", {})
    lines = [
        f"📊 Успішних: *{STATS.get('success', 0)}*",
        f"❌ Помилок: {STATS.get('errors', 0)}",
        f"📦 Відправлено: {human_bytes(STATS.get('bytes', 0))}",
        f"👥 Юзерів: {len(USERS)}",
        f"⚡ Кеш URL: {len(URL_CACHE)} записів",
        f"⏱ Аптайм: {uptime_text()}",
    ]
    if platforms:
        lines += ["", "📡 Платформи:"]
        for p, c in sorted(platforms.items(), key=lambda x: -x[1]):
            lines.append(f"  • {p}: {c}")
    if update.effective_message:
        await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


async def health_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    record_user(update)
    msg = update.effective_message
    if not msg:
        return
    ck    = cookies_file()
    tasks = len(ACTIVE_TASKS)
    files = len(list(DOWNLOAD_DIR.glob("*")))
    disk  = sum(p.stat().st_size for p in DOWNLOAD_DIR.glob("*") if p.is_file())
    mode  = "webhook" if WEBHOOK_URL else "polling"
    await msg.reply_text(
        f"🩺 *Health check*\n"
        f"Python: `{sys.version.split()[0]}`\n"
        f"yt-dlp: `{getattr(getattr(yt_dlp, 'version', None), '__version__', '?')}`\n"
        f"ffmpeg: {'✅ `' + str(FFMPEG_PATH) + '`' if FFMPEG_PATH else '❌ не знайдено'}\n"
        f"cookies.txt: {'✅' if ck else '❌ не знайдено'}\n"
        f"Режим: `{mode}`\n"
        f"downloads/: {files} файлів ({human_bytes(disk)})\n"
        f"Активних: {tasks}/{PARALLEL_DOWNLOADS}\n"
        f"Max upload: {human_bytes(MAX_UPLOAD_BYTES)}\n"
        f"Якість: {quality_for(chat_id(update))}\n"
        f"Аптайм: {uptime_text()}",
        parse_mode="Markdown",
    )


async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        t = time.monotonic()
        m = await update.effective_message.reply_text("🏓 Pong!")
        ms = int((time.monotonic() - t) * 1000)
        await m.edit_text(f"🏓 Pong! `{ms}ms`", parse_mode="Markdown")


async def clean_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        n = clean_old_files(True)
        cache_cleanup()
        await update.effective_message.reply_text(f"🧹 Видалено файлів: {n}")


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
    if not ACTIVE_TASKS:
        await msg.reply_text("Черга порожня.")
        return
    lines = [f"📋 Активних: {len(ACTIVE_TASKS)}", ""]
    for cid_key, task in ACTIVE_TASKS.items():
        elapsed = seconds_text(time.time() - task["started_at"])
        lines.append(
            f"• {task['platform']} | "
            f"{'🎵' if task['audio'] else '🎬'} | "
            f"{task['quality']} | ⏱{elapsed}"
        )
    await msg.reply_text("\n".join(lines))


async def cookies_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    path = cookies_file()
    if not path:
        await msg.reply_text(
            "❌ cookies.txt не знайдено або формат неправильний.\n"
            "Поклади поруч із bot.py. Перший рядок: `# Netscape HTTP Cookie File`",
            parse_mode="Markdown",
        )
        return
    await msg.reply_text(
        f"✅ cookies.txt знайдено\nШлях: `{path}`\nРозмір: {human_bytes(Path(path).stat().st_size)}",
        parse_mode="Markdown",
    )


async def platforms_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(
            "📡 *Підтримувані платформи:*\n" + "\n".join(f"  • {p}" for p in URL_PATTERNS),
            parse_mode="Markdown",
        )


async def reset_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    STATS.clear()
    STATS.update({"success": 0, "errors": 0, "bytes": 0, "platforms": {}})
    save_stats()
    if update.effective_message:
        await update.effective_message.reply_text("✅ Статистику очищено.")


async def update_ytdlp_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    status = await msg.reply_text("🔄 Оновлюю yt-dlp...")

    def job() -> None:
        env = os.environ.copy()
        env["PIP_NO_CACHE_DIR"] = "1"
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--user", "--no-cache-dir", "-U", "yt-dlp"],
            env=env,
        )

    try:
        await asyncio.get_running_loop().run_in_executor(None, job)
        importlib.invalidate_caches()
        ver = getattr(getattr(yt_dlp, "version", None), "__version__", "?")
        await safe_edit(status, f"✅ yt-dlp оновлено до `{ver}`.")
    except Exception as e:
        await safe_edit(status, f"❌ Не вдалося оновити yt-dlp:\n{safe_text(e, 600)}")


# ─────────────────────────── Callbacks ────────────────────────
async def quality_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    value = query.data.split(":")[1] if ":" in query.data else ""
    if value not in {"best", "fast", "mobile"}:
        return
    cid = int(query.message.chat.id)
    SETTINGS.setdefault("quality", {})[str(cid)] = value
    save_settings()
    await query.edit_message_text(f"✅ Якість: *{value}*", parse_mode="Markdown")


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    action = query.data.split(":")[1] if ":" in query.data else ""
    cid    = int(query.message.chat.id)
    if action == "quality":
        await query.edit_message_text(
            f"Поточна якість: *{quality_for(cid)}*\nОбери:",
            parse_mode="Markdown",
            reply_markup=quality_keyboard(),
        )
    elif action == "clearcache":
        URL_CACHE.clear()
        await query.edit_message_text("✅ Кеш URL очищено.", reply_markup=settings_keyboard(cid))
    elif action == "stats":
        lines = [
            f"📊 Успішних: {STATS.get('success', 0)}",
            f"❌ Помилок: {STATS.get('errors', 0)}",
            f"📦 Відправлено: {human_bytes(STATS.get('bytes', 0))}",
        ]
        await query.answer("\n".join(lines), show_alert=True)


# ─────────────────────────── Admin ────────────────────────────
def sync_owner_id(update: Update) -> None:
    global OWNER_ID
    user = update.effective_user
    if not user or not user.username:
        return
    uname = user.username.lower()
    uid = int(user.id)
    if uname != ADMIN_USERNAME:
        return
    if OWNER_ID is None:
        OWNER_ID = uid
        save_owner_id()
    if uid == OWNER_ID:
        ADMIN_IDS.add(uid)


def require_admin(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        sync_owner_id(update)
        uid = user_id(update)
        uname = (update.effective_user.username or "").lower() if update.effective_user else ""
        if uid not in ADMIN_IDS or uname != ADMIN_USERNAME or (OWNER_ID is not None and uid != OWNER_ID):
            if update.effective_message:
                await update.effective_message.reply_text("🚫 Тільки для адмінів.")
            return
        await func(update, context)
    wrapper.__name__ = func.__name__
    return wrapper


@require_admin
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    text = " ".join(context.args) if context.args else ""
    if not text:
        await msg.reply_text("❌ Використання: /broadcast <текст>")
        return
    status = await msg.reply_text(f"📡 Надсилаю {len(USERS)} юзерам...")
    ok, fail = 0, 0
    for uid_str in list(USERS.keys()):
        try:
            await context.bot.send_message(chat_id=int(uid_str), text=text)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await safe_edit(status, f"✅ Надіслано: {ok}\n❌ Помилок: {fail}")


@require_admin
async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not USERS:
        await msg.reply_text("Юзерів поки немає.")
        return
    lines = [f"👥 Юзерів: {len(USERS)}", ""]
    for uid_str, info in list(USERS.items())[:30]:
        name  = info.get("first_name") or info.get("username") or "—"
        uname = f"@{info['username']}" if info.get("username") else ""
        ban   = "🚫" if int(uid_str) in BANNED_USERS else ""
        lines.append(f"• `{uid_str}` {name} {uname} {ban}")
    if len(USERS) > 30:
        lines.append(f"… і ще {len(USERS) - 30}")
    await msg.reply_text("\n".join(lines), parse_mode="Markdown")


@require_admin
async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not context.args or not context.args[0].isdigit():
        await msg.reply_text("❌ Використання: /ban <user_id>")
        return
    uid = int(context.args[0])
    BANNED_USERS.add(uid)
    save_bans()
    await msg.reply_text(f"🚫 Юзер {uid} заблокований.")


@require_admin
async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not context.args or not context.args[0].isdigit():
        await msg.reply_text("❌ Використання: /unban <user_id>")
        return
    uid = int(context.args[0])
    BANNED_USERS.discard(uid)
    save_bans()
    await msg.reply_text(f"✅ Юзер {uid} розблокований.")




@require_admin
async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    now = datetime.utcnow()
    recent = 0
    for u in USERS.values():
        try:
            if now - datetime.fromisoformat(u.get("joined", "")) <= timedelta(hours=24):
                recent += 1
        except Exception:
            pass
    limit = user_limit()
    txt = (
        f"👑 Власник: {OWNER_HANDLE}\n"
        f"🆔 Owner ID: `{OWNER_ID or 'ще не зафіксовано'}`\n"
        f"👥 Користувачів: {len(USERS)}\n"
        f"🕒 Нових за 24г: {recent}\n"
        f"🚫 Заблокованих: {len(BANNED_USERS)}\n"
        f"🎚 Ліміт юзерів: {'off' if limit == 0 else limit}"
    )
    await msg.reply_text(txt, parse_mode="Markdown")


@require_admin
async def user_limit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not context.args:
        current = user_limit()
        await msg.reply_text(f"Поточний ліміт: {'off' if current == 0 else current}")
        return
    arg = context.args[0].lower()
    if arg == 'off':
        value = 0
    elif arg.isdigit():
        value = max(0, int(arg))
    else:
        await msg.reply_text("❌ Використання: /userlimit <n|off>")
        return
    SETTINGS.setdefault("limits", {})["max_users"] = value
    save_settings()
    await msg.reply_text(f"✅ Ліміт юзерів: {'off' if value == 0 else value}")


# ─────────────────────────── Error handler ────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Unhandled error:", exc_info=context.error)


# ─────────────────────────── Scheduled tasks ──────────────────
async def scheduled_cleanup(context: ContextTypes.DEFAULT_TYPE) -> None:
    n = clean_old_files(False)
    cache_cleanup()
    if n:
        log.info("Scheduled cleanup: removed %d files", n)


# ─────────────────────────── Main ─────────────────────────────
def main() -> None:
    clean_old_files(False)

    app = Application.builder().token(TOKEN).build()

    # Scheduled cleanup кожні 2 години
    app.job_queue.run_repeating(scheduled_cleanup, interval=7200, first=60)

    # Команди
    app.add_handler(CommandHandler("start",       start_command))
    app.add_handler(CommandHandler("help",        start_command))
    app.add_handler(CommandHandler("dl",          dl_command))
    app.add_handler(CommandHandler("video",       video_command))
    app.add_handler(CommandHandler("audio",       audio_command))
    app.add_handler(CommandHandler("thumb",       thumb_command))
    app.add_handler(CommandHandler("sub",         sub_command))
    app.add_handler(CommandHandler("clip",        clip_command))
    app.add_handler(CommandHandler("info",        info_command))
    app.add_handler(CommandHandler("formats",     formats_command))
    app.add_handler(CommandHandler("platforms",   platforms_command))
    app.add_handler(CommandHandler("quality",     quality_command))
    app.add_handler(CommandHandler("settings",    settings_command))
    app.add_handler(CommandHandler("history",     history_command))
    app.add_handler(CommandHandler("stats",       stats_command))
    app.add_handler(CommandHandler("resetstats",  reset_stats_command))
    app.add_handler(CommandHandler("health",      health_command))
    app.add_handler(CommandHandler("cookies",     cookies_command))
    app.add_handler(CommandHandler("clean",       clean_command))
    app.add_handler(CommandHandler("cancel",      cancel_command))
    app.add_handler(CommandHandler("queue",       queue_command))
    app.add_handler(CommandHandler("ping",        ping_command))
    app.add_handler(CommandHandler("updateytdlp", update_ytdlp_command))
    app.add_handler(CommandHandler("broadcast",   broadcast_command))
    app.add_handler(CommandHandler("ad",          broadcast_command))
    app.add_handler(CommandHandler("users",       users_command))
    app.add_handler(CommandHandler("ban",         ban_command))
    app.add_handler(CommandHandler("unban",       unban_command))
    app.add_handler(CommandHandler("adminstats",  admin_stats_command))
    app.add_handler(CommandHandler("userlimit",   user_limit_command))

    # Callbacks
    app.add_handler(CallbackQueryHandler(quality_callback,  pattern=r"^quality:"))
    app.add_handler(CallbackQueryHandler(settings_callback, pattern=r"^settings:"))

    # Повідомлення
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    log.info("ffmpeg=%s", FFMPEG_PATH or "не знайдено")
    log.info("cookies=%s", cookies_file() or "не знайдено")
    log.info("owner=%s admins=%s", OWNER_HANDLE, ADMIN_IDS or "ще не зафіксовано")
    log.info("webhook_url=%s", WEBHOOK_URL or "не задано (polling режим)")

    if WEBHOOK_URL:
        # ── Webhook режим (Render / production) ──────────────────
        port = int(os.environ.get("PORT", 8443))
        log.info("Запускаю в webhook режимі на порту %d", port)
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{TOKEN}",
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        # ── Polling режим (локальна розробка) ────────────────────
        log.info("Запускаю в polling режимі")
        # Прибираємо старий webhook якщо є
        requests.get(
            f"https://api.telegram.org/bot{TOKEN}/deleteWebhook",
            params={"drop_pending_updates": "true"},
            timeout=10,
        )
        app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
