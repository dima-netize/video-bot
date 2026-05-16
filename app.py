from __future__ import annotations

import importlib
import os
import site
import subprocess
import sys
from pathlib import Path

BASE_DIR_BOOT = Path(__file__).resolve().parent
PY_VER = f"python{sys.version_info.major}.{sys.version_info.minor}"
for p in (
    BASE_DIR_BOOT / ".local" / "lib" / PY_VER / "site-packages",
    Path(site.getusersitepackages()),
):
    if p.exists() and str(p) not in sys.path:
        sys.path.insert(0, str(p))
        site.addsitedir(str(p))

def module_ok(name: str) -> bool:
    try:
        importlib.import_module(name)
        return True
    except Exception:
        return False

def pip_install(package: str) -> None:
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

def ensure_deps() -> None:
    if os.environ.get("DISABLE_AUTO_INSTALL", "0") == "1":
        return
    deps = [
        ("requests", "requests>=2.31.0"),
        ("yt_dlp", "yt-dlp"),
        ("telegram.ext", "python-telegram-bot==20.7"),
    ]
    for mod, pkg in deps:
        if not module_ok(mod):
            pip_install(pkg)
    bad = [mod for mod, _ in deps if not module_ok(mod)]
    if bad:
        raise RuntimeError("Не вдалося встановити: " + ", ".join(bad))

ensure_deps()

# Imports below intentionally happen after dependency bootstrap (ensure_deps).
# ruff: noqa: E402

import asyncio
import glob
import json
import logging
import re
import shutil
import time
import traceback
from datetime import datetime
from functools import partial
from threading import Event
from typing import Any
from urllib.parse import urljoin

import requests
import yt_dlp
from telegram import Update
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано TOKEN у Startup / Environment")

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

STATS_FILE = BASE_DIR / "bot_stats.json"
SETTINGS_FILE = BASE_DIR / "bot_settings.json"
SUBSCRIBERS_FILE = BASE_DIR / "bot_subscribers.json"

MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(49 * 1024 * 1024)))
PROGRESS_THROTTLE = float(os.environ.get("PROGRESS_THROTTLE", "1.3"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
OLD_FILE_TTL = int(os.environ.get("OLD_FILE_TTL", str(60 * 60 * 3)))
MAX_LINKS_PER_MESSAGE = int(os.environ.get("MAX_LINKS_PER_MESSAGE", "3"))
PARALLEL_DOWNLOADS = max(1, int(os.environ.get("PARALLEL_DOWNLOADS", "1")))
PARALLEL_LIMIT = asyncio.Semaphore(PARALLEL_DOWNLOADS)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("video-bot")

FFMPEG_PATH = os.environ.get("FFMPEG_PATH") if os.environ.get("FFMPEG_PATH") else shutil.which("ffmpeg")

URL_RE = re.compile(r"https?://[^\s<>\"]+", re.I)
DIRECT_VIDEO_RE = re.compile(r"https?://[^\s<>\"]+\.(?:mp4|mov|webm|m4v)(?:\?[^\s<>\"]*)?", re.I)

URL_PATTERNS: dict[str, re.Pattern[str]] = {
    "youtube": re.compile(r"(?:youtube\.com/(?:watch\?v=|shorts/|live/)|youtu\.be/|m\.youtube\.com/watch\?v=)", re.I),
    "tiktok": re.compile(r"(?:tiktok\.com/@[\w.-]+/video/\d+|tiktok\.com/t/|vt\.tiktok\.com/|vm\.tiktok\.com/|www\.tiktok\.com/)", re.I),
    "instagram": re.compile(r"instagram\.com/(?:reel|reels|p|tv|stories)/", re.I),
    "twitter": re.compile(r"(?:twitter\.com|x\.com)/\w+/status/\d+", re.I),
    "vimeo": re.compile(r"vimeo\.com/(?:\d+|channels/[^/]+/\d+)", re.I),
    "reddit": re.compile(r"reddit\.com/r/\w+/comments/", re.I),
    "facebook": re.compile(r"facebook\.com/(?:watch/\?v=|watch\?v=|reel/|share/r/|[\w.]+/videos/)", re.I),
    "likee": re.compile(r"likee\.video/|likee\.com/", re.I),
    "snapchat": re.compile(r"snapchat\.com/(?:spotlight|add)/", re.I),
    "pinterest": re.compile(r"pinterest\.[a-z.]+/pin/\d+", re.I),
}

HELP_TEXT = """🎥 Fast Video Downloader Bot

Кинь посилання — бот завантажить відео і покаже прогрес.

/video <url> — скачати відео
/audio <url> — скачати аудіо
/audio у відповідь на посилання — аудіо
/info <url> — інформація про відео
/formats <url> — доступні формати
/quality best — найкраща якість
/quality fast — швидше, до 720p
/quality mobile — легший файл, до 480p
/stats — статистика
/health — стан бота
/cookies — перевірити cookies.txt
/queue — активне завантаження
/cancel — скасувати активне завантаження
/clean — очистити старі файли
/settings — поточні налаштування
/howto — як користуватися ботом
/announce <текст> — розсилка (тільки адмін)
/subscribers — скільки чатів підписано (адмін)
/shutdown — зупинити бота (тільки адмін)
/platforms — платформи
/updateytdlp — оновити yt-dlp
/resetstats — очистити статистику

YouTube на free-серверах часто просить cookies.txt. Це захист YouTube, а не помилка коду.
"""

CANCEL_EVENTS: dict[int, Event] = {}
ACTIVE_TASKS: dict[int, dict[str, Any]] = {}
BOT_STARTED_AT = time.time()
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "dimagymenjuk").lower().lstrip("@")

class DownloadCancelled(Exception):
    pass

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

STATS = read_json(STATS_FILE, {"success": 0, "errors": 0, "bytes": 0, "platforms": {}})
SETTINGS = read_json(SETTINGS_FILE, {"quality": {}})
SUBSCRIBERS = set(int(x) for x in read_json(SUBSCRIBERS_FILE, []))

def save_stats() -> None:
    write_json(STATS_FILE, STATS)

def save_settings() -> None:
    write_json(SETTINGS_FILE, SETTINGS)

def save_subscribers() -> None:
    write_json(SUBSCRIBERS_FILE, sorted(SUBSCRIBERS))

def is_admin(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    return (user.username or "").lower() == ADMIN_USERNAME

def register_chat(update: Update) -> None:
    cid = chat_id(update)
    if not cid:
        return
    if cid not in SUBSCRIBERS:
        SUBSCRIBERS.add(cid)
        save_subscribers()

def stats_ok(platform: str, size: int) -> None:
    STATS["success"] = int(STATS.get("success", 0)) + 1
    STATS["bytes"] = int(STATS.get("bytes", 0)) + int(size or 0)
    STATS.setdefault("platforms", {})
    STATS["platforms"][platform] = int(STATS["platforms"].get(platform, 0)) + 1
    save_stats()

def stats_fail() -> None:
    STATS["errors"] = int(STATS.get("errors", 0)) + 1
    save_stats()

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

def safe_text(value: Any, limit: int = 220) -> str:
    return (re.sub(r"\s+", " ", str(value or "video")).strip() or "video")[:limit]

def chat_id(update: Update) -> int:
    return int(update.effective_chat.id) if update.effective_chat else 0

def quality_for(cid: int) -> str:
    return SETTINGS.get("quality", {}).get(str(cid), "fast")

def cookies_file() -> str | None:
    for p in [Path("/etc/secrets/cookies.txt"), BASE_DIR / "cookies.txt", BASE_DIR / "cookies" / "cookies.txt"]:
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
    return out[:MAX_LINKS_PER_MESSAGE]

def normalize_url(raw: str | None) -> str | None:
    if not raw:
        return None
    value = raw.strip().strip("<>()[]{}.,;\"' ")
    if not value:
        return None
    if not re.match(r"^https?://", value, re.I):
        return None
    return value

def detect_platform(url: str) -> str | None:
    for name, pat in URL_PATTERNS.items():
        if pat.search(url):
            return name
    return None

def safe_filename(prefix: str, url: str, ext: str = "mp4") -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
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

def progress_text(prefix: str, done: int, total: int | None, start: float) -> str:
    elapsed = max(time.monotonic() - start, 0.1)
    speed = done / elapsed if done else 0
    if total:
        pct = max(0, min(100, int(done * 100 / total)))
        eta = int((total - done) / speed) if speed else 0
        return f"{prefix} {pct}%\n{human_bytes(done)} / {human_bytes(total)}\nШвидкість: {human_bytes(speed)}/s\nETA: {seconds_text(eta)}"
    return f"{prefix}\nЗавантажено: {human_bytes(done)}\nШвидкість: {human_bytes(speed)}/s"

def friendly_error(platform: str | None, error: str) -> str:
    err, low = str(error or ""), str(error or "").lower()
    if "exit code 137" in low or "killed" in low:
        return "Серверу не вистачило ресурсів. Постав /quality mobile і спробуй ще раз."
    if platform == "youtube" and any(x in low for x in ["sign in to confirm", "not a bot", "use --cookies", "cookies"]):
        return ("YouTube просить cookies.txt.\n\n"
                "Поклади правильний cookies.txt поруч із app.py і перезапусти сервер.\n"
                "Перший рядок має бути:\n# Netscape HTTP Cookie File\n\n"
                "Без cookies YouTube часто не працює на free-серверах.")
    if "requested format is not available" in low:
        return "Ця якість недоступна. Спробуй /quality fast або /quality mobile."
    if "ffmpeg" in low and not FFMPEG_PATH:
        return "На сервері немає ffmpeg. Постав /quality fast або /quality mobile."
    if "unsupported url" in low:
        return "Посилання не підтримується або платформа змінила захист."
    if "private" in low or "login" in low:
        return "Відео приватне або потрібен вхід в акаунт. Потрібен правильний cookies.txt."
    return safe_text(err, 900)

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

def ytdlp_opts(platform: str | None, audio: bool, quality: str, hook=None) -> dict[str, Any]:
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    if platform == "tiktok":
        ua = "com.zhiliaoapp.musically/2022600030 (Linux; U; Android 12; en_US; Pixel 6; Build/SP1A.210812.016)"
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
        "concurrent_fragment_downloads": 2,
        "http_chunk_size": 6 * 1024 * 1024,
        "http_headers": {"User-Agent": ua},
        "progress_hooks": [hook] if hook else [],
    }
    if FFMPEG_PATH:
        opts["ffmpeg_location"] = FFMPEG_PATH
        if not audio:
            opts["merge_output_format"] = "mp4"
        if audio:
            opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}]
    ck = cookies_file()
    if ck:
        opts["cookiefile"] = ck
    if platform == "youtube":
        opts["extractor_args"] = {"youtube": {"player_client": ["android", "web"]}}
    if platform == "tiktok":
        opts["extractor_args"] = {"tiktok": {"app_version": "26.2.0", "manifest_app_version": "26.2.0"}}
    return opts

def stream_download(url: str, filepath: Path, title: str, progress_cb=None, cancel_event: Event | None = None, headers: dict[str, str] | None = None):
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

def tiktok_fallback(url: str, progress_cb=None, cancel_event: Event | None = None):
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://tikwm.com/"}
    try:
        r = requests.get("https://tikwm.com/api/", params={"url": url, "hd": "1"}, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            return None, data.get("msg") or "tikwm.com не зміг отримати відео."
        item = data.get("data") or {}
        video_url = item.get("hdplay") or item.get("play") or item.get("wmplay")
        if not video_url:
            return None, "tikwm.com не повернув пряме посилання."
        video_url = urljoin("https://tikwm.com", video_url)
        return stream_download(video_url, safe_filename("tiktok", url), safe_text(item.get("title") or "TikTok video"), progress_cb, cancel_event, headers)
    except Exception as e:
        return None, f"tikwm.com: {e}"

def instagram_fallback(url: str, progress_cb=None, cancel_event: Event | None = None):
    fixed = url.replace("www.instagram.com", "www.ddinstagram.com").replace("instagram.com", "ddinstagram.com")
    try:
        r = requests.get(fixed, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        video_url = None
        for pat in [r'<video[^>]+src="([^"]+)"', r'property="og:video"\s+content="([^"]+)"', r'property="og:video:secure_url"\s+content="([^"]+)"', r'"video_url":"([^"]+)"']:
            m = re.search(pat, r.text)
            if m:
                video_url = m.group(1).replace("\\u0026", "&").replace("\\/", "/")
                break
        if not video_url:
            return None, "Instagram fallback не знайшов відео. Можливо, потрібні cookies.txt."
        return stream_download(video_url, safe_filename("instagram", url), "Instagram video", progress_cb, cancel_event, {"User-Agent": "Mozilla/5.0"})
    except Exception as e:
        return None, f"Instagram fallback: {e}"

def download_direct(url: str, progress_cb=None, cancel_event: Event | None = None):
    ext = url.split("?")[0].split(".")[-1].lower()
    if ext not in {"mp4", "mov", "webm", "m4v"}:
        ext = "mp4"
    return stream_download(url, safe_filename("direct", url, ext), "Пряме відео", progress_cb, cancel_event)

def download_via_ytdlp(url: str, platform: str | None, audio: bool, quality: str, progress_cb=None, cancel_event: Event | None = None):
    start = time.monotonic()
    def hook(d: dict[str, Any]) -> None:
        if cancel_event and cancel_event.is_set():
            raise DownloadCancelled("Завантаження скасовано.")
        if not progress_cb:
            return
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done = d.get("downloaded_bytes") or 0
            progress_cb(progress_text("⏳ Завантажую", int(done or 0), int(total or 0), start))
        elif d.get("status") == "finished":
            progress_cb("🔧 Обробляю файл...")
    try:
        with yt_dlp.YoutubeDL(ytdlp_opts(platform, audio, quality, hook)) as ydl:
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

def download_media(url: str, platform: str | None, audio: bool, quality: str, progress_cb=None, cancel_event: Event | None = None):
    if DIRECT_VIDEO_RE.search(url) and not audio:
        return download_direct(url, progress_cb, cancel_event)
    if platform == "tiktok" and not audio:
        if progress_cb:
            progress_cb("🔁 Пробую TikTok no-watermark...")
        p, r = tiktok_fallback(url, progress_cb, cancel_event)
        if p:
            return p, r
        if progress_cb:
            progress_cb("🔁 TikTok fallback не зміг. Пробую yt-dlp...")
    p, r = download_via_ytdlp(url, platform, audio, quality, progress_cb, cancel_event)
    if p:
        return p, r
    if not audio and platform == "instagram":
        if progress_cb:
            progress_cb("🔁 Пробую Instagram fallback...")
        p2, r2 = instagram_fallback(url, progress_cb, cancel_event)
        if p2:
            return p2, r2
        return None, f"{r}\n\nInstagram fallback: {r2}"
    return None, r

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
        lines.append(f"Перегляди: {info.get('view_count'):,}".replace(",", " "))
    if info.get("like_count") is not None:
        lines.append(f"Лайки: {info.get('like_count'):,}".replace(",", " "))
    lines += ["", f"URL: {info.get('webpage_url') or url}"]
    return "\n".join(lines)[:3900]

def extract_formats_text(url: str, platform: str | None, quality: str) -> str:
    opts = ytdlp_opts(platform, False, quality)
    opts["skip_download"] = True
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = first_entry(ydl.extract_info(url, download=False))
    lines = [f"🎞 Формати для: {safe_text(info.get('title'), 120)}", ""]
    count = 0
    for fmt in info.get("formats") or []:
        if count >= 35:
            break
        h, fps = fmt.get("height"), fmt.get("fps")
        size = fmt.get("filesize") or fmt.get("filesize_approx")
        label = f"{h}p" if h else (fmt.get("format_note") or fmt.get("resolution") or "audio")
        if fps:
            label += f"/{int(fps)}fps"
        media = "audio" if fmt.get("vcodec") == "none" else ("video-only" if fmt.get("acodec") == "none" else "video")
        lines.append(f"• {fmt.get('format_id', '?')}: {label} ({fmt.get('ext', '?')}, {media}){(' ~' + human_bytes(size)) if size else ''}")
        count += 1
    if count == 0:
        lines.append("Формати не знайдено.")
    return "\n".join(lines)[:3900]

async def safe_edit(message, text: str) -> None:
    try:
        await message.edit_text(text[:3900])
    except RetryAfter as e:
        await asyncio.sleep(float(e.retry_after) + 0.2)
    except BadRequest as e:
        if "message is not modified" not in str(e).lower():
            log.debug("edit error: %s", e)
    except TelegramError as e:
        log.debug("telegram error: %s", e)

async def send_media(update: Update, filepath: str, title: str, is_audio: bool = False, progress_cb=None) -> int:
    msg = update.effective_message
    if not msg:
        remove_file(filepath)
        return 0
    try:
        size = Path(filepath).stat().st_size
        if size > MAX_UPLOAD_BYTES:
            await msg.reply_text("❌ Файл більший за ліміт Telegram Bot API.\nПостав /quality mobile або /quality fast і спробуй ще раз.")
            return 0
        if progress_cb:
            progress_cb("📤 Надсилаю файл у Telegram...")
        with open(filepath, "rb") as f:
            if is_audio and Path(filepath).suffix.lower() == ".mp3":
                await msg.reply_audio(audio=f, title=title[:64], caption=f"🎵 {title[:180]}", read_timeout=180, write_timeout=180, connect_timeout=60, pool_timeout=60)
            elif is_audio:
                await msg.reply_document(document=f, caption=f"🎵 {title[:180]}\nФайл відправлено як документ, бо на сервері немає ffmpeg для MP3.", read_timeout=180, write_timeout=180, connect_timeout=60, pool_timeout=60)
            else:
                await msg.reply_video(video=f, caption=f"✅ {title[:200]}", supports_streaming=True, read_timeout=180, write_timeout=180, connect_timeout=60, pool_timeout=60)
        return size
    except Exception:
        log.exception("send error")
        await msg.reply_text("❌ Не вдалося надіслати файл у Telegram.")
        return 0
    finally:
        remove_file(filepath)

async def download_and_send(update: Update, url: str, platform: str, audio: bool = False) -> None:
    msg = update.effective_message
    if not msg:
        return
    cid, quality = chat_id(update), quality_for(chat_id(update))
    cancel_event = Event()
    CANCEL_EVENTS[cid] = cancel_event
    ACTIVE_TASKS[cid] = {"url": url, "platform": platform, "audio": audio, "quality": quality, "started_at": time.time()}
    async with PARALLEL_LIMIT:
        status = await msg.reply_text("🎵 Готую аудіо..." if audio else "⏳ Починаю завантаження...")
        loop = asyncio.get_running_loop()
        last_time, last_text = [0.0], [""]
        def progress_cb(text: str) -> None:
            now = time.monotonic()
            important = text.startswith(("🔧", "📤", "✅", "🔁", "❌"))
            if text == last_text[0] or (now - last_time[0] < PROGRESS_THROTTLE and not important):
                return
            last_time[0], last_text[0] = now, text
            asyncio.run_coroutine_threadsafe(safe_edit(status, text), loop)
        try:
            job = partial(download_media, url, platform, audio, quality, progress_cb, cancel_event)
            path, title = await loop.run_in_executor(None, job)
            if not path:
                stats_fail()
                await safe_edit(status, f"❌ {title}")
                return
            await safe_edit(status, "✅ Завантажено. Готую відправку...")
            sent = await send_media(update, path, title, audio, progress_cb)
            if sent:
                stats_ok(platform, sent)
            try:
                await status.delete()
            except Exception:
                pass
        finally:
            CANCEL_EVENTS.pop(cid, None)
            ACTIVE_TASKS.pop(cid, None)
            clean_old_files(False)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or not msg.text:
        return
    register_chat(update)
    urls = extract_urls(msg.text)
    if not urls:
        await msg.reply_text("❌ Надішли посилання на відео.")
        return
    if len(urls) > 1:
        await msg.reply_text(f"Знайшов {len(urls)} посилання. Оброблю по черзі.")
    for url in urls:
        platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)
        if not platform:
            await msg.reply_text(f"❌ Платформа не підтримується:\n{url}")
            continue
        await download_and_send(update, url, platform, False)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        register_chat(update)
        await update.effective_message.reply_text(HELP_TEXT)

async def video_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    register_chat(update)
    url = normalize_url(context.args[0]) if context.args else None
    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = normalize_url(found[0]) if found else None
    if not url:
        await msg.reply_text("❌ Використання: /video <посилання>")
        return
    platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return
    await download_and_send(update, url, platform, False)

async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    register_chat(update)
    url = normalize_url(context.args[0]) if context.args else None
    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = normalize_url(found[0]) if found else None
    if not url:
        await msg.reply_text("❌ Використання: /audio <посилання> або /audio у відповідь на посилання.")
        return
    platform = detect_platform(url)
    if not platform:
        await msg.reply_text("❌ Платформа не підтримується для аудіо.")
        return
    await download_and_send(update, url, platform, True)

async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await info_or_formats(update, context, "info")

async def formats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await info_or_formats(update, context, "formats")

async def info_or_formats(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str) -> None:
    msg = update.effective_message
    if not msg:
        return
    url = normalize_url(context.args[0]) if context.args else None
    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = normalize_url(found[0]) if found else None
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
        result = await asyncio.get_running_loop().run_in_executor(None, partial(fn, url, platform, quality_for(chat_id(update))))
        await safe_edit(status, result)
    except Exception as e:
        await safe_edit(status, f"❌ {friendly_error(platform, str(e))}")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    platforms = STATS.get("platforms", {})
    lines = [
        f"📊 Успішних завантажень: {STATS.get('success', 0)}",
        f"❌ Помилок: {STATS.get('errors', 0)}",
        f"📦 Всього відправлено: {human_bytes(STATS.get('bytes', 0))}",
    ]
    if platforms:
        lines += ["", "За платформами:"]
        lines += [f"• {p}: {c}" for p, c in sorted(platforms.items())]
    if update.effective_message:
        await update.effective_message.reply_text("\n".join(lines))

async def quality_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    cid = chat_id(update)
    if not context.args:
        await msg.reply_text(f"Поточна якість: {quality_for(cid)}\n/quality best\n/quality fast\n/quality mobile")
        return
    value = context.args[0].lower().strip()
    if value not in {"best", "fast", "mobile"}:
        await msg.reply_text("❌ Доступно тільки: best, fast, mobile")
        return
    SETTINGS.setdefault("quality", {})[str(cid)] = value
    save_settings()
    await msg.reply_text(f"✅ Якість змінено на: {value}")

async def clean_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(f"🧹 Видалено файлів: {clean_old_files(True)}")

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    event = CANCEL_EVENTS.get(chat_id(update))
    if not event:
        await msg.reply_text("Немає активного завантаження для скасування.")
        return
    event.set()
    await msg.reply_text("🛑 Скасовую завантаження...")

async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    task = ACTIVE_TASKS.get(chat_id(update))
    if not task:
        await msg.reply_text("Активного завантаження в цьому чаті немає.")
        return
    await msg.reply_text(
        "⏳ Активне завантаження:\n"
        f"Платформа: {task['platform']}\nТип: {'audio' if task['audio'] else 'video'}\n"
        f"Якість: {task['quality']}\nЧас: {seconds_text(time.time() - task['started_at'])}\nURL: {task['url'][:300]}"
    )

async def cookies_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    path = cookies_file()
    if not path:
        await msg.reply_text("❌ cookies.txt не знайдено або формат неправильний.\nПоклади cookies.txt поруч із app.py.\nПерший рядок:\n# Netscape HTTP Cookie File")
        return
    await msg.reply_text(f"✅ cookies.txt знайдено\nШлях: {path}\nРозмір: {human_bytes(Path(path).stat().st_size)}")

async def health_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    ck = cookies_file()
    await msg.reply_text(
        "🩺 Health check\n"
        f"Python: {sys.version.split()[0]}\n"
        f"yt-dlp: {getattr(getattr(yt_dlp, 'version', None), '__version__', '?')}\n"
        f"ffmpeg: {'✅ ' + FFMPEG_PATH if FFMPEG_PATH else '❌ не знайдено'}\n"
        f"cookies.txt: {'✅ ' + ck if ck else '❌ не знайдено'}\n"
        f"downloads/: {len(list(DOWNLOAD_DIR.glob('*')))} файлів\n"
        f"parallel: {PARALLEL_DOWNLOADS}\n"
        f"max upload: {human_bytes(MAX_UPLOAD_BYTES)}\n"
        f"quality: {quality_for(chat_id(update))}\n"
        f"uptime: {seconds_text(time.time() - BOT_STARTED_AT)}"
    )

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    register_chat(update)
    await msg.reply_text(
        "⚙️ Поточні налаштування:\n"
        f"Якість: {quality_for(chat_id(update))}\n"
        f"Паралельних завантажень: {PARALLEL_DOWNLOADS}\n"
        f"Ліміт файлу: {human_bytes(MAX_UPLOAD_BYTES)}\n"
        f"Throttle прогресу: {PROGRESS_THROTTLE:.1f}с\n"
        f"TTL старих файлів: {seconds_text(OLD_FILE_TTL)}"
    )

async def howto_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    register_chat(update)
    await msg.reply_text(
        "🧭 Як користуватись:\n"
        "1) Надішли посилання — бот сам спробує скачати відео.\n"
        "2) Для аудіо: /audio <посилання>.\n"
        "3) Якщо файл завеликий: /quality fast або /quality mobile.\n"
        "4) Якщо YouTube не качається — перевір /cookies.\n"
        "5) Для діагностики: /health, /settings, /queue, /cancel."
    )

async def platforms_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text("Підтримую:\n" + "\n".join(f"• {p}" for p in URL_PATTERNS))

async def reset_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    STATS.clear()
    STATS.update({"success": 0, "errors": 0, "bytes": 0, "platforms": {}})
    save_stats()
    if update.effective_message:
        await update.effective_message.reply_text("✅ Статистику очищено.")

async def announce_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    register_chat(update)
    if not is_admin(update):
        await msg.reply_text("⛔ Ця команда тільки для власника бота.")
        return
    text = " ".join(context.args).strip() if context.args else ""
    if not text and msg.reply_to_message and msg.reply_to_message.text:
        text = msg.reply_to_message.text.strip()
    if not text:
        await msg.reply_text("❌ Використання: /announce <текст> або у відповідь на повідомлення.")
        return
    sent = 0
    failed = 0
    for cid in sorted(SUBSCRIBERS):
        try:
            await context.bot.send_message(chat_id=cid, text=f"📢 Оголошення від @dimagymenjuk\n\n{text[:3800]}")
            sent += 1
        except Exception:
            failed += 1
    await msg.reply_text(f"✅ Розіслано: {sent}\n❌ Помилок: {failed}\n👥 Всього підписників: {len(SUBSCRIBERS)}")

async def subscribers_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not is_admin(update):
        await msg.reply_text("⛔ Ця команда тільки для власника бота.")
        return
    await msg.reply_text(f"👥 Підписано чатів: {len(SUBSCRIBERS)}")

async def shutdown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not is_admin(update):
        await msg.reply_text("⛔ Ця команда тільки для власника бота.")
        return
    await msg.reply_text("🛑 Зупиняю бота...")
    context.application.stop_running()

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text("pong ✅")

async def update_ytdlp_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    status = await msg.reply_text("🔄 Оновлюю yt-dlp...")
    def job() -> None:
        env = os.environ.copy()
        env["PIP_NO_CACHE_DIR"] = "1"
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "--no-cache-dir", "-U", "yt-dlp"], env=env)
    try:
        await asyncio.get_running_loop().run_in_executor(None, job)
        await safe_edit(status, "✅ yt-dlp оновлено. Натисни Stop → Start.")
    except Exception as e:
        await safe_edit(status, f"❌ Не вдалося оновити yt-dlp:\n{safe_text(e, 600)}")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Unhandled error:", exc_info=context.error)

def delete_webhook() -> None:
    try:
        requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook", params={"drop_pending_updates": "true"}, timeout=10)
    except Exception:
        pass

def main() -> None:
    clean_old_files(False)
    delete_webhook()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", start_command))
    app.add_handler(CommandHandler("video", video_command))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("formats", formats_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("resetstats", reset_stats_command))
    app.add_handler(CommandHandler("platforms", platforms_command))
    app.add_handler(CommandHandler("quality", quality_command))
    app.add_handler(CommandHandler("clean", clean_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("queue", queue_command))
    app.add_handler(CommandHandler("cookies", cookies_command))
    app.add_handler(CommandHandler("health", health_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("howto", howto_command))
    app.add_handler(CommandHandler("announce", announce_command))
    app.add_handler(CommandHandler("subscribers", subscribers_command))
    app.add_handler(CommandHandler("shutdown", shutdown_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CommandHandler("updateytdlp", update_ytdlp_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)
    log.info("Бот запущено. ffmpeg=%s", FFMPEG_PATH or "не знайдено")
    log.info("cookies=%s", cookies_file() or "не знайдено")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
