"""
Video Downloader Bot Pro
YouTube, TikTok, Instagram, Twitter/X, Vimeo, Reddit, Facebook, Likee, Snapchat, Pinterest.

ENV:
TOKEN=your_telegram_bot_token
Optional:
YOUTUBE_COOKIES_B64=base64_of_cookies_txt
MAX_UPLOAD_BYTES=51380224
PARALLEL_DOWNLOADS=2
"""

from __future__ import annotations

import asyncio
import base64
import glob
import json
import logging
import os
import re
import shutil
import subprocess
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from threading import Event
from typing import Any
from urllib.parse import urljoin

import requests
import yt_dlp
from telegram import Update
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

try:
    import imageio_ffmpeg
except Exception:
    imageio_ffmpeg = None


# ─────────────────────────────────────────────────────────────
# НАЛАШТУВАННЯ
# ─────────────────────────────────────────────────────────────

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано змінну оточення TOKEN")

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

STATS_FILE = BASE_DIR / "bot_stats.json"
SETTINGS_FILE = BASE_DIR / "bot_settings.json"

MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(49 * 1024 * 1024)))
PROGRESS_THROTTLE = float(os.environ.get("PROGRESS_THROTTLE", "1.3"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
OLD_FILE_TTL = int(os.environ.get("OLD_FILE_TTL", str(60 * 60 * 3)))
MAX_LINKS_PER_MESSAGE = int(os.environ.get("MAX_LINKS_PER_MESSAGE", "3"))
PARALLEL_DOWNLOADS = int(os.environ.get("PARALLEL_DOWNLOADS", "2"))

PARALLEL_LIMIT = asyncio.Semaphore(PARALLEL_DOWNLOADS)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("video-bot-pro")


# ─────────────────────────────────────────────────────────────
# FFMPEG
# ─────────────────────────────────────────────────────────────

def find_ffmpeg() -> str | None:
    env_path = os.environ.get("FFMPEG_PATH")

    if env_path and Path(env_path).exists():
        return env_path

    if imageio_ffmpeg:
        try:
            path = imageio_ffmpeg.get_ffmpeg_exe()

            if path and Path(path).exists():
                return path

        except Exception:
            pass

    return shutil.which("ffmpeg")


FFMPEG_PATH = find_ffmpeg()


# ─────────────────────────────────────────────────────────────
# URL-ПАТЕРНИ
# ─────────────────────────────────────────────────────────────

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
    "likee": re.compile(
        r"likee\.video/|likee\.com/",
        re.I,
    ),
    "snapchat": re.compile(
        r"snapchat\.com/(?:spotlight|add)/",
        re.I,
    ),
    "pinterest": re.compile(
        r"pinterest\.[a-z.]+/pin/\d+",
        re.I,
    ),
}


HELP_TEXT = """
🎥 Video Downloader Bot Pro

Кинь посилання — бот завантажить відео і покаже прогрес.

Команди:
/video <url> — скачати відео
/audio <url> — скачати MP3
/audio у відповідь на повідомлення з посиланням — MP3
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
/platforms — платформи
/resetstats — очистити статистику

Підтримка:
YouTube, TikTok, Instagram, Twitter/X, Vimeo, Reddit, Facebook, Likee, Snapchat, Pinterest, прямі MP4/MOV/WEBM.

Для YouTube на сервері часто потрібен cookies.txt у форматі Netscape.
""".strip()


# ─────────────────────────────────────────────────────────────
# JSON STORAGE
# ─────────────────────────────────────────────────────────────

def read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default

        return json.loads(path.read_text(encoding="utf-8"))

    except Exception:
        logger.exception("Не вдалося прочитати JSON: %s", path)
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
        logger.exception("Не вдалося записати JSON: %s", path)


# ─────────────────────────────────────────────────────────────
# СТАТИСТИКА І СТАН
# ─────────────────────────────────────────────────────────────

@dataclass
class Stats:
    success: int = 0
    errors: int = 0
    downloaded_bytes: int = 0
    platforms: dict[str, int] = field(default_factory=dict)

    @classmethod
    def load(cls) -> "Stats":
        data = read_json(STATS_FILE, {})

        if not isinstance(data, dict):
            return cls()

        return cls(
            success=int(data.get("success", 0)),
            errors=int(data.get("errors", 0)),
            downloaded_bytes=int(data.get("downloaded_bytes", 0)),
            platforms=dict(data.get("platforms", {})),
        )

    def save(self) -> None:
        write_json(
            STATS_FILE,
            {
                "success": self.success,
                "errors": self.errors,
                "downloaded_bytes": self.downloaded_bytes,
                "platforms": self.platforms,
            },
        )

    def ok(self, platform: str, size: int = 0) -> None:
        self.success += 1
        self.downloaded_bytes += max(0, int(size or 0))
        self.platforms[platform] = self.platforms.get(platform, 0) + 1
        self.save()

    def fail(self) -> None:
        self.errors += 1
        self.save()

    def text(self) -> str:
        lines = [
            f"📊 Успішних завантажень: {self.success}",
            f"❌ Помилок: {self.errors}",
            f"📦 Всього відправлено: {human_bytes(self.downloaded_bytes)}",
        ]

        if self.platforms:
            lines.append("")
            lines.append("За платформами:")

            for platform, count in sorted(self.platforms.items()):
                lines.append(f"• {platform}: {count}")

        return "\n".join(lines)


stats = Stats.load()

settings_data = read_json(SETTINGS_FILE, {})

USER_QUALITY: dict[int, str] = {
    int(k): v
    for k, v in settings_data.get("quality", {}).items()
    if str(v) in {"best", "fast", "mobile"}
}

CANCEL_EVENTS: dict[int, Event] = {}
ACTIVE_TASKS: dict[int, dict[str, Any]] = {}


class DownloadCancelled(Exception):
    pass


# ─────────────────────────────────────────────────────────────
# ДОПОМІЖНІ ФУНКЦІЇ
# ─────────────────────────────────────────────────────────────

def save_settings() -> None:
    write_json(
        SETTINGS_FILE,
        {
            "quality": {
                str(k): v
                for k, v in USER_QUALITY.items()
            }
        },
    )


def ensure_cookies_from_env() -> None:
    """
    Створює cookies.txt із Base64-змінної, якщо така є.

    Підтримує:
    YOUTUBE_COOKIES_B64
    COOKIES_TXT_B64
    """
    env_value = os.environ.get("YOUTUBE_COOKIES_B64") or os.environ.get("COOKIES_TXT_B64")

    if not env_value:
        return

    target = BASE_DIR / "cookies.txt"

    if target.exists() and target.stat().st_size > 50:
        return

    try:
        raw = base64.b64decode(env_value.strip())
        target.write_bytes(raw)
        logger.info("cookies.txt створено зі змінної середовища")

    except Exception:
        logger.exception("Не вдалося створити cookies.txt із Base64")


def cookies_file() -> str | None:
    """
    Шукає cookies.txt у різних місцях:
    1. /etc/secrets/cookies.txt — Render Secret Files
    2. поруч із app.py — fps.ms / Pterodactyl
    3. ./cookies/cookies.txt
    """
    ensure_cookies_from_env()

    possible_paths = [
        Path("/etc/secrets/cookies.txt"),
        BASE_DIR / "cookies.txt",
        BASE_DIR / "cookies" / "cookies.txt",
    ]

    for path in possible_paths:
        if not path.exists():
            continue

        try:
            lines = path.read_text(
                encoding="utf-8",
                errors="ignore",
            ).splitlines()

            first_line = lines[0].strip() if lines else ""

            if first_line in {
                "# Netscape HTTP Cookie File",
                "# HTTP Cookie File",
            }:
                return str(path)

            logger.warning(
                "cookies.txt знайдено, але формат неправильний: %s",
                first_line,
            )

        except Exception:
            logger.exception("Не вдалося прочитати cookies.txt: %s", path)

    return None


def chat_id(update: Update) -> int:
    return int(update.effective_chat.id) if update.effective_chat else 0


def safe_text(value: Any, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "video")).strip()
    return text[:limit] or "video"


def safe_filename(prefix: str, url: str, ext: str = "mp4") -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug = url.split("?")[0].rstrip("/").split("/")[-1] or "video"
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", slug)[:35] or "video"
    return DOWNLOAD_DIR / f"{prefix}_{slug}_{ts}.{ext}"


def extract_urls(text: str) -> list[str]:
    urls: list[str] = []

    for url in URL_RE.findall(text or ""):
        url = url.strip().strip(".,;)\n\r\t ")

        if url and url not in urls:
            urls.append(url)

    return urls[:MAX_LINKS_PER_MESSAGE]


def detect_platform(url: str) -> str | None:
    for name, pattern in URL_PATTERNS.items():
        if pattern.search(url):
            return name

    return None


def human_bytes(num: int | float | None) -> str:
    if not num:
        return "0 B"

    num = float(num)

    for unit in ["B", "KB", "MB", "GB"]:
        if num < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(num)} B"

            return f"{num:.1f} {unit}"

        num /= 1024

    return f"{num:.1f} GB"


def seconds_text(seconds: int | float | None) -> str:
    if not seconds:
        return "0с"

    seconds = int(seconds)
    hours, rest = divmod(seconds, 3600)
    minutes, sec = divmod(rest, 60)

    if hours:
        return f"{hours}г {minutes}хв {sec}с"

    if minutes:
        return f"{minutes}хв {sec}с"

    return f"{sec}с"


def progress_text(prefix: str, done: int, total: int | None, start: float) -> str:
    elapsed = max(time.monotonic() - start, 0.1)
    speed = done / elapsed if done else 0

    if total:
        pct = max(0, min(100, int(done * 100 / total)))
        eta = int((total - done) / speed) if speed > 0 else 0

        return (
            f"{prefix} {pct}%\n"
            f"{human_bytes(done)} / {human_bytes(total)}\n"
            f"Швидкість: {human_bytes(speed)}/s\n"
            f"ETA: {seconds_text(eta)}"
        )

    return (
        f"{prefix}\n"
        f"Завантажено: {human_bytes(done)}\n"
        f"Швидкість: {human_bytes(speed)}/s"
    )


def quality_for(chat: int) -> str:
    return USER_QUALITY.get(chat, "best")


def format_selector(platform: str | None, audio: bool, quality: str) -> str:
    if audio:
        return "bestaudio[ext=m4a]/bestaudio/best"

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

    if platform == "youtube" and FFMPEG_PATH:
        return "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/best[ext=mp4]/best"

    return "best[ext=mp4]/best"


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


def remove_file(path: str | Path | None) -> None:
    try:
        if path:
            Path(path).unlink(missing_ok=True)

    except OSError:
        pass


def first_entry(info: dict[str, Any]) -> dict[str, Any]:
    entries = info.get("entries")

    if not entries:
        return info

    entries = [entry for entry in entries if entry]
    return entries[0] if entries else info


def find_file(info: dict[str, Any], ydl) -> str | None:
    candidates: list[str] = []

    for item in info.get("requested_downloads") or []:
        if isinstance(item, dict):
            candidates += [
                item.get("filepath"),
                item.get("_filename"),
            ]

    candidates += [
        info.get("filepath"),
        info.get("_filename"),
    ]

    try:
        candidates.append(ydl.prepare_filename(info))
    except Exception:
        pass

    if info.get("id"):
        candidates += glob.glob(str(DOWNLOAD_DIR / f"*{info['id']}*"))

    existing = [
        str(Path(candidate))
        for candidate in candidates
        if candidate and Path(candidate).exists()
    ]

    existing.sort(
        key=lambda file_path: Path(file_path).stat().st_mtime,
        reverse=True,
    )

    return existing[0] if existing else None


def friendly_error(platform: str | None, error: str) -> str:
    err = str(error or "")
    low = err.lower()

    if platform == "youtube" and (
        "sign in to confirm" in low
        or "not a bot" in low
        or "use --cookies" in low
        or "cookies" in low
    ):
        return (
            "YouTube заблокував запит із сервера і просить cookies.txt.\n\n"
            "Що зробити:\n"
            "1. Зайди в YouTube у браузері.\n"
            "2. Експортуй cookies у форматі Netscape.\n"
            "3. Поклади файл cookies.txt поруч із app.py.\n"
            "4. Перезапусти сервер.\n\n"
            "Якщо cookies вже є — вони могли протухнути."
        )

    if "requested format is not available" in low:
        return "Ця якість недоступна. Спробуй /quality fast або /quality mobile."

    if "ffmpeg" in low and not FFMPEG_PATH:
        return "Потрібен ffmpeg. Додай imageio-ffmpeg у requirements.txt або встанови ffmpeg на сервері."

    if "unsupported url" in low:
        return "Посилання не підтримується або платформа змінила захист."

    if "private" in low or "login" in low:
        return "Відео приватне або потрібен вхід в акаунт. Для цього потрібен правильний cookies.txt."

    return safe_text(err, 900)


# ─────────────────────────────────────────────────────────────
# STREAM DOWNLOAD
# ─────────────────────────────────────────────────────────────

def stream_download(
    url: str,
    filepath: Path,
    title: str,
    progress_cb=None,
    cancel_event: Event | None = None,
    headers: dict[str, str] | None = None,
):
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

        return str(filepath), title

    except DownloadCancelled as error:
        remove_file(filepath)
        return None, str(error)

    except Exception as error:
        remove_file(filepath)
        return None, f"Помилка прямого завантаження: {error}"


# ─────────────────────────────────────────────────────────────
# YT-DLP OPTIONS
# ─────────────────────────────────────────────────────────────

def base_ytdlp_opts(
    platform: str | None,
    audio: bool,
    quality: str,
    progress_hook=None,
) -> dict[str, Any]:
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
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
        "concurrent_fragment_downloads": 4,
        "http_chunk_size": 10 * 1024 * 1024,
        "http_headers": {
            "User-Agent": user_agent,
        },
        "progress_hooks": [progress_hook] if progress_hook else [],
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

    cookie_path = cookies_file()

    if cookie_path:
        opts["cookiefile"] = cookie_path

    if platform == "youtube":
        opts["extractor_args"] = {
            "youtube": {
                "player_client": ["android", "web", "tv"],
            }
        }

    if platform == "tiktok":
        opts["extractor_args"] = {
            "tiktok": {
                "app_version": "26.2.0",
                "manifest_app_version": "26.2.0",
            }
        }

    return opts


# ─────────────────────────────────────────────────────────────
# YT-DLP DOWNLOAD
# ─────────────────────────────────────────────────────────────

def download_via_ytdlp(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
):
    start = time.monotonic()

    def hook(data: dict[str, Any]) -> None:
        if cancel_event and cancel_event.is_set():
            raise DownloadCancelled("Завантаження скасовано.")

        if not progress_cb:
            return

        status = data.get("status")

        if status == "downloading":
            total = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
            done = data.get("downloaded_bytes") or 0

            progress_cb(
                progress_text(
                    "⏳ Завантажую",
                    int(done or 0),
                    int(total or 0),
                    start,
                )
            )

        elif status == "finished":
            progress_cb("🔧 Обробляю файл...")

    opts = base_ytdlp_opts(platform, audio, quality, hook)

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            info = first_entry(info)

            filepath = find_file(info, ydl)

            if audio and filepath:
                mp3_path = str(Path(filepath).with_suffix(".mp3"))

                if Path(mp3_path).exists():
                    filepath = mp3_path

            if not filepath or not Path(filepath).exists():
                return None, "Файл після завантаження не знайдено."

            title = safe_text(info.get("title"), 180)
            return filepath, title

    except DownloadCancelled as error:
        return None, str(error)

    except Exception as error:
        return None, friendly_error(platform, str(error))


# ─────────────────────────────────────────────────────────────
# FALLBACK ДЛЯ TIKTOK
# ─────────────────────────────────────────────────────────────

def tiktok_fallback(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://tikwm.com/",
    }

    try:
        response = requests.get(
            "https://tikwm.com/api/",
            params={
                "url": url,
                "hd": "1",
            },
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()
        data = response.json()

        if data.get("code") != 0:
            return None, data.get("msg") or "tikwm.com не зміг отримати відео."

        item = data.get("data") or {}
        video_url = item.get("hdplay") or item.get("play") or item.get("wmplay")

        if not video_url:
            return None, "tikwm.com не повернув пряме посилання."

        video_url = urljoin("https://tikwm.com", video_url)

        return stream_download(
            video_url,
            safe_filename("tiktok", url),
            safe_text(item.get("title") or "TikTok video"),
            progress_cb,
            cancel_event,
            headers,
        )

    except Exception as error:
        return None, f"tikwm.com: {error}"


# ─────────────────────────────────────────────────────────────
# FALLBACK ДЛЯ INSTAGRAM
# ─────────────────────────────────────────────────────────────

def instagram_fallback(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
):
    fixed_url = (
        url
        .replace("www.instagram.com", "www.ddinstagram.com")
        .replace("instagram.com", "ddinstagram.com")
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
    }

    try:
        response = requests.get(
            fixed_url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()
        html = response.text
        video_url = None

        patterns = [
            r'<video[^>]+src="([^"]+)"',
            r'property="og:video"\s+content="([^"]+)"',
            r'property="og:video:secure_url"\s+content="([^"]+)"',
            r'"video_url":"([^"]+)"',
        ]

        for pattern in patterns:
            match = re.search(pattern, html)

            if match:
                video_url = (
                    match.group(1)
                    .replace("\\u0026", "&")
                    .replace("\\/", "/")
                )
                break

        if not video_url:
            return None, "Instagram fallback не знайшов відео. Можливо, потрібні cookies.txt."

        return stream_download(
            video_url,
            safe_filename("instagram", url),
            "Instagram video",
            progress_cb,
            cancel_event,
            headers,
        )

    except Exception as error:
        return None, f"Instagram fallback: {error}"


# ─────────────────────────────────────────────────────────────
# ПРЯМІ MP4/MOV/WEBM
# ─────────────────────────────────────────────────────────────

def download_direct(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
):
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


# ─────────────────────────────────────────────────────────────
# ГОЛОВНА ФУНКЦІЯ ЗАВАНТАЖЕННЯ
# ─────────────────────────────────────────────────────────────

def download_media(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
):
    if DIRECT_VIDEO_RE.search(url) and not audio:
        return download_direct(
            url,
            progress_cb,
            cancel_event,
        )

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

    if audio:
        return None, result

    if platform == "tiktok":
        if progress_cb:
            progress_cb("🔁 yt-dlp не зміг. Пробую TikTok fallback...")

        path2, result2 = tiktok_fallback(
            url,
            progress_cb,
            cancel_event,
        )

        if path2:
            return path2, result2

        return None, f"{result}\n\nFallback TikTok: {result2}"

    if platform == "instagram":
        if progress_cb:
            progress_cb("🔁 yt-dlp не зміг. Пробую Instagram fallback...")

        path2, result2 = instagram_fallback(
            url,
            progress_cb,
            cancel_event,
        )

        if path2:
            return path2, result2

        return None, f"{result}\n\nFallback Instagram: {result2}"

    return None, result


# ─────────────────────────────────────────────────────────────
# INFO / FORMATS
# ─────────────────────────────────────────────────────────────

def extract_info_text(
    url: str,
    platform: str | None,
    quality: str,
) -> str:
    opts = base_ytdlp_opts(platform, audio=False, quality=quality)
    opts["skip_download"] = True

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        info = first_entry(info)

    title = safe_text(info.get("title"), 180)
    uploader = safe_text(info.get("uploader") or info.get("channel") or "невідомо", 120)
    duration = seconds_text(info.get("duration"))
    views = info.get("view_count")
    like_count = info.get("like_count")
    webpage_url = info.get("webpage_url") or url

    lines = [
        f"ℹ️ {title}",
        f"Автор: {uploader}",
        f"Тривалість: {duration}",
    ]

    if views is not None:
        lines.append(f"Перегляди: {views:,}".replace(",", " "))

    if like_count is not None:
        lines.append(f"Лайки: {like_count:,}".replace(",", " "))

    lines.append(f"Платформа: {platform or 'unknown'}")
    lines.append("")
    lines.append(f"URL: {webpage_url}")

    return "\n".join(lines)[:3900]


def extract_formats_text(
    url: str,
    platform: str | None,
    quality: str,
) -> str:
    opts = base_ytdlp_opts(platform, audio=False, quality=quality)
    opts["skip_download"] = True

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        info = first_entry(info)

    lines = [
        f"🎞 Формати для: {safe_text(info.get('title'), 120)}",
        "",
    ]

    count = 0

    for fmt in info.get("formats") or []:
        if count >= 35:
            break

        format_id = fmt.get("format_id", "?")
        ext = fmt.get("ext", "?")
        height = fmt.get("height")
        fps = fmt.get("fps")
        size = fmt.get("filesize") or fmt.get("filesize_approx")
        note = fmt.get("format_note") or fmt.get("resolution") or "audio"
        vcodec = fmt.get("vcodec")
        acodec = fmt.get("acodec")

        quality_label = f"{height}p" if height else note

        if fps:
            quality_label += f"/{int(fps)}fps"

        size_text = f" ~{human_bytes(size)}" if size else ""

        media_type = "video"

        if vcodec == "none":
            media_type = "audio"
        elif acodec == "none":
            media_type = "video-only"

        lines.append(
            f"• {format_id}: {quality_label} ({ext}, {media_type}){size_text}"
        )

        count += 1

    if count == 0:
        lines.append("Формати не знайдено.")

    return "\n".join(lines)[:3900]


# ─────────────────────────────────────────────────────────────
# СТИСНЕННЯ ВІДЕО
# ─────────────────────────────────────────────────────────────

def compress_video(
    filepath: str,
    progress_cb=None,
) -> str | None:
    if not FFMPEG_PATH:
        return None

    source = Path(filepath)
    target = source.with_name(source.stem + "_compressed.mp4")

    cmd = [
        FFMPEG_PATH,
        "-y",
        "-i",
        str(source),
        "-vf",
        "scale=720:-2:force_original_aspect_ratio=decrease",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "32",
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-movflags",
        "+faststart",
        str(target),
    ]

    try:
        if progress_cb:
            progress_cb("📦 Файл великий. Стискаю відео для Telegram...")

        subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=420,
        )

        if target.exists() and target.stat().st_size < source.stat().st_size:
            return str(target)

    except Exception:
        logger.exception("Не вдалося стиснути відео")

    return None


# ─────────────────────────────────────────────────────────────
# TELEGRAM HELPERS
# ─────────────────────────────────────────────────────────────

async def safe_edit(
    message,
    text: str,
) -> None:
    try:
        await message.edit_text(text[:3900])

    except RetryAfter as error:
        await asyncio.sleep(float(error.retry_after) + 0.2)

    except BadRequest as error:
        if "message is not modified" not in str(error).lower():
            logger.debug("edit_text error: %s", error)

    except TelegramError as error:
        logger.debug("Telegram error: %s", error)


async def send_media(
    update: Update,
    filepath: str,
    title: str,
    is_audio: bool = False,
    progress_cb=None,
) -> int:
    message = update.effective_message

    if not message:
        remove_file(filepath)
        return 0

    files_to_delete = {filepath}
    final_path = filepath
    sent_size = 0

    try:
        size = Path(final_path).stat().st_size

        if size > MAX_UPLOAD_BYTES and not is_audio:
            compressed = compress_video(
                final_path,
                progress_cb,
            )

            if compressed:
                files_to_delete.add(compressed)
                final_path = compressed
                size = Path(final_path).stat().st_size

        if size > MAX_UPLOAD_BYTES:
            await message.reply_text(
                "❌ Файл більший за ліміт Telegram Bot API.\n"
                "Постав /quality fast або /quality mobile і спробуй ще раз."
            )
            return 0

        if progress_cb:
            progress_cb("📤 Надсилаю файл у Telegram...")

        with open(final_path, "rb") as file:
            if is_audio:
                await message.reply_audio(
                    audio=file,
                    title=title[:64],
                    caption=f"🎵 {title[:180]}",
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                    pool_timeout=60,
                )
            else:
                await message.reply_video(
                    video=file,
                    caption=f"✅ {title[:200]}",
                    supports_streaming=True,
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                    pool_timeout=60,
                )

        sent_size = size
        return sent_size

    except Exception:
        logger.exception("Помилка надсилання")
        await message.reply_text("❌ Не вдалося надіслати файл у Telegram.")
        return 0

    finally:
        for path in files_to_delete:
            remove_file(path)


# ─────────────────────────────────────────────────────────────
# ОСНОВНА ЛОГІКА
# ─────────────────────────────────────────────────────────────

async def download_and_send(
    update: Update,
    url: str,
    platform: str,
    audio: bool = False,
) -> None:
    message = update.effective_message

    if not message:
        return

    current_chat_id = chat_id(update)
    quality = quality_for(current_chat_id)
    cancel_event = Event()

    CANCEL_EVENTS[current_chat_id] = cancel_event

    ACTIVE_TASKS[current_chat_id] = {
        "url": url,
        "platform": platform,
        "audio": audio,
        "quality": quality,
        "started_at": time.time(),
    }

    async with PARALLEL_LIMIT:
        status = await message.reply_text(
            "🎵 Готую аудіо..." if audio else "⏳ Починаю завантаження..."
        )

        loop = asyncio.get_running_loop()
        last_time = [0.0]
        last_text = [""]

        def progress_cb(text: str) -> None:
            now = time.monotonic()
            important = text.startswith(("🔧", "📦", "📤", "✅", "🔁", "❌"))

            if text == last_text[0]:
                return

            if now - last_time[0] < PROGRESS_THROTTLE and not important:
                return

            last_time[0] = now
            last_text[0] = text

            asyncio.run_coroutine_threadsafe(
                safe_edit(status, text),
                loop,
            )

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

            path, title = await loop.run_in_executor(
                None,
                job,
            )

            if not path:
                stats.fail()
                await safe_edit(status, f"❌ {title}")
                return

            await safe_edit(
                status,
                "✅ Завантажено. Готую відправку...",
            )

            sent_size = await send_media(
                update,
                path,
                title,
                is_audio=audio,
                progress_cb=progress_cb,
            )

            if sent_size:
                stats.ok(platform, sent_size)

            try:
                await status.delete()
            except Exception:
                pass

        finally:
            CANCEL_EVENTS.pop(current_chat_id, None)
            ACTIVE_TASKS.pop(current_chat_id, None)
            clean_old_files(False)


async def handle_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message or not message.text:
        return

    urls = extract_urls(message.text)

    if not urls:
        await message.reply_text("❌ Надішли посилання на відео.")
        return

    if len(urls) > 1:
        await message.reply_text(
            f"Знайшов {len(urls)} посилання. Оброблю по черзі."
        )

    for url in urls:
        platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)

        if not platform:
            await message.reply_text(
                f"❌ Платформа не підтримується:\n{url}"
            )
            continue

        await download_and_send(
            update,
            url,
            platform,
            audio=False,
        )


# ─────────────────────────────────────────────────────────────
# КОМАНДИ
# ─────────────────────────────────────────────────────────────

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(HELP_TEXT)


async def video_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message:
        return

    url = context.args[0].strip() if context.args else None

    if not url and message.reply_to_message and message.reply_to_message.text:
        found = extract_urls(message.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await message.reply_text("❌ Використання: /video <посилання>")
        return

    platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)

    if not platform:
        await message.reply_text("❌ Платформа не підтримується.")
        return

    await download_and_send(
        update,
        url,
        platform,
        audio=False,
    )


async def audio_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message:
        return

    url = context.args[0].strip() if context.args else None

    if not url and message.reply_to_message and message.reply_to_message.text:
        found = extract_urls(message.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await message.reply_text(
            "❌ Використання: /audio <посилання> або /audio у відповідь на посилання."
        )
        return

    platform = detect_platform(url)

    if not platform:
        await message.reply_text("❌ Платформа не підтримується для аудіо.")
        return

    await download_and_send(
        update,
        url,
        platform,
        audio=True,
    )


async def info_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message:
        return

    url = context.args[0].strip() if context.args else None

    if not url and message.reply_to_message and message.reply_to_message.text:
        found = extract_urls(message.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await message.reply_text("❌ Використання: /info <посилання>")
        return

    if DIRECT_VIDEO_RE.search(url):
        await message.reply_text("ℹ️ Це пряме посилання на файл. Для нього /info не потрібна.")
        return

    platform = detect_platform(url)

    if not platform:
        await message.reply_text("❌ Платформа не підтримується.")
        return

    status = await message.reply_text("🔎 Отримую інформацію...")
    loop = asyncio.get_running_loop()

    try:
        result = await loop.run_in_executor(
            None,
            partial(
                extract_info_text,
                url,
                platform,
                quality_for(chat_id(update)),
            ),
        )

        await safe_edit(status, result)

    except Exception as error:
        await safe_edit(
            status,
            f"❌ {friendly_error(platform, str(error))}",
        )


async def formats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message:
        return

    url = context.args[0].strip() if context.args else None

    if not url and message.reply_to_message and message.reply_to_message.text:
        found = extract_urls(message.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await message.reply_text("❌ Використання: /formats <посилання>")
        return

    platform = detect_platform(url)

    if not platform:
        await message.reply_text("❌ Платформа не підтримується.")
        return

    status = await message.reply_text("🔎 Отримую формати...")
    loop = asyncio.get_running_loop()

    try:
        result = await loop.run_in_executor(
            None,
            partial(
                extract_formats_text,
                url,
                platform,
                quality_for(chat_id(update)),
            ),
        )

        await safe_edit(status, result)

    except Exception as error:
        await safe_edit(
            status,
            f"❌ {friendly_error(platform, str(error))}",
        )


async def stats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(stats.text())


async def platforms_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(
            "Підтримую:\n" + "\n".join(f"• {platform}" for platform in URL_PATTERNS)
        )


async def quality_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message:
        return

    current_chat_id = chat_id(update)

    if not context.args:
        await message.reply_text(
            f"Поточна якість: {quality_for(current_chat_id)}\n"
            "/quality best — найкраща якість\n"
            "/quality fast — швидше, до 720p\n"
            "/quality mobile — легший файл, до 480p"
        )
        return

    value = context.args[0].lower().strip()

    if value not in {"best", "fast", "mobile"}:
        await message.reply_text("❌ Доступно тільки: best, fast, mobile")
        return

    USER_QUALITY[current_chat_id] = value
    save_settings()

    await message.reply_text(f"✅ Якість змінено на: {value}")


async def clean_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    removed = clean_old_files(True)

    if update.effective_message:
        await update.effective_message.reply_text(
            f"🧹 Видалено файлів: {removed}"
        )


async def cancel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message:
        return

    event = CANCEL_EVENTS.get(chat_id(update))

    if not event:
        await message.reply_text("Немає активного завантаження для скасування.")
        return

    event.set()

    await message.reply_text("🛑 Скасовую завантаження...")


async def queue_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message:
        return

    task = ACTIVE_TASKS.get(chat_id(update))

    if not task:
        await message.reply_text("Активного завантаження в цьому чаті немає.")
        return

    elapsed = seconds_text(time.time() - task["started_at"])

    await message.reply_text(
        "⏳ Активне завантаження:\n"
        f"Платформа: {task['platform']}\n"
        f"Тип: {'audio' if task['audio'] else 'video'}\n"
        f"Якість: {task['quality']}\n"
        f"Час: {elapsed}\n"
        f"URL: {task['url'][:300]}"
    )


async def cookies_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message:
        return

    path = cookies_file()

    if not path:
        await message.reply_text(
            "❌ cookies.txt не знайдено або формат неправильний.\n\n"
            "Правильний перший рядок:\n"
            "# Netscape HTTP Cookie File"
        )
        return

    size = Path(path).stat().st_size

    await message.reply_text(
        "✅ cookies.txt знайдено\n"
        f"Шлях: {path}\n"
        f"Розмір: {human_bytes(size)}"
    )


async def health_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message

    if not message:
        return

    cookie_path = cookies_file()
    files_count = len(list(DOWNLOAD_DIR.glob("*")))

    lines = [
        "🩺 Health check",
        f"ffmpeg: {'✅ ' + FFMPEG_PATH if FFMPEG_PATH else '❌ не знайдено'}",
        f"cookies.txt: {'✅ ' + cookie_path if cookie_path else '❌ не знайдено'}",
        f"downloads/: {files_count} файлів",
        f"parallel: {PARALLEL_DOWNLOADS}",
        f"max upload: {human_bytes(MAX_UPLOAD_BYTES)}",
        f"quality: {quality_for(chat_id(update))}",
    ]

    await message.reply_text("\n".join(lines))


async def reset_stats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    global stats

    stats = Stats()
    stats.save()

    if update.effective_message:
        await update.effective_message.reply_text("✅ Статистику очищено.")


async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    logger.error(
        "Необроблений виняток:",
        exc_info=context.error,
    )


# ─────────────────────────────────────────────────────────────
# ЗАПУСК
# ─────────────────────────────────────────────────────────────

def delete_webhook() -> None:
    try:
        requests.get(
            f"https://api.telegram.org/bot{TOKEN}/deleteWebhook",
            params={
                "drop_pending_updates": "true",
            },
            timeout=10,
        )

    except Exception:
        pass


def main() -> None:
    ensure_cookies_from_env()
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

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_message,
        )
    )

    app.add_error_handler(error_handler)

    logger.info(
        "Бот запущено. ffmpeg=%s",
        FFMPEG_PATH or "не знайдено",
    )

    logger.info(
        "cookies=%s",
        cookies_file() or "не знайдено",
    )

    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
