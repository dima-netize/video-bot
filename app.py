"""
Video Downloader Bot Pro для fps.ms/Pterodactyl.
Сам ставить requests / yt-dlp / python-telegram-bot / imageio-ffmpeg, якщо їх немає.
"""
from __future__ import annotations

import importlib
import os
import site
import subprocess
import sys
from pathlib import Path

BASE_DIR_BOOT = Path(__file__).resolve().parent
PY_VER = f"python{sys.version_info.major}.{sys.version_info.minor}"
LOCAL_SITE = BASE_DIR_BOOT / ".local" / "lib" / PY_VER / "site-packages"
USER_SITE = Path(site.getusersitepackages())

for p in (LOCAL_SITE, USER_SITE):
    if p.exists() and str(p) not in sys.path:
        sys.path.insert(0, str(p))
        site.addsitedir(str(p))


def module_ok(name: str) -> bool:
    try:
        importlib.import_module(name)
        return True
    except Exception:
        return False


def pip_install(pkg: str) -> None:
    print(f"[BOOT] Installing: {pkg}", flush=True)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "-U", pkg])

    if USER_SITE.exists() and str(USER_SITE) not in sys.path:
        sys.path.insert(0, str(USER_SITE))
        site.addsitedir(str(USER_SITE))

    importlib.invalidate_caches()


def ensure_dependencies() -> None:
    if os.environ.get("DISABLE_AUTO_INSTALL", "0") == "1":
        return

    deps = [
        ("requests", "requests>=2.32.0"),
        ("yt_dlp", "yt-dlp[default]"),
        ("telegram.ext", "python-telegram-bot==20.7"),
        ("imageio_ffmpeg", "imageio-ffmpeg>=0.6.0"),
    ]

    missing = [pkg for mod, pkg in deps if not module_ok(mod)]

    for pkg in missing:
        pip_install(pkg)

    failed = [mod for mod, _ in deps if not module_ok(mod)]

    if failed:
        raise RuntimeError("Не вдалося встановити: " + ", ".join(failed))


ensure_dependencies()

import asyncio
import base64
import glob
import json
import logging
import re
import shutil
import time
import traceback
from dataclasses import dataclass, field
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

try:
    import imageio_ffmpeg
except Exception:
    imageio_ffmpeg = None


TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано TOKEN у Startup / Environment")

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

STATS_FILE = BASE_DIR / "bot_stats.json"
SETTINGS_FILE = BASE_DIR / "bot_settings.json"

MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(49 * 1024 * 1024)))
PROGRESS_THROTTLE = float(os.environ.get("PROGRESS_THROTTLE", "1.4"))
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

/video <url> — скачати відео
/audio <url> — скачати MP3
/audio у відповідь на посилання — MP3
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

Підтримка: YouTube, TikTok, Instagram, Twitter/X, Vimeo, Reddit, Facebook, Likee, Snapchat, Pinterest, прямі MP4/MOV/WEBM.

Для YouTube на сервері часто потрібен cookies.txt у форматі Netscape.
""".strip()


def read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default

        return json.loads(path.read_text(encoding="utf-8"))

    except Exception:
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
        logger.exception("Не вдалося записати JSON")


@dataclass
class Stats:
    success: int = 0
    errors: int = 0
    downloaded_bytes: int = 0
    platforms: dict[str, int] = field(default_factory=dict)

    @classmethod
    def load(cls) -> "Stats":
        d = read_json(STATS_FILE, {})

        if not isinstance(d, dict):
            return cls()

        return cls(
            success=int(d.get("success", 0)),
            errors=int(d.get("errors", 0)),
            downloaded_bytes=int(d.get("downloaded_bytes", 0)),
            platforms=dict(d.get("platforms", {})),
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
            lines += ["", "За платформами:"]
            lines += [f"• {p}: {c}" for p, c in sorted(self.platforms.items())]

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
    env_value = os.environ.get("YOUTUBE_COOKIES_B64") or os.environ.get("COOKIES_TXT_B64")

    if not env_value:
        return

    target = BASE_DIR / "cookies.txt"

    if target.exists() and target.stat().st_size > 50:
        return

    try:
        target.write_bytes(base64.b64decode(env_value.strip()))
        logger.info("cookies.txt створено зі змінної середовища")

    except Exception:
        logger.exception("Не вдалося створити cookies.txt із Base64")


def cookies_file() -> str | None:
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

            first = lines[0].strip() if lines else ""

            if first in {"# Netscape HTTP Cookie File", "# HTTP Cookie File"}:
                return str(path)

        except Exception:
            pass

    return None


def chat_id(update: Update) -> int:
    return int(update.effective_chat.id) if update.effective_chat else 0


def safe_text(value: Any, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "video")).strip()

    if not text:
        text = "video"

    return text[:limit]


def safe_filename(prefix: str, url: str, ext: str = "mp4") -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug = url.split("?")[0].rstrip("/").split("/")[-1] or "video"
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", slug)[:35] or "video"

    return DOWNLOAD_DIR / f"{prefix}_{slug}_{ts}.{ext}"


def extract_urls(text: str) -> list[str]:
    out: list[str] = []

    for url in URL_RE.findall(text or ""):
        url = url.strip().strip(".,;)\n\r\t ")

        if url and url not in out:
            out.append(url)

    return out[:MAX_LINKS_PER_MESSAGE]


def detect_platform(url: str) -> str | None:
    for name, pat in URL_PATTERNS.items():
        if pat.search(url):
            return name

    return None


def human_bytes(num: int | float | None) -> str:
    if not num:
        return "0 B"

    n = float(num)

    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(n)} B"

            return f"{n:.1f} {unit}"

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

    for p in DOWNLOAD_DIR.glob("*"):
        try:
            if p.is_file() and (force or now - p.stat().st_mtime > OLD_FILE_TTL):
                p.unlink()
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

    entries = [e for e in entries if e]

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
        str(Path(c))
        for c in candidates
        if c and Path(c).exists()
    ]

    existing.sort(
        key=lambda x: Path(x).stat().st_mtime,
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
            "YouTube просить cookies.txt.\n\n"
            "Поклади правильний cookies.txt поруч із app.py і перезапусти сервер.\n"
            "Перший рядок має бути:\n"
            "# Netscape HTTP Cookie File"
        )

    if "requested format is not available" in low:
        return "Ця якість недоступна. Спробуй /quality fast або /quality mobile."

    if "ffmpeg" in low and not FFMPEG_PATH:
        return "Потрібен ffmpeg. Код пробує використати imageio-ffmpeg, але пакет не встановився."

    if "unsupported url" in low:
        return "Посилання не підтримується або платформа змінила захист."

    if "private" in low or "login" in low:
        return "Відео приватне або потрібен вхід в акаунт. Потрібен правильний cookies.txt."

    return safe_text(err, 900)


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
        ) as r:
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
                        progress_cb(
                            progress_text(
                                "⏳ Завантажую файл",
                                done,
                                total,
                                start,
                            )
                        )

        return str(filepath), title

    except DownloadCancelled as e:
        remove_file(filepath)
        return None, str(e)

    except Exception as e:
        remove_file(filepath)
        return None, f"Помилка прямого завантаження: {e}"


def base_ytdlp_opts(
    platform: str | None,
    audio: bool,
    quality: str,
    progress_hook=None,
) -> dict[str, Any]:
    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    )

    if platform == "tiktok":
        ua = (
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
            "User-Agent": ua,
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

    ck = cookies_file()

    if ck:
        opts["cookiefile"] = ck

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


def download_via_ytdlp(
    url: str,
    platform: str | None,
    audio: bool,
    quality: str,
    progress_cb=None,
    cancel_event: Event | None = None,
):
    start = time.monotonic()

    def hook(d: dict[str, Any]) -> None:
        if cancel_event and cancel_event.is_set():
            raise DownloadCancelled("Завантаження скасовано.")

        if not progress_cb:
            return

        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done = d.get("downloaded_bytes") or 0

            progress_cb(
                progress_text(
                    "⏳ Завантажую",
                    int(done or 0),
                    int(total or 0),
                    start,
                )
            )

        elif d.get("status") == "finished":
            progress_cb("🔧 Обробляю файл...")

    try:
        opts = base_ytdlp_opts(
            platform,
            audio,
            quality,
            hook,
        )

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(
                url,
                download=True,
            )

            info = first_entry(info)
            filepath = find_file(info, ydl)

            if audio and filepath:
                mp3 = str(Path(filepath).with_suffix(".mp3"))

                if Path(mp3).exists():
                    filepath = mp3

            if not filepath or not Path(filepath).exists():
                return None, "Файл після завантаження не знайдено."

            return filepath, safe_text(info.get("title"), 180)

    except DownloadCancelled as e:
        return None, str(e)

    except Exception as e:
        return None, friendly_error(platform, str(e))


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
        r = requests.get(
            "https://tikwm.com/api/",
            params={
                "url": url,
                "hd": "1",
            },
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

        r.raise_for_status()
        data = r.json()

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

    except Exception as e:
        return None, f"tikwm.com: {e}"


def instagram_fallback(
    url: str,
    progress_cb=None,
    cancel_event: Event | None = None,
):
    fixed = (
        url
        .replace("www.instagram.com", "www.ddinstagram.com")
        .replace("instagram.com", "ddinstagram.com")
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
    }

    try:
        r = requests.get(
            fixed,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

        r.raise_for_status()

        video_url = None

        patterns = [
            r'<video[^>]+src="([^"]+)"',
            r'property="og:video"\s+content="([^"]+)"',
            r'property="og:video:secure_url"\s+content="([^"]+)"',
            r'"video_url":"([^"]+)"',
        ]

        for pat in patterns:
            m = re.search(pat, r.text)

            if m:
                video_url = (
                    m.group(1)
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

    except Exception as e:
        return None, f"Instagram fallback: {e}"


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

        p2, r2 = tiktok_fallback(
            url,
            progress_cb,
            cancel_event,
        )

        if p2:
            return p2, r2

        return None, f"{result}\n\nFallback TikTok: {r2}"

    if platform == "instagram":
        if progress_cb:
            progress_cb("🔁 yt-dlp не зміг. Пробую Instagram fallback...")

        p2, r2 = instagram_fallback(
            url,
            progress_cb,
            cancel_event,
        )

        if p2:
            return p2, r2

        return None, f"{result}\n\nFallback Instagram: {r2}"

    return None, result


def extract_info_text(
    url: str,
    platform: str | None,
    quality: str,
) -> str:
    opts = base_ytdlp_opts(
        platform,
        audio=False,
        quality=quality,
    )

    opts["skip_download"] = True

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(
            url,
            download=False,
        )

        info = first_entry(info)

    lines = [
        f"ℹ️ {safe_text(info.get('title'), 180)}",
        f"Автор: {safe_text(info.get('uploader') or info.get('channel') or 'невідомо', 120)}",
        f"Тривалість: {seconds_text(info.get('duration'))}",
    ]

    if info.get("view_count") is not None:
        lines.append(
            f"Перегляди: {info.get('view_count'):,}".replace(",", " ")
        )

    if info.get("like_count") is not None:
        lines.append(
            f"Лайки: {info.get('like_count'):,}".replace(",", " ")
        )

    lines += [
        f"Платформа: {platform or 'unknown'}",
        "",
        f"URL: {info.get('webpage_url') or url}",
    ]

    return "\n".join(lines)[:3900]


def extract_formats_text(
    url: str,
    platform: str | None,
    quality: str,
) -> str:
    opts = base_ytdlp_opts(
        platform,
        audio=False,
        quality=quality,
    )

    opts["skip_download"] = True

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(
            url,
            download=False,
        )

        info = first_entry(info)

    lines = [
        f"🎞 Формати для: {safe_text(info.get('title'), 120)}",
        "",
    ]

    formats = info.get("formats") or []

    for i, fmt in enumerate(formats):
        if i >= 35:
            break

        height = fmt.get("height")
        fps = fmt.get("fps")
        size = fmt.get("filesize") or fmt.get("filesize_approx")
        ext = fmt.get("ext", "?")
        format_id = fmt.get("format_id", "?")
        note = fmt.get("format_note") or fmt.get("resolution") or "audio"

        label = f"{height}p" if height else note

        if fps:
            label += f"/{int(fps)}fps"

        if fmt.get("vcodec") == "none":
            media = "audio"
        elif fmt.get("acodec") == "none":
            media = "video-only"
        else:
            media = "video"

        size_text = f" ~{human_bytes(size)}" if size else ""

        lines.append(
            f"• {format_id}: {label} ({ext}, {media}){size_text}"
        )

    if len(lines) <= 2:
        lines.append("Формати не знайдено.")

    return "\n".join(lines)[:3900]


def compress_video(
    filepath: str,
    progress_cb=None,
) -> str | None:
    if not FFMPEG_PATH:
        return None

    src = Path(filepath)
    dst = src.with_name(src.stem + "_compressed.mp4")

    cmd = [
        FFMPEG_PATH,
        "-y",
        "-i",
        str(src),
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
        str(dst),
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

        if dst.exists() and dst.stat().st_size < src.stat().st_size:
            return str(dst)

    except Exception:
        return None

    return None


async def safe_edit(message, text: str) -> None:
    try:
        await message.edit_text(text[:3900])

    except RetryAfter as e:
        await asyncio.sleep(float(e.retry_after) + 0.2)

    except BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.debug("edit_text error: %s", e)

    except TelegramError as e:
        logger.debug("telegram error: %s", e)


async def send_media(
    update: Update,
    filepath: str,
    title: str,
    is_audio: bool = False,
    progress_cb=None,
) -> int:
    msg = update.effective_message

    if not msg:
        remove_file(filepath)
        return 0

    delete_later = {filepath}
    final_path = filepath

    try:
        size = Path(final_path).stat().st_size

        if size > MAX_UPLOAD_BYTES and not is_audio:
            compressed = compress_video(
                final_path,
                progress_cb,
            )

            if compressed:
                delete_later.add(compressed)
                final_path = compressed
                size = Path(final_path).stat().st_size

        if size > MAX_UPLOAD_BYTES:
            await msg.reply_text(
                "❌ Файл більший за ліміт Telegram Bot API. "
                "Постав /quality fast або /quality mobile і спробуй ще раз."
            )
            return 0

        if progress_cb:
            progress_cb("📤 Надсилаю файл у Telegram...")

        with open(final_path, "rb") as f:
            if is_audio:
                await msg.reply_audio(
                    audio=f,
                    title=title[:64],
                    caption=f"🎵 {title[:180]}",
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                    pool_timeout=60,
                )
            else:
                await msg.reply_video(
                    video=f,
                    caption=f"✅ {title[:200]}",
                    supports_streaming=True,
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                    pool_timeout=60,
                )

        return size

    except Exception:
        logger.exception("Помилка надсилання")
        await msg.reply_text("❌ Не вдалося надіслати файл у Telegram.")
        return 0

    finally:
        for p in delete_later:
            remove_file(p)


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
    quality = quality_for(cid)
    cancel_event = Event()

    CANCEL_EVENTS[cid] = cancel_event

    ACTIVE_TASKS[cid] = {
        "url": url,
        "platform": platform,
        "audio": audio,
        "quality": quality,
        "started_at": time.time(),
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

            sent = await send_media(
                update,
                path,
                title,
                is_audio=audio,
                progress_cb=progress_cb,
            )

            if sent:
                stats.ok(platform, sent)

            try:
                await status.delete()
            except Exception:
                pass

        finally:
            CANCEL_EVENTS.pop(cid, None)
            ACTIVE_TASKS.pop(cid, None)
            clean_old_files(False)


async def handle_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    msg = update.effective_message

    if not msg or not msg.text:
        return

    urls = extract_urls(msg.text)

    if not urls:
        await msg.reply_text("❌ Надішли посилання на відео.")
        return

    if len(urls) > 1:
        await msg.reply_text(
            f"Знайшов {len(urls)} посилання. Оброблю по черзі."
        )

    for url in urls:
        platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)

        if not platform:
            await msg.reply_text(
                f"❌ Платформа не підтримується:\n{url}"
            )
            continue

        await download_and_send(
            update,
            url,
            platform,
            audio=False,
        )


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

    platform = "direct" if DIRECT_VIDEO_RE.search(url) else detect_platform(url)

    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
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
    msg = update.effective_message

    if not msg:
        return

    url = context.args[0].strip() if context.args else None

    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await msg.reply_text(
            "❌ Використання: /audio <посилання> або /audio у відповідь на посилання."
        )
        return

    platform = detect_platform(url)

    if not platform:
        await msg.reply_text("❌ Платформа не підтримується для аудіо.")
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
    msg = update.effective_message

    if not msg:
        return

    url = context.args[0].strip() if context.args else None

    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await msg.reply_text("❌ Використання: /info <посилання>")
        return

    if DIRECT_VIDEO_RE.search(url):
        await msg.reply_text("ℹ️ Це пряме посилання на файл. Для нього /info не потрібна.")
        return

    platform = detect_platform(url)

    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return

    status = await msg.reply_text("🔎 Отримую інформацію...")

    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            partial(
                extract_info_text,
                url,
                platform,
                quality_for(chat_id(update)),
            ),
        )

        await safe_edit(status, result)

    except Exception as e:
        await safe_edit(
            status,
            f"❌ {friendly_error(platform, str(e))}",
        )


async def formats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    msg = update.effective_message

    if not msg:
        return

    url = context.args[0].strip() if context.args else None

    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found = extract_urls(msg.reply_to_message.text)
        url = found[0] if found else None

    if not url:
        await msg.reply_text("❌ Використання: /formats <посилання>")
        return

    platform = detect_platform(url)

    if not platform:
        await msg.reply_text("❌ Платформа не підтримується.")
        return

    status = await msg.reply_text("🔎 Отримую формати...")

    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            partial(
                extract_formats_text,
                url,
                platform,
                quality_for(chat_id(update)),
            ),
        )

        await safe_edit(status, result)

    except Exception as e:
        await safe_edit(
            status,
            f"❌ {friendly_error(platform, str(e))}",
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
            "Підтримую:\n" + "\n".join(f"• {p}" for p in URL_PATTERNS)
        )


async def quality_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    msg = update.effective_message

    if not msg:
        return

    cid = chat_id(update)

    if not context.args:
        await msg.reply_text(
            f"Поточна якість: {quality_for(cid)}\n"
            "/quality best — найкраща\n"
            "/quality fast — до 720p\n"
            "/quality mobile — до 480p"
        )
        return

    value = context.args[0].lower().strip()

    if value not in {"best", "fast", "mobile"}:
        await msg.reply_text("❌ Доступно тільки: best, fast, mobile")
        return

    USER_QUALITY[cid] = value
    save_settings()

    await msg.reply_text(f"✅ Якість змінено на: {value}")


async def clean_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(
            f"🧹 Видалено файлів: {clean_old_files(True)}"
        )


async def cancel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    msg = update.effective_message

    if not msg:
        return

    event = CANCEL_EVENTS.get(chat_id(update))

    if not event:
        await msg.reply_text("Немає активного завантаження для скасування.")
        return

    event.set()

    await msg.reply_text("🛑 Скасовую завантаження...")


async def queue_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    msg = update.effective_message

    if not msg:
        return

    task = ACTIVE_TASKS.get(chat_id(update))

    if not task:
        await msg.reply_text("Активного завантаження в цьому чаті немає.")
        return

    await msg.reply_text(
        "⏳ Активне завантаження:\n"
        f"Платформа: {task['platform']}\n"
        f"Тип: {'audio' if task['audio'] else 'video'}\n"
        f"Якість: {task['quality']}\n"
        f"Час: {seconds_text(time.time() - task['started_at'])}\n"
        f"URL: {task['url'][:300]}"
    )


async def cookies_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    msg = update.effective_message

    if not msg:
        return

    path = cookies_file()

    if not path:
        await msg.reply_text(
            "❌ cookies.txt не знайдено або формат неправильний.\n"
            "Правильний перший рядок:\n"
            "# Netscape HTTP Cookie File"
        )
        return

    await msg.reply_text(
        f"✅ cookies.txt знайдено\n"
        f"Шлях: {path}\n"
        f"Розмір: {human_bytes(Path(path).stat().st_size)}"
    )


async def health_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    msg = update.effective_message

    if not msg:
        return

    ck = cookies_file()

    lines = [
        "🩺 Health check",
        f"ffmpeg: {'✅ ' + FFMPEG_PATH if FFMPEG_PATH else '❌ не знайдено'}",
        f"cookies.txt: {'✅ ' + ck if ck else '❌ не знайдено'}",
        f"downloads/: {len(list(DOWNLOAD_DIR.glob('*')))} файлів",
        f"parallel: {PARALLEL_DOWNLOADS}",
        f"max upload: {human_bytes(MAX_UPLOAD_BYTES)}",
        f"quality: {quality_for(chat_id(update))}",
    ]

    await msg.reply_text("\n".join(lines))


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
