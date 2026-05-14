"""
Швидкий Video Downloader Bot
YouTube, TikTok, Instagram, Twitter, Vimeo, Reddit, Facebook, Likee, Snapchat
Найкраща якість | Прогрес у реальному часі
"""

from __future__ import annotations

import asyncio
import glob
import logging
import os
import re
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from typing import Callable

import requests
import yt_dlp
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ── Налаштування ─────────────────────────────────────────────────────────────

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано змінну оточення TOKEN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

MAX_VIDEO_BYTES = 50 * 1024 * 1024   # 50 МБ — ліміт Telegram для video
PROGRESS_THROTTLE = 1.5              # секунди між оновленнями прогресу

# ── Патерни URL ───────────────────────────────────────────────────────────────

URL_PATTERNS: dict[str, re.Pattern] = {
    name: re.compile(pat, re.IGNORECASE)
    for name, pat in {
        "youtube":   r"(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)[\w-]+",
        "tiktok":    r"(?:https?://)?(?:www\.)?(?:tiktok\.com/@[\w.-]+/video/\d+|vt\.tiktok\.com/\w+|vm\.tiktok\.com/\w+)",
        "instagram": r"(?:https?://)?(?:www\.)?instagram\.com/(?:reel|p|tv|stories)/[\w-]+",
        "twitter":   r"(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/\w+/status/\d+",
        "vimeo":     r"(?:https?://)?(?:www\.)?vimeo\.com/\d+",
        "reddit":    r"(?:https?://)?(?:www\.)?reddit\.com/r/\w+/comments/\w+",
        "facebook":  r"(?:https?://)?(?:www\.)?facebook\.com/(?:watch/?v=|[\w.]+/videos/)\d+",
        "likee":     r"(?:https?://)?(?:www\.)?likee\.com/v/\w+",
        "snapchat":  r"(?:https?://)?(?:www\.)?snapchat\.com/spotlight/\w+",
    }.items()
}

DIRECT_VIDEO_RE = re.compile(
    r"https?://\S+\.(?:mp4|mov|webm)(?:\?\S*)?", re.IGNORECASE
)

HELP_TEXT = (
    "🎥 *Video Downloader Bot*\n"
    "Кинь посилання — отримай відео в найкращій якості\\.\n\n"
    "Підтримую: YouTube, TikTok, Instagram, Twitter/X, Vimeo, "
    "Reddit, Facebook, Likee, Snapchat\\.\n"
    "Також приймаю прямі посилання на \\.mp4/\\.mov/\\.webm\n\n"
    "🎵 `/audio` у відповідь на посилання — тільки звук \\(MP3\\)\\.\n"
    "📊 `/stats` — статистика завантажень\\."
)

# ── Стан ─────────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    total: int = 0
    platforms: dict[str, int] = field(default_factory=dict)

    def record(self, platform: str) -> None:
        self.total += 1
        self.platforms[platform] = self.platforms.get(platform, 0) + 1

    def render(self) -> str:
        lines = [f"📊 Всього завантажень: {self.total}"]
        lines += [f"• {p}: {c}" for p, c in sorted(self.platforms.items())]
        return "\n".join(lines)

stats = Stats()

# Захист від паралельних завантажень одним юзером
_user_locks: dict[int, asyncio.Lock] = {}

def _get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]

# ── Допоміжні функції ─────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _cookies_file() -> str | None:
    path = "cookies.txt"
    return path if os.path.exists(path) else None


def _safe_filename(prefix: str, url: str, ext: str = "mp4") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug = re.sub(r"\W+", "_", url.split("/")[-1])[:30]
    return os.path.join(DOWNLOAD_DIR, f"{prefix}_{slug}_{ts}.{ext}")


def _extract_url(text: str) -> str | None:
    """Витягує перше URL з довільного тексту."""
    m = re.search(r"https?://\S+", text)
    return m.group(0) if m else None


def _detect_platform(url: str) -> str | None:
    for name, pat in URL_PATTERNS.items():
        if pat.search(url):
            return name
    return None

# ── Завантаження ──────────────────────────────────────────────────────────────

def _build_ydl_opts(
    platform: str | None,
    audio: bool,
    progress_hook: Callable | None,
) -> dict:
    opts: dict = {
        "format": "bestaudio/best" if audio else "bestvideo+bestaudio/best",
        "outtmpl": os.path.join(DOWNLOAD_DIR, "%(title)s_%(id)s.%(ext)s"),
        "merge_output_format": None if audio else "mp4",
        "quiet": True,
        "no_warnings": True,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 12; Pixel 6) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Mobile Safari/537.36"
            )
        },
    }

    if platform == "youtube":
        opts["extractor_args"] = {"youtube": {"player_client": ["android", "web"]}}

    if audio:
        opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }]

    ck = _cookies_file()
    if ck:
        opts["cookiefile"] = ck

    if progress_hook:
        opts["progress_hooks"] = [progress_hook]

    return opts


def download_media(
    url: str,
    platform: str | None = None,
    audio: bool = False,
    progress_cb: Callable[[str], None] | None = None,
) -> tuple[str | None, str]:
    """Блокуюче завантаження. Повертає (шлях, назва) або (None, повідомлення_про_помилку)."""

    last_call: list[float] = [0.0]

    def hook(d: dict) -> None:
        if progress_cb and d["status"] == "downloading":
            now = time.monotonic()
            if now - last_call[0] < PROGRESS_THROTTLE:
                return
            last_call[0] = now
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            done  = d.get("downloaded_bytes", 0)
            speed = d.get("speed") or 0
            if total:
                pct   = int(done * 100 / total)
                speed_s = f" • {speed / 1_048_576:.1f} MB/s" if speed else ""
                progress_cb(
                    f"⏳ {pct}% ({done / 1_048_576:.1f}/{total / 1_048_576:.1f} MB{speed_s})"
                )

    opts = _build_ydl_opts(platform, audio, hook if progress_cb else None)

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info     = ydl.extract_info(url, download=True)
            filepath = ydl.prepare_filename(info)

            if audio:
                filepath = os.path.splitext(filepath)[0] + ".mp3"
            elif not os.path.exists(filepath):
                candidates = glob.glob(
                    os.path.join(DOWNLOAD_DIR, f"*{info['id']}*")
                )
                filepath = candidates[0] if candidates else None

            if not filepath or not os.path.exists(filepath):
                return None, "Файл після завантаження не знайдено."

            return filepath, info.get("title", "video")

    except yt_dlp.utils.DownloadError as e:
        msg = str(e)
        if "Sign in" in msg or "login" in msg.lower():
            return None, (
                "Потрібна авторизація. Додайте файл cookies.txt або спробуйте пізніше."
            )
        return None, f"Помилка завантаження: {msg[:300]}"
    except Exception as e:  # noqa: BLE001
        logger.exception("Несподівана помилка download_media")
        return None, f"Внутрішня помилка: {e}"


def download_direct(url: str) -> tuple[str | None, str]:
    """Завантажує пряме відео-посилання через requests."""
    ext = (url.split(".")[-1].split("?")[0] or "mp4")[:4]
    filepath = _safe_filename("direct", url, ext)
    try:
        with requests.get(
            url,
            stream=True,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"},
        ) as resp:
            resp.raise_for_status()
            with open(filepath, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65_536):
                    fh.write(chunk)
        return filepath, "Пряме відео"
    except requests.RequestException as e:
        if os.path.exists(filepath):
            os.remove(filepath)
        return None, f"Не вдалося завантажити: {e}"

# ── Надсилання файлу ──────────────────────────────────────────────────────────

async def send_media(
    update: Update,
    filepath: str,
    title: str,
    is_audio: bool = False,
) -> None:
    size = os.path.getsize(filepath)
    try:
        with open(filepath, "rb") as fh:
            if is_audio:
                await update.message.reply_audio(audio=fh, title=title[:64])
            elif size > MAX_VIDEO_BYTES:
                await update.message.reply_document(
                    document=fh, caption=f"📁 {title[:200]}"
                )
            else:
                await update.message.reply_video(
                    video=fh,
                    caption=f"✅ {title[:200]}",
                    supports_streaming=True,
                )
    except Exception:
        logger.exception("Помилка надсилання файлу")
        await update.message.reply_text("❌ Не вдалося надіслати файл.")
    finally:
        try:
            os.remove(filepath)
        except OSError:
            pass

# ── Обробники команд ──────────────────────────────────────────────────────────

async def _download_and_send(
    update: Update,
    url: str,
    platform: str,
    audio: bool = False,
) -> None:
    """Спільна логіка для handle_message та audio_command."""
    user_id = update.message.from_user.id
    lock     = _get_user_lock(user_id)

    if lock.locked():
        await update.message.reply_text(
            "⏳ Зачекай — попереднє завантаження ще триває."
        )
        return

    async with lock:
        msg  = await update.message.reply_text(
            "🎵 Завантажую аудіо..." if audio else "⏳ Починаю завантаження..."
        )
        loop = asyncio.get_running_loop()

        # !! Захоплюємо loop ДО передачі в executor — інакше get_running_loop()
        # з потоку executor кине RuntimeError.
        last_edit: list[float] = [0.0]

        def progress_cb(text: str) -> None:
            now = time.monotonic()
            if now - last_edit[0] < PROGRESS_THROTTLE:
                return
            last_edit[0] = now
            asyncio.run_coroutine_threadsafe(msg.edit_text(text), loop)

        path, title = await loop.run_in_executor(
            None, download_media, url, platform, audio, progress_cb
        )

        if path is None:
            await msg.edit_text(f"❌ {title}")
            return

        try:
            await msg.delete()
        except Exception:
            pass

        await send_media(update, path, title, is_audio=audio)
        stats.record(platform)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()

    # 1. Пряме посилання на відео-файл
    if m := DIRECT_VIDEO_RE.search(text):
        direct_url = m.group(0)
        msg = await update.message.reply_text("⏳ Завантажую пряме відео...")
        loop = asyncio.get_running_loop()
        path, title = await loop.run_in_executor(None, download_direct, direct_url)
        if path is None:
            await msg.edit_text(f"❌ {title}")
            return
        try:
            await msg.delete()
        except Exception:
            pass
        await send_media(update, path, title)
        stats.record("direct")
        return

    # 2. Витягуємо URL і визначаємо платформу
    url = _extract_url(text)
    if not url:
        await update.message.reply_text(
            "❌ Надішли посилання на YouTube, TikTok, Instagram тощо."
        )
        return

    platform = _detect_platform(url)
    if not platform:
        await update.message.reply_text(
            "❌ Платформа не підтримується. Спробуй YouTube, TikTok, Instagram тощо."
        )
        return

    await _download_and_send(update, url, platform, audio=False)


async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reply = update.message.reply_to_message
    if not reply or not reply.text:
        await update.message.reply_text(
            "❌ Дай команду /audio у відповідь на повідомлення з посиланням."
        )
        return

    url = _extract_url(reply.text)
    if not url:
        await update.message.reply_text("❌ У відповіді не знайдено посилання.")
        return

    platform = _detect_platform(url)
    if not platform:
        await update.message.reply_text(
            "❌ Платформа не підтримується для аудіо."
        )
        return

    await _download_and_send(update, url, platform, audio=True)


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(stats.render())


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode="MarkdownV2")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Необроблений виняток:", exc_info=context.error)

# ── Запуск ────────────────────────────────────────────────────────────────────

def _delete_webhook() -> None:
    try:
        requests.get(
            f"https://api.telegram.org/bot{TOKEN}/deleteWebhook",
            params={"drop_pending_updates": "true"},
            timeout=10,
        )
    except Exception:  # noqa: BLE001
        pass


def main() -> None:
    _delete_webhook()

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Бот запущено.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
