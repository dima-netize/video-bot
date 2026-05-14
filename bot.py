"""
Швидкий Video Downloader Bot
YouTube, TikTok, Instagram, Twitter, Vimeo, Reddit, Facebook, Likee, Snapchat, Pinterest
TikTok через tikwm.com (резерв) | Найкраща якість | Прогрес з ETA
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

MAX_VIDEO_BYTES = 50 * 1024 * 1024   # 50 МБ
PROGRESS_THROTTLE = 1.5              # секунди між оновленнями
PARALLEL_LIMIT = asyncio.Semaphore(3)  # максимум 3 одночасних завантажень

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
        "pinterest": r"(?:https?://)?(?:www\.)?pinterest\.com/pin/\d+",
    }.items()
}

DIRECT_VIDEO_RE = re.compile(r"https?://\S+\.(?:mp4|mov|webm)(?:\?\S*)?", re.IGNORECASE)

HELP_TEXT = (
    "🎥 *Video Downloader Bot*\n"
    "Кинь посилання — отримай відео в найкращій якості\\.\n\n"
    "Підтримую: YouTube, TikTok, Instagram, Twitter/X, Vimeo, "
    "Reddit, Facebook, Likee, Snapchat, Pinterest\\.\n"
    "Також приймаю прямі посилання на \\.mp4/\\.mov/\\.webm\n\n"
    "🎵 `/audio` у відповідь на посилання — тільки звук \\(MP3\\)\\.\n"
    "📊 `/stats` — статистика завантажень\\.\n"
    "🎞 `/formats <url>` — подивитися доступні якості\\.\n\n"
    "⚠️ Якщо YouTube просить авторизацію — додай файл `cookies.txt` "
    "(інструкція в кінці повідомлення після коду)\\."
)

# ── Статистика ────────────────────────────────────────────────────────────────
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

# ── Допоміжні функції ─────────────────────────────────────────────────────────
def _cookies_file() -> str | None:
    return "cookies.txt" if os.path.exists("cookies.txt") else None

def _safe_filename(prefix: str, url: str, ext: str = "mp4") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug = re.sub(r"\W+", "_", url.split("/")[-1])[:30]
    return os.path.join(DOWNLOAD_DIR, f"{prefix}_{slug}_{ts}.{ext}")

def _extract_url(text: str) -> str | None:
    m = re.search(r"https?://\S+", text)
    return m.group(0) if m else None

def _detect_platform(url: str) -> str | None:
    for name, pat in URL_PATTERNS.items():
        if pat.search(url):
            return name
    return None

# ── Завантаження через yt-dlp ─────────────────────────────────────────────────
def _download_via_ytdlp(
    url: str,
    platform: str | None = None,
    audio: bool = False,
    progress_cb: Callable[[str], None] | None = None,
) -> tuple[str | None, str]:
    tiktok_ua = "com.zhiliaoapp.musically/2022600030 (Linux; U; Android 12; en_US; Pixel 6; Build/SP1A.210812.016; Cronet/58.0.2991.0)"
    default_ua = "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"

    opts = {
        "format": "bestaudio/best" if audio else "bestvideo+bestaudio/best",
        "outtmpl": os.path.join(DOWNLOAD_DIR, "%(title)s_%(id)s.%(ext)s"),
        "merge_output_format": None if audio else "mp4",
        "quiet": True,
        "no_warnings": True,
        "http_headers": {"User-Agent": tiktok_ua if platform == "tiktok" else default_ua},
    }

    # YouTube – всі можливі клієнти
    if platform == "youtube":
        opts["extractor_args"] = {
            "youtube": {"player_client": ["android_vr", "android", "web", "tv"]}
        }
    elif platform == "tiktok":
        opts["extractor_args"] = {
            "tiktok": {
                "app_version": "26.2.0",
                "manifest_app_version": "26.2.0",
            }
        }

    if audio:
        opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}]

    ck = _cookies_file()
    if ck:
        opts["cookiefile"] = ck

    def hook(d: dict) -> None:
        if progress_cb and d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            done = d.get("downloaded_bytes", 0)
            speed = d.get("speed") or 0
            if total:
                pct = int(done * 100 / total)
                eta = (total - done) / speed if speed else 0
                eta_str = f" • ETA {int(eta//60)}хв {int(eta%60)}с" if eta else ""
                speed_str = f" • {speed/1_048_576:.1f} MB/s" if speed else ""
                progress_cb(f"⏳ {pct}% ({done/1_048_576:.1f}/{total/1_048_576:.1f} MB{speed_str}{eta_str})")
            elif done:
                progress_cb(f"⏳ Завантажено {done/1_048_576:.1f} MB…")

    if progress_cb:
        opts["progress_hooks"] = [hook]

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filepath = ydl.prepare_filename(info)
            if audio:
                mp3_path = os.path.splitext(filepath)[0] + ".mp3"
                if os.path.exists(mp3_path):
                    filepath = mp3_path
                else:
                    candidates = glob.glob(os.path.join(DOWNLOAD_DIR, f"*{info['id']}*"))
                    audio_exts = {".m4a", ".webm", ".ogg", ".opus", ".mp3", ".aac"}
                    candidates = [c for c in candidates if os.path.splitext(c)[1] in audio_exts]
                    filepath = candidates[0] if candidates else None
            elif not os.path.exists(filepath):
                candidates = glob.glob(os.path.join(DOWNLOAD_DIR, f"*{info['id']}*"))
                filepath = candidates[0] if candidates else None
            if not filepath or not os.path.exists(filepath):
                return None, "Файл після завантаження не знайдено."
            return filepath, info.get("title", "video")
    except Exception as e:
        return None, str(e)

# ── Резервні методи ──────────────────────────────────────────────────────────

def _download_tiktok_fallback(url: str) -> tuple[str | None, str]:
    """Завантажує TikTok через tikwm.com (без водяного знаку)."""
    api = "https://tikwm.com/api/"
    try:
        resp = requests.get(api, params={"url": url}, timeout=15)
        data = resp.json()
        if data.get("code") != 0:
            return None, "tikwm.com не зміг отримати відео"
        video_url = data["data"].get("hdplay") or data["data"].get("play")
        if not video_url:
            return None, "tikwm.com не знайшов пряме посилання"
        filepath = _safe_filename("tiktok", url)
        r = requests.get(video_url, stream=True, timeout=20)
        if r.status_code != 200:
            return None, f"tikwm.com: помилка завантаження {r.status_code}"
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        return filepath, "TikTok без водяного знаку"
    except Exception as e:
        return None, f"tikwm.com: {e}"

def _download_instagram_fallback(url: str) -> tuple[str | None, str]:
    """Резервний метод: ddinstagram.com."""
    insta_url = url.replace("instagram.com", "ddinstagram.com")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        resp = requests.get(insta_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None, "ddinstagram не відповів."
        match = re.search(r'<video[^>]+src="([^"]+)"', resp.text)
        if not match:
            return None, "Відео на сторінці ddinstagram не знайдено."
        video_url = match.group(1)
        filepath = _safe_filename("instagram", url)
        r = requests.get(video_url, stream=True, timeout=20)
        if r.status_code != 200:
            return None, "Не вдалося завантажити відео з ddinstagram."
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        return filepath, "Instagram Reel"
    except Exception as e:
        return None, f"Помилка через ddinstagram: {e}"

# ── Головна функція завантаження (з резервними методами) ─────────────────────
def download_media(
    url: str,
    platform: str | None = None,
    audio: bool = False,
    progress_cb: Callable[[str], None] | None = None,
) -> tuple[str | None, str]:
    """Завантажує відео/аудіо. Для TikTok та Instagram використовує резервні методи."""
    # Спочатку пробуємо yt-dlp
    path, err = _download_via_ytdlp(url, platform, audio, progress_cb)
    if path is not None:
        return path, err  # успіх

    # Якщо TikTok – пробуємо tikwm.com
    if platform == "tiktok":
        logger.info("yt-dlp для TikTok не вдалося, пробуємо tikwm.com")
        fallback_path, fallback_title = _download_tiktok_fallback(url)
        if fallback_path:
            return fallback_path, fallback_title
        else:
            return None, f"yt-dlp: {err[:80]} | tikwm: {fallback_title}"

    # Якщо Instagram – пробуємо ddinstagram
    if platform == "instagram":
        logger.info("yt-dlp для Instagram не вдалося, пробуємо ddinstagram")
        fallback_path, fallback_title = _download_instagram_fallback(url)
        if fallback_path:
            return fallback_path, fallback_title
        else:
            return None, f"yt-dlp: {err[:80]} | ddinstagram: {fallback_title}"

    # Інші платформи – просто помилка
    return None, err

def download_direct(url: str) -> tuple[str | None, str]:
    ext = (url.split(".")[-1].split("?")[0] or "mp4")[:4]
    filepath = _safe_filename("direct", url, ext)
    try:
        with requests.get(url, stream=True, timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as resp:
            resp.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
        return filepath, "Пряме відео"
    except Exception as e:
        if os.path.exists(filepath):
            os.remove(filepath)
        return None, f"Помилка прямого завантаження: {e}"

# ── Надсилання файлу ──────────────────────────────────────────────────────────
async def send_media(update: Update, filepath: str, title: str, is_audio: bool = False) -> None:
    size = os.path.getsize(filepath)
    try:
        with open(filepath, "rb") as f:
            if is_audio:
                await update.message.reply_audio(audio=f, title=title[:64])
            elif size > MAX_VIDEO_BYTES:
                await update.message.reply_document(document=f, caption=f"📁 {title[:200]}")
            else:
                await update.message.reply_video(video=f, caption=f"✅ {title[:200]}", supports_streaming=True)
    except Exception:
        logger.exception("Помилка надсилання")
        await update.message.reply_text("❌ Не вдалося надіслати файл.")
    finally:
        try:
            os.remove(filepath)
        except OSError:
            pass

# ── Обробники команд ──────────────────────────────────────────────────────────
async def _download_and_send(update: Update, url: str, platform: str, audio: bool = False) -> None:
    async with PARALLEL_LIMIT:
        msg = await update.message.reply_text("🎵 Завантажую аудіо..." if audio else "⏳ Починаю завантаження...")
        loop = asyncio.get_running_loop()
        last_edit = [0.0]

        def progress_cb(text: str) -> None:
            now = time.monotonic()
            if now - last_edit[0] < PROGRESS_THROTTLE:
                return
            last_edit[0] = now
            asyncio.run_coroutine_threadsafe(msg.edit_text(text), loop)

        path, title = await loop.run_in_executor(None, download_media, url, platform, audio, progress_cb)
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
    # Пряме відео
    if m := DIRECT_VIDEO_RE.search(text):
        url = m.group(0)
        msg = await update.message.reply_text("⏳ Завантажую пряме відео...")
        loop = asyncio.get_running_loop()
        path, title = await loop.run_in_executor(None, download_direct, url)
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

    # Платформа
    url = _extract_url(text)
    if not url:
        await update.message.reply_text("❌ Надішли посилання на YouTube, TikTok, Instagram тощо.")
        return
    platform = _detect_platform(url)
    if not platform:
        await update.message.reply_text("❌ Платформа не підтримується.")
        return
    await _download_and_send(update, url, platform, audio=False)

async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reply = update.message.reply_to_message
    if not reply or not reply.text:
        await update.message.reply_text("❌ Дай команду /audio у відповідь на повідомлення з посиланням.")
        return
    url = _extract_url(reply.text)
    if not url:
        await update.message.reply_text("❌ У відповіді не знайдено посилання.")
        return
    platform = _detect_platform(url)
    if not platform:
        await update.message.reply_text("❌ Платформа не підтримується для аудіо.")
        return
    await _download_and_send(update, url, platform, audio=True)

async def formats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("❌ Використання: /formats <посилання>")
        return
    url = context.args[0]
    msg = await update.message.reply_text("Отримую формати...")
    try:
        with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
            info = ydl.extract_info(url, download=False)
            fmts = info.get('formats', [])
            out = "🎞 **Доступні формати:**\n"
            for f in fmts[:25]:
                note = f.get('format_note', '?')
                ext = f.get('ext', '')
                size = f.get('filesize') or f.get('filesize_approx')
                size_str = f" ~{size/1_048_576:.1f} MB" if size else ""
                out += f"- {note} ({ext}){size_str}\n"
            await msg.edit_text(out[:4000], parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"Помилка: {str(e)[:200]}")

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
    except Exception:
        pass

def main() -> None:
    _delete_webhook()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("formats", formats_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)
    logger.info("Бот запущено.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
