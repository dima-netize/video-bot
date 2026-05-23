#!/usr/bin/env python3
"""
Video Downloader Bot — Покращена версія
Підтримка cookies • Резервні методи • Прогрес • Кеш
"""

import asyncio
import glob
import logging
import os
import re
import shutil
import sys
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

import requests
import yt_dlp
from telegram import Update, BotCommand, BotCommandScopeDefault
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters
)
from telegram.error import BadRequest, RetryAfter

# ─── Завантаження токена ────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("❌ Не задано TOKEN у змінних оточення!")

WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").rstrip("/") or None
PORT = int(os.environ.get("PORT", "5000"))
MAX_UPLOAD_BYTES = 49 * 1024 * 1024  # 49 MB

# Обмеження на кількість паралельних завантажень (щоб не заблокували)
PARALLEL_LIMIT = asyncio.Semaphore(2)

# Перевірка наявності ffmpeg (потрібен для злиття відео+аудіо)
FFMPEG = shutil.which("ffmpeg")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bot")

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

# ─── Регулярки ──────────────────────────────────────────────
URL_RE = re.compile(r"https?://[^\s<>\"]+", re.I)
DIRECT_VIDEO_RE = re.compile(r"https?://[^\s<>\"]+\.(?:mp4|mov|webm|m4v)(?:\?[^\s<>\"]*)?", re.I)

PLATFORM_PATTERNS = {
    "youtube":    re.compile(r"(?:youtube\.com/(?:watch\?v=|shorts/|live/)|youtu\.be/)", re.I),
    "tiktok":     re.compile(r"(?:tiktok\.com|vt\.tiktok\.com|vm\.tiktok\.com)", re.I),
    "instagram":  re.compile(r"instagram\.com/(?:reel|p|tv|stories)", re.I),
    "twitter":    re.compile(r"(?:twitter\.com|x\.com)/\w+/status/", re.I),
    "facebook":   re.compile(r"facebook\.com/(?:watch|reel|videos)", re.I),
    "vimeo":      re.compile(r"vimeo\.com/\d+", re.I),
    "reddit":     re.compile(r"reddit\.com/r/\w+/comments/", re.I),
    "twitch":     re.compile(r"twitch\.tv/(?:videos|clips)/", re.I),
    "dailymotion":re.compile(r"dailymotion\.com/video/", re.I),
    "rumble":     re.compile(r"rumble\.com/v", re.I),
    "pinterest":  re.compile(r"pinterest\.[a-z]+/pin/\d+", re.I),
    "bilibili":   re.compile(r"bilibili\.com/video/", re.I),
    "streamable": re.compile(r"streamable\.com/", re.I),
}

# Кеш file_id (url -> (file_id, час))
url_cache: Dict[str, Tuple[str, float]] = {}

def extract_urls(text: str) -> List[str]:
    """Повертає перше знайдене посилання в тексті."""
    return URL_RE.findall(text)[:1]

def detect_platform(url: str) -> Optional[str]:
    for name, pat in PLATFORM_PATTERNS.items():
        if pat.search(url):
            return name
    return None

def safe_filename(prefix: str, url: str, ext: str = "mp4") -> Path:
    """Генерує унікальне ім'я файлу, уникаючи колізій."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", url.split("?")[0].split("/")[-1][:35]) or "video"
    return DOWNLOAD_DIR / f"{prefix}_{slug}_{ts}.{ext}"

def human_bytes(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} GB"

# ─── Прогрес-бар ────────────────────────────────────────────
def progress_text(prefix: str, done: int, total: Optional[int], start: float) -> str:
    elapsed = max(time.monotonic() - start, 0.1)
    speed = done / elapsed if done else 0
    if total:
        percent = min(100, int(done * 100 / total))
        eta = int((total - done) / speed) if speed else 0
        bar = "▓" * (percent // 7) + "░" * (14 - percent // 7)
        return f"{prefix}\n[{bar}] {percent}%\n{human_bytes(done)} / {human_bytes(total)}  ⚡ {human_bytes(speed)}/s  ETA {eta}с"
    return f"{prefix}\n{human_bytes(done)}  ⚡ {human_bytes(speed)}/s"

# ─── yt-dlp з cookies та розширеними опціями ───────────────
def ytdlp_download(url: str, platform: Optional[str], audio: bool = False,
                   progress_cb=None, cancel_event=None) -> Tuple[Optional[str], str]:
    """
    Завантажує відео/аудіо за допомогою yt-dlp.
    Використовує файл cookies.txt, якщо він існує.
    """
    # Базовий User-Agent
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    if platform == "tiktok":
        ua = "com.zhiliaoapp.musically/2022600030 (Linux; U; Android 12; en_US; Pixel 6; Build/SP1A.210812.016)"
    elif platform == "instagram":
        ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1"

    opts = {
        "format": "bestaudio/best" if audio else "bestvideo+bestaudio/best",
        "outtmpl": str(DOWNLOAD_DIR / "%(title).80s_%(id)s.%(ext)s"),
        "merge_output_format": "mp4" if not audio else None,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "retries": 5,
        "socket_timeout": 30,
        "http_headers": {"User-Agent": ua},
        "progress_hooks": [],
    }

    if FFMPEG and not audio:
        opts["ffmpeg_location"] = FFMPEG
    if audio and FFMPEG:
        opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192"
        }]

    # --- Підключення cookies (важливо для YouTube та Instagram) ---
    cookies_path = BASE_DIR / "cookies.txt"
    if cookies_path.exists():
        opts["cookiefile"] = str(cookies_path)
        logger.info("Використовую cookies із %s", cookies_path)
    else:
        logger.warning("Файл cookies.txt не знайдено. Функціонал може бути обмежений.")

    # Спеціальні параметри для різних платформ
    if platform == "youtube":
        opts["extractor_args"] = {
            "youtube": {
                "player_client": ["android_vr", "android", "ios", "web", "tv_embedded"],
                "skip": ["webpage"]
            }
        }
        opts["http_headers"].update({
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.youtube.com",
            "Referer": "https://www.youtube.com/"
        })
    elif platform == "instagram":
        opts["extractor_args"] = {"instagram": {"api": "web"}}
        opts["http_headers"].update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "X-IG-App-ID": "936619743392459",
        })
    elif platform == "tiktok":
        opts["extractor_args"] = {"tiktok": {"app_version": "28.0.0", "manifest_app_version": "28.0.0"}}

    start_time = time.monotonic()

    def hook(d):
        if cancel_event and cancel_event.is_set():
            raise Exception("Cancelled")
        if progress_cb and d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            done = d.get("downloaded_bytes", 0)
            progress_cb(progress_text("⏳ Завантажую", done, total, start_time))

    opts["progress_hooks"].append(hook)

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filepath = None
            if audio and FFMPEG:
                candidate = ydl.prepare_filename(info)
                candidate_mp3 = str(Path(candidate).with_suffix(".mp3"))
                if os.path.exists(candidate_mp3):
                    filepath = candidate_mp3
            if not filepath:
                filepath = ydl.prepare_filename(info)
            if not os.path.exists(filepath):
                vid = info.get("id", "")
                candidates = glob.glob(str(DOWNLOAD_DIR / f"*{vid}*"))
                filepath = candidates[0] if candidates else None
            if not filepath or not os.path.exists(filepath):
                return None, "Файл не знайдено після завантаження"
            title = info.get("title", "video")[:200]
            return filepath, title
    except Exception as e:
        return None, str(e)[:300]

# ─── Резервний метод для TikTok (tikwm.com) ─────────────────
def tiktok_fallback(url: str, progress_cb=None) -> Tuple[Optional[str], str]:
    try:
        resp = requests.get("https://tikwm.com/api/", params={"url": url, "hd": "1"}, timeout=15)
        data = resp.json()
        if data.get("code") != 0:
            return None, data.get("msg", "tikwm.com помилка")
        video_url = data["data"].get("hdplay") or data["data"].get("play")
        if not video_url:
            return None, "Не знайдено відео"
        filepath = safe_filename("tiktok", url)
        with requests.get(video_url, stream=True, timeout=30) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            done = 0
            start = time.monotonic()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
                    done += len(chunk)
                    if progress_cb and total:
                        progress_cb(progress_text("⏳ TikTok HD (резерв)", done, total, start))
        return str(filepath), "TikTok без водяного знаку (резерв)"
    except Exception as e:
        return None, f"TikTok fallback: {e}"

# ─── Резервний метод для Instagram (прямий API) ─────────────
def instagram_fallback(url: str, progress_cb=None) -> Tuple[Optional[str], str]:
    """
    Намагається отримати пряме відео через публічний Instagram API.
    Працює лише для публічних акаунтів.
    """
    try:
        shortcode_match = re.search(r"/(?:reel|p|tv)/([^/?]+)", url)
        if not shortcode_match:
            return None, "Не вдалося витягнути shortcode"
        shortcode = shortcode_match.group(1)

        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "X-IG-App-ID": "936619743392459",
        }
        api_url = f"https://www.instagram.com/p/{shortcode}/?__a=1&__d=1"
        resp = requests.get(api_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None, f"API повернув {resp.status_code}"

        data = resp.json()
        video_url = None
        try:
            items = data["items"][0]
            if "video_versions" in items:
                video_url = items["video_versions"][0]["url"]
            elif "carousel_media" in items:
                for media in items["carousel_media"]:
                    if "video_versions" in media:
                        video_url = media["video_versions"][0]["url"]
                        break
        except (KeyError, IndexError):
            pass

        if not video_url:
            return None, "Не знайдено відео в Instagram API"

        filepath = safe_filename("instagram", url)
        with requests.get(video_url, stream=True, timeout=30) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            done = 0
            start = time.monotonic()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
                    done += len(chunk)
                    if progress_cb and total:
                        progress_cb(progress_text("⏳ Instagram (API резерв)", done, total, start))
        return str(filepath), "Instagram Reel (резерв)"
    except Exception as e:
        return None, f"Instagram fallback: {e}"

# ─── Пряме завантаження (mp4) ───────────────────────────────
def direct_download(url: str, progress_cb=None) -> Tuple[Optional[str], str]:
    ext = url.split("?")[0].split(".")[-1] or "mp4"
    filepath = safe_filename("direct", url, ext)
    try:
        with requests.get(url, stream=True, timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            done = 0
            start = time.monotonic()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
                    done += len(chunk)
                    if progress_cb and total:
                        progress_cb(progress_text("⏳ Пряме відео", done, total, start))
        return str(filepath), "Пряме відео"
    except Exception as e:
        if filepath.exists():
            filepath.unlink()
        return None, f"Пряме завантаження: {e}"

# ─── Основна функція завантаження з резервом ─────────────────
async def download_media(url: str, platform: Optional[str], audio: bool = False,
                         progress_cb=None, cancel_event=None) -> Tuple[Optional[str], str]:
    # Якщо пряме посилання на mp4 – качаємо напряму
    if DIRECT_VIDEO_RE.search(url) and not audio:
        return await asyncio.get_running_loop().run_in_executor(
            None, direct_download, url, progress_cb)

    loop = asyncio.get_running_loop()
    # Спроба через yt-dlp (з cookies)
    path, msg = await loop.run_in_executor(None, ytdlp_download, url, platform, audio, progress_cb, cancel_event)
    if path:
        return path, msg

    # Резервні методи для конкретних платформ (тільки відео)
    if platform == "tiktok" and not audio:
        path2, msg2 = await loop.run_in_executor(None, tiktok_fallback, url, progress_cb)
        if path2:
            return path2, msg2
        return None, f"TikTok: yt-dlp: {msg[:80]} | резерв: {msg2}"
    if platform == "instagram" and not audio:
        path2, msg2 = await loop.run_in_executor(None, instagram_fallback, url, progress_cb)
        if path2:
            return path2, msg2
        return None, f"Instagram: yt-dlp: {msg[:80]} | API: {msg2}"
    return None, msg

# ─── Надсилання файлу в Telegram ────────────────────────────
async def send_media(update: Update, filepath: str, title: str, is_audio=False, url: str = ""):
    msg = update.effective_message
    if not msg:
        return
    size = os.path.getsize(filepath)
    if size > MAX_UPLOAD_BYTES:
        await msg.reply_text("❌ Файл більше 49 МБ – Telegram не дозволяє надіслати.")
        return
    try:
        with open(filepath, "rb") as f:
            if is_audio:
                sent = await msg.reply_audio(audio=f, title=title[:64], caption=f"🎵 {title[:180]}")
            else:
                sent = await msg.reply_video(video=f, caption=f"✅ {title[:200]}", supports_streaming=True)
        # Кешуємо file_id
        if url:
            cache_key = f"{url}|{'audio' if is_audio else 'video'}"
            file_id = sent.audio.file_id if is_audio else sent.video.file_id
            url_cache[cache_key] = (file_id, time.time())
    except Exception as e:
        logger.error(f"Помилка надсилання: {e}")
        await msg.reply_text("❌ Не вдалося надіслати відео/аудіо.")
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

# ─── Обробка URL (основна логіка) ───────────────────────────
async def process_url(update: Update, url: str, audio: bool):
    platform = detect_platform(url)
    if not platform:
        if DIRECT_VIDEO_RE.search(url):
            platform = "direct"
        else:
            await update.message.reply_text("❌ Платформа не підтримується.\nСпробуйте YouTube, TikTok, Instagram, Twitter, Vimeo тощо.")
            return

    # Перевірка кешу
    cache_key = f"{url}|{'audio' if audio else 'video'}"
    cached = url_cache.get(cache_key)
    if cached and time.time() - cached[1] < 3600:  # 1 година
        file_id = cached[0]
        if audio:
            await update.message.reply_audio(audio=file_id, caption="🎵 (з кешу)")
        else:
            await update.message.reply_video(video=file_id, caption="✅ (з кешу)")
        return

    status_msg = await update.message.reply_text("⏳ Підготовка до завантаження...")
    loop = asyncio.get_running_loop()
    cancel_event = asyncio.Event()
    last_edit = [0.0]

    def progress_cb(text: str):
        now = time.monotonic()
        if now - last_edit[0] > 1.5:
            asyncio.run_coroutine_threadsafe(status_msg.edit_text(text), loop)
            last_edit[0] = now

    try:
        path, title = await download_media(url, platform, audio, progress_cb, cancel_event)
        if path is None:
            await status_msg.edit_text(f"❌ Не вдалося завантажити:\n{title}")
            return
        await status_msg.edit_text("📤 Надсилаю в Telegram...")
        await send_media(update, path, title, audio, url)
        await status_msg.delete()
    except Exception as e:
        logger.exception("Помилка в process_url")
        await status_msg.edit_text(f"❌ Помилка: {e}")

# ─── Команди бота ───────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cookies_exists = (BASE_DIR / "cookies.txt").exists()
    cookies_note = "🍪 **Cookies знайдено** – YouTube/Instagram будуть працювати краще." if cookies_exists else "🍪 **Немає cookies.txt** – покладіть файл cookies поряд з ботом для обходу обмежень."
    await update.message.reply_text(
        f"🎥 *Video Downloader Bot*\n\n"
        f"{cookies_note}\n\n"
        f"📌 Підтримувані платформи:\n"
        f"YouTube, TikTok, Instagram, Twitter, Facebook, Vimeo, Reddit та інші.\n\n"
        f"📖 Команди:\n"
        f"/video `<url>` – відео\n"
        f"/audio `<url>` – аудіо\n"
        f"/formats `<url>` – всі формати\n"
        f"/info `<url>` – інформація\n"
        f"/ping – перевірка\n\n"
        f"Або просто надішліть посилання.",
        parse_mode="Markdown"
    )

async def video_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = context.args[0] if context.args else None
    if not url:
        await update.message.reply_text("❌ Використання: `/video https://...`", parse_mode="Markdown")
        return
    await process_url(update, url, audio=False)

async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = context.args[0] if context.args else None
    if not url:
        await update.message.reply_text("❌ Використання: `/audio https://...`", parse_mode="Markdown")
        return
    await process_url(update, url, audio=True)

async def formats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = context.args[0] if context.args else None
    if not url:
        await update.message.reply_text("❌ Використання: `/formats https://...`", parse_mode="Markdown")
        return
    platform = detect_platform(url)
    if not platform:
        await update.message.reply_text("❌ Платформа не підтримується.")
        return
    status = await update.message.reply_text("🔍 Отримую формати...")
    try:
        def get_formats():
            opts = {"quiet": True}
            cookies_path = BASE_DIR / "cookies.txt"
            if cookies_path.exists():
                opts["cookiefile"] = str(cookies_path)
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False).get("formats", [])
        loop = asyncio.get_running_loop()
        fmts = await loop.run_in_executor(None, get_formats)
        if not fmts:
            await status.edit_text("Формати не знайдено.")
            return
        lines = ["🎞 *Формати (до 20):*"]
        for f in fmts[:20]:
            height = f.get("height")
            ext = f.get("ext", "?")
            size = f.get("filesize") or f.get("filesize_approx")
            note = f"{height}p" if height else f.get("format_note", "audio")
            size_str = human_bytes(size) if size else ""
            lines.append(f"• {note} ({ext}) {size_str}")
        await status.edit_text("\n".join(lines)[:3900], parse_mode="Markdown")
    except Exception as e:
        await status.edit_text(f"❌ Помилка: {e}")

async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = context.args[0] if context.args else None
    if not url:
        await update.message.reply_text("❌ Використання: `/info https://...`", parse_mode="Markdown")
        return
    platform = detect_platform(url)
    if not platform:
        await update.message.reply_text("❌ Платформа не підтримується.")
        return
    status = await update.message.reply_text("🔍 Отримую інформацію...")
    try:
        def get_info():
            opts = {"quiet": True}
            cookies_path = BASE_DIR / "cookies.txt"
            if cookies_path.exists():
                opts["cookiefile"] = str(cookies_path)
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)
        info = await asyncio.get_running_loop().run_in_executor(None, get_info)
        text = (
            f"ℹ️ *{info.get('title', '—')[:200]}*\n"
            f"👤 {info.get('uploader', '—')}\n"
            f"⏱ {info.get('duration', 0)} сек\n"
            f"👁 {info.get('view_count', 0)}\n"
            f"📅 {info.get('upload_date', '—')}"
        )
        await status.edit_text(text[:3900], parse_mode="Markdown")
    except Exception as e:
        await status.edit_text(f"❌ Помилка: {e}")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = time.monotonic()
    msg = await update.message.reply_text("🏓")
    ms = int((time.monotonic() - t) * 1000)
    await msg.edit_text(f"🏓 Pong! `{ms} ms`", parse_mode="Markdown")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    urls = extract_urls(text)
    if not urls:
        return
    await process_url(update, urls[0], audio=False)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Помилка:", exc_info=context.error)

# ─── Налаштування команд меню ───────────────────────────────
async def setup_commands(app: Application):
    cmds = [
        BotCommand("start", "Запустити бота"),
        BotCommand("video", "Завантажити відео"),
        BotCommand("audio", "Завантажити аудіо (mp3)"),
        BotCommand("formats", "Показати доступні формати"),
        BotCommand("info", "Інформація про відео"),
        BotCommand("ping", "Перевірка затримки"),
    ]
    await app.bot.set_my_commands(cmds, scope=BotCommandScopeDefault())

# ─── Головна функція ────────────────────────────────────────
def main():
    app = Application.builder().token(TOKEN).post_init(setup_commands).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("video", video_command))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("formats", formats_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    if WEBHOOK_URL:
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{TOKEN}",
            drop_pending_updates=True
        )
    else:
        requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
