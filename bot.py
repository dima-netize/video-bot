#!/usr/bin/env python3
"""
Ultra Video Downloader Bot
Найкраща якість • Резервні методи для TikTok/Instagram • Кеш • Прогрес
"""

import asyncio, glob, logging, os, re, shutil, sys, time, json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

import requests
import yt_dlp
from telegram import Update, BotCommand, BotCommandScopeDefault
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from telegram.error import BadRequest, RetryAfter

# ─── Завантаження змінних оточення (якщо є .env) ──────────────────────────
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
PARALLEL_LIMIT = asyncio.Semaphore(2)
URL_CACHE_TTL = 3600  # 1 година

FFMPEG = shutil.which("ffmpeg")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bot")

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

# ─── Регулярки для платформ ───────────────────────────────────────────────
URL_RE = re.compile(r"https?://[^\s<>\"]+", re.I)
DIRECT_VIDEO_RE = re.compile(r"https?://[^\s<>\"]+\.(?:mp4|mov|webm|m4v)(?:\?[^\s<>\"]*)?", re.I)

PLATFORM_PATTERNS = {
    "youtube": re.compile(r"(?:youtube\.com/(?:watch\?v=|shorts/|live/)|youtu\.be/)", re.I),
    "tiktok": re.compile(r"(?:tiktok\.com|vt\.tiktok\.com|vm\.tiktok\.com)", re.I),
    "instagram": re.compile(r"instagram\.com/(?:reel|p|tv|stories)", re.I),
    "twitter": re.compile(r"(?:twitter\.com|x\.com)/\w+/status/", re.I),
    "facebook": re.compile(r"facebook\.com/(?:watch|reel|videos)", re.I),
    "vimeo": re.compile(r"vimeo\.com/\d+", re.I),
    "reddit": re.compile(r"reddit\.com/r/\w+/comments/", re.I),
    "twitch": re.compile(r"twitch\.tv/(?:videos|clips)/", re.I),
    "dailymotion": re.compile(r"dailymotion\.com/video/", re.I),
    "rumble": re.compile(r"rumble\.com/v", re.I),
    "pinterest": re.compile(r"pinterest\.[a-z]+/pin/\d+", re.I),
    "bilibili": re.compile(r"bilibili\.com/video/", re.I),
    "streamable": re.compile(r"streamable\.com/", re.I),
}

# Простий кеш для file_id
url_cache: Dict[str, Tuple[str, float]] = {}

def extract_urls(text: str) -> List[str]:
    """Витягує до 3 перших URL із тексту."""
    return URL_RE.findall(text)[:3]

def detect_platform(url: str) -> Optional[str]:
    for name, pat in PLATFORM_PATTERNS.items():
        if pat.search(url):
            return name
    return None

def safe_filename(prefix: str, url: str, ext: str = "mp4") -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug = url.split("?")[0].rstrip("/").split("/")[-1] or "video"
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", slug)[:35] or "video"
    return DOWNLOAD_DIR / f"{prefix}_{slug}_{ts}.{ext}"

# ─── Прогрес-бар ───────────────────────────────────────────────────────────
def progress_text(prefix: str, done: int, total: int | None, start: float) -> str:
    elapsed = max(time.monotonic() - start, 0.1)
    speed = done / elapsed if done else 0
    if total:
        pct = min(100, int(done * 100 / total))
        eta = int((total - done) / speed) if speed else 0
        return (
            f"{prefix}\n[{('▓' * (pct // 7)).ljust(14, '░')}] {pct}%\n"
            f"{human_bytes(done)} / {human_bytes(total)}  ⚡ {human_bytes(speed)}/s  ETA {eta}с"
        )
    return f"{prefix}\n{human_bytes(done)}  ⚡ {human_bytes(speed)}/s"

def human_bytes(n: int | float) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024 or unit == "GB":
            return f"{int(n)} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"

# ─── Завантаження через yt-dlp (основний метод) ──────────────────────────
def ytdlp_download(
    url: str, platform: str | None, audio: bool = False,
    progress_cb=None, cancel_event: asyncio.Event | None = None
) -> Tuple[Optional[str], str]:
    ua = "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36"
    if platform == "tiktok":
        ua = "com.zhiliaoapp.musically/2022600030 (Linux; U; Android 12; en_US; Pixel 6; Build/SP1A.210812.016)"

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
        opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}]

    # Спроба з cookies (якщо є файл)
    cookies = BASE_DIR / "cookies.txt"
    if cookies.exists():
        opts["cookiefile"] = str(cookies)

    # YouTube без cookies – набір клієнтів
    if platform == "youtube":
        opts["extractor_args"] = {"youtube": {"player_client": ["android_vr", "android", "web"]}}

    if platform == "tiktok":
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
            # Знайти файл
            filepath = None
            if audio and FFMPEG:
                # спробуємо mp3
                candidate = ydl.prepare_filename(info)
                candidate = str(Path(candidate).with_suffix(".mp3"))
                if Path(candidate).exists():
                    filepath = candidate
            if not filepath:
                filepath = ydl.prepare_filename(info)
            if not os.path.exists(filepath):
                # пошук за id
                vid = info.get("id", "")
                candidates = glob.glob(str(DOWNLOAD_DIR / f"*{vid}*"))
                filepath = candidates[0] if candidates else None
            if not filepath or not os.path.exists(filepath):
                return None, "Файл не знайдено після завантаження"
            return filepath, info.get("title", "video")[:200]
    except Exception as e:
        return None, str(e)[:300]

# ─── Резервні методи ──────────────────────────────────────────────────────
def tiktok_fallback(url: str, progress_cb=None) -> Tuple[Optional[str], str]:
    """Через tikwm.com (без водяного знаку)"""
    try:
        resp = requests.get("https://tikwm.com/api/", params={"url": url, "hd": "1"}, timeout=15)
        data = resp.json()
        if data.get("code") != 0:
            return None, data.get("msg", "tikwm.com помилка")
        video_url = data["data"].get("hdplay") or data["data"].get("play")
        if not video_url:
            return None, "Не знайдено відео"
        # завантажити потік
        filepath = safe_filename("tiktok", url)
        with requests.get(video_url, stream=True, timeout=30) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            done = 0
            start = time.monotonic()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    done += len(chunk)
                    if progress_cb and total:
                        progress_cb(progress_text("⏳ TikTok HD", done, total, start))
        return str(filepath), "TikTok без водяного знаку"
    except Exception as e:
        return None, f"TikTok fallback: {e}"

def instagram_fallback(url: str, progress_cb=None) -> Tuple[Optional[str], str]:
    """Через ddinstagram.com"""
    fixed = url.replace("instagram.com", "ddinstagram.com")
    try:
        resp = requests.get(fixed, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if resp.status_code != 200:
            return None, "ddinstagram не відповів"
        # шукаємо відео
        match = re.search(r'<video[^>]+src="([^"]+)"', resp.text)
        if not match:
            # спроба знайти og:video
            match = re.search(r'property="og:video"[^>]+content="([^"]+)"', resp.text)
        if not match:
            return None, "Відео не знайдено на сторінці"
        video_url = match.group(1).replace("\\u0026", "&")
        filepath = safe_filename("instagram", url)
        # завантаження
        with requests.get(video_url, stream=True, timeout=30) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            done = 0
            start = time.monotonic()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    done += len(chunk)
                    if progress_cb and total:
                        progress_cb(progress_text("⏳ Instagram", done, total, start))
        return str(filepath), "Instagram Reel"
    except Exception as e:
        return None, f"Instagram fallback: {e}"

# ─── Головна функція завантаження ─────────────────────────────────────────
async def download_media(
    url: str, platform: str | None, audio: bool = False,
    progress_cb=None, cancel_event: asyncio.Event | None = None
) -> Tuple[Optional[str], str]:
    """Завантажує відео/аудіо. Повертає (шлях, назва) або (None, помилка)"""
    # Пряме відео
    if DIRECT_VIDEO_RE.search(url) and not audio:
        # спрощене пряме завантаження
        return await asyncio.get_running_loop().run_in_executor(
            None, lambda: direct_download(url, progress_cb)
        )

    # Основна спроба через yt-dlp
    loop = asyncio.get_running_loop()
    path, msg = await loop.run_in_executor(None, ytdlp_download, url, platform, audio, progress_cb, cancel_event)
    if path:
        return path, msg

    # Резервні методи
    if platform == "tiktok" and not audio:
        path2, msg2 = await loop.run_in_executor(None, tiktok_fallback, url, progress_cb)
        if path2:
            return path2, msg2
        return None, f"TikTok: yt-dlp: {msg[:80]} | tikwm: {msg2}"
    if platform == "instagram" and not audio:
        path2, msg2 = await loop.run_in_executor(None, instagram_fallback, url, progress_cb)
        if path2:
            return path2, msg2
        return None, f"Instagram: yt-dlp: {msg[:80]} | ddinstagram: {msg2}"
    return None, msg

def direct_download(url: str, progress_cb=None) -> Tuple[Optional[str], str]:
    """Пряме завантаження відеофайлу"""
    ext = url.split("?")[0].split(".")[-1] or "mp4"
    filepath = safe_filename("direct", url, ext)
    try:
        with requests.get(url, stream=True, timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            done = 0
            start = time.monotonic()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    done += len(chunk)
                    if progress_cb and total:
                        progress_cb(progress_text("⏳ Пряме відео", done, total, start))
        return str(filepath), "Пряме відео"
    except Exception as e:
        if filepath.exists():
            filepath.unlink()
        return None, f"Пряме завантаження: {e}"

# ─── Надсилання в Telegram ─────────────────────────────────────────────────
async def send_media(update: Update, filepath: str, title: str, is_audio: bool = False):
    msg = update.effective_message
    if not msg: return
    size = os.path.getsize(filepath)
    if size > MAX_UPLOAD_BYTES:
        await msg.reply_text("❌ Файл перевищує 50 МБ і не може бути надісланий через бота.")
        return
    try:
        with open(filepath, "rb") as f:
            if is_audio:
                await msg.reply_audio(audio=f, title=title[:64], caption=f"🎵 {title[:180]}")
            else:
                await msg.reply_video(video=f, caption=f"✅ {title[:200]}", supports_streaming=True)
    except Exception as e:
        logger.error(f"Send error: {e}")
        await msg.reply_text("❌ Помилка надсилання файлу.")
    finally:
        try:
            os.remove(filepath)
        except OSError:
            pass

# ─── Кеш ──────────────────────────────────────────────────────────────────
def cache_get(url: str, audio: bool) -> Optional[Tuple[str, str]]:
    key = f"{url}|{'audio' if audio else 'video'}"
    entry = url_cache.get(key)
    if entry and time.time() - entry[1] < URL_CACHE_TTL:
        return entry[0], ""  # file_id, заглушка
    return None

def cache_set(url: str, audio: bool, file_id: str):
    key = f"{url}|{'audio' if audio else 'video'}"
    url_cache[key] = (file_id, time.time())

# ─── Команди бота ─────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎥 *Video Downloader Bot*\n\n"
        "Кинь посилання на відео з YouTube, TikTok, Instagram та інших платформ — я завантажу його в найкращій якості.\n\n"
        "Команди:\n"
        "/video <url> — завантажити відео\n"
        "/audio <url> — завантажити аудіо (MP3)\n"
        "/formats <url> — показати доступні формати\n"
        "/info <url> — інформація про відео\n"
        "/ping — перевірка зв'язку\n"
        "/cancel — скасувати поточне завантаження\n"
        "/queue — активні завантаження\n\n"
        "Просто надішліть посилання — бот сам визначить платформу.",
        parse_mode="Markdown"
    )

async def video_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = context.args[0] if context.args else None
    if not url:
        await update.message.reply_text("❌ Використання: /video <посилання>")
        return
    await process_url(update, url, audio=False)

async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = context.args[0] if context.args else None
    if not url:
        await update.message.reply_text("❌ Використання: /audio <посилання>")
        return
    await process_url(update, url, audio=True)

async def formats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = context.args[0] if context.args else None
    if not url:
        await update.message.reply_text("❌ Використання: /formats <посилання>")
        return
    platform = detect_platform(url)
    if not platform:
        await update.message.reply_text("❌ Платформа не підтримується.")
        return
    status = await update.message.reply_text("🔍 Отримую формати...")
    try:
        def get_formats():
            with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
                info = ydl.extract_info(url, download=False)
                return info.get("formats", [])
        loop = asyncio.get_running_loop()
        fmts = await loop.run_in_executor(None, get_formats)
        if not fmts:
            await status.edit_text("Формати не знайдено.")
            return
        lines = ["🎞 *Доступні формати:*", ""]
        for f in fmts[:25]:
            h = f.get("height")
            ext = f.get("ext", "?")
            size = f.get("filesize") or f.get("filesize_approx")
            note = f"{h}p" if h else (f.get("format_note") or "audio")
            size_str = f" ~{human_bytes(size)}" if size else ""
            lines.append(f"• {note} ({ext}){size_str}")
        await status.edit_text("\n".join(lines)[:3900], parse_mode="Markdown")
    except Exception as e:
        await status.edit_text(f"❌ Помилка: {e}")

async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = context.args[0] if context.args else None
    if not url:
        await update.message.reply_text("❌ Використання: /info <посилання>")
        return
    platform = detect_platform(url)
    if not platform:
        await update.message.reply_text("❌ Платформа не підтримується.")
        return
    status = await update.message.reply_text("🔍 Отримую інформацію...")
    try:
        def get_info():
            with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
                return ydl.extract_info(url, download=False)
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, get_info)
        lines = [
            f"ℹ️ *{info.get('title', 'без назви')}*",
            f"👤 {info.get('uploader') or '—'}",
            f"⏱ {info.get('duration')} сек",
            f"👁 {info.get('view_count', 0)}",
            f"📅 {info.get('upload_date', '—')}",
        ]
        await status.edit_text("\n".join(lines)[:3900], parse_mode="Markdown")
    except Exception as e:
        await status.edit_text(f"❌ Помилка: {e}")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start_t = time.monotonic()
    msg = await update.message.reply_text("🏓")
    ms = int((time.monotonic() - start_t) * 1000)
    await msg.edit_text(f"🏓 Pong! `{ms} ms`")

async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # просто показуємо, що черга порожня (активні завантаження не зберігаємо)
    await update.message.reply_text("Черга порожня.")

# Основний обробник текстових повідомлень
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    urls = extract_urls(text)
    if not urls:
        return  # ігноруємо
    for url in urls[:1]:  # тільки перше посилання
        platform = detect_platform(url)
        if not platform:
            if DIRECT_VIDEO_RE.search(url):
                platform = "direct"
            else:
                await update.message.reply_text("❌ Платформа не підтримується.")
                continue
        await process_url(update, url, audio=False, platform=platform)

async def process_url(update: Update, url: str, audio: bool, platform: str = None):
    if platform is None:
        platform = detect_platform(url)
        if not platform and DIRECT_VIDEO_RE.search(url):
            platform = "direct"
    if not platform:
        await update.message.reply_text("❌ Не вдалося визначити платформу.")
        return

    # Кеш
    cached = cache_get(url, audio)
    if cached:
        file_id, _ = cached
        if audio:
            await update.message.reply_audio(audio=file_id, caption="🎵 (з кешу)")
        else:
            await update.message.reply_video(video=file_id, caption="✅ (з кешу)")
        return

    status = await update.message.reply_text("⏳ Починаю завантаження...")
    loop = asyncio.get_running_loop()
    cancel_event = asyncio.Event()

    last_edit = [0.0]
    def progress_cb(text: str):
        now = time.monotonic()
        if now - last_edit[0] > 1.5:
            asyncio.run_coroutine_threadsafe(status.edit_text(text), loop)
            last_edit[0] = now

    try:
        path, title = await download_media(url, platform, audio, progress_cb, cancel_event)
        if path is None:
            await status.edit_text(f"❌ {title}")
            return
        await status.edit_text("📤 Надсилаю...")
        # Надсилання
        file_size = os.path.getsize(path)
        try:
            with open(path, "rb") as f:
                if audio:
                    msg = await update.message.reply_audio(audio=f, title=title[:64], caption=f"🎵 {title[:180]}")
                else:
                    msg = await update.message.reply_video(video=f, caption=f"✅ {title[:200]}", supports_streaming=True)
                # зберегти file_id в кеш
                if audio and msg.audio:
                    cache_set(url, audio, msg.audio.file_id)
                elif not audio and msg.video:
                    cache_set(url, audio, msg.video.file_id)
        except Exception as e:
            await status.edit_text(f"❌ Помилка надсилання: {e}")
        else:
            await status.delete()
        finally:
            if os.path.exists(path):
                os.remove(path)
    except Exception as e:
        await status.edit_text(f"❌ Помилка: {e}")

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Скасування не реалізовано в повній мірі (потрібно зберігати cancel_event)
    await update.message.reply_text("🛑 Скасування не підтримується в цій версії.")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception while handling update:", exc_info=context.error)

# ─── Налаштування меню команд ─────────────────────────────────────────────
async def setup_commands(app: Application):
    commands = [
        BotCommand("start", "Запустити бота"),
        BotCommand("video", "Завантажити відео"),
        BotCommand("audio", "Завантажити аудіо"),
        BotCommand("formats", "Формати відео"),
        BotCommand("info", "Інформація про відео"),
        BotCommand("ping", "Пінг"),
        BotCommand("cancel", "Скасувати завантаження"),
        BotCommand("queue", "Черга"),
    ]
    try:
        await app.bot.set_my_commands(commands, scope=BotCommandScopeDefault())
    except Exception as e:
        logger.warning(f"Не вдалося встановити меню команд: {e}")

# ─── Головна функція ──────────────────────────────────────────────────────
def main():
    app = Application.builder().token(TOKEN).post_init(setup_commands).build()

    # Обробники команд
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("video", video_command))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("formats", formats_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("queue", queue_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    if WEBHOOK_URL:
        logger.info("Запуск у режимі webhook")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{TOKEN}",
            drop_pending_updates=True,
        )
    else:
        logger.info("Запуск у режимі polling")
        # видалити можливий вебхук
        requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
