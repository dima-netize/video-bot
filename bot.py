"""
Telegram Video Downloader Bot
Підтримувані платформи: YouTube, TikTok, Instagram, Twitter (X)
Завантажує відео без водяних знаків у найкращій якості.
"""
import os
import re
import asyncio
import logging
import traceback
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter, Retry
import yt_dlp
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CommandHandler

# --- Налаштування ---
# Токен береться ТІЛЬКИ зі змінної оточення (Render -> Environment)
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано змінну оточення TOKEN")

# Логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Папка для тимчасових файлів
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Telegram ліміт на відео = 50 МБ
MAX_VIDEO_SIZE_MB = 50
TELEGRAM_MAX_SIZE = MAX_VIDEO_SIZE_MB * 1024 * 1024

# Регулярні вирази для розпізнавання посилань
URL_PATTERNS = {
    "youtube": r"(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)[\w-]+",
    "tiktok": r"(?:https?://)?(?:www\.)?tiktok\.com/@[\w.-]+/video/\d+",
    "instagram": r"(?:https?://)?(?:www\.)?instagram\.com/(?:reel|p)/[\w-]+",
    "twitter": r"(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/[\w]+/status/\d+",
}

# Текст підказки
HELP_TEXT = """
🎥 **Привіт! Я — твій персональний відео-завантажувач.**
Просто кинь мені посилання на:
• YouTube (звичайне, Shorts)
• TikTok
• Instagram (Reels, дописи)
• Twitter (X)

Отримаєш відео у найкращій якості **без водяних знаків**.
"""

# --- Налаштування сесії requests з повторними спробами ---
def get_retry_session(retries=3, backoff_factor=0.5):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# --- Допоміжні функції ---
def make_filepath(prefix: str, url: str, ext: str = "mp4") -> str:
    """Генерує унікальний шлях для файлу."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    short_id = re.sub(r'\W+', '_', url.split("/")[-1])[:30]
    return os.path.join(DOWNLOAD_DIR, f"{prefix}_{short_id}_{timestamp}.{ext}")

# --- Завантажувачі ---
def download_youtube(url: str) -> str | None:
    """YouTube (yt-dlp)"""
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
        'outtmpl': os.path.join(DOWNLOAD_DIR, '%(id)s.%(ext)s'),
        'merge_output_format': 'mp4',
        'quiet': True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filepath = ydl.prepare_filename(info)
            # Якщо файл не mp4, виправляємо
            if not os.path.exists(filepath):
                base, _ = os.path.splitext(filepath)
                filepath = base + ".mp4"
            return filepath
    except Exception as e:
        logger.error(f"YouTube: {e}")
        return None

def download_tiktok(url: str) -> str | None:
    """TikTok через tikwm.com (без водяного знаку)"""
    api_url = "https://tikwm.com/api/"
    session = get_retry_session()
    try:
        resp = session.get(api_url, params={"url": url}, timeout=15)
        data = resp.json()
        if data.get("code") != 0:
            return None
        video_url = data["data"].get("hdplay") or data["data"].get("play")
        if not video_url:
            return None
        filepath = make_filepath("tiktok", url)
        r = session.get(video_url, stream=True, timeout=20)
        if r.status_code != 200:
            return None
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return filepath
    except Exception as e:
        logger.error(f"TikTok: {e}")
        return None

def download_instagram(url: str) -> str | None:
    """Instagram через ddinstagram (публічний проксі)"""
    insta_url = url.replace("instagram.com", "ddinstagram.com")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    session = get_retry_session()
    try:
        resp = session.get(insta_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        # Шукаємо відео на сторінці
        match = re.search(r'<video[^>]+src="([^"]+)"', resp.text)
        if not match:
            return None
        video_url = match.group(1)
        filepath = make_filepath("instagram", url)
        r = session.get(video_url, stream=True, timeout=20)
        if r.status_code != 200:
            return None
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return filepath
    except Exception as e:
        logger.error(f"Instagram: {e}")
        return None

def download_twitter(url: str) -> str | None:
    """Twitter/X через fxtwitter.com"""
    api_url = url.replace("twitter.com", "fxtwitter.com").replace("x.com", "fxtwitter.com")
    session = get_retry_session()
    try:
        resp = session.get(api_url, timeout=15)
        if resp.status_code != 200:
            return None
        # Шукаємо пряме посилання на mp4
        match = re.search(r'<meta property="og:video"[^>]+content="([^"]+)"', resp.text)
        if not match:
            match = re.search(r'<video[^>]+src="([^"]+)"', resp.text)
        if not match:
            return None
        video_url = match.group(1)
        filepath = make_filepath("twitter", url)
        r = session.get(video_url, stream=True, timeout=20)
        if r.status_code != 200:
            return None
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return filepath
    except Exception as e:
        logger.error(f"Twitter: {e}")
        return None

# --- Telegram-обробники ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє вхідне повідомлення з посиланням."""
    text = update.message.text.strip()
    platform = None
    for name, pattern in URL_PATTERNS.items():
        if re.search(pattern, text):
            platform = name
            break
    if not platform:
        await update.message.reply_text("❌ Надішли посилання на YouTube, TikTok, Instagram або Twitter.")
        return

    msg = await update.message.reply_text(f"⏳ Завантажую відео з {platform}...")

    # Вибираємо функцію завантаження
    download_func = {
        "youtube": download_youtube,
        "tiktok": download_tiktok,
        "instagram": download_instagram,
        "twitter": download_twitter,
    }.get(platform)

    if not download_func:
        await msg.edit_text("❌ Помилка платформи.")
        return

    # Запускаємо синхронне завантаження в окремому потоці
    loop = asyncio.get_running_loop()
    try:
        video_path = await loop.run_in_executor(None, download_func, text)
    except Exception as e:
        logger.error(f"Помилка завантаження: {e}")
        video_path = None

    if not video_path or not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
        await msg.edit_text(f"❌ Не вдалося завантажити відео з {platform}. Можливо, воно приватне або тимчасово недоступне.")
        return

    # Надсилаємо файл
    file_size = os.path.getsize(video_path)
    try:
        if file_size > TELEGRAM_MAX_SIZE:
            with open(video_path, "rb") as vid:
                await update.message.reply_document(
                    document=vid,
                    caption=f"📁 Відео з {platform} (оригінальний файл, >50 МБ)"
                )
        else:
            with open(video_path, "rb") as vid:
                await update.message.reply_video(
                    video=vid,
                    caption=f"✅ Ось твоє відео з {platform} без водяних знаків!",
                    supports_streaming=True
                )
        await msg.delete()
    except Exception as e:
        logger.error(f"Помилка надсилання: {e}")
        await msg.edit_text("❌ Помилка надсилання файлу.")
    finally:
        # Видаляємо тимчасовий файл
        if os.path.exists(video_path):
            os.remove(video_path)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логування помилок."""
    logger.error(msg="Помилка при обробці оновлення:", exc_info=context.error)

# --- Головна функція ---
def main():
    """Запуск бота."""
    print("DEBUG: bot.py started", flush=True)
    print("DEBUG: TOKEN exists:", bool(TOKEN), flush=True)

    if not TOKEN:
        raise RuntimeError(
            "Токен не знайдено. Додай змінну TOKEN у Render -> Environment."
        )

    # Створюємо додаток
    app = Application.builder().token(TOKEN).build()

    # Реєструємо обробники
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Бот запущено...")
    app.run_polling()

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("FATAL ERROR:")
        traceback.print_exc()
        raise
