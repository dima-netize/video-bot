"""
Універсальний Telegram бот для завантаження відео без водяних знаків.
Платформи: YouTube, TikTok, Instagram, Twitter (X)
Використовує yt-dlp для максимальної надійності.
"""
import os
import re
import asyncio
import logging
import traceback
from datetime import datetime

import yt_dlp
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CommandHandler

# --- Налаштування ---
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано змінну оточення TOKEN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Telegram обмеження: відео до 50 МБ, документи до 2000 МБ
MAX_VIDEO_SIZE_MB = 50
TELEGRAM_MAX_VIDEO = MAX_VIDEO_SIZE_MB * 1024 * 1024
TELEGRAM_MAX_DOCUMENT = 2000 * 1024 * 1024  # 2 ГБ

# Розширені регулярки для всіх типів посилань
URL_PATTERNS = {
    "youtube": r"(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)[\w-]+",
    "tiktok": r"(?:https?://)?(?:www\.)?(?:tiktok\.com/@[\w.-]+/video/\d+|vt\.tiktok\.com/\w+|vm\.tiktok\.com/\w+)",
    "instagram": r"(?:https?://)?(?:www\.)?instagram\.com/(?:reel|p|tv|stories)/[\w-]+",
    "twitter": r"(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/\w+/status/\d+",
}

HELP_TEXT = """
🎥 **Привіт! Я — твій відео-завантажувач.**
Надішли мені посилання на:
• YouTube (Shorts, звичайні)
• TikTok (усі формати)
• Instagram (Reels, TV, Stories)
• Twitter (X)

Отримаєш відео у найкращій якості без водяних знаків.
"""

def make_filepath(prefix: str, url: str, ext: str = "mp4") -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    short_id = re.sub(r'\W+', '_', url.split("/")[-1])[:30]
    return os.path.join(DOWNLOAD_DIR, f"{prefix}_{short_id}_{timestamp}.{ext}")

def download_with_ytdlp(url: str, platform: str = None) -> str | None:
    """Завантажує відео через yt-dlp з оптимальними налаштуваннями."""
    # Базові опції для всіх
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
        'outtmpl': os.path.join(DOWNLOAD_DIR, '%(id)s.%(ext)s'),
        'merge_output_format': 'mp4',
        'quiet': True,
        'no_warnings': True,
    }

    # Спеціальні аргументи для TikTok (емуляція мобільного додатку, щоб отримати без водяного знаку)
    if platform == "tiktok":
        ydl_opts['extractor_args'] = {
            'tiktok': {
                'app_version': '26.2.0',
                'manifest_app_version': '26.2.0',
            }
        }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filepath = ydl.prepare_filename(info)
            # Якщо файл не mp4 (іноді буває mkv/webm), перейменовуємо
            if not os.path.exists(filepath):
                base, _ = os.path.splitext(filepath)
                filepath = base + ".mp4"
            return filepath if os.path.exists(filepath) else None
    except Exception as e:
        logger.error(f"yt-dlp помилка для {url}: {e}")
        return None

def download_instagram_fallback(url: str) -> str | None:
    """Резервний метод для Instagram через ddinstagram (якщо заробить)."""
    try:
        import requests
        insta_url = url.replace("instagram.com", "ddinstagram.com")
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(insta_url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        match = re.search(r'<video[^>]+src="([^"]+)"', resp.text)
        if not match:
            return None
        video_url = match.group(1)
        filepath = make_filepath("instagram", url)
        r = requests.get(video_url, stream=True, timeout=20)
        if r.status_code != 200:
            return None
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return filepath
    except Exception:
        return None

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    platform = None
    for name, pattern in URL_PATTERNS.items():
        if re.search(pattern, text):
            platform = name
            break

    if not platform:
        await update.message.reply_text(
            "❌ Будь ласка, надішли пряме посилання на YouTube, TikTok, Instagram або Twitter."
        )
        return

    msg = await update.message.reply_text(f"⏳ Завантажую відео з {platform}...")

    # Основне завантаження через yt-dlp
    loop = asyncio.get_running_loop()
    video_path = await loop.run_in_executor(None, download_with_ytdlp, text, platform)

    # Якщо Instagram не вийшло через yt-dlp, пробуємо запасний варіант
    if not video_path and platform == "instagram":
        video_path = await loop.run_in_executor(None, download_instagram_fallback, text)

    # Якщо файл порожній або не існує
    if not video_path or not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
        error_msg = f"❌ Не вдалося завантажити відео з {platform}."
        if platform == "instagram":
            error_msg += "\nМожливо, відео приватне. Я можу завантажувати тільки публічні Reels/Stories."
        await msg.edit_text(error_msg)
        return

    # Надсилання файлу
    file_size = os.path.getsize(video_path)
    try:
        if file_size > TELEGRAM_MAX_DOCUMENT:
            await msg.edit_text("❌ Файл завеликий (>2 ГБ), не можу надіслати.")
            return

        if file_size > TELEGRAM_MAX_VIDEO:
            # Надсилаємо як документ
            with open(video_path, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    caption=f"📁 Відео з {platform} (більше 50 МБ)"
                )
        else:
            # Надсилаємо як відео
            with open(video_path, "rb") as f:
                await update.message.reply_video(
                    video=f,
                    caption=f"✅ Тримай відео з {platform} без водяних знаків!",
                    supports_streaming=True
                )
        await msg.delete()
    except Exception as e:
        logger.error(f"Помилка надсилання: {e}")
        await msg.edit_text("❌ Помилка надсилання файлу.")
    finally:
        if os.path.exists(video_path):
            os.remove(video_path)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(msg="Помилка при обробці оновлення:", exc_info=context.error)

def main():
    print("DEBUG: bot.py started", flush=True)
    print("DEBUG: TOKEN exists:", bool(TOKEN), flush=True)

    if not TOKEN:
        raise RuntimeError("Токен не знайдено. Додай змінну TOKEN у Render -> Environment.")

    app = Application.builder().token(TOKEN).build()
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
