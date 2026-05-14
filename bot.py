"""
Telegram бот для завантаження відео без водяних знаків
Платформи: YouTube, TikTok, Instagram, Twitter (X)
Автор: твій помічник
"""
import os
import re
import asyncio
import logging
import traceback
from datetime import datetime

import aiohttp
import yt_dlp
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CommandHandler

# --- Налаштування ---
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано змінну оточення TOKEN")

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Константи ---
DOWNLOAD_DIR = "downloads"
MAX_VIDEO_SIZE_MB = 50
TELEGRAM_MAX_SIZE = MAX_VIDEO_SIZE_MB * 1024 * 1024

# Регулярки для розпізнавання посилань
URL_PATTERNS = {
    "youtube": r"(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)[\w-]+",
    "tiktok": r"(?:https?://)?(?:www\.)?tiktok\.com/@[\w.-]+/video/\d+",
    "instagram": r"(?:https?://)?(?:www\.)?instagram\.com/(?:reel|p)/[\w-]+",
    "twitter": r"(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/[\w]+/status/\d+",
}

HELP_TEXT = """
🎥 Привіт! Я — твій персональний відео-завантажувач.
Просто кинь мені посилання на:
• YouTube (звичайне, Shorts)
• TikTok
• Instagram (Reels, дописи)
• Twitter (X)

Отримаєш відео у найкращій якості без водяних знаків.
"""

# --- Допоміжні функції ---
def make_filepath(prefix: str, url: str, ext: str = "mp4") -> str:
    """Створює унікальний шлях для збереження."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    # Беремо останню частину URL як ідентифікатор
    short_id = re.sub(r'\W+', '_', url.split("/")[-1])[:30]
    return os.path.join(DOWNLOAD_DIR, f"{prefix}_{short_id}_{timestamp}.{ext}")

async def download_file(session: aiohttp.ClientSession, url: str, filepath: str) -> bool:
    """Асинхронно завантажує файл за URL."""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                return False
            with open(filepath, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 1024):
                    f.write(chunk)
        return os.path.exists(filepath)
    except Exception as e:
        logger.error(f"Помилка завантаження {url}: {e}")
        return False

# --- Завантажувачі для кожної платформи ---
async def download_youtube(url: str) -> str | None:
    """Завантаження з YouTube (використовує yt-dlp у окремому потоці)."""
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
        'outtmpl': os.path.join(DOWNLOAD_DIR, '%(id)s.%(ext)s'),
        'merge_output_format': 'mp4',
        'quiet': True,
    }
    try:
        loop = asyncio.get_running_loop()
        # Запускаємо yt-dlp у пулі потоків, щоб не блокувати event loop
        info = await loop.run_in_executor(
            None,
            lambda: _sync_extract(ydl_opts, url)
        )
        if not info:
            return None
        filepath = info.get("requested_downloads", [{}])[0].get("filepath")
        if not filepath:
            # Спробуємо вгадати
            filepath = os.path.join(DOWNLOAD_DIR, f"{info['id']}.mp4")
        return filepath if os.path.exists(filepath) else None
    except Exception as e:
        logger.error(f"YouTube: {e}")
        return None

def _sync_extract(ydl_opts, url):
    """Синхронне вилучення для yt-dlp."""
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(url, download=True)

async def download_tiktok(url: str) -> str | None:
    """TikTok через tikwm.com (асинхронно)."""
    api_url = "https://tikwm.com/api/"
    params = {"url": url}
    filepath = make_filepath("tiktok", url)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(api_url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
            if data.get("code") != 0:
                return None
            video_url = data["data"].get("hdplay") or data["data"].get("play")
            if not video_url:
                return None
            # Завантажуємо саме відео
            success = await download_file(session, video_url, filepath)
            return filepath if success else None
        except Exception as e:
            logger.error(f"TikTok: {e}")
            return None

async def download_instagram(url: str) -> str | None:
    """Instagram через ddinstagram (асинхронно)."""
    insta_url = url.replace("instagram.com", "ddinstagram.com")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(insta_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return await download_instagram_ytdlp(url)
                html = await resp.text()
            # Шукаємо відео-тег
            match = re.search(r'<video[^>]+src="([^"]+)"', html)
            if not match:
                return None
            video_url = match.group(1)
            filepath = make_filepath("instagram", url)
            success = await download_file(session, video_url, filepath)
            return filepath if success else None
        except Exception as e:
            logger.error(f"Instagram dd: {e}")
            return await download_instagram_ytdlp(url)

async def download_instagram_ytdlp(url: str) -> str | None:
    """Резервний метод для Instagram через yt-dlp."""
    ydl_opts = {
        'format': 'best',
        'outtmpl': os.path.join(DOWNLOAD_DIR, '%(id)s.%(ext)s'),
        'quiet': True,
    }
    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, lambda: _sync_extract(ydl_opts, url))
        if info:
            return info.get("requested_downloads", [{}])[0].get("filepath")
    except Exception as e:
        logger.error(f"Instagram yt-dlp: {e}")
    return None

async def download_twitter(url: str) -> str | None:
    """Twitter/X через fxtwitter.com (отримує пряме посилання)."""
    # Замінюємо на fxtwitter для отримання API
    api_url = url.replace("twitter.com", "fxtwitter.com").replace("x.com", "fxtwitter.com")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
            # Шукаємо пряме посилання на mp4
            match = re.search(r'<meta property="og:video"[^>]+content="([^"]+)"', html)
            if not match:
                # Іноді є тільки відео-тег
                match = re.search(r'<video[^>]+src="([^"]+)"', html)
            if not match:
                return None
            video_url = match.group(1)
            filepath = make_filepath("twitter", url)
            success = await download_file(session, video_url, filepath)
            return filepath if success else None
        except Exception as e:
            logger.error(f"Twitter: {e}")
            return None

# --- Основний обробник повідомлень ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    chat_id = update.message.chat_id

    # Визначаємо платформу
    platform = None
    for name, pattern in URL_PATTERNS.items():
        if re.search(pattern, text):
            platform = name
            break
    if not platform:
        await update.message.reply_text("❌ Це не схоже на підтримуване посилання. Надішли посилання на YouTube, TikTok, Instagram або Twitter.")
        return

    # Відправляємо повідомлення про початок
    msg = await update.message.reply_text(f"⏳ Завантажую відео з {platform}...")

    # Викликаємо потрібну функцію
    download_func = {
        "youtube": download_youtube,
        "tiktok": download_tiktok,
        "instagram": download_instagram,
        "twitter": download_twitter,
    }.get(platform)

    if not download_func:
        await msg.edit_text("❌ Помилка: невідома платформа.")
        return

    # Виконуємо завантаження
    video_path = await download_func(text)
    if not video_path or not os.path.exists(video_path):
        await msg.edit_text(f"❌ Не вдалося завантажити відео з {platform}. Можливо, воно приватне або тимчасово недоступне.")
        return

    # Перевіряємо розмір
    file_size = os.path.getsize(video_path)
    if file_size > TELEGRAM_MAX_SIZE:
        # Пробуємо надіслати як документ (до 2 ГБ для ботів)
        try:
            with open(video_path, "rb") as vid:
                await update.message.reply_document(
                    document=vid,
                    caption=f"📁 Відео з {platform} (оригінальний файл, >50 МБ)"
                )
            await msg.delete()
        except Exception as e:
            logger.error(f"Не вдалося надіслати великий файл: {e}")
            await msg.edit_text("❌ Відео завелике навіть для документа (>2 ГБ).")
    else:
        # Надсилаємо як відео
        with open(video_path, "rb") as vid:
            await update.message.reply_video(
                video=vid,
                caption=f"✅ Ось твоє відео з {platform} без водяних знаків!",
                supports_streaming=True
            )
        await msg.delete()

    # Видаляємо тимчасовий файл
    try:
        os.remove(video_path)
        logger.info(f"Видалено: {video_path}")
    except Exception as e:
        logger.warning(f"Не вдалося видалити {video_path}: {e}")

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логування помилок."""
    logger.error(msg="Помилка при обробці оновлення:", exc_info=context.error)

def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    app = Application.builder().token(TOKEN).build()

    # Додаємо обробники
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Бот запущено...")
    app.run_polling()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("FATAL ERROR:")
        traceback.print_exc()
        raise
