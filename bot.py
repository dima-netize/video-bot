"""
Універсальний Telegram Video Downloader
YouTube, TikTok, Instagram, Twitter, Vimeo, Reddit, Pinterest
Без водяних знаків | Вибір якості | Прогрес
"""
import os
import re
import asyncio
import logging
import traceback
from datetime import datetime

import yt_dlp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CommandHandler, CallbackQueryHandler

# --- Налаштування ---
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Не задано змінну оточення TOKEN")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

MAX_VIDEO_SIZE_MB = 50
TELEGRAM_MAX_VIDEO = MAX_VIDEO_SIZE_MB * 1024 * 1024

# Розширені патерни
URL_PATTERNS = {
    "youtube": r"(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)[\w-]+",
    "tiktok": r"(?:https?://)?(?:www\.)?(?:tiktok\.com/@[\w.-]+/video/\d+|vt\.tiktok\.com/\w+|vm\.tiktok\.com/\w+)",
    "instagram": r"(?:https?://)?(?:www\.)?instagram\.com/(?:reel|p|tv|stories)/[\w-]+",
    "twitter": r"(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/\w+/status/\d+",
    "vimeo": r"(?:https?://)?(?:www\.)?vimeo\.com/\d+",
    "reddit": r"(?:https?://)?(?:www\.)?reddit\.com/r/\w+/comments/\w+/\w+",
    "pinterest": r"(?:https?://)?(?:www\.)?pinterest\.com/pin/\d+",
}

HELP_TEXT = """
🎥 **Привіт! Я — твій відео-завантажувач.**
Просто кинь мені посилання, і отримаєш відео без водяних знаків.

🔗 **Підтримувані сайти:**
• YouTube (Shorts, звичайні)
• TikTok
• Instagram (Reels, TV, Stories)
• Twitter (X)
• Vimeo
• Reddit (відеопости)
• Pinterest

✨ **Команди:**
/start - Показати це повідомлення
/formats <посилання> - Подивитися доступні якості (YouTube)
"""

def make_filepath(prefix: str, url: str, ext: str = "mp4") -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    short_id = re.sub(r'\W+', '_', url.split("/")[-1])[:30]
    return os.path.join(DOWNLOAD_DIR, f"{prefix}_{short_id}_{timestamp}.{ext}")

# --- Завантаження з підтримкою якості ---
QUALITY_OPTIONS = {
    "1080": "bestvideo[height<=1080]+bestaudio/best",
    "720": "bestvideo[height<=720]+bestaudio/best",
    "480": "bestvideo[height<=480]+bestaudio/best",
    "найкраща": "best",
}

def download_video(url: str, quality: str = "найкраща", platform: str = None) -> tuple:
    """
    Повертає (шлях до файлу, назва відео) або (None, повідомлення про помилку)
    """
    format_str = QUALITY_OPTIONS.get(quality, QUALITY_OPTIONS["найкраща"])

    ydl_opts = {
        'format': format_str,
        'outtmpl': os.path.join(DOWNLOAD_DIR, '%(title)s_%(id)s.%(ext)s'),
        'merge_output_format': 'mp4',
        'quiet': True,
        'no_warnings': True,
        'progress_hooks': [],  # будемо використовувати для прогрес-бару, але поки що просто
    }

    # Спеціальні налаштування для TikTok
    if platform == "tiktok":
        ydl_opts['extractor_args'] = {
            'tiktok': {
                'app_version': '26.2.0',
                'manifest_app_version': '26.2.0',
            }
        }

    # Для YouTube іноді потрібно обійти обмеження
    if platform == "youtube":
        ydl_opts['extractor_args'] = {
            'youtube': {
                'skip': 'webpage',  # пришвидшує отримання даних
            }
        }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            title = info.get('title', 'video')
            # Знайдемо реальний файл
            filepath = ydl.prepare_filename(info)
            if not os.path.exists(filepath):
                # Іноді розширення не mp4, знайдемо будь-який файл з id
                import glob
                possible = glob.glob(os.path.join(DOWNLOAD_DIR, f"*{info['id']}*"))
                if possible:
                    filepath = possible[0]
                else:
                    return None, "Не вдалося знайти завантажений файл."
            return filepath, title
    except Exception as e:
        error_msg = str(e)
        if "Sign in to confirm your age" in error_msg:
            return None, "Це відео має вікові обмеження. Я не можу завантажити його без входу в обліковий запис."
        elif "Video unavailable" in error_msg:
            return None, "Відео недоступне (можливо, приватне або видалене)."
        else:
            return None, f"Помилка завантаження: {error_msg[:200]}"

# --- Клавіатура для вибору якості ---
def quality_keyboard(url: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("1080p", callback_data=f"q|1080|{url}"),
            InlineKeyboardButton("720p", callback_data=f"q|720|{url}"),
        ],
        [
            InlineKeyboardButton("480p", callback_data=f"q|480|{url}"),
            InlineKeyboardButton("Найкраща", callback_data=f"q|best|{url}"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)

# --- Обробники Telegram ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    platform = None
    for name, pattern in URL_PATTERNS.items():
        if re.search(pattern, text):
            platform = name
            break
    if not platform:
        await update.message.reply_text("❌ Надішли пряме посилання на підтримуваний сайт.")
        return

    # Для YouTube пропонуємо вибір якості
    if platform == "youtube":
        await update.message.reply_text(
            "🎬 **YouTube** — обери якість:",
            parse_mode="Markdown",
            reply_markup=quality_keyboard(text)
        )
        return

    # Для інших платформ завантажуємо одразу
    msg = await update.message.reply_text(f"⏳ Завантажую {platform}...")
    loop = asyncio.get_running_loop()
    filepath, title_or_error = await loop.run_in_executor(None, download_video, text, "найкраща", platform)

    if filepath is None:
        await msg.edit_text(f"❌ {title_or_error}")
        return

    await send_video(update, filepath, platform, title_or_error, msg)

async def button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if not data.startswith("q|"):
        return
    _, quality, url = data.split("|")
    quality_label = {"1080": "1080p", "720": "720p", "480": "480p", "best": "найкраща"}
    await query.edit_message_text(f"⏳ Завантажую YouTube у {quality_label.get(quality, quality)}...")
    loop = asyncio.get_running_loop()
    filepath, title_or_error = await loop.run_in_executor(None, download_video, url, quality, "youtube")
    if filepath is None:
        await query.edit_message_text(f"❌ {title_or_error}")
        return
    await send_video(query, filepath, "youtube", title_or_error, None)

async def send_video(update_or_query, filepath, platform, title, status_message=None):
    """Надсилає відео або документ."""
    file_size = os.path.getsize(filepath)
    try:
        if file_size > TELEGRAM_MAX_VIDEO:
            # Надсилаємо як документ
            with open(filepath, "rb") as f:
                if hasattr(update_or_query, 'message'):
                    await update_or_query.message.reply_document(
                        document=f,
                        caption=f"📁 {title} ({platform})"
                    )
                else:  # callback_query
                    await update_or_query.message.reply_document(
                        document=f,
                        caption=f"📁 {title} ({platform})"
                    )
        else:
            with open(filepath, "rb") as f:
                if hasattr(update_or_query, 'message'):
                    await update_or_query.message.reply_video(
                        video=f,
                        caption=f"✅ {title}",
                        supports_streaming=True
                    )
                else:
                    await update_or_query.message.reply_video(
                        video=f,
                        caption=f"✅ {title}",
                        supports_streaming=True
                    )
        if status_message:
            await status_message.delete()
        elif hasattr(update_or_query, 'message'):
            pass  # CallbackQuery вже змінив повідомлення
    except Exception as e:
        logger.error(f"Send error: {e}")
        if status_message:
            await status_message.edit_text("❌ Помилка надсилання файлу.")
        else:
            if hasattr(update_or_query, 'message'):
                await update_or_query.message.reply_text("❌ Помилка надсилання.")
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

async def formats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показує доступні формати для посилання."""
    if not context.args:
        await update.message.reply_text("Використання: /formats <посилання на YouTube>")
        return
    url = context.args[0]
    msg = await update.message.reply_text("Отримую список форматів...")
    try:
        with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
            info = ydl.extract_info(url, download=False)
            formats = info.get('formats', [])
            text = "**Доступні формати:**\n"
            for f in formats[:20]:  # максимум 20 рядків
                note = f.get('format_note', '')
                ext = f.get('ext', '')
                filesize = f.get('filesize', 0)
                if filesize:
                    size_mb = filesize / (1024*1024)
                    text += f"- {note} ({ext}) - {size_mb:.1f} MB\n"
                else:
                    text += f"- {note} ({ext})\n"
            await msg.edit_text(text[:4000], parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"Помилка: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(msg="Помилка при обробці оновлення:", exc_info=context.error)

def main():
    print("DEBUG: bot.py started", flush=True)
    if not TOKEN:
        raise RuntimeError("Токен не знайдено. Додай змінну TOKEN у Render -> Environment.")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("formats", formats_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_click))
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
