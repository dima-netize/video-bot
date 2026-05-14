"""
Універсальний Telegram Video Downloader
Найкраща якість автоматично | YouTube з cookies | Facebook, Likee, Snapchat
Команда /audio для MP3
"""
import os, re, asyncio, logging, traceback, requests
from datetime import datetime
import yt_dlp
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CommandHandler

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
    "facebook": r"(?:https?://)?(?:www\.)?facebook\.com/(?:watch/?v=|[\w.]+/videos/)\d+",
    "likee": r"(?:https?://)?(?:www\.)?likee\.com/v/\w+",
    "snapchat": r"(?:https?://)?(?:www\.)?snapchat\.com/spotlight/\w+",
}

HELP_TEXT = """
🎥 **Привіт! Я — твій відео-завантажувач.**
Просто кинь мені посилання, і отримаєш відео **в найкращій якості** без водяних знаків.

🔗 **Підтримувані сайти:**
• YouTube (Shorts, звичайні)
• TikTok, Instagram, Twitter, Vimeo, Reddit, Pinterest
• Facebook, Likee, Snapchat

🎵 **Команда `/audio`**
Відповідь на повідомлення з посиланням командою `/audio`, і я надішлю тільки звук (MP3).
"""

def get_cookies_file():
    """Шукає cookies.txt у корені проєкту."""
    if os.path.exists("cookies.txt"):
        return "cookies.txt"
    return None

def make_filepath(prefix: str, url: str, ext: str = "mp4") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    sid = re.sub(r'\W+', '_', url.split("/")[-1])[:30]
    return os.path.join(DOWNLOAD_DIR, f"{prefix}_{sid}_{ts}.{ext}")

def download_media(url: str, platform: str = None, audio_only: bool = False):
    """
    Завантажує відео або аудіо. Повертає (шлях_до_файлу, назва) або (None, помилка).
    """
    ydl_opts = {
        'outtmpl': os.path.join(DOWNLOAD_DIR, '%(title)s_%(id)s.%(ext)s'),
        'quiet': True,
        'merge_output_format': 'mp4',
    }

    if audio_only:
        ydl_opts['format'] = 'bestaudio/best'
        ydl_opts['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }]
    else:
        ydl_opts['format'] = 'bestvideo+bestaudio/best'

    # Спеціальні налаштування для TikTok
    if platform == "tiktok":
        ydl_opts['extractor_args'] = {
            'tiktok': {'app_version': '26.2.0', 'manifest_app_version': '26.2.0'}
        }

    # Cookies для YouTube
    cookies = get_cookies_file()
    if cookies:
        ydl_opts['cookiefile'] = cookies
        logger.info("Використовую cookies.txt")
    else:
        # Без cookies додаємо User-Agent, щоб зменшити блокіровку
        ydl_opts['http_headers'] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            # Знаходимо завантажений файл
            if audio_only:
                # Після конвертації розширення .mp3
                filepath = ydl.prepare_filename(info)
                filepath = filepath.rsplit('.', 1)[0] + '.mp3'
            else:
                filepath = ydl.prepare_filename(info)
                if not os.path.exists(filepath):
                    import glob
                    candidates = glob.glob(os.path.join(DOWNLOAD_DIR, f"*{info['id']}*"))
                    filepath = candidates[0] if candidates else None
            if not filepath or not os.path.exists(filepath):
                return None, "Не вдалося знайти завантажений файл."
            return filepath, info.get('title', 'media')
    except Exception as e:
        err = str(e)
        if "Sign in" in err or "confirm you" in err:
            return None, "Потрібна авторизація. Додай файл cookies.txt (див. інструкцію)."
        elif "Video unavailable" in err:
            return None, "Відео недоступне (приватне або видалене)."
        else:
            return None, f"Помилка: {err[:200]}"

async def send_media(update: Update, filepath: str, title: str, is_audio: bool = False):
    """Надсилає файл у чат."""
    size = os.path.getsize(filepath)
    try:
        if is_audio:
            with open(filepath, "rb") as f:
                await update.message.reply_audio(audio=f, title=title[:64])
        elif size > TELEGRAM_MAX_VIDEO:
            with open(filepath, "rb") as f:
                await update.message.reply_document(document=f, caption=f"📁 {title}")
        else:
            with open(filepath, "rb") as f:
                await update.message.reply_video(video=f, caption=f"✅ {title}", supports_streaming=True)
    except Exception as e:
        logger.error(f"Send error: {e}")
        await update.message.reply_text("❌ Помилка надсилання файлу.")
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    platform = None
    for name, pattern in URL_PATTERNS.items():
        if re.search(pattern, text):
            platform = name
            break
    if not platform:
        await update.message.reply_text("❌ Надішли посилання на підтримуваний сайт.")
        return

    msg = await update.message.reply_text(f"⏳ Завантажую {platform} у найкращій якості...")
    loop = asyncio.get_running_loop()
    filepath, title = await loop.run_in_executor(None, download_media, text, platform, False)
    if filepath is None:
        await msg.edit_text(f"❌ {title}")
        return
    await send_media(update, filepath, title)
    await msg.delete()

async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє команду /audio у відповідь на повідомлення з посиланням."""
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Будь ласка, дай команду `/audio` у відповідь на повідомлення з посиланням.")
        return
    text = update.message.reply_to_message.text.strip()
    platform = None
    for name, pattern in URL_PATTERNS.items():
        if re.search(pattern, text):
            platform = name
            break
    if not platform:
        await update.message.reply_text("❌ У відповіді має бути посилання на підтримуваний сайт.")
        return

    msg = await update.message.reply_text(f"🎵 Завантажую аудіо з {platform}...")
    loop = asyncio.get_running_loop()
    filepath, title = await loop.run_in_executor(None, download_media, text, platform, True)
    if filepath is None:
        await msg.edit_text(f"❌ {title}")
        return
    await send_media(update, filepath, title, is_audio=True)
    await msg.delete()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(msg="Помилка при обробці оновлення:", exc_info=context.error)

def main():
    # Скидаємо вебхук, щоб уникнути конфліктів
    try:
        requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except Exception as e:
        logger.warning(f"Webhook cleanup failed: {e}")

    if not TOKEN:
        raise RuntimeError("Токен не знайдено. Додай змінну TOKEN у Render -> Environment.")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("audio", audio_command))
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
