"""
Універсальний Video Downloader Bot
Найкраща якість | Прогрес зі швидкістю | Прямі посилання | YouTube, TikTok, Instagram, Twitter, Vimeo, Reddit, Facebook, Likee, Snapchat
"""
import os, re, asyncio, logging, traceback, time
from datetime import datetime
import requests
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
MAX_VIDEO_MB = 50
TELEGRAM_MAX = MAX_VIDEO_MB * 1024 * 1024

# Патерни для розпізнавання платформ
URL_PATTERNS = {
    "youtube": r"(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)[\w-]+",
    "tiktok": r"(?:https?://)?(?:www\.)?(?:tiktok\.com/@[\w.-]+/video/\d+|vt\.tiktok\.com/\w+|vm\.tiktok\.com/\w+)",
    "instagram": r"(?:https?://)?(?:www\.)?instagram\.com/(?:reel|p|tv|stories)/[\w-]+",
    "twitter": r"(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/\w+/status/\d+",
    "vimeo": r"(?:https?://)?(?:www\.)?vimeo\.com/\d+",
    "reddit": r"(?:https?://)?(?:www\.)?reddit\.com/r/\w+/comments/\w+/\w+",
    "facebook": r"(?:https?://)?(?:www\.)?facebook\.com/(?:watch/?v=|[\w.]+/videos/)\d+",
    "likee": r"(?:https?://)?(?:www\.)?likee\.com/v/\w+",
    "snapchat": r"(?:https?://)?(?:www\.)?snapchat\.com/spotlight/\w+",
}
# Прямі посилання на відеофайли
DIRECT_VIDEO_PATTERN = r"https?://\S+\.(?:mp4|mov|webm)(?:\?\S*)?"

HELP_TEXT = """
🎥 **Вітаю! Я — Video Downloader Bot.**
Надішли мені посилання — і я миттєво завантажу відео в найкращій якості **без водяних знаків**.

🔗 **Підтримую:**
YouTube • TikTok • Instagram • Twitter • Vimeo • Reddit • Facebook • Likee • Snapchat
🎞️ Також приймаю **прямі посилання** на .mp4 / .mov / .webm

🎵 **Команди:**
/audio (у відповідь на повідомлення з посиланням) — надішлю тільки музику (MP3)
/formats <посилання> — покажу доступні якості без завантаження
/stats — статистика завантажень бота
"""

# Статистика
stats = {"total": 0, "platforms": {}}

def get_cookies():
    if os.path.exists("cookies.txt"):
        return "cookies.txt"
    return None

def make_filepath(prefix, url, ext="mp4"):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    sid = re.sub(r'\W+', '_', url.split("/")[-1])[:30]
    return os.path.join(DOWNLOAD_DIR, f"{prefix}_{sid}_{ts}.{ext}")

# --- Клас для оновлення прогресу ---
class ProgressUpdater:
    def __init__(self, msg, loop):
        self.msg = msg
        self.loop = loop
        self.last_percent = -1
        self.start_time = time.time()
    def hook(self, d):
        if d['status'] == 'downloading':
            downloaded = d.get('downloaded_bytes', 0)
            total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
            if total:
                percent = int(downloaded * 100 / total)
                speed = d.get('speed', 0)
                if speed:
                    speed_mb = speed / (1024*1024)
                    text = f"⏳ {percent}% ({downloaded/(1024*1024):.1f} / {total/(1024*1024):.1f} MB) • {speed_mb:.1f} MB/s"
                else:
                    text = f"⏳ {percent}% ({downloaded/(1024*1024):.1f} / {total/(1024*1024):.1f} MB)"
                if percent != self.last_percent:
                    asyncio.run_coroutine_threadsafe(self._update(text), self.loop)
                    self.last_percent = percent
    async def _update(self, text):
        try:
            await self.msg.edit_text(text)
        except:
            pass

# --- Завантаження через yt-dlp ---
def download_ytdlp(url, platform=None, audio_only=False):
    progress = None  # буде заповнено пізніше
    opts = {
        'format': 'bestaudio/best' if audio_only else 'bestvideo+bestaudio/best',
        'outtmpl': os.path.join(DOWNLOAD_DIR, '%(title)s_%(id)s.%(ext)s'),
        'merge_output_format': 'mp4' if not audio_only else None,
        'quiet': True,
        'http_headers': {'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36'},
    }
    if audio_only:
        opts['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }]
    else:
        opts['postprocessors'] = [{'key': 'FFmpegFixupM4a'}]  # лагодження звуку

    # YouTube без cookies – мобільний клієнт
    if platform == "youtube":
        opts['extractor_args'] = {'youtube': {'player_client': ['android', 'web']}}

    # Додаємо cookies якщо є
    cookies = get_cookies()
    if cookies:
        opts['cookiefile'] = cookies
        logger.info("Використовую cookies.txt")

    # Зберігаємо прогрес
    def progress_hook(d):
        if progress:
            progress.hook(d)

    opts['progress_hooks'] = [progress_hook]

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if audio_only:
                filepath = ydl.prepare_filename(info)
                filepath = filepath.rsplit('.', 1)[0] + '.mp3'
            else:
                filepath = ydl.prepare_filename(info)
                if not os.path.exists(filepath):
                    import glob
                    candidates = glob.glob(os.path.join(DOWNLOAD_DIR, f"*{info['id']}*"))
                    filepath = candidates[0] if candidates else None
            if not filepath or not os.path.exists(filepath):
                return None, "Файл не знайдено"
            return filepath, info.get('title', 'video')
    except Exception as e:
        err = str(e)
        if "Sign in" in err:
            return None, "Потрібна авторизація (YouTube вимагає вхід). Спробуйте пізніше або додайте файл cookies.txt"
        return None, f"Помилка: {err[:200]}"

# --- Завантаження прямих посилань ---
def download_direct(url):
    try:
        resp = requests.get(url, stream=True, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36'
        })
        resp.raise_for_status()
        total = int(resp.headers.get('content-length', 0))
        filepath = make_filepath("direct", url, url.split('.')[-1].split('?')[0])
        downloaded = 0
        with open(filepath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
        return filepath, "Пряме відео"
    except Exception as e:
        return None, f"Помилка прямого завантаження: {str(e)[:200]}"

# --- Надсилання файлу ---
async def send_file(update, filepath, title, is_audio=False):
    size = os.path.getsize(filepath)
    try:
        if is_audio:
            with open(filepath, 'rb') as f:
                await update.message.reply_audio(audio=f, title=title[:64])
        elif size > TELEGRAM_MAX:
            with open(filepath, 'rb') as f:
                await update.message.reply_document(document=f, caption=f"📁 {title}")
        else:
            with open(filepath, 'rb') as f:
                await update.message.reply_video(video=f, caption=f"✅ {title}", supports_streaming=True)
    except Exception as e:
        logger.error(f"Send error: {e}")
        await update.message.reply_text("❌ Не вдалося надіслати файл.")
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

# --- Обробник повідомлень ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    # Перевірка на пряме відео
    if re.match(DIRECT_VIDEO_PATTERN, text):
        msg = await update.message.reply_text("⏳ Завантажую пряме відео...")
        loop = asyncio.get_running_loop()
        filepath, title = await loop.run_in_executor(None, download_direct, text)
        if filepath is None:
            await msg.edit_text(f"❌ {title}")
            return
        await msg.delete()
        await send_file(update, filepath, title)
        stats['total'] += 1
        stats['platforms']['direct'] = stats['platforms'].get('direct', 0) + 1
        return

    # Визначаємо платформу
    platform = None
    for name, pat in URL_PATTERNS.items():
        if re.search(pat, text):
            platform = name
            break
    if not platform:
        await update.message.reply_text("❌ Надішли посилання на підтримуваний сайт або пряме відео.")
        return

    msg = await update.message.reply_text("⏳ Починаю завантаження...")
    loop = asyncio.get_running_loop()
    progress = ProgressUpdater(msg, loop)
    # Передаємо прогрес у download_ytdlp
    import types
    # Невеликий хак: зберігаємо прогрес в замиканні
    def _download_with_progress():
        prog_ref = progress
        def hook(d):
            prog_ref.hook(d)
        return download_ytdlp_inner(url, platform, False, hook)
    # Краще переробимо download_ytdlp, щоб приймала прогрес
    filepath, title = await loop.run_in_executor(None, download_ytdlp_with_progress, url, platform, False, progress)
    if filepath is None:
        await msg.edit_text(f"❌ {title}")
        return
    await msg.delete()
    await send_file(update, filepath, title)
    stats['total'] += 1
    stats['platforms'][platform] = stats['platforms'].get(platform, 0) + 1

# Оновлена функція, що приймає прогрес
def download_ytdlp_with_progress(url, platform, audio_only, progress):
    opts = {
        'format': 'bestaudio/best' if audio_only else 'bestvideo+bestaudio/best',
        'outtmpl': os.path.join(DOWNLOAD_DIR, '%(title)s_%(id)s.%(ext)s'),
        'merge_output_format': 'mp4' if not audio_only else None,
        'quiet': True,
        'http_headers': {'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36'},
    }
    if audio_only:
        opts['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }]
    if platform == "youtube":
        opts['extractor_args'] = {'youtube': {'player_client': ['android', 'web']}}
    cookies = get_cookies()
    if cookies:
        opts['cookiefile'] = cookies
    # Прогрес-хук
    opts['progress_hooks'] = [progress.hook]
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if audio_only:
                filepath = ydl.prepare_filename(info)
                filepath = filepath.rsplit('.', 1)[0] + '.mp3'
            else:
                filepath = ydl.prepare_filename(info)
                if not os.path.exists(filepath):
                    import glob
                    candidates = glob.glob(os.path.join(DOWNLOAD_DIR, f"*{info['id']}*"))
                    filepath = candidates[0] if candidates else None
            if not filepath or not os.path.exists(filepath):
                return None, "Файл не знайдено"
            return filepath, info.get('title', 'video')
    except Exception as e:
        err = str(e)
        if "Sign in" in err:
            return None, "Потрібна авторизація (YouTube). Додайте cookies.txt"
        return None, f"Помилка: {err[:200]}"

# --- Команди ---
async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Використовуйте /audio у відповідь на повідомлення з посиланням.")
        return
    text = update.message.reply_to_message.text.strip()
    platform = None
    for name, pat in URL_PATTERNS.items():
        if re.search(pat, text):
            platform = name
            break
    if not platform:
        await update.message.reply_text("❌ У відповіді має бути посилання на підтримуваний сайт.")
        return
    msg = await update.message.reply_text("🎵 Завантажую аудіо...")
    loop = asyncio.get_running_loop()
    progress = ProgressUpdater(msg, loop)
    filepath, title = await loop.run_in_executor(None, download_ytdlp_with_progress, text, platform, True, progress)
    if filepath is None:
        await msg.edit_text(f"❌ {title}")
        return
    await msg.delete()
    await send_file(update, filepath, title, is_audio=True)

async def formats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Використання: /formats <посилання>")
        return
    url = context.args[0]
    msg = await update.message.reply_text("Отримую список форматів...")
    try:
        with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
            info = ydl.extract_info(url, download=False)
            formats = info.get('formats', [])
            out = "🎞 **Доступні формати:**\n"
            for f in formats[:20]:
                note = f.get('format_note', '?')
                ext = f.get('ext', '')
                size = f.get('filesize') or f.get('filesize_approx')
                if size:
                    out += f"- {note} ({ext}) — {size/(1024*1024):.1f} MB\n"
                else:
                    out += f"- {note} ({ext})\n"
            await msg.edit_text(out[:4000], parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"Помилка: {str(e)[:200]}")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = "📊 **Статистика бота:**\n"
    txt += f"Всього завантажень: {stats['total']}\n"
    for p, c in stats['platforms'].items():
        txt += f"• {p}: {c}\n"
    await update.message.reply_text(txt, parse_mode="Markdown")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception while handling an update:", exc_info=context.error)

def main():
    # Скидання вебхука
    try:
        requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except: pass

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("formats", formats_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Бот запущено...")
    app.run_polling()

if __name__ == "__main__":
    try:
        main()
    except:
        traceback.print_exc()
