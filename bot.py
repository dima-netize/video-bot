from __future__ import annotations
import importlib, os, site, subprocess, sys
from pathlib import Path

BASE_DIR_BOOT = Path(__file__).resolve().parent
PY_VER = f"python{sys.version_info.major}.{sys.version_info.minor}"
for _p in (BASE_DIR_BOOT/".local"/"lib"/PY_VER/"site-packages", Path(site.getusersitepackages())):
    if _p.exists() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p)); site.addsitedir(str(_p))

def _ok(n):
    try: importlib.import_module(n); return True
    except: return False

def _pip(pkg):
    print(f"[BOOT] pip install {pkg}", flush=True)
    env = os.environ.copy(); env["PIP_NO_CACHE_DIR"] = "1"
    subprocess.check_call([sys.executable,"-m","pip","install","--user","--no-cache-dir","-U",pkg], env=env)
    u = Path(site.getusersitepackages())
    if u.exists() and str(u) not in sys.path: sys.path.insert(0,str(u)); site.addsitedir(str(u))
    importlib.invalidate_caches()

for mod, pkg in [("requests","requests>=2.31.0"),("yt_dlp","yt-dlp"),("telegram.ext","python-telegram-bot==20.7")]:
    if not _ok(mod): _pip(pkg)

import asyncio, collections, glob, json, logging, re, shutil, time
from datetime import datetime
from functools import partial
from threading import Event
from typing import Any
from urllib.parse import urljoin

import requests
import yt_dlp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

# ── Config ────────────────────────────────────────────────────
TOKEN       = os.environ.get("TOKEN","").strip()
WEBHOOK_URL = os.environ.get("WEBHOOK_URL","").rstrip("/") or None
PORT        = int(os.environ.get("PORT","10000"))

if not TOKEN: raise ValueError("Не задано TOKEN")

BASE_DIR     = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

SETTINGS_FILE = BASE_DIR / "settings.json"
STATS_FILE    = BASE_DIR / "stats.json"

MAX_UPLOAD   = int(os.environ.get("MAX_UPLOAD_BYTES", str(49*1024*1024)))
FFMPEG       = os.environ.get("FFMPEG_PATH") or shutil.which("ffmpeg")
START_TIME   = time.time()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")

URL_RE = re.compile(r"https?://[^\s<>\"]+", re.I)
DIRECT_RE = re.compile(r"https?://[^\s<>\"]+\.(?:mp4|mov|webm|m4v)(?:\?[^\s]+)?", re.I)

PLATFORMS = {
    "youtube":     re.compile(r"(youtube\.com/(watch\?v=|shorts/|live/)|youtu\.be/|music\.youtube\.com)", re.I),
    "tiktok":      re.compile(r"(tiktok\.com/|vt\.tiktok\.com/|vm\.tiktok\.com/)", re.I),
    "instagram":   re.compile(r"instagram\.com/(reel|p|tv|stories)/", re.I),
    "twitter":     re.compile(r"(twitter\.com|x\.com)/\w+/status/\d+", re.I),
    "facebook":    re.compile(r"facebook\.com/(watch|reel|share|.+/videos)", re.I),
    "vimeo":       re.compile(r"vimeo\.com/\d+", re.I),
    "reddit":      re.compile(r"reddit\.com/r/\w+/comments/", re.I),
    "pinterest":   re.compile(r"pinterest\.[a-z]+/pin/\d+", re.I),
    "twitch":      re.compile(r"twitch\.tv/(videos/\d+|clips/)", re.I),
    "dailymotion": re.compile(r"dailymotion\.com/video/", re.I),
    "rumble":      re.compile(r"rumble\.com/v", re.I),
    "bilibili":    re.compile(r"bilibili\.com/video/", re.I),
    "streamable":  re.compile(r"streamable\.com/", re.I),
}

HELP = """🎥 *Video Downloader Bot*

Просто кинь посилання — бот завантажить відео.

*Команди:*
/video `<url>` — відео
/audio `<url>` — аудіо MP3
/thumb `<url>` — обкладинка
/info `<url>` — інформація
/quality — змінити якість
/history — останні завантаження
/stats — статистика
/ping — перевірка
/platforms — підтримувані платформи
/cancel — скасувати завантаження

*Якість:*
• `best` — максимальна
• `fast` — до 720p (за замовчуванням)
• `mobile` — до 480p (маленький файл)

_Якщо YouTube не завантажується — потрібен cookies.txt на сервері._
"""

# ── State ─────────────────────────────────────────────────────
SETTINGS: dict = {}
STATS:    dict = {"ok":0,"err":0,"bytes":0,"platforms":{}}
HISTORY:  dict = {}
CANCEL:   dict[int, Event] = {}
ACTIVE:   dict[int, dict]  = {}
RATE:     dict[int, collections.deque] = {}
CACHE:    dict[str, tuple] = {}

try: SETTINGS = json.loads(SETTINGS_FILE.read_text())
except: pass
try: STATS = json.loads(STATS_FILE.read_text())
except: pass
try: HISTORY = json.loads((BASE_DIR/"history.json").read_text())
except: pass

def save(path, data):
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        tmp.replace(path)
    except: pass

def quality_for(cid): return SETTINGS.get("quality",{}).get(str(cid),"fast")
def human_bytes(n):
    if not n: return "0 B"
    v=float(n)
    for u in ["B","KB","MB","GB"]:
        if v<1024 or u=="GB": return f"{int(v)} B" if u=="B" else f"{v:.1f} {u}"
        v/=1024
def seconds_text(s):
    if not s: return "0с"
    s=int(s); h,r=divmod(s,3600); m,sec=divmod(r,60)
    return f"{h}г {m}хв {sec}с" if h else (f"{m}хв {sec}с" if m else f"{sec}с")
def safe_text(v, n=200): return (re.sub(r"\s+"," ",str(v or "video")).strip() or "video")[:n]
def cid(u): return int(u.effective_chat.id) if u.effective_chat else 0
def uid(u): return int(u.effective_user.id) if u.effective_user else 0
def extract_urls(text):
    out=[]
    for u in URL_RE.findall(text or ""):
        u=u.strip().strip(".,;)\n\r\t ")
        if u and u not in out: out.append(u)
    return out[:3]
def detect(url):
    for n,p in PLATFORMS.items():
        if p.search(url): return n
    return None
def safe_fn(prefix, url, ext="mp4"):
    ts=datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    slug=re.sub(r"[^a-zA-Z0-9_-]+","_",url.split("?")[0].rstrip("/").split("/")[-1] or "v")[:30]
    return DOWNLOAD_DIR/f"{prefix}_{slug}_{ts}.{ext}"
def rm(p):
    try: Path(p).unlink(missing_ok=True) if p else None
    except: pass
def clean(force=False):
    now=time.time(); n=0
    for p in DOWNLOAD_DIR.glob("*"):
        try:
            if p.is_file() and (force or now-p.stat().st_mtime>10800): p.unlink(); n+=1
        except: pass
    return n
def cookies():
    for p in [Path("/etc/secrets/cookies.txt"), BASE_DIR/"cookies.txt"]:
        if not p.exists(): continue
        try:
            f=p.read_text(errors="ignore").splitlines()[0].strip()
            if f in {"# Netscape HTTP Cookie File","# HTTP Cookie File"}: return str(p)
        except: pass
    return None
def pbar(pct, w=14):
    pct=max(0,min(100,pct)); f=int(w*pct/100)
    return f"[{'▓'*f}{'░'*(w-f)}] {pct}%"
def ptext(prefix, done, total, start):
    elapsed=max(time.monotonic()-start,0.1); speed=done/elapsed if done else 0
    if total:
        pct=int(done*100/total); eta=int((total-done)/speed) if speed else 0
        return f"{prefix}\n{pbar(pct)}\n{human_bytes(done)} / {human_bytes(total)}\n⚡ {human_bytes(speed)}/s  ETA {seconds_text(eta)}"
    return f"{prefix}\n{human_bytes(done)}\n⚡ {human_bytes(speed)}/s"
def check_rate(u):
    now=time.time(); dq=RATE.setdefault(u,collections.deque())
    while dq and now-dq[0]>60: dq.popleft()
    if len(dq)>=5: return False
    dq.append(now); return True
def cache_get(url, audio):
    e=CACHE.get(f"{url}|{'a' if audio else 'v'}")
    if not e: return None
    fid,title,ts=e
    if time.time()-ts>3600: CACHE.pop(f"{url}|{'a' if audio else 'v'}",None); return None
    return fid,title
def cache_set(url, audio, fid, title):
    CACHE[f"{url}|{'a' if audio else 'v'}"]=(fid,title,time.time())
def rec_history(u, url, title, platform):
    k=str(u); HISTORY.setdefault(k,[])
    HISTORY[k].insert(0,{"url":url,"title":safe_text(title,70),"platform":platform,"ts":datetime.utcnow().isoformat()})
    HISTORY[k]=HISTORY[k][:10]
    save(BASE_DIR/"history.json", HISTORY)

class Cancelled(Exception): pass

# ── yt-dlp ────────────────────────────────────────────────────
def first_entry(info):
    e=info.get("entries")
    if not e: return info
    items=[x for x in e if x]; return items[0] if items else info

def find_file(info, ydl):
    cands=[]
    for item in info.get("requested_downloads") or []:
        if isinstance(item,dict): cands+=[item.get("filepath"),item.get("_filename")]
    cands+=[info.get("filepath"),info.get("_filename")]
    try: cands.append(ydl.prepare_filename(info))
    except: pass
    if info.get("id"): cands+=glob.glob(str(DOWNLOAD_DIR/f"*{info['id']}*"))
    ex=[str(Path(c)) for c in cands if c and Path(c).exists()]
    ex.sort(key=lambda x:Path(x).stat().st_mtime,reverse=True)
    return ex[0] if ex else None

def fmt_sel(platform, audio, quality):
    if audio: return "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best"
    if quality=="mobile": return "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best[height<=480]/best"
    if quality=="fast": return "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]/best"
    if platform=="youtube" and FFMPEG: return "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/best[ext=mp4]/best"
    return "best[ext=mp4]/best"

def ytdlp_opts(platform, audio, quality, hook=None, extra=None):
    ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
    if platform=="tiktok": ua="com.zhiliaoapp.musically/2022600030 (Linux; U; Android 12)"
    opts={
        "format":fmt_sel(platform,audio,quality),
        "outtmpl":str(DOWNLOAD_DIR/"%(extractor_key)s_%(id)s_%(title).80s.%(ext)s"),
        "quiet":True,"no_warnings":True,"noplaylist":True,"restrictfilenames":True,
        "retries":8,"fragment_retries":8,"socket_timeout":30,"continuedl":True,
        "concurrent_fragment_downloads":3,"http_chunk_size":6*1024*1024,
        "http_headers":{"User-Agent":ua},
        "progress_hooks":[hook] if hook else [],
    }
    if FFMPEG:
        opts["ffmpeg_location"]=FFMPEG
        if audio: opts["postprocessors"]=[{"key":"FFmpegExtractAudio","preferredcodec":"mp3","preferredquality":"192"}]
        else: opts["merge_output_format"]="mp4"
    ck=cookies()
    if ck: opts["cookiefile"]=ck
    if platform in ("youtube","youtube_music"):
        opts["extractor_args"]={"youtube":{"player_client":["android","android_vr","web"],"player_skip":["webpage","configs"]}}
        opts["geo_bypass"]=True
    if platform=="tiktok":
        opts["extractor_args"]={"tiktok":{"app_version":"26.2.0","manifest_app_version":"26.2.0"}}
    if extra: opts.update(extra)
    return opts

def friendly_err(platform, err):
    low=str(err).lower()
    if "exit code 137" in low or "killed" in low: return "⚠️ Серверу не вистачило ресурсів. Спробуй /quality mobile."
    if platform=="youtube" and any(x in low for x in ["sign in","not a bot","cookies"]): return "🍪 YouTube просить cookies.txt на сервері."
    if "requested format" in low: return "⚠️ Якість недоступна. Спробуй /quality fast."
    if "unsupported url" in low: return "❌ Посилання не підтримується."
    if any(x in low for x in ["private","login","members-only"]): return "🔒 Відео приватне або потрібен вхід."
    if any(x in low for x in ["network","timeout","connection"]): return "🌐 Помилка мережі. Спробуй ще раз."
    if "429" in low: return "⏳ Платформа тимчасово заблокувала. Зачекай 5-10 хв."
    return safe_text(err, 700)

def is_transient(err):
    low=str(err).lower()
    return any(x in low for x in ["network","connection","timeout","reset by peer","503","502","429"])

# ── Stream download ───────────────────────────────────────────
def stream_dl(url, filepath, title, pcb=None, cancel=None, headers=None):
    headers=headers or {"User-Agent":"Mozilla/5.0"}
    start=time.monotonic(); done=0
    try:
        with requests.get(url,stream=True,timeout=30,headers=headers) as r:
            r.raise_for_status()
            total=int(r.headers.get("content-length") or 0)
            with open(filepath,"wb") as f:
                for chunk in r.iter_content(256*1024):
                    if cancel and cancel.is_set(): raise Cancelled()
                    if not chunk: continue
                    f.write(chunk); done+=len(chunk)
                    if pcb: pcb(ptext("⏳ Завантажую файл",done,total,start))
        return str(filepath),title
    except Cancelled: rm(filepath); return None,"Скасовано."
    except Exception as e: rm(filepath); return None,f"Помилка завантаження: {e}"

# ── TikTok fallbacks ──────────────────────────────────────────
def tiktok_tikwm(url, pcb=None, cancel=None):
    h={"User-Agent":"Mozilla/5.0","Referer":"https://tikwm.com/"}
    try:
        r=requests.get("https://tikwm.com/api/",params={"url":url,"hd":"1"},headers=h,timeout=30)
        r.raise_for_status(); data=r.json()
        if data.get("code")!=0: return None,data.get("msg","tikwm помилка")
        item=data.get("data") or {}; vurl=item.get("hdplay") or item.get("play")
        if not vurl: return None,"tikwm не повернув відео"
        return stream_dl(urljoin("https://tikwm.com",vurl),safe_fn("tiktok",url),safe_text(item.get("title") or "TikTok"),pcb,cancel,h)
    except Exception as e: return None,f"tikwm: {e}"

# ── Instagram fallbacks ───────────────────────────────────────
def _ig_video_url(html):
    for pat in [r'<video[^>]+src="([^"]+)"',r'property="og:video"\s+content="([^"]+)"',
                r'property="og:video:secure_url"\s+content="([^"]+)"',r'"video_url":"([^"]+)"',
                r'<source[^>]+src="([^"]+\.mp4[^"]*)"']:
        m=re.search(pat,html)
        if m: return m.group(1).replace("\\u0026","&").replace("\\/","/")
    return None

def instagram_dl(url, pcb=None, cancel=None):
    # Прибираємо параметри з URL
    clean=re.sub(r"\?.*$","",url.rstrip("/"))
    ua={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36","Accept-Language":"en-US,en;q=0.9"}

    # 1. ddinstagram (виправлений — без подвоєння)
    mirror1=re.sub(r"(?:www\.)?instagram\.com","ddinstagram.com",clean)
    for mirror in [mirror1, clean.replace("instagram.com","instagramez.com")]:
        try:
            if pcb:
                d=re.search(r"://([^/]+)",mirror); pcb(f"🔁 Instagram через {d.group(1) if d else 'дзеркало'}...")
            r=requests.get(mirror,headers=ua,timeout=25,allow_redirects=True)
            r.raise_for_status()
            vurl=_ig_video_url(r.text)
            if vurl:
                return stream_dl(vurl,safe_fn("instagram",url),"Instagram video",pcb,cancel,ua)
        except Exception as e: log.debug("ig mirror fail: %s",e)

    # 2. yt-dlp з мобільним UA (без cookies)
    try:
        if pcb: pcb("🔁 Instagram через yt-dlp...")
        opts={"format":"best[ext=mp4]/best","outtmpl":str(DOWNLOAD_DIR/"ig_%(id)s.%(ext)s"),
              "quiet":True,"no_warnings":True,"noplaylist":True,
              "http_headers":{"User-Agent":"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"}}
        ck=cookies()
        if ck: opts["cookiefile"]=ck
        with yt_dlp.YoutubeDL(opts) as ydl:
            info=first_entry(ydl.extract_info(url,download=True))
            path=find_file(info,ydl)
            if path and Path(path).exists():
                return path,safe_text(info.get("title") or "Instagram video",180)
    except Exception as e: log.debug("ig ytdlp fail: %s",e)

    # 3. yt-dlp стандартний
    try:
        if pcb: pcb("🔁 Instagram yt-dlp (стандарт)...")
        opts=ytdlp_opts("instagram",False,"fast")
        with yt_dlp.YoutubeDL(opts) as ydl:
            info=first_entry(ydl.extract_info(url,download=True))
            path=find_file(info,ydl)
            if path and Path(path).exists():
                return path,safe_text(info.get("title") or "Instagram video",180)
    except Exception as e: log.debug("ig ytdlp2 fail: %s",e)

    return None,"❌ Instagram: не вдалося завантажити. Відео може бути приватним або потрібен cookies.txt."

# ── yt-dlp download ───────────────────────────────────────────
def dl_ytdlp(url, platform, audio, quality, pcb=None, cancel=None, extra=None):
    start=time.monotonic()
    def hook(d):
        if cancel and cancel.is_set(): raise Cancelled()
        if not pcb: return
        if d.get("status")=="downloading":
            pcb(ptext("⏳ Завантажую",int(d.get("downloaded_bytes") or 0),int(d.get("total_bytes") or d.get("total_bytes_estimate") or 0),start))
        elif d.get("status")=="finished": pcb("🔧 Обробляю файл...")
    try:
        with yt_dlp.YoutubeDL(ytdlp_opts(platform,audio,quality,hook,extra)) as ydl:
            info=first_entry(ydl.extract_info(url,download=True))
            path=find_file(info,ydl)
            if audio and path and FFMPEG:
                mp3=str(Path(path).with_suffix(".mp3"))
                if Path(mp3).exists(): path=mp3
            if not path or not Path(path).exists(): return None,"Файл не знайдено після завантаження."
            return path,safe_text(info.get("title"),180)
    except Cancelled: return None,"Скасовано."
    except Exception as e: return None,friendly_err(platform,str(e))

def download_media(url, platform, audio, quality, pcb=None, cancel=None):
    if DIRECT_RE.search(url) and not audio:
        ext=url.split("?")[0].split(".")[-1].lower()
        if ext not in {"mp4","mov","webm","m4v"}: ext="mp4"
        return stream_dl(url,safe_fn("direct",url,ext),"Відео",pcb,cancel)

    # TikTok — спочатку без watermark
    if platform=="tiktok" and not audio:
        if pcb: pcb("🔁 TikTok no-watermark...")
        p,r=tiktok_tikwm(url,pcb,cancel)
        if p: return p,r
        if pcb: pcb("🔁 TikTok через yt-dlp...")

    # Instagram — окремий обробник
    if platform=="instagram" and not audio:
        return instagram_dl(url,pcb,cancel)

    # Всі інші — yt-dlp з ретраями
    last=""
    for attempt in range(1,4):
        if cancel and cancel.is_set(): return None,"Скасовано."
        if attempt>1:
            if pcb: pcb(f"🔁 Спроба {attempt}/3...")
            time.sleep(2**(attempt-1))
        p,r=dl_ytdlp(url,platform,audio,quality,pcb,cancel)
        if p: return p,r
        last=r
        if not is_transient(r): break
    return None,last

# ── Telegram helpers ──────────────────────────────────────────
async def safe_edit(msg, text):
    try: await msg.edit_text(text[:3900],parse_mode="Markdown")
    except RetryAfter as e: await asyncio.sleep(float(e.retry_after)+0.2)
    except (BadRequest,TelegramError): pass

async def send_file(update, filepath, title, audio=False, pcb=None):
    msg=update.effective_message
    if not msg: rm(filepath); return 0,""
    try:
        size=Path(filepath).stat().st_size
        if size>MAX_UPLOAD:
            await msg.reply_text("❌ Файл більший за 50MB.\nСпробуй /quality mobile."); return 0,""
        if pcb: pcb("📤 Надсилаю у Telegram...")
        with open(filepath,"rb") as f:
            if audio and Path(filepath).suffix.lower()==".mp3":
                s=await msg.reply_audio(audio=f,title=title[:64],caption=f"🎵 {title[:180]}",read_timeout=180,write_timeout=180,connect_timeout=60,pool_timeout=60)
                return size,str(s.audio.file_id if s.audio else "")
            else:
                s=await msg.reply_video(video=f,caption=f"✅ {title[:200]}",supports_streaming=True,read_timeout=180,write_timeout=180,connect_timeout=60,pool_timeout=60)
                return size,str(s.video.file_id if s.video else "")
    except Exception: await msg.reply_text("❌ Не вдалося надіслати файл."); return 0,""
    finally: rm(filepath)

PARALLEL = asyncio.Semaphore(2)

async def do_download(update, url, platform, audio=False):
    msg=update.effective_message
    if not msg: return
    c=cid(update); u=uid(update)
    quality=quality_for(c)

    if not check_rate(u):
        await msg.reply_text("⏳ Забагато завантажень. Ліміт 5 за хвилину."); return

    cached=cache_get(url,audio)
    if cached:
        fid,title=cached
        st=await msg.reply_text("⚡ З кешу. Надсилаю...")
        try:
            if audio: await msg.reply_audio(audio=fid,title=title[:64],caption=f"🎵 {title[:180]}")
            else: await msg.reply_video(video=fid,caption=f"✅ {title[:200]} _(кеш)_",parse_mode="Markdown")
            await st.delete(); return
        except: CACHE.pop(f"{url}|{'a' if audio else 'v'}",None); await safe_edit(st,"Кеш застарів. Завантажую...")

    cancel=Event()
    CANCEL[c]=cancel
    ACTIVE[c]={"url":url,"platform":platform,"audio":audio,"quality":quality,"started":time.time()}

    async with PARALLEL:
        st=await msg.reply_text("🎵 Готую аудіо..." if audio else "⏳ Починаю завантаження...")
        loop=asyncio.get_running_loop(); lt=[0.]; lx=[""]
        def pcb(text):
            now=time.monotonic()
            imp=text.startswith(("🔧","📤","✅","🔁","❌","⚡"))
            if text==lx[0] or (now-lt[0]<1.5 and not imp): return
            lt[0]=now; lx[0]=text
            asyncio.run_coroutine_threadsafe(safe_edit(st,text),loop)
        try:
            path,title=await loop.run_in_executor(None,partial(download_media,url,platform,audio,quality,pcb,cancel))
            if not path:
                STATS["err"]=int(STATS.get("err",0))+1; save(STATS_FILE,STATS)
                await safe_edit(st,f"❌ {title}"); return
            await safe_edit(st,"✅ Завантажено. Надсилаю...")
            size,fid=await send_file(update,path,title,audio,pcb)
            if size:
                STATS["ok"]=int(STATS.get("ok",0))+1
                STATS["bytes"]=int(STATS.get("bytes",0))+size
                STATS.setdefault("platforms",{})[platform]=int(STATS["platforms"].get(platform,0))+1
                save(STATS_FILE,STATS)
                rec_history(u,url,title,platform)
                if fid: cache_set(url,audio,fid,title)
            try: await st.delete()
            except: pass
        finally:
            CANCEL.pop(c,None); ACTIVE.pop(c,None); clean()

# ── Keyboards ─────────────────────────────────────────────────
def kb_quality():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🏆 best",callback_data="q:best"),
        InlineKeyboardButton("⚡ fast",callback_data="q:fast"),
        InlineKeyboardButton("📱 mobile",callback_data="q:mobile"),
    ]])

# ── Handlers ──────────────────────────────────────────────────
async def cmd_start(u,ctx):
    if u.effective_message: await u.effective_message.reply_text(HELP,parse_mode="Markdown")

async def cmd_handle(u,ctx):
    msg=u.effective_message
    if not msg or not msg.text: return
    urls=extract_urls(msg.text)
    if not urls: await msg.reply_text("❌ Надішли посилання на відео."); return
    if len(urls)>1: await msg.reply_text(f"🔗 Знайдено {len(urls)} посилання. Оброблю по черзі.")
    for url in urls:
        platform="direct" if DIRECT_RE.search(url) else detect(url)
        if not platform: await msg.reply_text(f"❌ Платформа не підтримується:\n`{url[:100]}`",parse_mode="Markdown"); continue
        await do_download(u,url,platform)

async def _get_url(u,ctx,cmd):
    msg=u.effective_message
    if not msg: return None
    url=ctx.args[0].strip() if ctx.args else None
    if not url and msg.reply_to_message and msg.reply_to_message.text:
        found=extract_urls(msg.reply_to_message.text); url=found[0] if found else None
    if not url: await msg.reply_text(f"❌ Використання: /{cmd} <посилання>")
    return url

async def cmd_video(u,ctx):
    url=await _get_url(u,ctx,"video")
    if not url: return
    platform="direct" if DIRECT_RE.search(url) else detect(url)
    if not platform: await u.effective_message.reply_text("❌ Платформа не підтримується."); return
    await do_download(u,url,platform)

async def cmd_audio(u,ctx):
    url=await _get_url(u,ctx,"audio")
    if not url: return
    platform=detect(url)
    if not platform: await u.effective_message.reply_text("❌ Платформа не підтримується."); return
    await do_download(u,url,platform,audio=True)

async def cmd_quality(u,ctx):
    msg=u.effective_message
    if not msg: return
    c=cid(u)
    if not ctx.args:
        await msg.reply_text(f"⚙️ Поточна якість: *{quality_for(c)}*\nОбери нову:",parse_mode="Markdown",reply_markup=kb_quality()); return
    v=ctx.args[0].lower()
    if v not in {"best","fast","mobile"}: await msg.reply_text("❌ Доступно: best, fast, mobile"); return
    SETTINGS.setdefault("quality",{})[str(c)]=v; save(SETTINGS_FILE,SETTINGS)
    await msg.reply_text(f"✅ Якість: *{v}*",parse_mode="Markdown")

async def cmd_thumb(u,ctx):
    url=await _get_url(u,ctx,"thumb")
    if not url: return
    platform=detect(url)
    st=await u.effective_message.reply_text("🖼 Завантажую обкладинку...")
    try:
        opts=ytdlp_opts(platform,False,"fast"); opts["skip_download"]=True; opts["writethumbnail"]=True
        opts["outtmpl"]=str(DOWNLOAD_DIR/"thumb_%(id)s.%(ext)s")
        loop=asyncio.get_running_loop()
        def job():
            with yt_dlp.YoutubeDL(opts) as ydl:
                info=first_entry(ydl.extract_info(url,download=True))
                vid_id=info.get("id","")
                files=sorted(glob.glob(str(DOWNLOAD_DIR/f"thumb_{vid_id}*")),key=lambda x:Path(x).stat().st_mtime,reverse=True)
                title=safe_text(info.get("title"),"")
                return (files[0] if files else None),title
        path,title=await loop.run_in_executor(None,job)
        if not path: await safe_edit(st,"❌ Обкладинку не знайдено."); return
        with open(path,"rb") as f: await u.effective_message.reply_photo(photo=f,caption=f"🖼 {title[:200]}")
        await st.delete(); rm(path)
    except Exception as e: await safe_edit(st,f"❌ {friendly_err(platform,str(e))}")

async def cmd_info(u,ctx):
    url=await _get_url(u,ctx,"info")
    if not url: return
    platform=detect(url)
    st=await u.effective_message.reply_text("🔎 Отримую дані...")
    try:
        opts=ytdlp_opts(platform,False,"fast"); opts["skip_download"]=True
        loop=asyncio.get_running_loop()
        def job():
            with yt_dlp.YoutubeDL(opts) as ydl:
                info=first_entry(ydl.extract_info(url,download=False))
            lines=[f"ℹ️ *{safe_text(info.get('title'),160)}*",
                   f"👤 {safe_text(info.get('uploader') or info.get('channel') or '-',100)}",
                   f"⏱ {seconds_text(info.get('duration'))}",f"📡 {platform or '?'}"]
            if info.get("view_count") is not None: lines.append(f"👁 {int(info['view_count']):,}".replace(",", " "))
            if info.get("upload_date"):
                d=str(info["upload_date"])
                if len(d)==8: lines.append(f"📅 {d[:4]}-{d[4:6]}-{d[6:]}")
            if info.get("description"): lines+=["",safe_text(info["description"],300)]
            return "\n".join(lines)[:3900]
        result=await loop.run_in_executor(None,job)
        await safe_edit(st,result)
    except Exception as e: await safe_edit(st,f"❌ {friendly_err(platform,str(e))}")

async def cmd_ping(u,ctx):
    msg=u.effective_message
    if msg:
        t=time.monotonic(); m=await msg.reply_text("🏓 Pong!")
        ms=int((time.monotonic()-t)*1000); await m.edit_text(f"🏓 Pong! `{ms}ms`",parse_mode="Markdown")

async def cmd_cancel(u,ctx):
    msg=u.effective_message
    if not msg: return
    ev=CANCEL.get(cid(u))
    if not ev: await msg.reply_text("Немає активного завантаження."); return
    ev.set(); await msg.reply_text("🛑 Скасовую...")

async def cmd_history(u,ctx):
    msg=u.effective_message
    if not msg: return
    hist=HISTORY.get(str(uid(u)),[])
    if not hist: await msg.reply_text("📋 Історія порожня."); return
    lines=[f"📋 *Останні {len(hist)} завантажень:*",""]
    for i,item in enumerate(hist,1):
        lines.append(f"{i}. [{item.get('platform','?')}] {item.get('title','')[:55]}\n   _{str(item.get('ts','-'))[:10]}_")
    await msg.reply_text("\n".join(lines)[:3900],parse_mode="Markdown")

async def cmd_stats(u,ctx):
    msg=u.effective_message
    if not msg: return
    pl=STATS.get("platforms",{})
    lines=["📊 *Статистика*","",
           f"Успішних: *{STATS.get('ok',0)}*",
           f"Помилок: {STATS.get('err',0)}",
           f"Відправлено: {human_bytes(STATS.get('bytes',0))}",
           f"Кеш: {len(CACHE)}",
           f"Аптайм: {seconds_text(time.time()-START_TIME)}"]
    if pl: lines+=["","📡 Платформи:"]+[f"  • {p}: {c}" for p,c in sorted(pl.items(),key=lambda x:-x[1])]
    await msg.reply_text("\n".join(lines),parse_mode="Markdown")

async def cmd_platforms(u,ctx):
    if u.effective_message:
        await u.effective_message.reply_text("📡 *Платформи:*\n"+"\n".join(f"• {n}" for n in PLATFORMS),parse_mode="Markdown")

async def quality_cb(u,ctx):
    q=u.callback_query
    if not q: return
    await q.answer()
    v=q.data.split(":",1)[1] if ":" in q.data else ""
    if v not in {"best","fast","mobile"}: return
    c=int(q.message.chat.id)
    SETTINGS.setdefault("quality",{})[str(c)]=v; save(SETTINGS_FILE,SETTINGS)
    await q.edit_message_text(f"✅ Якість: *{v}*",parse_mode="Markdown")

async def err_handler(update,ctx):
    log.exception("Error",exc_info=ctx.error)

# ── Main ──────────────────────────────────────────────────────
def main():
    clean(False)
    app=Application.builder().token(TOKEN).build()
    H=app.add_handler
    H(CommandHandler("start",  cmd_start))
    H(CommandHandler("help",   cmd_start))
    H(CommandHandler("video",  cmd_video))
    H(CommandHandler("dl",     cmd_video))
    H(CommandHandler("audio",  cmd_audio))
    H(CommandHandler("quality",cmd_quality))
    H(CommandHandler("thumb",  cmd_thumb))
    H(CommandHandler("info",   cmd_info))
    H(CommandHandler("ping",   cmd_ping))
    H(CommandHandler("cancel", cmd_cancel))
    H(CommandHandler("history",cmd_history))
    H(CommandHandler("stats",  cmd_stats))
    H(CommandHandler("platforms",cmd_platforms))
    H(CallbackQueryHandler(quality_cb,pattern=r"^q:"))
    H(MessageHandler(filters.TEXT & ~filters.COMMAND,cmd_handle))
    app.add_error_handler(err_handler)

    if WEBHOOK_URL:
        log.info("Webhook: %s port %d",WEBHOOK_URL,PORT)
        app.run_webhook(listen="0.0.0.0",port=PORT,url_path=TOKEN,
                        webhook_url=f"{WEBHOOK_URL}/{TOKEN}",drop_pending_updates=True)
    else:
        log.info("Polling режим")
        try: requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook",params={"drop_pending_updates":"true"},timeout=10)
        except: pass
        app.run_polling(drop_pending_updates=True,allowed_updates=Update.ALL_TYPES)

if __name__=="__main__":
    try: main()
    except Exception: import traceback; traceback.print_exc()
