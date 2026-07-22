#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          TELEGRAM FILE STORE BOT — COMPLETE PRODUCTION BUILD (PART 1)        ║
║  Features: Clone Management | Permanent Storage | Multi-DB | Batch Links     ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import logging
import os
import random
import string
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase
from pyrogram import Client, filters, idle
from pyrogram.enums import MessageMediaType, ChatMemberStatus
from pyrogram.errors import FloodWait, UserNotParticipant
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
)

load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════════
# 1. LOGGER SETUP
# ═══════════════════════════════════════════════════════════════════════════════

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_FILE = "bot.log"

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("FileStoreBot")
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(ch)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5)
    fh.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(fh)
    logger.propagate = False
    return logger

log = setup_logger()

# ═══════════════════════════════════════════════════════════════════════════════
# 2. CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

class Config:
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    BOT_USERNAME: str = os.getenv("BOT_USERNAME", "")
    OWNER_ID: int = int(os.getenv("OWNER_ID", "0"))
    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "FileStoreBot")
    DB_CHANNEL_ID: int = int(os.getenv("DB_CHANNEL_ID", "0"))
    LOG_CHANNEL_ID: int = int(os.getenv("LOG_CHANNEL_ID", "0"))
    FORCE_SUB_CHANNELS: List[int] = [int(x.strip()) for x in os.getenv("FORCE_SUB_CHANNELS", "").split(",") if x.strip()]
    SHORTENER_API: Optional[str] = os.getenv("SHORTENER_API") or None
    SHORTENER_KEY: Optional[str] = os.getenv("SHORTENER_KEY") or None
    ADMIN_IDS: List[int] = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
    BATCH_SEND_DELAY: float = float(os.getenv("BATCH_SEND_DELAY", "0.35"))
    DELIVERY_WORKERS: int = int(os.getenv("DELIVERY_WORKERS", "5"))

    @classmethod
    def validate(cls):
        required = [
            ("API_ID", cls.API_ID), ("API_HASH", cls.API_HASH),
            ("BOT_TOKEN", cls.BOT_TOKEN), ("BOT_USERNAME", cls.BOT_USERNAME),
            ("OWNER_ID", cls.OWNER_ID), ("MONGODB_URI", cls.MONGODB_URI),
            ("DB_CHANNEL_ID", cls.DB_CHANNEL_ID),
        ]
        for name, val in required:
            if not val or val in (0, "", "0"):
                raise ValueError(f"❌ Critical Config Missing: {name}")

config = Config()

# ═══════════════════════════════════════════════════════════════════════════════
# 3. UTILITIES & HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def generate_token(length: int = 12) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_clone_id() -> str:
    return f"c_{uuid.uuid4().hex[:10]}"

def fmt_size(size: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def fmt_time(seconds: int) -> str:
    parts = []
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)

def extract_file_info(msg: Message) -> Optional[Dict[str, Any]]:
    media = msg.media
    if media == MessageMediaType.DOCUMENT:
        d = msg.document
        return {"file_id": d.file_id, "file_unique_id": d.file_unique_id, "file_type": "document", "file_name": d.file_name or f"doc_{d.file_id[:8]}", "file_size": d.file_size or 0}
    elif media == MessageMediaType.PHOTO:
        p = msg.photo
        return {"file_id": p.file_id, "file_unique_id": p.file_unique_id, "file_type": "photo", "file_name": f"photo_{p.file_unique_id}.jpg", "file_size": p.file_size}
    elif media == MessageMediaType.VIDEO:
        v = msg.video
        return {"file_id": v.file_id, "file_unique_id": v.file_unique_id, "file_type": "video", "file_name": v.file_name or f"video_{v.file_unique_id}.mp4", "file_size": v.file_size or 0}
    elif media == MessageMediaType.AUDIO:
        a = msg.audio
        return {"file_id": a.file_id, "file_unique_id": a.file_unique_id, "file_type": "audio", "file_name": a.file_name or f"audio_{a.file_unique_id}.mp3", "file_size": a.file_size or 0}
    return None

async def check_sub(client: Client, uid: int, cid: int) -> Tuple[bool, str]:
    try:
        chat = await client.get_chat(cid)
        link = chat.invite_link or (f"https://t.me/{chat.username}" if chat.username else "")
        try:
            m = await client.get_chat_member(cid, uid)
            return m.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER), link
        except UserNotParticipant: return False, link
    except: return True, ""

async def check_all_subs(client: Client, uid: int, channels: List[int]) -> Tuple[bool, Optional[int], str]:
    for c in channels:
        ok, link = await check_sub(client, uid, c)
        if not ok: return False, c, link
    return True, None, ""

async def shorten_url_api(url: str) -> Optional[str]:
    if not config.SHORTENER_API or not config.SHORTENER_KEY: return None
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            r = await h.get(config.SHORTENER_API, params={"api": config.SHORTENER_KEY, "url": url})
            if r.status_code == 200:
                d = r.json()
                return d.get("shortenedUrl") or d.get("short_url") or d.get("url")
    except: pass
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# 4. MULTI-DATABASE ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class Database:
    _connections: Dict[str, Tuple[AsyncIOMotorClient, AsyncIOMotorDatabase]] = {}
    _init_lock = asyncio.Lock()

    async def connect(self, name: str = "main", uri: Optional[str] = None, db_name: Optional[str] = None) -> AsyncIOMotorDatabase:
        if name in self._connections:
            return self._connections[name][1]

        async with self._init_lock:
            if name in self._connections: return self._connections[name][1]
            eff_uri = uri or config.MONGODB_URI
            eff_db = db_name or config.DATABASE_NAME

            client = AsyncIOMotorClient(eff_uri, serverSelectionTimeoutMS=5000, maxPoolSize=100)
            await client.admin.command("ping")
            db = client[eff_db]
            self._connections[name] = (client, db)
            log.info(f"✅ Database [DB:{name}] Connected Engine Active")
            return db

    async def col(self, name: str, db_name: str = "main") -> AsyncIOMotorCollection:
        db = await self.connect(db_name)
        return db[name]

database = Database()

# ═══════════════════════════════════════════════════════════════════════════════
# 5. REPOSITORY LAYER
# ═══════════════════════════════════════════════════════════════════════════════

class Repo:
    @staticmethod
    async def ensure_indexes(db_name: str = "main"):
        db = await database.connect(db_name)
        for coll, keys in [("users", [("user_id", 1)]), ("files", [("file_id", 1)]), ("batches", [("batch_id", 1)]), ("clones", [("clone_id", 1)])]:
            try: await db[coll].create_index(keys, background=True)
            except: pass

    @staticmethod
    async def add_user(uid: int, clone: str = "main", username: str = "", first: str = "", last: str = "", db_name: str = "main"):
        col = await database.col("users", db_name)
        now = datetime.now(timezone.utc).isoformat()
        await col.update_one(
            {"clone_id": clone, "user_id": uid},
            {"$setOnInsert": {"clone_id": clone, "user_id": uid, "joined_at": now, "is_banned": False},
             "$set": {"last_active": now, "username": username, "first_name": first, "last_name": last}},
            upsert=True
        )

    @staticmethod
    async def is_banned(uid: int, clone: str = "main", db_name: str = "main") -> bool:
        col = await database.col("users", db_name)
        u = await col.find_one({"clone_id": clone, "user_id": uid})
        return u.get("is_banned", False) if u else False

    @staticmethod
    async def ban_user(uid: int, clone: str = "main", db_name: str = "main") -> bool:
        col = await database.col("users", db_name)
        r = await col.update_one({"clone_id": clone, "user_id": uid}, {"$set": {"is_banned": True}})
        return r.modified_count > 0

    @staticmethod
    async def unban_user(uid: int, clone: str = "main", db_name: str = "main") -> bool:
        col = await database.col("users", db_name)
        r = await col.update_one({"clone_id": clone, "user_id": uid}, {"$set": {"is_banned": False}})
        return r.modified_count > 0

    @staticmethod
    async def store_file(data: dict, db_name: str = "main") -> str:
        col = await database.col("files", db_name)
        r = await col.insert_one(data)
        return str(r.inserted_id)

    @staticmethod
    async def get_file_by_id(fid: str, clone: str = "main", db_name: str = "main") -> Optional[dict]:
        col = await database.col("files", db_name)
        return await col.find_one({"clone_id": clone, "file_id": fid})

    @staticmethod
    async def create_batch(data: dict, db_name: str = "main") -> str:
        col = await database.col("batches", db_name)
        r = await col.insert_one(data)
        return str(r.inserted_id)

    @staticmethod
    async def get_batch_by_id(bid: str, clone: str = "main", db_name: str = "main") -> Optional[dict]:
        col = await database.col("batches", db_name)
        return await col.find_one({"clone_id": clone, "batch_id": bid})

    @staticmethod
    async def register_clone(data: dict) -> str:
        col = await database.col("clones", "main")
        r = await col.insert_one(data)
        return str(r.inserted_id)

    @staticmethod
    async def get_clone(clone_id: str) -> Optional[dict]:
        col = await database.col("clones", "main")
        return await col.find_one({"clone_id": clone_id})

    @staticmethod
    async def delete_clone(clone_id: str) -> bool:
        col = await database.col("clones", "main")
        r = await col.delete_one({"clone_id": clone_id})
        return r.deleted_count > 0

    @staticmethod
    async def get_all_clones() -> List[dict]:
        col = await database.col("clones", "main")
        return await col.find({"status": "active"}).to_list(length=1000)

    @staticmethod
    async def get_settings(clone: str = "main", db_name: str = "main") -> dict:
        col = await database.col("settings", db_name)
        s = await col.find_one({"clone_id": clone})
        defaults = {
            "start_msg": "👋 Hello {first_name}!\n\nI am a File Store Bot. Send me any file and tap `/done` when finished.",
            "force_sub_msg": "⚠️ Please join our updates channel to use this bot!",
            "force_subs": [],
            "protect": True
        }
        if s and s.get("data"): defaults.update(s["data"])
        return defaults

    @staticmethod
    async def set_settings(clone: str, updates: dict, db_name: str = "main"):
        col = await database.col("settings", db_name)
        await col.update_one({"clone_id": clone}, {"$set": {"data": updates}}, upsert=True)

    @staticmethod
    async def add_mod(uid: int, clone: str = "main", db_name: str = "main"):
        col = await database.col("moderators", db_name)
        await col.update_one({"clone_id": clone, "user_id": uid}, {"$set": {"user_id": uid}}, upsert=True)

    @staticmethod
    async def is_mod(uid: int, clone: str = "main", db_name: str = "main") -> bool:
        if uid == config.OWNER_ID or uid in config.ADMIN_IDS: return True
        col = await database.col("moderators", db_name)
        return bool(await col.find_one({"clone_id": clone, "user_id": uid}))

    @staticmethod
    async def get_db_name_for_clone(clone_id: str) -> str:
        if clone_id == "main": return "main"
        cd = await Repo.get_clone(clone_id)
        if cd and cd.get("mongo_uri"):
            try: await database.connect(clone_id, uri=cd["mongo_uri"])
            except: return "main"
            return clone_id
        return "main"

repo = Repo()

# ═══════════════════════════════════════════════════════════════════════════════
# 6. GLOBAL STATE & DELIVERY ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

_start_time = time.time()
main_client: Optional[Client] = None
_clients: Dict[str, Client] = {}
_upload_buffers: Dict[str, Dict[str, Tuple[List[dict], float]]] = defaultdict(lambda: defaultdict(lambda: ([], 0.0)))
_delivery_queue: asyncio.Queue = asyncio.Queue()

class DeliveryEngine:
    def __init__(self): 
        self._workers: List[asyncio.Task] = []

    async def start(self, count: int = 5):
        for i in range(count): 
            self._workers.append(asyncio.create_task(self._worker(i+1)))

    async def stop(self):
        for t in self._workers: t.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)

    def enqueue(self, clone: str, uid: int, chat_id: int, batch_id: str, msg_id: int, protect: bool = True):
        _delivery_queue.put_nowait({
            "clone": clone, "uid": uid, "chat_id": chat_id, 
            "batch_id": batch_id, "msg_id": msg_id, "protect": protect
        })

    async def _worker(self, wid: int):
        while True:
            job = await _delivery_queue.get()
            try: await self._execute(job, wid)
            except Exception as e: log.error(f"Worker {wid} error: {e}")
            finally: _delivery_queue.task_done()

    async def _execute(self, job: dict, wid: int):
        clone = job["clone"]; uid = job["uid"]; chat_id = job["chat_id"]; batch_id = job["batch_id"]
        progress_msg_id = job["msg_id"]; protect = job["protect"]

        db_name = await repo.get_db_name_for_clone(clone)
        batch = await repo.get_batch_by_id(batch_id, clone, db_name)
        if not batch:
            # Fallback to main DB
            batch = await repo.get_batch_by_id(batch_id, "main", "main")
        if not batch: return

        files = batch.get("files", []); total = len(files); sent = 0; failed = 0
        client = _clients.get(clone) or main_client
        if not client: return

        for idx, fm in enumerate(files):
            try:
                await client.copy_message(chat_id, config.DB_CHANNEL_ID, fm["db_msg_id"], caption=fm.get("caption", ""), protect_content=protect)
                sent += 1
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                await client.copy_message(chat_id, config.DB_CHANNEL_ID, fm["db_msg_id"], caption=fm.get("caption", ""), protect_content=protect)
                sent += 1
            except Exception as e:
                log.error(f"Error copying message: {e}")
                failed += 1
            await asyncio.sleep(config.BATCH_SEND_DELAY)

        try:
            await client.edit_message_text(chat_id, progress_msg_id, f"✅ **Delivery Complete!**\n\nSent: {sent}/{total}\nFailed: {failed}\n💾 Status: Permanent Storage")
        except: pass

delivery_engine = DeliveryEngine()

# ═══════════════════════════════════════════════════════════════════════════════
# 7. KEYBOARDS & UI HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

class KB:
    @staticmethod
    def main(is_owner: bool = False):
        b = [[InlineKeyboardButton("📚 Help", callback_data="help")]]
        if is_owner: b.append([InlineKeyboardButton("⚙️ Admin Dashboard", callback_data="admin_dashboard")])
        return InlineKeyboardMarkup(b)

    @staticmethod
    def admin():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Stats", callback_data="a_stats"), InlineKeyboardButton("🤖 Clones", callback_data="a_clones")],
            [InlineKeyboardButton("📢 Broadcast Guidance", callback_data="a_broadcast")],
            [InlineKeyboardButton("✖️ Close", callback_data="close")],
        ])

    @staticmethod
    def back(cb: str = "admin_dashboard"):
        return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=cb)]])

    @staticmethod
    def sub_link(link: str):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Join Channel", url=link)],
            [InlineKeyboardButton("🔄 Verify Membership", callback_data="check_sub")],
        ])

kb = KB()

# ═══════════════════════════════════════════════════════════════════════════════
# 8. CLONE & BOT HANDLER REGISTRATION
# ═══════════════════════════════════════════════════════════════════════════════

class CloneManager:
    async def init_main(self) -> Client:
        global main_client
        main_client = Client("main_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN, workers=50)
        return main_client

    def _register_handlers(self, client: Client, clone_id: str):
        async def _db(): return await repo.get_db_name_for_clone(clone_id)

        # /start command (FIXED)
        async def start_c(c: Client, m: Message):
            db_name = await _db(); user = m.from_user
            if not user: return
            await repo.add_user(user.id, clone_id, user.username or "", user.first_name or "", user.last_name or "", db_name)

            if await repo.is_banned(user.id, clone_id, db_name):
                await m.reply("❌ आप इस Bot पर बैन हैं।"); return

            settings = await repo.get_settings(clone_id, db_name)
            fsubs = settings.get("force_subs", []) or config.FORCE_SUB_CHANNELS
            if fsubs:
                all_ok, _, link = await check_all_subs(c, user.id, fsubs)
                if not all_ok:
                    msg = settings.get("force_sub_msg", "⚠️ कृपया Channel Join करें!")
                    await m.reply(msg, reply_markup=kb.sub_link(link)); return

            args = m.text.split(maxsplit=1)
            param = args[1].strip() if len(args) > 1 else ""

            # Single File Fetch
            if param.startswith("f_"):
                fid = param
                fd = await repo.get_file_by_id(fid, clone_id, db_name)
                if not fd:
                    fd = await repo.get_file_by_id(fid, "main", "main")
                if not fd: 
                    await m.reply("❌ File नहीं मिली।"); return
                await c.copy_message(m.chat.id, config.DB_CHANNEL_ID, fd["db_msg_id"], caption=fd.get("caption",""), protect_content=settings.get("protect", True))
                return

            # Batch Collection Fetch (FIXED Prefix Issue)
            if param.startswith("b_"):
                bid = param
                batch = await repo.get_batch_by_id(bid, clone_id, db_name)
                if not batch:
                    # Search in main DB fallback
                    batch = await repo.get_batch_by_id(bid, "main", "main")
                if not batch: 
                    await m.reply("❌ Batch नहीं मिला।"); return
                
                info = await m.reply(f"📦 **Batch Delivery Started!**\nFiles: {batch['total_files']} | Size: {fmt_size(batch['total_size'])}")
                delivery_engine.enqueue(clone_id, user.id, m.chat.id, bid, info.id, settings.get("protect", True))
                return

            msg = settings.get("start_msg", "👋 Welcome!").format(first_name=user.first_name or "User")
            await m.reply(msg, reply_markup=kb.main(user.id == config.OWNER_ID or user.id in config.ADMIN_IDS))

        # /done command (Buffer link generator)
        async def done_c(c: Client, m: Message):
            db_name = await _db(); user = m.from_user
            if not user: return
            key = str(user.id)
            buf = _upload_buffers[clone_id].get(key)

            if not buf or not buf[0]:
                await m.reply("📭 Buffer में कोई फाइल नहीं है। पहले फाइलें भेजें।")
                return

            files, _ = buf
            total_size = sum(f.get("file_size", 0) for f in files)
            token = generate_token()
            batch_id = f"b_{token}"

            data = {
                "batch_id": batch_id, "clone_id": clone_id, "owner_id": user.id,
                "access_token": token, "files": files,
                "total_files": len(files), "total_size": total_size,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "auto_delete_at": None,  # PERMANENT STORAGE
                "deleted": False,
            }

            await repo.create_batch(data, db_name)
            del _upload_buffers[clone_id][key]

            bot_uname = config.BOT_USERNAME
            cd = await repo.get_clone(clone_id)
            if cd: bot_uname = cd.get("bot_username", bot_uname)

            link = f"https://t.me/{bot_uname}?start={batch_id}"
            short = await shorten_url_api(link)
            if short: link = short

            await m.reply(
                f"✅ **Batch Upload Complete!**\n\n"
                f"📄 **कुल Files:** {len(files)}\n"
                f"📏 **कुल Size:** {fmt_size(total_size)}\n"
                f"💾 **Storage:** Permanent (Safe Forever)\n\n"
                f"🔗 **Single Collection Link:**\n`{link}`",
                disable_web_page_preview=True
            )

        # /cancel command
        async def cancel_c(c: Client, m: Message):
            user = m.from_user
            if not user: return
            key = str(user.id)
            buf = _upload_buffers[clone_id].pop(key, None)
            if buf and buf[0]:
                await m.reply(f"❌ Upload cancelled. {len(buf[0])} buffered files cleared.")
            else:
                await m.reply("📭 Buffer खाली है।")

        # Media Upload Buffer Handler
        async def media_c(c: Client, m: Message):
            db_name = await _db(); user = m.from_user
            if not user or await repo.is_banned(user.id, clone_id, db_name): return

            info = extract_file_info(m)
            if not info: return

            try:
                fwd = await m.copy(config.DB_CHANNEL_ID, protect_content=True)
                db_msg_id = fwd.id
            except Exception as e:
                await m.reply(f"❌ Channel copy error: {e}"); return

            token = generate_token()
            fid = f"f_{token}"
            
            fd = {
                "file_id": fid, "clone_id": clone_id, "user_id": user.id,
                "file_name": info["file_name"], "file_size": info["file_size"],
                "db_msg_id": db_msg_id, "caption": m.caption or "",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "auto_delete_at": None,
                "deleted": False
            }
            await repo.store_file(fd, db_name)

            key = str(user.id)
            buf_files, _ = _upload_buffers[clone_id][key]
            buf_files.append(fd)
            _upload_buffers[clone_id][key] = (buf_files, time.time())

            await m.reply(
                f"📥 **File Received!** (Total in Buffer: {len(buf_files)})\n"
                f"📄 `{info['file_name'][:35]}`\n\n"
                f"⚠️ **Single Link generate karne ke liye `/done` dabayein.**"
            )

        # Callbacks
        async def cb_c(c: Client, q: CallbackQuery):
            data = q.data; uid = q.from_user.id; db_name = await _db()
            await q.answer()

            if data == "close": await q.message.delete()
            elif data == "help":
                await q.message.edit_text(
                    "📚 **Help & Guide**\n\n"
                    "• **Files Bhejin:** Directly files chat me bhejte rahein\n"
                    "• **/done:** Upload khatam hone par Single Link paane ke liye bhejie\n"
                    "• **/cancel:** Pending upload list ko khali karne ke liye\n\n"
                    "Note: Sabhi files **Permanently Safe** rahengi.",
                    reply_markup=kb.back("main_dash")
                )
            elif data == "check_sub":
                settings = await repo.get_settings(clone_id, db_name)
                fsubs = settings.get("force_subs", []) or config.FORCE_SUB_CHANNELS
                all_ok, _, _ = await check_all_subs(c, uid, fsubs)
                if all_ok: await q.message.edit_text("✅ Verification Successful! Now use /start.")
                else: await q.answer("❌ Abhi bhi channels join nahi hain!", show_alert=True)
            elif data.startswith("a_") or data == "admin_dashboard":
                await admin_callback_handler(c, q, clone_id)

        client.add_handler(MessageHandler(start_c, filters.command("start") & filters.private))
        client.add_handler(MessageHandler(done_c, filters.command("done") & filters.private))
        client.add_handler(MessageHandler(cancel_c, filters.command("cancel") & filters.private))
        client.add_handler(MessageHandler(media_c, filters.private & ~filters.command([
            "start","done","cancel","admin","broadcast","add_clone","delete_clone","add_fsub","add_mod","ban","unban"
        ])))
        client.add_handler(CallbackQueryHandler(cb_c))

    async def load_all_clones(self):
        clones = await repo.get_all_clones()
        for c in clones:
            try:
                if c.get("mongo_uri"): await database.connect(c["clone_id"], uri=c["mongo_uri"])
                client = Client(f"clone_{c['clone_id']}", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=c["bot_token"], workers=20)
                self._register_handlers(client, c["clone_id"])
                await client.start()
                _clients[c["clone_id"]] = client
                log.info(f"Loaded Clone: @{c.get('bot_username','?')}")
            except Exception as e: log.error(f"Clone loading failed {c['clone_id']}: {e}")

    async def create_clone(self, token: str, mongo_uri: Optional[str] = None) -> Optional[dict]:
        temp = Client("_temp", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=token, in_memory=True)
        try:
            await temp.start()
            me = temp.me; await temp.stop()
            cid = generate_clone_id()
            data = {"clone_id": cid, "bot_token": token, "bot_username": me.username or f"bot_{me.id}", "bot_id": me.id, "status": "active", "mongo_uri": mongo_uri, "created_at": datetime.now(timezone.utc).isoformat()}
            await repo.register_clone(data)

            if mongo_uri: await database.connect(cid, uri=mongo_uri)

            client = Client(f"clone_{cid}", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=token, workers=20)
            self._register_handlers(client, cid)
            await client.start()
            _clients[cid] = client
            return data
        except Exception as e:
            log.error(f"Failed to create clone: {e}"); return None

clone_mgr = CloneManager()

# ═══════════════════════════════════════════════════════════════════════════════
# 9. ADMIN ACTIONS & COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

async def admin_callback_handler(c: Client, q: CallbackQuery, clone_id: str):
    data = q.data; msg = q.message; uid = q.from_user.id
    if uid != config.OWNER_ID and uid not in config.ADMIN_IDS:
        await q.answer("❌ Unauthorized", show_alert=True); return

    if data in ["admin_dashboard", "main_dash"]:
        await msg.edit_text("<b>⚙️ Admin Control Panel</b>", reply_markup=kb.admin()); return

    if data == "a_stats":
        clones = await repo.get_all_clones()
        up = fmt_time(int(time.time() - _start_time))
        await msg.edit_text(f"<b>📊 Statistics</b>\n\nClones Active: <code>{len(clones)}</code>\nEngine Uptime: <code>{up}</code>\nStorage: Permanent Enabled", reply_markup=kb.back())

    elif data == "a_clones":
        clones = await repo.get_all_clones()
        txt = f"<b>🤖 Active Clones ({len(clones)}):</b>\n\n"
        for cl in clones: txt += f"• @{cl.get('bot_username')} (<code>{cl['clone_id']}</code>)\n"
        txt += "\nClone add karne ke liye command send karein:\n`/add_clone <TOKEN> [MONGO_URI]`"
        await msg.edit_text(txt, reply_markup=kb.back())

    elif data == "a_broadcast":
        await msg.edit_text("📢 **Broadcast:** Kisi bhi message ko reply karke `/broadcast` likhein.", reply_markup=kb.back())

async def admin_cmd(c: Client, m: Message):
    if m.from_user.id in config.ADMIN_IDS or m.from_user.id == config.OWNER_ID:
        await m.reply("⚙️ **Admin Dashboard**", reply_markup=kb.admin())

async def broadcast_cmd(c: Client, m: Message):
    if m.from_user.id not in config.ADMIN_IDS and m.from_user.id != config.OWNER_ID: return
    if not m.reply_to_message: await m.reply("Reply to a message to broadcast."); return
    
    pm = await m.reply("📢 Broadcasting...")
    col = await database.col("users", "main")
    users = await col.find({}, {"user_id": 1}).to_list(length=100000)
    succ = 0; fail = 0

    for u in users:
        try:
            await m.reply_to_message.copy(u["user_id"])
            succ += 1
        except: fail += 1
        await asyncio.sleep(0.04)

    await pm.edit_text(f"✅ **Broadcast Done!**\nSuccess: {succ} | Failed: {fail}")

async def add_clone_cmd(c: Client, m: Message):
    if m.from_user.id != config.OWNER_ID: return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 2:
        await m.reply("Usage: `/add_clone <BOT_TOKEN> [CUSTOM_MONGO_URI]`")
        return
    token = parts[1].strip()
    m_uri = parts[2].strip() if len(parts) > 2 else None

    res = await clone_mgr.create_clone(token, m_uri)
    if res: await m.reply(f"✅ Clone Active: @{res['bot_username']} (ID: `{res['clone_id']}`)")
    else: await m.reply("❌ Clone Setup Failed.")

async def delete_clone_cmd(c: Client, m: Message):
    if m.from_user.id != config.OWNER_ID: return
    parts = m.text.split()
    if len(parts) < 2: await m.reply("Usage: `/delete_clone <CLONE_ID>`"); return
    cid = parts[1].strip()
    await repo.delete_clone(cid)
    await m.reply(f"🗑 Clone `{cid}` removed.")

async def ban_cmd(c: Client, m: Message):
    if not await repo.is_mod(m.from_user.id): return
    if not m.reply_to_message and len(m.text.split()) < 2: return
    uid = m.reply_to_message.from_user.id if m.reply_to_message else int(m.text.split()[1])
    await repo.ban_user(uid)
    await m.reply(f"🚫 User `{uid}` banned.")

async def unban_cmd(c: Client, m: Message):
    if not await repo.is_mod(m.from_user.id): return
    if not m.reply_to_message and len(m.text.split()) < 2: return
    uid = m.reply_to_message.from_user.id if m.reply_to_message else int(m.text.split()[1])
    await repo.unban_user(uid)
    await m.reply(f"✅ User `{uid}` unbanned.")

# ═══════════════════════════════════════════════════════════════════════════════
# 10. ENGINE BOOTSTRAPPER
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    global _start_time
    _start_time = time.time()

    log.info("Starting File Store Engine...")
    Config.validate()
    await database.connect("main")
    await repo.ensure_indexes("main")

    mc = await clone_mgr.init_main()

    mc.add_handler(MessageHandler(admin_cmd, filters.command("admin") & filters.private))
    mc.add_handler(MessageHandler(broadcast_cmd, filters.command("broadcast") & filters.private))
    mc.add_handler(MessageHandler(add_clone_cmd, filters.command("add_clone") & filters.private))
    mc.add_handler(MessageHandler(delete_clone_cmd, filters.command("delete_clone") & filters.private))
    mc.add_handler(MessageHandler(ban_cmd, filters.command("ban") & filters.private))
    mc.add_handler(MessageHandler(unban_cmd, filters.command("unban") & filters.private))

    clone_mgr._register_handlers(mc, "main")

    await mc.start()
    log.info(f"✅ Main Bot Active: @{config.BOT_USERNAME}")

    await clone_mgr.load_all_clones()
    await delivery_engine.start(config.DELIVERY_WORKERS)

    log.info("🚀 System Fully Active!")
    await idle()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Engine Stopped.")
    
    
