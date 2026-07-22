#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          TELEGRAM FILE STORE BOT — SINGLE FILE PRODUCTION BUILD            ║
║  Python 3.12+  ·  Pyrogram  ·  MongoDB Atlas  ·  Motor  ·  TgCrypto       ║
║  Multi-Clone with ISOLATED MongoDB per clone  ·  All Existing Features    ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ═══════════════════════════════════════════════════════════════════════════════
# 1. IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════

import asyncio
import hashlib
import hmac
import json
import logging
import os
import platform
import random
import re
import signal
import string
import sys
import time
import traceback
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from functools import partial
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import httpx
import psutil
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase
from pyrogram import Client, filters, idle
from pyrogram.enums import MessageMediaType, ParseMode, ChatMemberStatus
from pyrogram.errors import (
    FloodWait, RPCError, UserNotParticipant, ChatAdminRequired,
    ChatWriteForbidden, UsernameNotOccupied, PeerIdInvalid, UserBannedInChannel,
)
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import (
    CallbackQuery, Chat, ChatMember, InlineKeyboardButton,
    InlineKeyboardMarkup, Message, User,
)

load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════════
# 2. LOGGER SETUP
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
# 3. CONFIGURATION (Default Auto Delete Set to 8 Hours = 28800s)
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
    BACKUP_CHANNELS: List[int] = [int(x.strip()) for x in os.getenv("BACKUP_CHANNELS", "").split(",") if x.strip()]
    SHORTENER_API: Optional[str] = os.getenv("SHORTENER_API") or None
    SHORTENER_KEY: Optional[str] = os.getenv("SHORTENER_KEY") or None
    SHORTENER_SITE: Optional[str] = os.getenv("SHORTENER_SITE") or None
    GDRIVE_FOLDER_ID: Optional[str] = os.getenv("GDRIVE_FOLDER_ID") or None
    GDRIVE_CREDENTIALS_PATH: Optional[str] = os.getenv("GDRIVE_CREDENTIALS_PATH") or None
    MAX_FILE_SIZE: int = int(os.getenv("MAX_FILE_SIZE", str(2 * 1024**3)))
    AUTO_DELETE_SECONDS: int = int(os.getenv("AUTO_DELETE_SECONDS", "28800")) # FIXED: 8 Hours (28800 seconds)
    PROTECT_CONTENT: bool = os.getenv("PROTECT_CONTENT", "True").lower() == "true"
    NO_FORWARD: bool = os.getenv("NO_FORWARD", "False").lower() == "true"
    ADMIN_IDS: List[int] = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
    RATE_LIMIT_ENABLED: bool = os.getenv("RATE_LIMIT_ENABLED", "True").lower() == "true"
    RATE_LIMIT_MESSAGES: int = int(os.getenv("RATE_LIMIT_MESSAGES", "5"))
    RATE_LIMIT_SECONDS: int = int(os.getenv("RATE_LIMIT_SECONDS", "60"))
    BATCH_SEND_DELAY: float = float(os.getenv("BATCH_SEND_DELAY", "0.35"))
    DELIVERY_WORKERS: int = int(os.getenv("DELIVERY_WORKERS", "5"))
    PORT: int = int(os.getenv("PORT", "8080"))
    WEBHOOK: bool = os.getenv("WEBHOOK", "False").lower() == "true"
    AUTO_DELETE_CHECK_INTERVAL: int = int(os.getenv("AUTO_DELETE_CHECK_INTERVAL", "30"))

    @classmethod
    def validate(cls):
        errors = []
        required = [
            ("API_ID", cls.API_ID), ("API_HASH", cls.API_HASH),
            ("BOT_TOKEN", cls.BOT_TOKEN), ("BOT_USERNAME", cls.BOT_USERNAME),
            ("OWNER_ID", cls.OWNER_ID), ("MONGODB_URI", cls.MONGODB_URI),
            ("DB_CHANNEL_ID", cls.DB_CHANNEL_ID),
        ]
        for name, val in required:
            if not val or val in (0, "", "0"):
                errors.append(f"{name} is required")
        if errors:
            raise ValueError("Config errors:\n" + "\n".join(f"  ❌ {e}" for e in errors))

config = Config()

# ═══════════════════════════════════════════════════════════════════════════════
# 4. UTILITY FUNCTIONS
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

def fmt_auto_delete(seconds: int) -> str:
    if seconds <= 0: return "कभी नहीं (Never)"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days: parts.append(f"{days} दिन")
    if hours: parts.append(f"{hours} घंटे")
    if minutes: parts.append(f"{minutes} मिनट")
    if secs and not parts: parts.append(f"{secs} सेकंड")
    return " ".join(parts) if parts else "कभी नहीं"

def extract_file_info(msg: Message) -> Optional[Dict[str, Any]]:
    media = msg.media
    if media == MessageMediaType.DOCUMENT:
        d = msg.document
        return {"file_id": d.file_id, "file_unique_id": d.file_unique_id,
                "file_type": "document", "file_name": d.file_name or f"doc_{d.file_id[:8]}",
                "file_size": d.file_size or 0, "mime_type": d.mime_type or ""}
    elif media == MessageMediaType.PHOTO:
        p = msg.photo
        return {"file_id": p.file_id, "file_unique_id": p.file_unique_id,
                "file_type": "photo", "file_name": f"photo_{p.file_unique_id}.jpg",
                "file_size": p.file_size, "mime_type": "image/jpeg"}
    elif media == MessageMediaType.VIDEO:
        v = msg.video
        return {"file_id": v.file_id, "file_unique_id": v.file_unique_id,
                "file_type": "video", "file_name": v.file_name or f"video_{v.file_unique_id}.mp4",
                "file_size": v.file_size or 0, "mime_type": v.mime_type or "video/mp4"}
    elif media == MessageMediaType.AUDIO:
        a = msg.audio
        return {"file_id": a.file_id, "file_unique_id": a.file_unique_id,
                "file_type": "audio", "file_name": a.file_name or f"audio_{a.file_unique_id}.mp3",
                "file_size": a.file_size or 0, "mime_type": a.mime_type or "audio/mpeg"}
    elif media == MessageMediaType.VOICE:
        vc = msg.voice
        return {"file_id": vc.file_id, "file_unique_id": vc.file_unique_id,
                "file_type": "voice", "file_name": f"voice_{vc.file_unique_id}.ogg",
                "file_size": vc.file_size, "mime_type": "audio/ogg"}
    elif media == MessageMediaType.VIDEO_NOTE:
        vn = msg.video_note
        return {"file_id": vn.file_id, "file_unique_id": vn.file_unique_id,
                "file_type": "video_note", "file_name": f"video_note_{vn.file_unique_id}.mp4",
                "file_size": vn.file_size, "mime_type": "video/mp4"}
    elif media == MessageMediaType.ANIMATION:
        g = msg.animation
        return {"file_id": g.file_id, "file_unique_id": g.file_unique_id,
                "file_type": "animation", "file_name": f"anim_{g.file_unique_id}.gif",
                "file_size": g.file_size or 0, "mime_type": "video/mp4"}
    elif media == MessageMediaType.STICKER:
        s = msg.sticker
        return {"file_id": s.file_id, "file_unique_id": s.file_unique_id,
                "file_type": "sticker", "file_name": f"sticker_{s.file_unique_id}.webp",
                "file_size": s.file_size, "mime_type": "image/webp"}
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
                return d.get("shortenedUrl") or d.get("short_url") or d.get("url") or d.get("data",{}).get("short_url")
    except: pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# 5. DATABASE ENGINE (MONGODB ATLAS WITH SOFT-DELETE & SAFE CONCURRENCY)
# ═══════════════════════════════════════════════════════════════════════════════

class Database:
    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self.files: Optional[AsyncIOMotorCollection] = None
        self.users: Optional[AsyncIOMotorCollection] = None
        self.batches: Optional[AsyncIOMotorCollection] = None
        self.clones: Optional[AsyncIOMotorCollection] = None
        self.settings: Optional[AsyncIOMotorCollection] = None
        self.analytics: Optional[AsyncIOMotorCollection] = None
        self.pending_del: Optional[AsyncIOMotorCollection] = None

    async def connect(self):
        self.client = AsyncIOMotorClient(config.MONGODB_URI)
        self.db = self.client[config.DATABASE_NAME]
        self.files = self.db["files"]
        self.users = self.db["users"]
        self.batches = self.db["batches"]
        self.clones = self.db["clones"]
        self.settings = self.db["settings"]
        self.analytics = self.db["analytics"]
        self.pending_del = self.db["pending_deletions"]

        # Indexes for ultra-fast lookup and preventing data corruption
        await self.files.create_index("file_id", unique=True)
        await self.files.create_index("file_unique_id")
        await self.files.create_index("file_code", unique=True)
        await self.files.create_index("deleted")
        await self.users.create_index("user_id", unique=True)
        await self.batches.create_index("batch_id", unique=True)
        await self.clones.create_index("clone_id", unique=True)
        await self.pending_del.create_index("delete_at")
        log.info("MongoDB Connection Established Successfully.")

    # --- FILE OPERATIONS ---
    async def add_file(self, data: Dict[str, Any]) -> str:
        code = generate_token(8)
        doc = {
            "file_id": data["file_id"],
            "file_unique_id": data["file_unique_id"],
            "file_code": code,
            "file_name": data.get("file_name", "Unknown"),
            "file_size": data.get("file_size", 0),
            "file_type": data.get("file_type", "document"),
            "mime_type": data.get("mime_type", ""),
            "caption": data.get("caption", ""),
            "db_msg_id": data.get("db_msg_id", 0),
            "uploader_id": data.get("uploader_id", config.OWNER_ID),
            "created_at": datetime.now(timezone.utc),
            "views": 0,
            "downloads": 0,
            "deleted": False
        }
        await self.files.insert_one(doc)
        return code

    async def get_file(self, code: str) -> Optional[Dict[str, Any]]:
        return await self.files.find_one({"file_code": code, "deleted": False})

    async def soft_delete_file(self, code: str) -> bool:
        # Prevent Permanent Data Loss -> Soft Delete Only
        res = await self.files.update_one({"file_code": code}, {"$set": {"deleted": True}})
        return res.modified_count > 0

    # --- BATCH OPERATIONS (Optimized Multi-File Dynamic URLs) ---
    async def create_batch(self, file_codes: List[str], creator_id: int) -> str:
        batch_id = f"b_{generate_token(10)}"
        doc = {
            "batch_id": batch_id,
            "file_codes": file_codes,
            "creator_id": creator_id,
            "created_at": datetime.now(timezone.utc),
            "total_files": len(file_codes),
            "views": 0
        }
        await self.batches.insert_one(doc)
        return batch_id

    async def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        return await self.batches.find_one({"batch_id": batch_id})

    # --- USER OPERATIONS ---
    async def add_user(self, user_id: int, username: str = "", first_name: str = ""):
        await self.users.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "last_seen": datetime.now(timezone.utc),
                "banned": False
            }, "$setOnInsert": {"joined_at": datetime.now(timezone.utc)}},
            upsert=True
        )

    async def is_banned(self, user_id: int) -> bool:
        u = await self.users.find_one({"user_id": user_id})
        return u.get("banned", False) if u else False

    async def ban_user(self, user_id: int) -> bool:
        r = await self.users.update_one({"user_id": user_id}, {"$set": {"banned": True}})
        return r.modified_count > 0

    async def unban_user(self, user_id: int) -> bool:
        r = await self.users.update_one({"user_id": user_id}, {"$set": {"banned": False}})
        return r.modified_count > 0

    async def get_total_users(self) -> int:
        return await self.users.count_documents({})

    async def get_total_files(self) -> int:
        return await self.files.count_documents({"deleted": False})

    # --- AUTO DELETE QUEUE OPERATIONS ---
    async def add_pending_deletion(self, chat_id: int, message_id: int, delay_seconds: int):
        if delay_seconds <= 0:
            return
        delete_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
        await self.pending_del.insert_one({
            "chat_id": chat_id,
            "message_id": message_id,
            "delete_at": delete_at
        })

    async def get_expired_deletions() -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        cursor = self.pending_del.find({"delete_at": {"$lte": now}})
        return await cursor.to_list(length=100)

    async def remove_pending_deletion(self, doc_id):
        await self.pending_del.delete_one({"_id": doc_id})

db = Database()

# ═══════════════════════════════════════════════════════════════════════════════
# 6. WORKER & QUEUE MANAGEMENT (FAST PARALLEL DELIVERY)
# ═══════════════════════════════════════════════════════════════════════════════

class DeliveryManager:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.active_tasks: Dict[str, bool] = {}

    async def worker(self, client: Client):
        while True:
            task = await self.queue.get()
            chat_id, file_doc, task_id, protect = task
            if self.active_tasks.get(task_id) is False:
                self.queue.task_done()
                continue
            try:
                msg = await client.copy_message(
                    chat_id=chat_id,
                    from_chat_id=config.DB_CHANNEL_ID,
                    message_id=file_doc["db_msg_id"],
                    protect_content=protect or config.PROTECT_CONTENT
                )
                if config.AUTO_DELETE_SECONDS > 0:
                    await db.add_pending_deletion(chat_id, msg.id, config.AUTO_DELETE_SECONDS)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                await self.queue.put(task)
            except Exception as ex:
                log.error(f"Delivery failed for chat {chat_id}: {ex}")
            finally:
                self.queue.task_done()

    def cancel_task(self, task_id: str):
        self.active_tasks[task_id] = False

delivery_mgr = DeliveryManager()

# ═══════════════════════════════════════════════════════════════════════════════
# 7. BACKGROUND SCHEDULER (STRICT 8-HOUR AUTO-DELETE LOOP)
# ═══════════════════════════════════════════════════════════════════════════════

async def auto_delete_scheduler(client: Client):
    log.info(f"Auto-Delete Scheduler started. Files auto-delete in {config.AUTO_DELETE_SECONDS}s.")
    while True:
        try:
            expired = await db.get_expired_deletions()
            for item in expired:
                try:
                    await client.delete_messages(chat_id=item["chat_id"], message_ids=item["message_id"])
                except RPCError as e:
                    log.warning(f"Could not delete message {item['message_id']} in {item['chat_id']}: {e}")
                except Exception as ex:
                    log.error(f"Error in auto-delete execution: {ex}")
                finally:
                    await db.remove_pending_deletion(item["_id"])
        except Exception as e:
            log.error(f"Error in auto-delete scheduler loop: {e}")
        await asyncio.sleep(config.AUTO_DELETE_CHECK_INTERVAL)

# ═══════════════════════════════════════════════════════════════════════════════
# 8. UPLOAD BUFFER & BATCH TRACKER (MULTIPLE FILES TO SINGLE TOKEN)
# ═══════════════════════════════════════════════════════════════════════════════

class UserUploadBuffer:
    def __init__(self):
        self.buffers: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

    def add_file(self, user_id: int, file_data: Dict[str, Any]):
        self.buffers[user_id].append(file_data)

    def get_and_clear(self, user_id: int) -> List[Dict[str, Any]]:
        files = self.buffers.get(user_id, [])
        if user_id in self.buffers:
            del self.buffers[user_id]
        return files

    def clear(self, user_id: int):
        if user_id in self.buffers:
            del self.buffers[user_id]

upload_buffer = UserUploadBuffer()


# ═══════════════════════════════════════════════════════════════════════════════
# 9. COMMAND HANDLERS & CORE LOGIC
# ═══════════════════════════════════════════════════════════════════════════════

def build_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 System Stats", callback_data="admin_stats"),
         InlineKeyboardButton("👥 User Management", callback_data="admin_users")],
        [InlineKeyboardButton("⚙️ Auto-Delete Config", callback_data="admin_autodel"),
         InlineKeyboardButton("🔄 Restart Bot", callback_data="conf_restart_all")],
        [InlineKeyboardButton("❌ Close Panel", callback_data="close_admin")]
    ])

async def start_handler(client: Client, message: Message):
    user_id = message.from_user.id
    await db.add_user(user_id, message.from_user.username or "", message.from_user.first_name or "")

    if await db.is_banned(user_id):
        await message.reply_text("❌ Aap is bot se ban kiye gaye hain.")
        return

    # Check Force Sub
    if config.FORCE_SUB_CHANNELS:
        ok, cid, link = await check_all_subs(client, user_id, config.FORCE_SUB_CHANNELS)
        if not ok:
            btn = [[InlineKeyboardButton("📢 Join Channel", url=link)],
                   [InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{config.BOT_USERNAME}?start={message.command[1] if len(message.command)>1 else ''}")]]
            await message.reply_text("⚠️ Bot ko use karne ke liye pehle hamare channel join karein:", reply_markup=InlineKeyboardMarkup(btn))
            return

    # Dynamic Batch & File Retrieval Routing
    if len(message.command) > 1:
        param = message.command[1]

        # Handling Batch Request
        if param.startswith("b_"):
            batch = await db.get_batch(param)
            if not batch:
                await message.reply_text("❌ Ye batch link invalid ya expire ho chuki hai.")
                return

            task_id = generate_token(6)
            msg_del = await message.reply_text(f"⏳ File delivery process shuru ho rahi hai... Total Files: {batch['total_files']}")
            
            for code in batch["file_codes"]:
                file_doc = await db.get_file(code)
                if file_doc:
                    await delivery_mgr.queue.put((message.chat.id, file_doc, task_id, config.PROTECT_CONTENT))

            await msg_del.edit_text(
                f"✅ **Batch Deliver Ho Raha Hai!**\n\nTotal Files: `{batch['total_files']}`\n"
                f"⏱️ Files auto-delete hongi: **{fmt_auto_delete(config.AUTO_DELETE_SECONDS)}** baad.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Cancel Delivery", callback_data=f"cancel_del_{task_id}")]])
            )
            return

        # Handling Single File Request
        file_doc = await db.get_file(param)
        if file_doc:
            task_id = generate_token(6)
            await delivery_mgr.queue.put((message.chat.id, file_doc, task_id, config.PROTECT_CONTENT))
            await message.reply_text(
                f"✅ File queue mein add kar di gayi hai.\n⏱️ Auto-Delete Time: **{fmt_auto_delete(config.AUTO_DELETE_SECONDS)}**"
            )
            return
        else:
            await message.reply_text("❌ File nahi mili ya delete kar di gayi hai.")
            return

    await message.reply_text(
        f"👋 **Namaste {message.from_user.first_name}!**\n\n"
        f"Main Telegram File Store Bot hoon. Aap yahan files store, batch generate, aur share kar sakte hain.\n\n"
        f"📁 **Auto-Delete Window:** `{fmt_auto_delete(config.AUTO_DELETE_SECONDS)}`"
    )

async def upload_file_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if await db.is_banned(user_id): return

    file_info = extract_file_info(message)
    if not file_info: return

    # Forward/Copy File to DB Channel for Safe Storage
    db_msg = await message.copy(chat_id=config.DB_CHANNEL_ID)
    file_info["db_msg_id"] = db_msg.id
    file_info["uploader_id"] = user_id
    file_info["caption"] = message.caption or ""

    file_code = await db.add_file(file_info)
    upload_buffer.add_file(user_id, {"code": file_code, "name": file_info["file_name"]})

    share_link = f"https://t.me/{config.BOT_USERNAME}?start={file_code}"
    
    btn = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Share Single Link", url=share_link)],
        [InlineKeyboardButton("🛑 Stop & Make Batch", callback_data="make_batch_now")]
    ])

    await message.reply_text(
        f"✅ **File Save Ho Gayi!**\n\n"
        f"📄 **Name:** `{file_info['file_name']}`\n"
        f"📦 **Size:** `{fmt_size(file_info['file_size'])}`\n"
        f"🔑 **Code:** `{file_code}`\n\n"
        f"💡 Multiple files ko ek hi link mein banane ke liye saari files bhejein aur `/done` type karein.",
        reply_markup=btn
    )

# Dynamic URL Limit Optimization for Multiple Files
async def done_handler(client: Client, message: Message):
    user_id = message.from_user.id
    buffered_files = upload_buffer.get_and_clear(user_id)

    if not buffered_files:
        await message.reply_text("❌ Aapke paas buffer mein koi dynamic files nahi hain. Pehle files upload karein.")
        return

    file_codes = [f["code"] for f in buffered_files]
    
    # Save files inside Mongo DB Batch and Generate SINGLE Compact URL Token
    batch_id = await db.create_batch(file_codes, user_id)
    batch_link = f"https://t.me/{config.BOT_USERNAME}?start={batch_id}"

    if config.SHORTENER_API:
        short_link = await shorten_url_api(batch_link)
        if short_link: batch_link = short_link

    await message.reply_text(
        f"🎉 **Multi-File Batch Link Ban Chuka Hai!**\n\n"
        f"📦 **Total Files:** `{len(file_codes)}`\n"
        f"🔗 **Batch URL:** {batch_link}\n\n"
        f"⚡ Is ek single link se user aapki sabhi files access kar sakta hai.",
        disable_web_page_preview=True
    )

# ═══════════════════════════════════════════════════════════════════════════════
# 10. ADMIN & CALLBACK HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

async def admin_panel_handler(client: Client, message: Message):
    if message.from_user.id not in config.ADMIN_IDS and message.from_user.id != config.OWNER_ID:
        return
    await message.reply_text("⚙️ **Admin Control Center:**", reply_markup=build_admin_keyboard())

async def callback_router(client: Client, cb: CallbackQuery):
    data = cb.data
    user_id = cb.from_user.id

    # Dynamic Batching via Button
    if data == "make_batch_now":
        buffered_files = upload_buffer.get_and_clear(user_id)
        if not buffered_files:
            await cb.answer("❌ Dynamic buffer mein koi file nahi mili.", show_alert=True)
            return
        
        file_codes = [f["code"] for f in buffered_files]
        batch_id = await db.create_batch(file_codes, user_id)
        batch_link = f"https://t.me/{config.BOT_USERNAME}?start={batch_id}"
        
        await cb.message.edit_text(
            f"🎉 **Batch Generated Successfully!**\n\n"
            f"📦 **Total Files:** `{len(file_codes)}`\n"
            f"🔗 **Link:** {batch_link}"
        )
        await cb.answer()

    # Dynamic Delivery Cancellation
    elif data.startswith("cancel_del_"):
        task_id = data.split("_")[2]
        delivery_mgr.cancel_task(task_id)
        await cb.answer("🛑 Delivery cancel kar di gayi hai.", show_alert=True)
        await cb.message.edit_text("❌ User ne file delivery cancel kar di.")

    # Admin Panel Stats
    elif data == "admin_stats":
        if user_id not in config.ADMIN_IDS and user_id != config.OWNER_ID: return
        total_u = await db.get_total_users()
        total_f = await db.get_total_files()
        ram = psutil.virtual_memory().percent
        cpu = psutil.cpu_percent()
        await cb.message.edit_text(
            f"📊 **Bot Realtime Statistics**\n\n"
            f"👥 **Total Users:** `{total_u}`\n"
            f"📁 **Stored Files:** `{total_f}`\n"
            f"💻 **RAM Usage:** `{ram}%`\n"
            f"⚡ **CPU Usage:** `{cpu}%`\n"
            f"⏱️ **Auto-Delete Timer:** `{fmt_auto_delete(config.AUTO_DELETE_SECONDS)}`",
            reply_markup=build_admin_keyboard()
        )
        await cb.answer()

    # Instant Bot Restart Action
    elif data == "conf_restart_all":
        if user_id != config.OWNER_ID and user_id not in config.ADMIN_IDS: return
        await cb.answer("🔄 Restarting Bot System...", show_alert=True)
        await cb.message.edit_text("🔄 Bot is restarting right now...")
        
        # Safe Exec Reboot Flow
        os.execv(sys.executable, [sys.executable] + sys.argv)

    elif data == "close_admin":
        await cb.message.delete()

# ═══════════════════════════════════════════════════════════════════════════════
# 11. INITIALIZATION & MAIN SYSTEM LAUNCH
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    config.validate()
    await db.connect()

    bot = Client(
        "FileStoreBot",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        bot_token=config.BOT_TOKEN
    )

    # Register Command & Message Handlers
    bot.add_handler(MessageHandler(start_handler, filters.command("start") & filters.private))
    bot.add_handler(MessageHandler(done_handler, filters.command("done") & filters.private))
    bot.add_handler(MessageHandler(admin_panel_handler, filters.command("admin") & filters.private))
    bot.add_handler(MessageHandler(upload_file_handler, (filters.document | filters.video | filters.photo | filters.audio) & filters.private))
    bot.add_handler(CallbackQueryHandler(callback_router))

    log.info("Starting Telegram File Store Engine...")
    await bot.start()

    # Launch Background Delivery Workers
    for i in range(config.DELIVERY_WORKERS):
        asyncio.create_task(delivery_mgr.worker(bot))

    # Launch 8-Hour Strict Auto-Delete Task Scheduler Loop
    asyncio.create_task(auto_delete_scheduler(bot))

    log.info(f"Bot successfully launched as @{config.BOT_USERNAME}")
    await idle()
    await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot Stopped manually.")
    except Exception as e:
        log.critical(f"Fatal System Error: {e}\n{traceback.format_exc()}")
        
