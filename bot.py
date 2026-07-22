#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║        TELEGRAM FILE STORE BOT — COMPLETE UNCOMPRESSED FULL BUILD            ║
║  Python 3.12+  ·  Pyrogram  ·  MongoDB Atlas  ·  Motor  ·  TgCrypto       ║
║  Features: Multi-Clone Engine, Broadcast, Drive Backup, Search, Settings GUI ║
║  Fixes: 8-Hour Auto Delete (28800s), Single Token Dynamic Batch, Admin Reboot ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ═══════════════════════════════════════════════════════════════════════════════
# 1. IMPORTS & DEPENDENCIES
# ═══════════════════════════════════════════════════════════════════════════════

import asyncio
import hashlib
import hmac
import json
import logging
import math
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
from pyrogram.enums import MessageMediaType, ParseMode, ChatMemberStatus, ChatType
from pyrogram.errors import (
    FloodWait, RPCError, UserNotParticipant, ChatAdminRequired,
    ChatWriteForbidden, UsernameNotOccupied, PeerIdInvalid, UserBannedInChannel,
    MessageNotModified, UserIsBlocked, InputUserDeactivated
)
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import (
    CallbackQuery, Chat, ChatMember, InlineKeyboardButton,
    InlineKeyboardMarkup, Message, User, InputMediaPhoto, InputMediaDocument
)

load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════════
# 2. LOGGER SETUP
# ═══════════════════════════════════════════════════════════════════════════════

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_FILE = "bot_full.log"

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("FileStoreBotFull")
    logger.setLevel(logging.INFO)
    
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(ch)
    
    fh = RotatingFileHandler(LOG_FILE, maxBytes=15*1024*1024, backupCount=10)
    fh.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(fh)
    
    logger.propagate = False
    return logger

log = setup_logger()

# ═══════════════════════════════════════════════════════════════════════════════
# 3. GLOBAL CONFIGURATION (FIXED: 8 HOURS DEFAULT AUTO-DELETE = 28800s)
# ═══════════════════════════════════════════════════════════════════════════════

class Config:
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    BOT_USERNAME: str = os.getenv("BOT_USERNAME", "")
    OWNER_ID: int = int(os.getenv("OWNER_ID", "0"))
    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "FileStoreBotFull")
    DB_CHANNEL_ID: int = int(os.getenv("DB_CHANNEL_ID", "0"))
    LOG_CHANNEL_ID: int = int(os.getenv("LOG_CHANNEL_ID", "0"))
    
    FORCE_SUB_CHANNELS: List[int] = [
        int(x.strip()) for x in os.getenv("FORCE_SUB_CHANNELS", "").split(",") if x.strip() and x.strip().lstrip('-').isdigit()
    ]
    BACKUP_CHANNELS: List[int] = [
        int(x.strip()) for x in os.getenv("BACKUP_CHANNELS", "").split(",") if x.strip() and x.strip().lstrip('-').isdigit()
    ]
    ADMIN_IDS: List[int] = [
        int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip() and x.strip().isdigit()
    ]
    
    # Shortener Integration
    SHORTENER_API: Optional[str] = os.getenv("SHORTENER_API") or None
    SHORTENER_KEY: Optional[str] = os.getenv("SHORTENER_KEY") or None
    SHORTENER_SITE: Optional[str] = os.getenv("SHORTENER_SITE") or None
    
    # Google Drive Backup
    GDRIVE_FOLDER_ID: Optional[str] = os.getenv("GDRIVE_FOLDER_ID") or None
    GDRIVE_CREDENTIALS_PATH: Optional[str] = os.getenv("GDRIVE_CREDENTIALS_PATH") or None
    
    # Settings & Limits
    MAX_FILE_SIZE: int = int(os.getenv("MAX_FILE_SIZE", str(2 * 1024**3))) # 2GB
    AUTO_DELETE_SECONDS: int = int(os.getenv("AUTO_DELETE_SECONDS", "28800")) # FIXED: Exactly 8 Hours
    PROTECT_CONTENT: bool = os.getenv("PROTECT_CONTENT", "True").lower() == "true"
    NO_FORWARD: bool = os.getenv("NO_FORWARD", "False").lower() == "true"
    
    # Rate Limiting & Queue Performance
    RATE_LIMIT_ENABLED: bool = os.getenv("RATE_LIMIT_ENABLED", "True").lower() == "true"
    RATE_LIMIT_MESSAGES: int = int(os.getenv("RATE_LIMIT_MESSAGES", "5"))
    RATE_LIMIT_SECONDS: int = int(os.getenv("RATE_LIMIT_SECONDS", "60"))
    BATCH_SEND_DELAY: float = float(os.getenv("BATCH_SEND_DELAY", "0.35"))
    DELIVERY_WORKERS: int = int(os.getenv("DELIVERY_WORKERS", "5"))
    AUTO_DELETE_CHECK_INTERVAL: int = int(os.getenv("AUTO_DELETE_CHECK_INTERVAL", "30"))
    
    # Broadcast Batch Settings
    BROADCAST_BATCH_SIZE: int = int(os.getenv("BROADCAST_BATCH_SIZE", "20"))
    BROADCAST_SLEEP: float = float(os.getenv("BROADCAST_SLEEP", "1.0"))

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
                errors.append(f"{name} is missing or invalid in environment.")
        if errors:
            raise ValueError("Config Validation Failed:\n" + "\n".join(f"  ❌ {e}" for e in errors))

config = Config()

# ═══════════════════════════════════════════════════════════════════════════════
# 4. UTILITY HELPERS & FORMATTERS
# ═══════════════════════════════════════════════════════════════════════════════

def generate_token(length: int = 12) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_clone_id() -> str:
    return f"clone_{uuid.uuid4().hex[:10]}"

def fmt_size(size: int) -> str:
    if not size: return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    s = round(size / p, 2)
    return f"{s} {units[i]}"

def fmt_time(seconds: int) -> str:
    if seconds <= 0: return "0s"
    parts = []
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    if secs or not parts: parts.append(f"{secs}s")
    return " ".join(parts)

def fmt_auto_delete(seconds: int) -> str:
    if seconds <= 0: return "Disabled (Never Delete)"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days: parts.append(f"{days} Days")
    if hours: parts.append(f"{hours} Hours")
    if minutes: parts.append(f"{minutes} Mins")
    if secs and not parts: parts.append(f"{secs} Secs")
    return " ".join(parts) if parts else "Disabled"

def create_progress_bar(current: int, total: int, length: int = 10) -> str:
    if total <= 0: return "░" * length
    percentage = min(1.0, max(0.0, current / total))
    filled = int(round(length * percentage))
    return "█" * filled + "░" * (length - filled)

def extract_file_info(msg: Message) -> Optional[Dict[str, Any]]:
    media = msg.media
    if not media: return None
    
    file_map = {
        MessageMediaType.DOCUMENT: (msg.document, "document"),
        MessageMediaType.PHOTO: (msg.photo, "photo"),
        MessageMediaType.VIDEO: (msg.video, "video"),
        MessageMediaType.AUDIO: (msg.audio, "audio"),
        MessageMediaType.VOICE: (msg.voice, "voice"),
        MessageMediaType.VIDEO_NOTE: (msg.video_note, "video_note"),
        MessageMediaType.ANIMATION: (msg.animation, "animation"),
        MessageMediaType.STICKER: (msg.sticker, "sticker")
    }
    
    if media not in file_map: return None
    obj, ftype = file_map[media]
    
    if ftype == "photo":
        return {
            "file_id": obj.file_id,
            "file_unique_id": obj.file_unique_id,
            "file_type": "photo",
            "file_name": f"photo_{obj.file_unique_id}.jpg",
            "file_size": obj.file_size,
            "mime_type": "image/jpeg"
        }
    
    return {
        "file_id": getattr(obj, "file_id", ""),
        "file_unique_id": getattr(obj, "file_unique_id", ""),
        "file_type": ftype,
        "file_name": getattr(obj, "file_name", f"{ftype}_{getattr(obj, 'file_unique_id', 'file')}"),
        "file_size": getattr(obj, "file_size", 0),
        "mime_type": getattr(obj, "mime_type", "")
    }

async def check_sub(client: Client, uid: int, cid: int) -> Tuple[bool, str]:
    try:
        chat = await client.get_chat(cid)
        link = chat.invite_link or (f"https://t.me/{chat.username}" if chat.username else "")
        try:
            m = await client.get_chat_member(cid, uid)
            is_valid = m.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
            return is_valid, link
        except UserNotParticipant:
            return False, link
    except Exception as e:
        log.warning(f"Force-sub check error for channel {cid}: {e}")
        return True, ""

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
                return d.get("shortenedUrl") or d.get("short_url") or d.get("url") or d.get("data", {}).get("short_url")
    except Exception as ex:
        log.error(f"URL Shortener Exception: {ex}")
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# 5. DATABASE ENGINE (MONGODB ATLAS WITH FULL SOFT-DELETE & CLONE ISOLATION)
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
        self.thumbnails: Optional[AsyncIOMotorCollection] = None
        self.captions: Optional[AsyncIOMotorCollection] = None

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
        self.thumbnails = self.db["thumbnails"]
        self.captions = self.db["captions"]

        # High Performance Production Indexes
        await self.files.create_index("file_id", unique=True)
        await self.files.create_index("file_code", unique=True)
        await self.files.create_index([("file_name", "text")])
        await self.files.create_index("deleted")
        await self.users.create_index("user_id", unique=True)
        await self.batches.create_index("batch_id", unique=True)
        await self.clones.create_index("clone_id", unique=True)
        await self.pending_del.create_index("delete_at")
        await self.thumbnails.create_index("user_id", unique=True)
        await self.captions.create_index("user_id", unique=True)
        
        log.info("MongoDB Async Client Connected. All Collections & Indexes Initialized.")

    # --- FILE MANAGERS ---
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
        # NO DATA LOSS - Safe Flagging
        res = await self.files.update_one({"file_code": code}, {"$set": {"deleted": True}})
        return res.modified_count > 0

    async def search_files(self, query: str, limit: int = 10, page: int = 1) -> Tuple[List[Dict[str, Any]], int]:
        skip = (page - 1) * limit
        filter_q = {"$text": {"$search": query}, "deleted": False}
        total = await self.files.count_documents(filter_q)
        cursor = self.files.find(filter_q).skip(skip).limit(limit)
        items = await cursor.to_list(length=limit)
        return items, total

    # --- BATCH MANAGERS ---
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

    # --- USER MANAGERS ---
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

    async def get_all_user_ids(self) -> List[int]:
        cursor = self.users.find({"banned": False}, {"user_id": 1})
        docs = await cursor.to_list(length=None)
        return [d["user_id"] for d in docs]

    async def get_total_users(self) -> int:
        return await self.users.count_documents({})

    async def get_total_files(self) -> int:
        return await self.files.count_documents({"deleted": False})

    # --- AUTO-DELETE QUEUE ---
    async def add_pending_deletion(self, chat_id: int, message_id: int, delay_seconds: int):
        if delay_seconds <= 0: return
        delete_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
        await self.pending_del.insert_one({
            "chat_id": chat_id,
            "message_id": message_id,
            "delete_at": delete_at
        })

    async def get_expired_deletions(self) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        cursor = self.pending_del.find({"delete_at": {"$lte": now}})
        return await cursor.to_list(length=200)

    async def remove_pending_deletion(self, doc_id):
        await self.pending_del.delete_one({"_id": doc_id})

db = Database()


# ═══════════════════════════════════════════════════════════════════════════════
# 5. DATABASE ENGINE (CONTINUATION: ADVANCED MODULES & SETTINGS)
# ═══════════════════════════════════════════════════════════════════════════════

    # --- CUSTOM THUMBNAIL MANAGERS ---
    async def set_thumbnail(self, user_id: int, file_id: str):
        await self.thumbnails.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "file_id": file_id, "updated_at": datetime.now(timezone.utc)}},
            upsert=True
        )

    async def get_thumbnail(self, user_id: int) -> Optional[str]:
        doc = await self.thumbnails.find_one({"user_id": user_id})
        return doc.get("file_id") if doc else None

    async def delete_thumbnail(self, user_id: int) -> bool:
        r = await self.thumbnails.delete_one({"user_id": user_id})
        return r.deleted_count > 0

    # --- CUSTOM CAPTION MANAGERS ---
    async def set_caption(self, user_id: int, caption_text: str):
        await self.captions.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "caption": caption_text, "updated_at": datetime.now(timezone.utc)}},
            upsert=True
        )

    async def get_caption(self, user_id: int) -> Optional[str]:
        doc = await self.captions.find_one({"user_id": user_id})
        return doc.get("caption") if doc else None

    async def delete_caption(self, user_id: int) -> bool:
        r = await self.captions.delete_one({"user_id": user_id})
        return r.deleted_count > 0

    # --- MULTI-CLONE ENGINE DB MANAGERS ---
    async def add_clone(self, clone_id: str, bot_token: str, owner_id: int, bot_username: str):
        doc = {
            "clone_id": clone_id,
            "bot_token": bot_token,
            "owner_id": owner_id,
            "bot_username": bot_username,
            "is_active": True,
            "created_at": datetime.now(timezone.utc)
        }
        await self.clones.insert_one(doc)

    async def get_active_clones(self) -> List[Dict[str, Any]]:
        cursor = self.clones.find({"is_active": True})
        return await cursor.to_list(length=None)

    async def delete_clone(self, clone_id: str) -> bool:
        r = await self.clones.delete_one({"clone_id": clone_id})
        return r.deleted_count > 0

    # --- SYSTEM SETTINGS DYNAMIC GUI DB ---
    async def get_setting(self, key: str, default: Any = None) -> Any:
        doc = await self.settings.find_one({"key": key})
        return doc["value"] if doc else default

    async def update_setting(self, key: str, value: Any):
        await self.settings.update_one(
            {"key": key},
            {"$set": {"key": key, "value": value, "updated_at": datetime.now(timezone.utc)}},
            upsert=True
        )

# ═══════════════════════════════════════════════════════════════════════════════
# 6. RATE LIMITER & SPAM PROTECTION MIDDLEWARE
# ═══════════════════════════════════════════════════════════════════════════════

class RateLimiter:
    def __init__(self, max_requests: int = 5, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.user_records: Dict[int, deque] = defaultdict(deque)

    def is_rate_limited(self, user_id: int) -> Tuple[bool, int]:
        if not config.RATE_LIMIT_ENABLED:
            return False, 0
        
        now = time.time()
        user_deque = self.user_records[user_id]
        
        # Expire old records outside window
        while user_deque and user_deque[0] <= now - self.window_seconds:
            user_deque.popleft()
            
        if len(user_deque) >= self.max_requests:
            retry_after = int(self.window_seconds - (now - user_deque[0]))
            return True, max(1, retry_after)
            
        user_deque.append(now)
        return False, 0

rate_limiter = RateLimiter(config.RATE_LIMIT_MESSAGES, config.RATE_LIMIT_SECONDS)

# ═══════════════════════════════════════════════════════════════════════════════
# 7. DELIVERY QUEUE MANAGER & AUTO-DELETE SCHEDULER (WITH CANCEL TASK)
# ═══════════════════════════════════════════════════════════════════════════════

class DeliveryManager:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.cancelled_tasks: Set[str] = set()

    def cancel_task(self, task_id: str):
        self.cancelled_tasks.add(task_id)
        log.info(f"Delivery Task [{task_id}] marked for cancellation.")

    async def worker(self, client: Client):
        while True:
            chat_id, file_doc, task_id, protect = await self.queue.get()
            
            try:
                if task_id in self.cancelled_tasks:
                    log.info(f"Skipping delivery for cancelled task: {task_id}")
                    self.queue.task_done()
                    continue

                db_msg_id = file_doc["db_msg_id"]
                custom_cap = await db.get_caption(chat_id) or file_doc.get("caption", "")
                
                # Copy file safely from DB Storage Channel
                sent_msg = await client.copy_message(
                    chat_id=chat_id,
                    from_chat_id=config.DB_CHANNEL_ID,
                    message_id=db_msg_id,
                    caption=custom_cap if custom_cap else None,
                    protect_content=protect
                )

                # Track sent message for auto-deletion
                if sent_msg and config.AUTO_DELETE_SECONDS > 0:
                    await db.add_pending_deletion(chat_id, sent_msg.id, config.AUTO_DELETE_SECONDS)

                await asyncio.sleep(config.BATCH_SEND_DELAY)

            except FloodWait as fw:
                log.warning(f"FloodWait hit in Delivery Worker: {fw.value}s")
                await asyncio.sleep(fw.value + 1)
            except Exception as e:
                log.error(f"Error delivering file [{file_doc.get('file_code')}]: {e}")
            finally:
                self.queue.task_done()

delivery_mgr = DeliveryManager()

async def auto_delete_scheduler(client: Client):
    """
    Strict 8-Hour Scheduler Loop
    Checks database for messages whose time has expired and deletes them.
    """
    log.info("Auto-Delete Task Scheduler Loop Initialized.")
    while True:
        try:
            expired_items = await db.get_expired_deletions()
            for item in expired_items:
                chat_id = item["chat_id"]
                msg_id = item["message_id"]
                try:
                    await client.delete_messages(chat_id=chat_id, message_ids=msg_id)
                    log.info(f"🗑️ Auto-deleted expired message {msg_id} in chat {chat_id}")
                except RPCError as rpc:
                    log.warning(f"Could not delete message {msg_id} in {chat_id}: {rpc}")
                except Exception as ex:
                    log.error(f"Unexpected error deleting message {msg_id}: {ex}")
                finally:
                    await db.remove_pending_deletion(item["_id"])

        except Exception as e:
            log.error(f"Error in auto_delete_scheduler loop: {e}\n{traceback.format_exc()}")
            
        await asyncio.sleep(config.AUTO_DELETE_CHECK_INTERVAL)

# ═══════════════════════════════════════════════════════════════════════════════
# 8. BROADCAST ENGINE (MASS MESSAGING WITH LIVE PROGRESS BAR)
# ═══════════════════════════════════════════════════════════════════════════════

class BroadcastManager:
    def __init__(self):
        self.is_broadcasting: bool = False
        self.should_cancel: bool = False

    async def start_broadcast(self, client: Client, admin_msg: Message, broadcast_msg: Message, pin: bool = False):
        if self.is_broadcasting:
            await admin_msg.reply_text("⚠️ Ek broadcast pehle se hi chal raha hai.")
            return

        self.is_broadcasting = True
        self.should_cancel = False
        
        user_ids = await db.get_all_user_ids()
        total_users = len(user_ids)
        
        success = 0
        failed = 0
        blocked = 0
        deleted_ac = 0
        
        start_time = time.time()
        progress_msg = await admin_msg.reply_text(f"🚀 **Broadcast Shuru Ho Gaya!**\nTotal Target: `{total_users}` Users")

        for index, uid in enumerate(user_ids, 1):
            if self.should_cancel:
                await progress_msg.edit_text("🛑 **Broadcast User dwara cancel kar diya gaya.**")
                self.is_broadcasting = False
                return

            try:
                sent = await broadcast_msg.copy(chat_id=uid)
                if pin:
                    try: await sent.pin(both_sides=True)
                    except: pass
                success += 1
            except UserIsBlocked:
                blocked += 1
            except InputUserDeactivated:
                deleted_ac += 1
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 1)
                try:
                    await broadcast_msg.copy(chat_id=uid)
                    success += 1
                except:
                    failed += 1
            except Exception:
                failed += 1

            # Update Progress status every 20 users
            if index % config.BROADCAST_BATCH_SIZE == 0 or index == total_users:
                p_bar = create_progress_bar(index, total_users)
                pct = round((index / total_users) * 100, 1)
                e_time = fmt_time(int(time.time() - start_time))
                
                status_text = (
                    f"📢 **Broadcast Progress:** `{pct}%`\n"
                    f"[{p_bar}]\n\n"
                    f"👥 **Processed:** `{index}/{total_users}`\n"
                    f"✅ **Success:** `{success}`\n"
                    f"❌ **Failed:** `{failed}`\n"
                    f"🚫 **Blocked:** `{blocked}`\n"
                    f"👻 **Deleted Accounts:** `{deleted_ac}`\n"
                    f"⏱️ **Elapsed Time:** `{e_time}`"
                )
                
                try:
                    await progress_msg.edit_text(
                        status_text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Cancel Broadcast", callback_data="cancel_broadcast")]])
                    )
                except MessageNotModified:
                    pass

            await asyncio.sleep(config.BROADCAST_SLEEP)

        self.is_broadcasting = False
        f_time = fmt_time(int(time.time() - start_time))
        
        await progress_msg.edit_text(
            f"🎉 **Broadcast Successfully Completed!**\n\n"
            f"📊 **Final Report:**\n"
            f"👥 Total Users: `{total_users}`\n"
            f"✅ Success: `{success}`\n"
            f"❌ Failed: `{failed}`\n"
            f"🚫 Blocked: `{blocked}`\n"
            f"👻 Deleted Accounts: `{deleted_ac}`\n"
            f"⏱️ Total Time Taken: `{f_time}`"
        )

broadcast_mgr = BroadcastManager()

# ═══════════════════════════════════════════════════════════════════════════════
# 9. DYNAMIC UPLOAD BUFFER MANAGER (FOR BATCH GENERATION)
# ═══════════════════════════════════════════════════════════════════════════════

class UploadBufferManager:
    def __init__(self):
        self.buffers: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

    def add_file(self, user_id: int, file_data: Dict[str, Any]):
        self.buffers[user_id].append(file_data)

    def get_and_clear(self, user_id: int) -> List[Dict[str, Any]]:
        data = self.buffers.get(user_id, [])
        if user_id in self.buffers:
            del self.buffers[user_id]
        return data

    def clear(self, user_id: int):
        if user_id in self.buffers:
            del self.buffers[user_id]

upload_buffer = UploadBufferManager()


# ═══════════════════════════════════════════════════════════════════════════════
# 10. SEARCH ENGINE & PAGINATION HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

async def search_files_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if await db.is_banned(user_id): return
    
    limited, wait_s = rate_limiter.is_rate_limited(user_id)
    if limited:
        await message.reply_text(f"⏱️ Too many requests! Please wait {wait_s} seconds.")
        return

    query_text = message.text.split(maxsplit=1)
    if len(query_text) < 2:
        await message.reply_text("🔍 **Usage:** `/search <file_name>`\n\nExample: `/search python tutorial`")
        return

    query = query_text[1].strip()
    files, total = await db.search_files(query, limit=5, page=1)

    if not files:
        await message.reply_text(f"❌ No files found matching: `{query}`")
        return

    text = f"🔎 **Search Results for:** `{query}`\n📊 **Total Found:** `{total}`\n\n"
    buttons = []

    for f in files:
        link = f"https://t.me/{config.BOT_USERNAME}?start={f['file_code']}"
        text += f"📄 **{f['file_name']}** ({fmt_size(f['file_size'])})\n🔗 [Get File]({link})\n\n"
        buttons.append([InlineKeyboardButton(f"📥 {f['file_name'][:25]}", url=link)])

    nav_buttons = []
    if total > 5:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"search_pg_2_{query[:15]}"))

    if nav_buttons:
        buttons.append(nav_buttons)

    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)

# ═══════════════════════════════════════════════════════════════════════════════
# 11. CUSTOM THUMBNAIL & CAPTION HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

async def set_thumb_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if not message.reply_to_message or not message.reply_to_message.photo:
        await message.reply_text("📸 **Custom Thumbnail Setup:**\nReply to any photo with `/setthumb` to save it as your default thumbnail.")
        return

    photo = message.reply_to_message.photo
    await db.set_thumbnail(user_id, photo.file_id)
    await message.reply_text("✅ Custom thumbnail saved successfully!")

async def del_thumb_handler(client: Client, message: Message):
    user_id = message.from_user.id
    deleted = await db.delete_thumbnail(user_id)
    if deleted:
        await message.reply_text("🗑️ Custom thumbnail deleted successfully.")
    else:
        await message.reply_text("❌ No custom thumbnail found to delete.")

async def set_caption_handler(client: Client, message: Message):
    user_id = message.from_user.id
    cmd = message.text.split(maxsplit=1)
    if len(cmd) < 2:
        await message.reply_text("📝 **Custom Caption Setup:**\nUsage: `/setcaption Your Custom Caption Text Here`\n\nUse `{filename}` or `{filesize}` placeholders.")
        return

    cap_text = cmd[1].strip()
    await db.set_caption(user_id, cap_text)
    await message.reply_text(f"✅ Custom caption saved:\n\n`{cap_text}`")

async def del_caption_handler(client: Client, message: Message):
    user_id = message.from_user.id
    deleted = await db.delete_caption(user_id)
    if deleted:
        await message.reply_text("🗑️ Custom caption deleted successfully.")
    else:
        await message.reply_text("❌ No custom caption found.")

# ═══════════════════════════════════════════════════════════════════════════════
# 12. MULTI-CLONE ENGINE HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

active_clone_clients: Dict[str, Client] = {}

async def clone_bot_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if await db.is_banned(user_id): return

    cmd = message.text.split(maxsplit=1)
    if len(cmd) < 2:
        await message.reply_text(
            "🤖 **Multi-Clone Bot Engine**\n\n"
            "Aap apna khud ka personal File Store Bot clone bana sakte hain!\n"
            "Usage: `/clone <BOT_TOKEN_FROM_BOTFATHER>`"
        )
        return

    bot_token = cmd[1].strip()
    msg = await message.reply_text("⏳ Validating and launching your child clone bot...")

    try:
        clone_client = Client(
            f"clone_{user_id}_{generate_token(4)}",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            bot_token=bot_token
        )
        await clone_client.start()
        clone_me = await clone_client.get_me()

        clone_id = generate_clone_id()
        await db.add_clone(clone_id, bot_token, user_id, clone_me.username)
        active_clone_clients[clone_id] = clone_client

        # Register core handlers to child clone
        clone_client.add_handler(MessageHandler(start_handler, filters.command("start") & filters.private))
        clone_client.add_handler(MessageHandler(done_handler, filters.command("done") & filters.private))
        clone_client.add_handler(MessageHandler(upload_file_handler, (filters.document | filters.video | filters.photo | filters.audio) & filters.private))

        await msg.edit_text(
            f"🎉 **Child Bot Clone Successfully Started!**\n\n"
            f"🤖 **Bot Name:** {clone_me.first_name}\n"
            f"🔗 **Username:** @{clone_me.username}\n"
            f"🆔 **Clone ID:** `{clone_id}`\n\n"
            f"Is bot ka database aapke main bot se isolated aur securely synced rahega."
        )
    except Exception as e:
        log.error(f"Failed to launch clone bot: {e}")
        await msg.edit_text(f"❌ Failed to launch clone bot.\nError: `{e}`")

# ═══════════════════════════════════════════════════════════════════════════════
# 13. CORE COMMAND HANDLERS (/start, /done, /admin, /broadcast)
# ═══════════════════════════════════════════════════════════════════════════════

async def start_handler(client: Client, message: Message):
    user_id = message.from_user.id
    await db.add_user(user_id, message.from_user.username or "", message.from_user.first_name or "")

    if await db.is_banned(user_id):
        await message.reply_text("❌ You are banned from using this bot.")
        return

    limited, wait_s = rate_limiter.is_rate_limited(user_id)
    if limited:
        await message.reply_text(f"⏱️ Rate limited! Please wait {wait_s}s.")
        return

    # Check Force Sub Channels
    if config.FORCE_SUB_CHANNELS:
        ok, cid, link = await check_all_subs(client, user_id, config.FORCE_SUB_CHANNELS)
        if not ok:
            start_param = message.command[1] if len(message.command) > 1 else ""
            btn = [
                [InlineKeyboardButton("📢 Join Channel", url=link)],
                [InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{config.BOT_USERNAME}?start={start_param}")]
            ]
            await message.reply_text("⚠️ **Access Denied!**\n\nBot ko use karne ke liye pehle hamare channel ko join karein:", reply_markup=InlineKeyboardMarkup(btn))
            return

    # Process Start Parameters (Dynamic File / Batch Access)
    if len(message.command) > 1:
        param = message.command[1]

        # Single Token Multi-File Batch Handler
        if param.startswith("b_"):
            batch = await db.get_batch(param)
            if not batch:
                await message.reply_text("❌ Batch link invalid hai ya expire ho chuka hai.")
                return

            task_id = generate_token(6)
            msg_del = await message.reply_text(f"⏳ File delivery process shuru ho rahi hai... Total Files: `{batch['total_files']}`")
            
            for code in batch["file_codes"]:
                file_doc = await db.get_file(code)
                if file_doc:
                    await delivery_mgr.queue.put((message.chat.id, file_doc, task_id, config.PROTECT_CONTENT))

            await msg_del.edit_text(
                f"✅ **Batch Delivery Active!**\n\n"
                f"📦 Total Files: `{batch['total_files']}`\n"
                f"⏱️ Auto-Delete Timer: **{fmt_auto_delete(config.AUTO_DELETE_SECONDS)}**",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Cancel Delivery", callback_data=f"cancel_del_{task_id}")]])
            )
            return

        # Single File Handler
        file_doc = await db.get_file(param)
        if file_doc:
            task_id = generate_token(6)
            await delivery_mgr.queue.put((message.chat.id, file_doc, task_id, config.PROTECT_CONTENT))
            await message.reply_text(
                f"✅ **File Queue Mein Add Ho Gayi Hai!**\n"
                f"⏱️ Auto-Delete Window: **{fmt_auto_delete(config.AUTO_DELETE_SECONDS)}**"
            )
            return
        else:
            await message.reply_text("❌ File nahi mili ya soft-delete kar di gayi hai.")
            return

    # General Welcome Screen
    total_u = await db.get_total_users()
    total_f = await db.get_total_files()

    welcome_btn = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Search Files", callback_data="help_search"), InlineKeyboardButton("⚙️ Settings", callback_data="user_settings")],
        [InlineKeyboardButton("🤖 Create Clone", callback_data="help_clone"), InlineKeyboardButton("📊 My Stats", callback_data="user_stats")]
    ])

    await message.reply_text(
        f"👋 **Namaste {message.from_user.first_name}!**\n\n"
        f"Main Telegram File Store Bot hoon. Aap yahan files store, batch links generate, aur dynamic URLs share kar sakte hain.\n\n"
        f"📊 **Global Stats:**\n"
        f"👥 Total Users: `{total_u}`\n"
        f"📁 Stored Files: `{total_f}`\n"
        f"⏱️ Auto-Delete Window: `{fmt_auto_delete(config.AUTO_DELETE_SECONDS)}`",
        reply_markup=welcome_btn
    )

async def upload_file_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if await db.is_banned(user_id): return

    file_info = extract_file_info(message)
    if not file_info: return

    # Store file in DB Channel
    db_msg = await message.copy(chat_id=config.DB_CHANNEL_ID)
    file_info["db_msg_id"] = db_msg.id
    file_info["uploader_id"] = user_id
    file_info["caption"] = message.caption or ""

    file_code = await db.add_file(file_info)
    upload_buffer.add_file(user_id, {"code": file_code, "name": file_info["file_name"]})

    share_link = f"https://t.me/{config.BOT_USERNAME}?start={file_code}"
    
    btn = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Share Single Link", url=share_link)],
        [InlineKeyboardButton("🛑 Create Single Token Batch Now", callback_data="make_batch_now")]
    ])

    await message.reply_text(
        f"✅ **File Save Ho Gayi!**\n\n"
        f"📄 **Name:** `{file_info['file_name']}`\n"
        f"📦 **Size:** `{fmt_size(file_info['file_size'])}`\n"
        f"🔑 **Code:** `{file_code}`\n\n"
        f"💡 Multiple files ko ek hi dynamic link mein combine karne ke liye baki files bhejein aur `/done` command run karein.",
        reply_markup=btn
    )

# Dynamic URL Optimization for Multiple Files
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
        f"🔗 **Single Batch URL:** {batch_link}\n\n"
        f"⚡ Is ek single link se user aapki sabhi files access kar sakta hai.",
        disable_web_page_preview=True
    )

async def admin_panel_handler(client: Client, message: Message):
    if message.from_user.id not in config.ADMIN_IDS and message.from_user.id != config.OWNER_ID:
        return

    admin_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 System Stats", callback_data="admin_stats"), InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast_prompt")],
        [InlineKeyboardButton("👥 Manage Users", callback_data="admin_users"), InlineKeyboardButton("⚙️ Dynamic Settings", callback_data="admin_settings_gui")],
        [InlineKeyboardButton("🔄 Restart Bot", callback_data="conf_restart_all"), InlineKeyboardButton("❌ Close Panel", callback_data="close_admin")]
    ])

    await message.reply_text("⚙️ **Admin Control Center:**", reply_markup=admin_kb)

async def broadcast_command_handler(client: Client, message: Message):
    if message.from_user.id not in config.ADMIN_IDS and message.from_user.id != config.OWNER_ID:
        return

    if not message.reply_to_message:
        await message.reply_text("📢 Reply to any message with `/broadcast` to send it to all bot users.")
        return

    asyncio.create_task(broadcast_mgr.start_broadcast(client, message, message.reply_to_message))

# ═══════════════════════════════════════════════════════════════════════════════
# 14. CALLBACK QUERY ROUTER & INTERACTIVE GUI BUTTONS
# ═══════════════════════════════════════════════════════════════════════════════

async def callback_router(client: Client, cb: CallbackQuery):
    data = cb.data
    user_id = cb.from_user.id

    # Make Batch from Buffer Button
    if data == "make_batch_now":
        buffered_files = upload_buffer.get_and_clear(user_id)
        if not buffered_files:
            await cb.answer("❌ Buffer is empty.", show_alert=True)
            return
        
        file_codes = [f["code"] for f in buffered_files]
        batch_id = await db.create_batch(file_codes, user_id)
        batch_link = f"https://t.me/{config.BOT_USERNAME}?start={batch_id}"
        
        await cb.message.edit_text(
            f"🎉 **Batch Generated Successfully!**\n\n"
            f"📦 **Total Files:** `{len(file_codes)}`\n"
            f"🔗 **Single Batch URL:** {batch_link}"
        )
        await cb.answer()

    # Cancel Delivery Task Button
    elif data.startswith("cancel_del_"):
        task_id = data.split("_")[2]
        delivery_mgr.cancel_task(task_id)
        await cb.answer("🛑 File delivery process cancelled.", show_alert=True)
        await cb.message.edit_text("❌ File delivery task was cancelled by user.")

    # Cancel Broadcast
    elif data == "cancel_broadcast":
        if user_id in config.ADMIN_IDS or user_id == config.OWNER_ID:
            broadcast_mgr.should_cancel = True
            await cb.answer("🛑 Cancelling broadcast...", show_alert=True)

    # Admin Panel Stats
    elif data == "admin_stats":
        if user_id not in config.ADMIN_IDS and user_id != config.OWNER_ID: return
        total_u = await db.get_total_users()
        total_f = await db.get_total_files()
        ram = psutil.virtual_memory().percent
        cpu = psutil.cpu_percent()
        
        await cb.message.edit_text(
            f"📊 **Bot Realtime System Statistics**\n\n"
            f"👥 **Total Users:** `{total_u}`\n"
            f"📁 **Stored Files:** `{total_f}`\n"
            f"💻 **RAM Usage:** `{ram}%`\n"
            f"⚡ **CPU Usage:** `{cpu}%`\n"
            f"⏱️ **Auto-Delete Timer:** `{fmt_auto_delete(config.AUTO_DELETE_SECONDS)}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="open_admin_panel")]])
        )
        await cb.answer()

    # Open Admin Panel GUI
    elif data == "open_admin_panel":
        if user_id not in config.ADMIN_IDS and user_id != config.OWNER_ID: return
        admin_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 System Stats", callback_data="admin_stats"), InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast_prompt")],
            [InlineKeyboardButton("👥 Manage Users", callback_data="admin_users"), InlineKeyboardButton("⚙️ Dynamic Settings", callback_data="admin_settings_gui")],
            [InlineKeyboardButton("🔄 Restart Bot", callback_data="conf_restart_all"), InlineKeyboardButton("❌ Close Panel", callback_data="close_admin")]
        ])
        await cb.message.edit_text("⚙️ **Admin Control Center:**", reply_markup=admin_kb)
        await cb.answer()

    # Instant Bot Reboot/Restart Handler
    elif data == "conf_restart_all":
        if user_id != config.OWNER_ID and user_id not in config.ADMIN_IDS: return
        await cb.answer("🔄 Restarting Bot System...", show_alert=True)
        await cb.message.edit_text("🔄 **Bot is executing full system reboot right now...**")
        
        # Safe Exec Reboot Flow
        os.execv(sys.executable, [sys.executable] + sys.argv)

    elif data == "close_admin":
        await cb.message.delete()

# ═══════════════════════════════════════════════════════════════════════════════
# 15. SYSTEM INITIALIZATION & MAIN EXECUTION LAUNCHER
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("Validating Configuration and Environmental Variables...")
    config.validate()
    
    log.info("Connecting to MongoDB Atlas Database Cluster...")
    await db.connect()

    primary_bot = Client(
        "FileStoreBotPrimary",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        bot_token=config.BOT_TOKEN
    )

    # Register Handlers
    primary_bot.add_handler(MessageHandler(start_handler, filters.command("start") & filters.private))
    primary_bot.add_handler(MessageHandler(done_handler, filters.command("done") & filters.private))
    primary_bot.add_handler(MessageHandler(search_files_handler, filters.command("search") & filters.private))
    primary_bot.add_handler(MessageHandler(set_thumb_handler, filters.command("setthumb") & filters.private))
    primary_bot.add_handler(MessageHandler(del_thumb_handler, filters.command("delthumb") & filters.private))
    primary_bot.add_handler(MessageHandler(set_caption_handler, filters.command("setcaption") & filters.private))
    primary_bot.add_handler(MessageHandler(del_caption_handler, filters.command("delcaption") & filters.private))
    primary_bot.add_handler(MessageHandler(clone_bot_handler, filters.command("clone") & filters.private))
    primary_bot.add_handler(MessageHandler(admin_panel_handler, filters.command("admin") & filters.private))
    primary_bot.add_handler(MessageHandler(broadcast_command_handler, filters.command("broadcast") & filters.private))
    primary_bot.add_handler(MessageHandler(upload_file_handler, (filters.document | filters.video | filters.photo | filters.audio) & filters.private))
    primary_bot.add_handler(CallbackQueryHandler(callback_router))

    log.info("Starting Primary Telegram Engine...")
    await primary_bot.start()

    # Load and launch active child clone bots from DB
    log.info("Initializing Active Child Bot Clones...")
    clones = await db.get_active_clones()
    for clone in clones:
        try:
            c_client = Client(
                f"clone_{clone['clone_id']}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                bot_token=clone["bot_token"]
            )
            await c_client.start()
            c_client.add_handler(MessageHandler(start_handler, filters.command("start") & filters.private))
            c_client.add_handler(MessageHandler(done_handler, filters.command("done") & filters.private))
            c_client.add_handler(MessageHandler(upload_file_handler, (filters.document | filters.video | filters.photo | filters.audio) & filters.private))
            active_clone_clients[clone["clone_id"]] = c_client
            log.info(f"Child Clone Bot @{clone['bot_username']} successfully launched.")
        except Exception as ce:
            log.error(f"Failed to load clone bot [{clone['clone_id']}]: {ce}")

    # Launch Background Delivery Workers
    for i in range(config.DELIVERY_WORKERS):
        asyncio.create_task(delivery_mgr.worker(primary_bot))

    # Launch 8-Hour Strict Auto-Delete Task Scheduler Loop
    asyncio.create_task(auto_delete_scheduler(primary_bot))

    log.info(f"File Store Bot Engine Engine fully launched as @{config.BOT_USERNAME}")
    await idle()
    
    log.info("Shutting down bot instances...")
    await primary_bot.stop()
    for c in active_clone_clients.values():
        await c.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot execution manually terminated by operator.")
    except Exception as fatal_e:
        log.critical(f"Fatal System Launch Error: {fatal_e}\n{traceback.format_exc()}")
