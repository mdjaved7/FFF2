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
# 3. CONFIGURATION
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
    AUTO_DELETE_SECONDS: int = int(os.getenv("AUTO_DELETE_SECONDS", "0"))
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
    AUTO_DELETE_CHECK_INTERVAL: int = int(os.getenv("AUTO_DELETE_CHECK_INTERVAL", "60"))

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
# 5. DATABASE LAYER — Multi-Connection Support (Main + Per-Clone MongoDB URIs)
# ═══════════════════════════════════════════════════════════════════════════════

class Database:
    """
    Supports multiple independent MongoDB connections.
    - "main" uses config.MONGODB_URI
    - Each clone can have its own URI stored in the clone document
    """
    _connections: Dict[str, Tuple[AsyncIOMotorClient, AsyncIOMotorDatabase]] = {}
    _init_lock = asyncio.Lock()

    async def connect(self, name: str = "main", uri: Optional[str] = None, db_name: Optional[str] = None) -> AsyncIOMotorDatabase:
        """Connect (or return existing) database for a given name key."""
        if name in self._connections:
            return self._connections[name][1]

        async with self._init_lock:
            # Double-check after acquiring lock
            if name in self._connections:
                return self._connections[name][1]

            effective_uri = uri or config.MONGODB_URI
            effective_db = db_name or config.DATABASE_NAME

            for attempt in range(1, 6):
                try:
                    log.info(f"[DB:{name}] Connecting (attempt {attempt}/5)...")
                    client = AsyncIOMotorClient(
                        effective_uri,
                        serverSelectionTimeoutMS=5000,
                        maxPoolSize=100,
                        minPoolSize=10,
                        retryWrites=True,
                        retryReads=True,
                        w="majority",
                    )
                    await client.admin.command("ping")
                    db = client[effective_db]
                    self._connections[name] = (client, db)
                    log.info(f"✅ [DB:{name}] Connected to {effective_db}")
                    return db
                except Exception as e:
                    log.error(f"[DB:{name}] Attempt {attempt} failed: {e}")
                    if attempt < 5:
                        await asyncio.sleep(2 * attempt)
                    else:
                        raise RuntimeError(f"[DB:{name}] Connection failed after 5 attempts")

    def get_db(self, name: str = "main") -> AsyncIOMotorDatabase:
        """Get an already-connected database. Raises if not connected."""
        if name not in self._connections:
            raise RuntimeError(f"Database '{name}' not connected. Call connect() first.")
        return self._connections[name][1]

    async def col(self, name: str, db_name: str = "main") -> AsyncIOMotorCollection:
        """Get a MongoDB collection from the specified database connection."""
        db = await self.connect(db_name)
        return db[name]

    async def close(self, name: Optional[str] = None):
        """Close specific connection or all connections."""
        if name:
            client, _ = self._connections.pop(name, (None, None))
            if client:
                client.close()
                log.info(f"[DB:{name}] Closed")
        else:
            for n in list(self._connections.keys()):
                client, _ = self._connections.pop(n, (None, None))
                if client:
                    try: client.close()
                    except: pass
            log.info("All DB connections closed")

    async def is_connected(self, name: str = "main") -> bool:
        try:
            if name in self._connections:
                await self._connections[name][0].admin.command("ping")
                return True
        except: pass
        return False

database = Database()

# ═══════════════════════════════════════════════════════════════════════════════
# 6. SINGLE REPOSITORY — Now accepts db_name for per-clone DB isolation
# ═══════════════════════════════════════════════════════════════════════════════

class Repo:
    """Unified MongoDB repository. Accepts `db_name` parameter to support clone-isolated databases."""

    # ── Indexes (created on the main DB only; clone DBs get their own at clone start) ──
    @staticmethod
    async def ensure_indexes(db_name: str = "main"):
        db = await database.connect(db_name)
        collections = {
            "users": [("user_id", 1), ("clone_id", 1)],
            "files": [("file_id", 1), ("clone_id", 1), ("access_token", 1)],
            "batches": [("batch_id", 1), ("clone_id", 1), ("access_token", 1)],
            "clones": [("clone_id", 1), ("bot_token", 1)],
            "settings": [("clone_id", 1)],
            "moderators": [("clone_id", 1), ("user_id", 1)],
            "tokens": [("token", 1)],
            "channels": [("clone_id", 1), ("channel_id", 1)],
            "stats": [("clone_id", 1), ("date", 1)],
            "logs": [("created_at", 1)],
        }
        for coll, keys in collections.items():
            try:
                await db[coll].create_index(keys, background=True)
            except Exception as e:
                log.warning(f"Index error for {coll} on {db_name}: {e}")
        log.info(f"✅ Indexes ensured for DB: {db_name}")

    # ── Users ──
    @staticmethod
    async def add_user(uid: int, clone: str = "main", username: str = "",
                       first: str = "", last: str = "", db_name: str = "main") -> bool:
        col = await database.col("users", db_name)
        now = datetime.now(timezone.utc).isoformat()
        try:
            await col.update_one(
                {"clone_id": clone, "user_id": uid},
                {"$setOnInsert": {"clone_id": clone, "user_id": uid, "username": username,
                                  "first_name": first, "last_name": last,
                                  "is_banned": False, "is_admin": uid == config.OWNER_ID or uid in config.ADMIN_IDS,
                                  "is_moderator": False, "joined_at": now,
                                  "total_files": 0, "total_batches": 0},
                 "$set": {"last_active": now, "username": username, "first_name": first, "last_name": last}},
                upsert=True)
            return True
        except: return False

    @staticmethod
    async def get_user(uid: int, clone: str = "main", db_name: str = "main") -> Optional[dict]:
        col = await database.col("users", db_name)
        return await col.find_one({"clone_id": clone, "user_id": uid})

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
    async def is_banned(uid: int, clone: str = "main", db_name: str = "main") -> bool:
        u = await Repo.get_user(uid, clone, db_name)
        return u.get("is_banned", False) if u else False

    @staticmethod
    async def count_users(clone: Optional[str] = None, db_name: str = "main") -> int:
        col = await database.col("users", db_name)
        f = {"clone_id": clone} if clone else {}
        return await col.count_documents(f)

    @staticmethod
    async def get_all_users(clone: Optional[str] = None, limit: int = 10000, db_name: str = "main") -> List[dict]:
        col = await database.col("users", db_name)
        f = {"clone_id": clone} if clone else {}
        return await col.find(f).limit(limit).to_list(length=limit)

    @staticmethod
    async def get_all_user_ids(clone: Optional[str] = None, db_name: str = "main") -> List[int]:
        col = await database.col("users", db_name)
        f = {"clone_id": clone} if clone else {}
        users = await col.find(f, {"user_id": 1}).to_list(length=100000)
        return [u["user_id"] for u in users]

    @staticmethod
    async def inc_user_files(uid: int, clone: str = "main", amount: int = 1, db_name: str = "main") -> bool:
        col = await database.col("users", db_name)
        r = await col.update_one({"clone_id": clone, "user_id": uid}, {"$inc": {"total_files": amount}})
        return r.modified_count > 0

    # ── Files ──
    @staticmethod
    async def store_file(data: dict, db_name: str = "main") -> str:
        col = await database.col("files", db_name)
        r = await col.insert_one(data)
        return str(r.inserted_id)

    @staticmethod
    async def get_file_by_token(token: str, clone: str = "main", db_name: str = "main") -> Optional[dict]:
        col = await database.col("files", db_name)
        return await col.find_one({"clone_id": clone, "access_token": token, "deleted": False})

    @staticmethod
    async def get_file_by_id(fid: str, clone: str = "main", db_name: str = "main") -> Optional[dict]:
        col = await database.col("files", db_name)
        return await col.find_one({"clone_id": clone, "file_id": fid, "deleted": False})

    @staticmethod
    async def count_files(clone: Optional[str] = None, db_name: str = "main") -> int:
        col = await database.col("files", db_name)
        f = {"deleted": False}
        if clone: f["clone_id"] = clone
        return await col.count_documents(f)

    @staticmethod
    async def total_storage(clone: Optional[str] = None, db_name: str = "main") -> int:
        col = await database.col("files", db_name)
        f = {"deleted": False}
        if clone: f["clone_id"] = clone
        pipe = [{"$match": f}, {"$group": {"_id": None, "total": {"$sum": "$file_size"}}}]
        r = await col.aggregate(pipe).to_list(length=1)
        return r[0]["total"] if r else 0

    @staticmethod
    async def get_expired_files(db_name: str = "main") -> List[dict]:
        col = await database.col("files", db_name)
        now = datetime.now(timezone.utc).isoformat()
        return await col.find({"auto_delete_at": {"$ne": None, "$lte": now}, "deleted": False}).to_list(length=500)

    @staticmethod
    async def soft_delete_file(fid: str, clone: str = "main", db_name: str = "main") -> bool:
        col = await database.col("files", db_name)
        r = await col.update_one({"clone_id": clone, "file_id": fid},
                                 {"$set": {"deleted": True, "deleted_at": datetime.now(timezone.utc).isoformat()}})
        return r.modified_count > 0

    @staticmethod
    async def inc_file_dl(fid: str, clone: str = "main", db_name: str = "main") -> bool:
        col = await database.col("files", db_name)
        r = await col.update_one({"clone_id": clone, "file_id": fid}, {"$inc": {"downloads": 1}})
        return r.modified_count > 0

    # ── Batches ──
    @staticmethod
    async def create_batch(data: dict, db_name: str = "main") -> str:
        col = await database.col("batches", db_name)
        r = await col.insert_one(data)
        return str(r.inserted_id)

    @staticmethod
    async def get_batch_by_token(token: str, clone: str = "main", db_name: str = "main") -> Optional[dict]:
        col = await database.col("batches", db_name)
        return await col.find_one({"clone_id": clone, "access_token": token, "deleted": False})

    @staticmethod
    async def get_batch_by_id(bid: str, clone: str = "main", db_name: str = "main") -> Optional[dict]:
        col = await database.col("batches", db_name)
        return await col.find_one({"clone_id": clone, "batch_id": bid, "deleted": False})

    @staticmethod
    async def update_batch_status(bid: str, clone: str, status: str, db_name: str = "main") -> bool:
        col = await database.col("batches", db_name)
        r = await col.update_one({"clone_id": clone, "batch_id": bid},
                                 {"$set": {"status": status, "updated_at": datetime.now(timezone.utc).isoformat()}})
        return r.modified_count > 0

    @staticmethod
    async def update_batch_progress(bid: str, clone: str, sent: int, failed: int, db_name: str = "main") -> bool:
        col = await database.col("batches", db_name)
        r = await col.update_one({"clone_id": clone, "batch_id": bid},
                                 {"$set": {"progress.sent": sent, "progress.failed": failed,
                                           "updated_at": datetime.now(timezone.utc).isoformat()}})
        return r.modified_count > 0

    @staticmethod
    async def inc_batch_dl(bid: str, clone: str = "main", db_name: str = "main") -> bool:
        col = await database.col("batches", db_name)
        r = await col.update_one({"clone_id": clone, "batch_id": bid}, {"$inc": {"downloads": 1}})
        return r.modified_count > 0

    @staticmethod
    async def soft_delete_batch(bid: str, clone: str = "main", db_name: str = "main") -> bool:
        col = await database.col("batches", db_name)
        r = await col.update_one({"clone_id": clone, "batch_id": bid},
                                 {"$set": {"deleted": True, "status": "deleted",
                                           "deleted_at": datetime.now(timezone.utc).isoformat()}})
        return r.modified_count > 0

    @staticmethod
    async def count_batches(clone: Optional[str] = None, status: Optional[str] = None, db_name: str = "main") -> int:
        col = await database.col("batches", db_name)
        f = {"deleted": False}
        if clone: f["clone_id"] = clone
        if status: f["status"] = status
        return await col.count_documents(f)

    @staticmethod
    async def get_batches(clone: Optional[str] = None, limit: int = 100, skip: int = 0, db_name: str = "main") -> List[dict]:
        col = await database.col("batches", db_name)
        f = {"deleted": False}
        if clone: f["clone_id"] = clone
        return await col.find(f).sort("created_at", -1).skip(skip).limit(limit).to_list(length=limit)

    @staticmethod
    async def get_expired_batches(db_name: str = "main") -> List[dict]:
        col = await database.col("batches", db_name)
        now = datetime.now(timezone.utc).isoformat()
        return await col.find({"auto_delete_at": {"$ne": None, "$lte": now}, "deleted": False}).to_list(length=500)

    @staticmethod
    async def batch_total_storage(clone: Optional[str] = None, db_name: str = "main") -> int:
        col = await database.col("batches", db_name)
        f = {"deleted": False}
        if clone: f["clone_id"] = clone
        pipe = [{"$match": f}, {"$group": {"_id": None, "total": {"$sum": "$total_size"}}}]
        r = await col.aggregate(pipe).to_list(length=1)
        return r[0]["total"] if r else 0

    # ── Clones (always stored in MAIN database) ──
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
    async def get_clone_by_token(token: str) -> Optional[dict]:
        col = await database.col("clones", "main")
        return await col.find_one({"bot_token": token})

    @staticmethod
    async def get_all_clones(status: Optional[str] = None) -> List[dict]:
        col = await database.col("clones", "main")
        f = {}
        if status: f["status"] = status
        return await col.find(f).to_list(length=1000)

    @staticmethod
    async def update_clone_status(clone_id: str, status: str) -> bool:
        col = await database.col("clones", "main")
        r = await col.update_one({"clone_id": clone_id}, {"$set": {"status": status}})
        return r.modified_count > 0

    @staticmethod
    async def delete_clone(clone_id: str) -> bool:
        col = await database.col("clones", "main")
        r = await col.delete_one({"clone_id": clone_id})
        return r.deleted_count > 0

    @staticmethod
    async def count_clones(status: Optional[str] = None) -> int:
        col = await database.col("clones", "main")
        f = {}
        if status: f["status"] = status
        return await col.count_documents(f)

    # ── Settings (per-clone, on clone's own DB) ──
    DEFAULTS = {
        "start_msg": "👋 नमस्ते {first_name}! मैं एक File Store Bot हूँ। मुझे कोई भी फाइल भेजें और मैं आपको एक शेयरेबल लिंक दूंगा।",
        "force_sub_msg": "⚠️ इस Bot का उपयोग करने के लिए कृपया नीचे दिए गए Channel को Join करें!",
        "auto_delete_msg": "⏱ {time} में यह फाइल अपने आप डिलीट हो जाएगी।",
        "auto_del_secs": 0,
        "no_forward": False,
        "protect": True,
        "token_required": False,
        "mode": "public",
        "custom_caption": "",
        "max_size": 2*1024**3,
    }

    @staticmethod
    async def get_settings(clone: str = "main", db_name: str = "main") -> dict:
        col = await database.col("settings", db_name)
        s = await col.find_one({"clone_id": clone})
        merged = dict(Repo.DEFAULTS)
        if s and s.get("data"): merged.update(s["data"])
        return merged

    @staticmethod
    async def set_settings(clone: str, updates: dict, db_name: str = "main") -> bool:
        col = await database.col("settings", db_name)
        existing = await col.find_one({"clone_id": clone})
        current = dict(Repo.DEFAULTS)
        if existing and existing.get("data"): current.update(existing["data"])
        current.update(updates)
        await col.update_one({"clone_id": clone}, {"$set": {"data": current}}, upsert=True)
        return True

    # ── Moderators (per-clone DB) ──
    @staticmethod
    async def add_mod(clone: str, uid: int, by: int = 0, db_name: str = "main") -> bool:
        col = await database.col("moderators", db_name)
        try:
            await col.insert_one({"clone_id": clone, "user_id": uid, "added_by": by,
                                  "added_at": datetime.now(timezone.utc).isoformat()})
            return True
        except: return False

    @staticmethod
    async def remove_mod(clone: str, uid: int, db_name: str = "main") -> bool:
        col = await database.col("moderators", db_name)
        r = await col.delete_one({"clone_id": clone, "user_id": uid})
        return r.deleted_count > 0

    @staticmethod
    async def is_mod(clone: str, uid: int, db_name: str = "main") -> bool:
        col = await database.col("moderators", db_name)
        return await col.find_one({"clone_id": clone, "user_id": uid}) is not None

    @staticmethod
    async def get_mods(clone: str, db_name: str = "main") -> List[dict]:
        col = await database.col("moderators", db_name)
        return await col.find({"clone_id": clone}).to_list(length=100)

    # ── Tokens ──
    @staticmethod
    async def create_token(clone: str, uid: int, token: str, days: int = 30, db_name: str = "main") -> dict:
        col = await database.col("tokens", db_name)
        now = datetime.now(timezone.utc)
        data = {"clone_id": clone, "user_id": uid, "token": token,
                "created_at": now.isoformat(),
                "expires_at": (now + timedelta(days=days)).isoformat() if days > 0 else None,
                "uses": 0, "max_uses": 0, "active": True}
        await col.insert_one(data)
        return data

    @staticmethod
    async def verify_token(token: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Check across all databases? No — tokens are per-clone, checked on the clone's own DB.
           We check on the main DB as well for main bot tokens."""
        col = await database.col("tokens", "main")
        t = await col.find_one({"token": token, "active": True})
        if not t: return False, "❌ Token नहीं मिला", None
        if t.get("expires_at"):
            if datetime.now(timezone.utc) > datetime.fromisoformat(t["expires_at"]):
                await col.update_one({"token": token}, {"$set": {"active": False}})
                return False, "❌ Token expired", None
        if t["max_uses"] > 0 and t["uses"] >= t["max_uses"]:
            return False, "❌ Token max uses reached", None
        await col.update_one({"token": token}, {"$inc": {"uses": 1}})
        return True, None, t.get("clone_id")

    # ── Channels ──
    @staticmethod
    async def add_channel(clone: str, cid: int, title: str = "", ctype: str = "force_sub", db_name: str = "main") -> bool:
        col = await database.col("channels", db_name)
        try:
            await col.insert_one({"clone_id": clone, "channel_id": cid, "title": title, "type": ctype})
            return True
        except: return False

    @staticmethod
    async def remove_channel(clone: str, cid: int, db_name: str = "main") -> bool:
        col = await database.col("channels", db_name)
        r = await col.delete_one({"clone_id": clone, "channel_id": cid})
        return r.deleted_count > 0

    @staticmethod
    async def get_channels(clone: str, ctype: str = "force_sub", db_name: str = "main") -> List[int]:
        col = await database.col("channels", db_name)
        docs = await col.find({"clone_id": clone, "type": ctype}).to_list(length=100)
        return [d["channel_id"] for d in docs]

    # ── Logs ──
    @staticmethod
    async def add_log(clone: str, uid: int, action: str, db_name: str = "main"):
        col = await database.col("logs", db_name)
        try:
            await col.insert_one({"clone_id": clone, "user_id": uid, "action": action,
                                  "created_at": datetime.now(timezone.utc).isoformat()})
        except: pass

    # ── Stats ──
    @staticmethod
    async def record_daily(clone: str, db_name: str = "main"):
        col = await database.col("stats", db_name)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        users = await Repo.count_users(clone, db_name)
        files = await Repo.count_files(clone, db_name)
        batches = await Repo.count_batches(clone, db_name=db_name)
        storage = await Repo.total_storage(clone, db_name) + await Repo.batch_total_storage(clone, db_name)
        await col.update_one(
            {"clone_id": clone, "date": today},
            {"$set": {"users": users, "files": files, "batches": batches, "storage": storage}},
            upsert=True)

    @staticmethod
    async def get_stats(clone: str, days: int = 7, db_name: str = "main") -> List[dict]:
        col = await database.col("stats", db_name)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        return await col.find({"clone_id": clone, "date": {"$gte": cutoff}}).sort("date", -1).to_list(length=365)

    # ── Helper: get db_name for a clone (uses clone's own mongo_uri if stored) ──
    @staticmethod
    async def get_db_name_for_clone(clone_id: str) -> str:
        """Returns the database connection name for a clone. If the clone has a custom mongo_uri,
           ensures that connection is established and returns the name. Otherwise returns 'main'."""
        if clone_id == "main": return "main"
        clone_data = await Repo.get_clone(clone_id)
        if clone_data and clone_data.get("mongo_uri"):
            # Ensure connection exists for this clone
            try:
                await database.connect(clone_id, uri=clone_data["mongo_uri"])
            except Exception as e:
                log.error(f"Failed to connect clone DB for {clone_id}: {e}")
                return "main"  # fallback
            return clone_id
        return "main"

repo = Repo()

# ═══════════════════════════════════════════════════════════════════════════════
# 7. GLOBAL STATE & REFERENCES
# ═══════════════════════════════════════════════════════════════════════════════

_start_time = time.time()
main_client: Optional[Client] = None
_clients: Dict[str, Client] = {}  # clone_id -> Client
_upload_buffers: Dict[str, Dict[str, Tuple[List[dict], float]]] = defaultdict(lambda: defaultdict(lambda: ([], 0.0)))
_delivery_queue: asyncio.Queue = asyncio.Queue()
_delivery_cancel: Dict[str, asyncio.Event] = {}
_delivery_active: Dict[str, asyncio.Task] = {}
_pending_deletions: Dict[str, List[Tuple[int, int, float]]] = defaultdict(list)

# ═══════════════════════════════════════════════════════════════════════════════
# 8. RATE LIMITER
# ═══════════════════════════════════════════════════════════════════════════════

class SlidingWindowLimiter:
    def __init__(self):
        self._windows: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))

    def check(self, uid: int, clone: str = "main") -> Tuple[bool, int]:
        if not config.RATE_LIMIT_ENABLED: return True, 0
        key = f"{clone}:{uid}"
        now = time.time()
        cutoff = now - config.RATE_LIMIT_SECONDS
        w = self._windows[key]
        while w and w[0] < cutoff: w.popleft()
        if len(w) >= config.RATE_LIMIT_MESSAGES:
            return False, int(cutoff + config.RATE_LIMIT_SECONDS - w[0] + 1)
        w.append(now)
        return True, 0

rate_limiter = SlidingWindowLimiter()

# ═══════════════════════════════════════════════════════════════════════════════
# 9. KEYBOARD BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

class KB:
    @staticmethod
    def main(is_owner: bool = False):
        b = [[InlineKeyboardButton("❓ Help", callback_data="help")]]
        if is_owner: b.append([InlineKeyboardButton("⚙️ Admin", callback_data="admin_dashboard")])
        return InlineKeyboardMarkup(b)

    @staticmethod
    def admin():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Stats", callback_data="a_stats"),
             InlineKeyboardButton("👥 Users", callback_data="a_users")],
            [InlineKeyboardButton("📁 Files", callback_data="a_files"),
             InlineKeyboardButton("📦 Batches", callback_data="a_batches")],
            [InlineKeyboardButton("🤖 Clones", callback_data="a_clones"),
             InlineKeyboardButton("📢 Broadcast", callback_data="a_broadcast")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="a_settings"),
             InlineKeyboardButton("🔄 Restart", callback_data="a_restart")],
            [InlineKeyboardButton("💾 Backup", callback_data="a_backup"),
             InlineKeyboardButton("✖️ Close", callback_data="close")],
        ])

    @staticmethod
    def back(cb: str = "admin_dashboard"):
        return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=cb)]])

    @staticmethod
    def confirm(action: str, clone: str = ""):
        cb = f"conf_{action}" + (f":{clone}" if clone else "")
        cc = f"cancel_{action}" + (f":{clone}" if clone else "")
        return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes", callback_data=cb),
                                      InlineKeyboardButton("❌ No", callback_data=cc)]])

    @staticmethod
    def sub_link(link: str):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Join Channel", url=link)],
            [InlineKeyboardButton("🔄 Retry", callback_data="check_sub")],
        ])

    @staticmethod
    def cancel_dl(bid: str, clone: str = "main"):
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_dl:{bid}:{clone}")]])

kb = KB()

# ═══════════════════════════════════════════════════════════════════════════════
# 10. AUTO-DELETE SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════════

class AutoDeleteScheduler:
    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        if self._running: return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        log.info("Auto-delete scheduler started")

    async def stop(self):
        self._running = False
        if self._task: self._task.cancel()
        try: await self._task
        except: pass

    def schedule(self, clone: str, chat_id: int, msg_id: int, secs: int):
        if secs <= 0: return
        _pending_deletions[clone].append((chat_id, msg_id, time.time() + secs))

    async def _loop(self):
        while self._running:
            try:
                now = time.time()
                for clone, items in list(_pending_deletions.items()):
                    client = _clients.get(clone) or main_client
                    if not client: continue
                    remaining = []
                    for chat_id, msg_id, del_at in items:
                        if del_at <= now:
                            try: await client.delete_messages(chat_id, msg_id)
                            except: pass
                        else: remaining.append((chat_id, msg_id, del_at))
                    _pending_deletions[clone] = remaining
            except Exception as e:
                log.error(f"Auto-delete error: {e}")
            await asyncio.sleep(config.AUTO_DELETE_CHECK_INTERVAL)

auto_del_sched = AutoDeleteScheduler()

# ═══════════════════════════════════════════════════════════════════════════════
# 11. BATCH DELIVERY ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class DeliveryEngine:
    def __init__(self):
        self._workers: List[asyncio.Task] = []

    async def start(self, count: int = 5):
        for i in range(count):
            t = asyncio.create_task(self._worker(i+1))
            self._workers.append(t)
        log.info(f"Started {count} delivery workers")

    async def stop(self):
        for t in self._workers: t.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    def enqueue(self, clone: str, uid: int, chat_id: int, batch_id: str,
                msg_id: int, auto_del: int = 0, protect: bool = True):
        _delivery_queue.put_nowait({
            "clone": clone, "uid": uid, "chat_id": chat_id,
            "batch_id": batch_id, "msg_id": msg_id,
            "auto_del": auto_del, "protect": protect,
        })

    def cancel(self, clone: str, uid: int, batch_id: str):
        key = f"{clone}:{uid}:{batch_id}"
        if key in _delivery_cancel:
            _delivery_cancel[key].set()

    async def _worker(self, wid: int):
        while True:
            job = await _delivery_queue.get()
            try: await self._execute(job, wid)
            except asyncio.CancelledError: raise
            except Exception as e: log.error(f"Worker {wid}: {e}")
            finally: _delivery_queue.task_done()

    async def _execute(self, job: dict, wid: int):
        clone = job["clone"]
        uid = job["uid"]
        chat_id = job["chat_id"]
        batch_id = job["batch_id"]
        progress_msg_id = job["msg_id"]
        auto_del = job["auto_del"]
        protect = job["protect"]

        # Determine which DB this clone uses
        db_name = await repo.get_db_name_for_clone(clone)

        cancel_key = f"{clone}:{uid}:{batch_id}"
        _delivery_cancel[cancel_key] = asyncio.Event()
        cancel_event = _delivery_cancel[cancel_key]

        batch = await repo.get_batch_by_id(batch_id, clone, db_name)
        if not batch: return
        await repo.update_batch_status(batch_id, clone, "delivering", db_name)

        files = batch.get("files", [])
        total = len(files)
        sent = 0
        failed = 0
        cancelled = False

        client = _clients.get(clone) or main_client
        if not client: return

        async def update_progress(s: int, f: int):
            try:
                bar = "▓" * int(15 * s / max(total, 1)) + "░" * (15 - int(15 * s / max(total, 1)))
                await client.edit_message_text(
                    chat_id, progress_msg_id,
                    f"📦 **Batch Delivery**\n\n`{bar}`\n{s}/{total} · {f} failed\n"
                    f"Click 👇 to cancel",
                    reply_markup=kb.cancel_dl(batch_id, clone))
            except: pass

        for idx, fm in enumerate(files):
            if cancel_event.is_set():
                cancelled = True; break

            retries = 3
            while retries > 0:
                try:
                    await client.copy_message(
                        chat_id=chat_id,
                        from_chat_id=config.DB_CHANNEL_ID,
                        message_id=fm["db_msg_id"],
                        caption=fm.get("caption", batch.get("caption", "")),
                        protect_content=protect,
                        disable_notification=True,
                    )
                    sent += 1
                    await repo.update_batch_progress(batch_id, clone, sent, failed, db_name)
                    break
                except FloodWait as e:
                    await asyncio.sleep(e.value + 1)
                    retries -= 1
                except Exception as e:
                    retries -= 1
                    if retries == 0: failed += 1
                    await asyncio.sleep(1)

            if idx % 3 == 0 or idx == total - 1:
                await update_progress(sent, failed)
            await asyncio.sleep(config.BATCH_SEND_DELAY)

        if not cancelled:
            await repo.update_batch_status(batch_id, clone, "completed" if failed == 0 else "partial", db_name)
            await repo.inc_batch_dl(batch_id, clone, db_name)
            try:
                t = fmt_auto_delete(auto_del) if auto_del > 0 else "Never"
                await client.edit_message_text(
                    chat_id, progress_msg_id,
                    f"{'✅' if failed == 0 else '⚠️'} **Delivery {'Complete' if failed == 0 else 'Partial'}**\n\n"
                    f"Sent: {sent}/{total}\nFailed: {failed}\n⏱ Auto-delete: {t}"
                )
            except: pass
            if auto_del > 0:
                auto_del_sched.schedule(clone, chat_id, progress_msg_id, auto_del)
        else:
            await repo.update_batch_status(batch_id, clone, "cancelled", db_name)
            try:
                await client.edit_message_text(
                    chat_id, progress_msg_id,
                    f"⏹ **Delivery Cancelled**\nSent: {sent}/{total}"
                )
            except: pass

        _delivery_cancel.pop(cancel_key, None)
        log.info(f"Delivery {batch_id[:12]}... done: {sent}/{total} (wid={wid})")

delivery_engine = DeliveryEngine()

# ═══════════════════════════════════════════════════════════════════════════════
# 12. CLONE MANAGER — Now with per-clone MongoDB URI support
# ═══════════════════════════════════════════════════════════════════════════════

class CloneManager:
    def get_client(self, cid: str = "main") -> Optional[Client]:
        if cid == "main": return main_client
        return _clients.get(cid)

    async def init_main(self) -> Client:
        global main_client
        main_client = Client("main_bot", api_id=config.API_ID, api_hash=config.API_HASH,
                             bot_token=config.BOT_TOKEN, workers=50, sleep_threshold=60)
        return main_client

    def _register_handlers(self, client: Client, clone_id: str):
        """
        Register all handlers for a bot client.
        Each clone handler resolves its own db_name via repo.get_db_name_for_clone().
        """

        # ── Helper to get db_name for this clone ──
        async def _db():
            return await repo.get_db_name_for_clone(clone_id)

        # ── start command ──
        async def start_c(c: Client, m: Message):
            db_name = await _db()
            user = m.from_user
            if not user: return
            await repo.add_user(user.id, clone_id, user.username or "",
                                user.first_name or "", user.last_name or "", db_name)

            if await repo.is_banned(user.id, clone_id, db_name):
                await m.reply("❌ आप इस Bot पर बैन हैं。")
                return

            ok, wait = rate_limiter.check(user.id, clone_id)
            if not ok:
                await m.reply(f"⏳ कृपया {wait} सेकंड रुकें।")
                return

            settings = await repo.get_settings(clone_id, db_name)
            fsubs = settings.get("force_subs", [])
            if not fsubs: fsubs = config.FORCE_SUB_CHANNELS
            if fsubs:
                all_ok, fc, link = await check_all_subs(c, user.id, fsubs)
                if not all_ok:
                    msg = settings.get("force_sub_msg", "⚠️ कृपया Channel Join करें!")
                    msg = msg.replace("{first_name}", user.first_name or "User")
                    msg = msg.replace("{time}", fmt_auto_delete(settings.get("auto_del_secs", 0)))
                    await m.reply(msg, reply_markup=kb.sub_link(link))
                    return

            args = m.text.split(maxsplit=1)
            param = args[1] if len(args) > 1 else ""

            if settings.get("token_required") and not param:
                await m.reply("🔑 इस Bot को Access Token चाहिए。 /start <token> का उपयोग करें。")
                return

            if param and not param.startswith(("f_", "b_")):
                valid, err, _ = await repo.verify_token(param)
                if not valid:
                    await m.reply(f"❌ {err}")
                    return

            if param.startswith("f_"):
                fid = param[2:]
                fd = await repo.get_file_by_id(fid, clone_id, db_name)
                if not fd:
                    await m.reply("❌ File नहीं मिली या डिलीट हो चुकी है।")
                    return
                caption = fd.get("caption", "")
                if settings.get("auto_del_secs", 0) > 0:
                    caption += f"\n\n⏱ {fmt_auto_delete(settings['auto_del_secs'])} में डिलीट होगी"
                try:
                    cp = await c.copy_message(m.chat.id, config.DB_CHANNEL_ID, fd["db_msg_id"],
                                              caption=caption, protect_content=settings.get("protect", True))
                    await repo.inc_file_dl(fid, clone_id, db_name)
                    if settings.get("auto_del_secs", 0) > 0:
                        auto_del_sched.schedule(clone_id, m.chat.id, cp.id, settings["auto_del_secs"])
                except Exception as e:
                    await m.reply(f"❌ File भेजने में त्रुटि: {e}")
                    log.error(f"File send error: {e}")
                return

            if param.startswith("b_"):
                bid = param[2:]
                batch = await repo.get_batch_by_id(bid, clone_id, db_name)
                if not batch or batch.get("deleted"):
                    await m.reply("❌ Batch नहीं मिला या डिलीट हो चुका है。")
                    return
                if batch.get("status") in ("delivering", "completed"):
                    await m.reply(f"❌ यह Batch पहले से `{batch['status']}` है।")
                    return
                info = await m.reply(
                    f"📦 **Batch Found!**\n"
                    f"{batch['total_files']} files, {fmt_size(batch['total_size'])}\n"
                    f"Starting delivery...",
                    reply_markup=kb.cancel_dl(bid, clone_id))
                delivery_engine.enqueue(clone_id, user.id, m.chat.id, bid, info.id,
                                        settings.get("auto_del_secs", 0), settings.get("protect", True))
                return

            msg = settings.get("start_msg", "👋 Hello {first_name}! Send me a file.")
            msg = msg.replace("{first_name}", user.first_name or "User")
            msg = msg.replace("{last_name}", user.last_name or "")
            msg = msg.replace("{username}", f"@{user.username}" if user.username else "User")
            msg = msg.replace("{bot_name}", (await c.get_me()).first_name or "Bot")
            if settings.get("auto_del_secs", 0) > 0:
                msg += f"\n\n⏱ Auto-delete: {fmt_auto_delete(settings['auto_del_secs'])}"
            await m.reply(msg, reply_markup=kb.main(user.id == config.OWNER_ID or user.id in config.ADMIN_IDS))

        # ── done command ──
        async def done_c(c: Client, m: Message):
            db_name = await _db()
            user = m.from_user
            if not user: return
            key = str(user.id)
            buf = _upload_buffers[clone_id].get(key)
            if not buf or not buf[0]:
                await m.reply("📭 Buffer में कोई फाइल नहीं है。 पहले फाइलें भेजें।")
                return

            files, _ = buf
            total_size = sum(f.get("file_size", 0) for f in files)
            token = generate_token()
            batch_id = f"b_{token}"

            data = {
                "batch_id": batch_id, "clone_id": clone_id, "owner_id": user.id,
                "access_token": token, "files": [{
                    "file_id": f["file_id"], "file_unique_id": f.get("file_unique_id", ""),
                    "file_type": f.get("file_type", "document"),
                    "file_name": f.get("file_name", "Unknown"),
                    "file_size": f.get("file_size", 0),
                    "mime_type": f.get("mime_type", ""),
                    "caption": f.get("caption", ""),
                    "db_msg_id": f["db_msg_id"],
                } for f in files],
                "total_files": len(files), "total_size": total_size,
                "caption": "", "status": "active",
                "downloads": 0, "progress": {"sent": 0, "failed": 0},
                "created_at": datetime.now(timezone.utc).isoformat(),
                "auto_delete_at": None, "deleted": False,
            }
            settings = await repo.get_settings(clone_id, db_name)
            ads = settings.get("auto_del_secs", config.AUTO_DELETE_SECONDS)
            if ads > 0:
                data["auto_delete_at"] = (datetime.now(timezone.utc) + timedelta(seconds=ads)).isoformat()

            await repo.create_batch(data, db_name)
            del _upload_buffers[clone_id][key]

            bot_uname = config.BOT_USERNAME
            cd = await repo.get_clone(clone_id)
            if cd: bot_uname = cd.get("bot_username", bot_uname)

            link = f"https://t.me/{bot_uname}?start=b_{token}"
            short = await shorten_url_api(link)
            if short: link = short

            await m.reply(
                f"✅ **Batch Created!**\n\n"
                f"📄 {len(files)} फाइलें\n📏 {fmt_size(total_size)}\n\n"
                f"🔗 `{link}`\n\n"
                f"Users पर क्लिक करने पर सभी files मिलेंगी。",
                disable_web_page_preview=True,
            )
            await repo.add_log(clone_id, user.id, f"Created batch {batch_id[:16]}...", db_name)

        # ── cancel command ──
        async def cancel_c(c: Client, m: Message):
            user = m.from_user
            if not user: return
            key = str(user.id)
            buf = _upload_buffers[clone_id].pop(key, None)
            if buf and buf[0]:
                await m.reply(f"❌ {len(buf[0])} buffered files कैंसिल किए गए।")
                return
            await m.reply("📭 Buffer खाली है या कोई active delivery नहीं है।")

        # ── media handler ──
        async def media_c(c: Client, m: Message):
            db_name = await _db()
            user = m.from_user
            if not user: return

            if await repo.is_banned(user.id, clone_id, db_name): return

            ok, wait = rate_limiter.check(user.id, clone_id)
            if not ok:
                await m.reply(f"⏳ कृपया {wait} सेकंड रुकें。")
                return

            settings = await repo.get_settings(clone_id, db_name)
            fsubs = settings.get("force_subs", [])
            if not fsubs: fsubs = config.FORCE_SUB_CHANNELS
            if fsubs:
                all_ok, _, _ = await check_all_subs(c, user.id, fsubs)
                if not all_ok:
                    await m.reply("⚠️ पहले channels join करें। /start")
                    return

            info = extract_file_info(m)
            if not info:
                await m.reply("❌ इस मीडिया टाइप को स्टोर नहीं कर सकते।")
                return

            if info["file_size"] > config.MAX_FILE_SIZE:
                await m.reply(f"❌ File बहुत बड़ी है। Max: {fmt_size(config.MAX_FILE_SIZE)}")
                return

            try:
                fwd = await m.copy(config.DB_CHANNEL_ID, protect_content=True)
                db_msg_id = fwd.id
            except Exception as e:
                await m.reply(f"❌ DB channel में कॉपी करने में त्रुटि: {e}")
                return

            for bc in config.BACKUP_CHANNELS:
                try: await m.copy(bc, protect_content=True)
                except: pass

            token = generate_token()
            fid = f"f_{token}"
            now = datetime.now(timezone.utc)
            fd = {
                "file_id": fid, "file_unique_id": info["file_unique_id"],
                "clone_id": clone_id, "user_id": user.id,
                "file_type": info["file_type"], "file_name": info["file_name"],
                "file_size": info["file_size"], "mime_type": info["mime_type"],
                "caption": m.caption or "", "db_msg_id": db_msg_id,
                "access_token": token, "downloads": 0,
                "created_at": now.isoformat(),
                "auto_delete_at": (now + timedelta(seconds=settings.get("auto_del_secs", 0))).isoformat()
                if settings.get("auto_del_secs", 0) > 0 else None,
                "deleted": False,
            }
            await repo.store_file(fd, db_name)
            await repo.inc_user_files(user.id, clone_id, 1, db_name)

            bot_uname = config.BOT_USERNAME
            cd = await repo.get_clone(clone_id)
            if cd: bot_uname = cd.get("bot_username", bot_uname)

            link = f"https://t.me/{bot_uname}?start=f_{fid}"
            short = await shorten_url_api(link)
            if short: link = short

            key = str(user.id)
            buf_files, _ = _upload_buffers[clone_id][key]
            buf_files.append(fd)
            _upload_buffers[clone_id][key] = (buf_files, time.time())
            count = len(buf_files)

            await m.reply(
                f"✅ **File Stored!**\n\n"
                f"📄 {info['file_name'][:40]}\n📏 {fmt_size(info['file_size'])}\n\n"
                f"🔗 `{link}`\n\n"
                f"📦 **Batch:** {count} file{'s' if count > 1 else ''}\n"
                f"और files भेजें या /done करें। /cancel से buffer खाली करें।",
                disable_web_page_preview=True,
            )
            await repo.add_log(clone_id, user.id, f"Stored {fid[:16]}...", db_name)

        # ── callback handler ──
        async def cb_c(c: Client, q: CallbackQuery):
            db_name = await _db()
            data = q.data
            uid = q.from_user.id
            await q.answer()

            if data == "close":
                await q.message.delete(); return

            if data == "check_sub":
                settings = await repo.get_settings(clone_id, db_name)
                fsubs = settings.get("force_subs", []) or config.FORCE_SUB_CHANNELS
                all_ok, _, _ = await check_all_subs(c, uid, fsubs)
                if all_ok:
                    await q.message.edit_text("✅ आप सभी channels के member हैं! /start का उपयोग करें।")
                else:
                    await q.answer("❌ अभी भी कुछ channels join नहीं हैं।", show_alert=True)
                return

            if data.startswith("a_") or data.startswith("conf_") or data.startswith("cancel_") \
               or data.startswith("settings_") or data.startswith("batch_") or data.startswith("cancel_dl:"):
                await admin_callback_handler(c, q, clone_id)
                return

            if data == "help":
                await q.message.edit_text(
                    "**📚 Help**\n\n"
                    "• कोई भी file भेजें → शेयरेबल लिंक\n"
                    "• और files भेजें → batch में जुड़ेंगी\n"
                    "• /done → batch फाइनल करें\n"
                    "• /cancel → buffer खाली करें\n\n"
                    "**Admin commands:**\n/admin, /stats, /broadcast, /add_clone, /backup",
                    reply_markup=kb.back())

        client.add_handler(MessageHandler(start_c, filters.command("start") & filters.private))
        client.add_handler(MessageHandler(done_c, filters.command("done") & filters.private))
        client.add_handler(MessageHandler(cancel_c, filters.command("cancel") & filters.private))
        client.add_handler(MessageHandler(media_c, filters.private & ~filters.command(list(
            ["start","done","cancel","admin","stats","broadcast","add_clone","delete_clone",
             "backup","myfiles","batches","set_start","add_fsub","remove_fsub",
             "add_mod","remove_mod","set_autodel","gen_token"]))))
        client.add_handler(CallbackQueryHandler(cb_c))

    async def load_all_clones(self):
        """Load all active clones from MAIN database. Each clone uses its own MongoDB if mongo_uri is set."""
        clones = await repo.get_all_clones("active")
        for c in clones:
            try:
                # If clone has custom mongo_uri, establish its DB connection first
                if c.get("mongo_uri"):
                    try:
                        await database.connect(c["clone_id"], uri=c["mongo_uri"])
                        await repo.ensure_indexes(c["clone_id"])
                        log.info(f"Clone DB connected: {c['clone_id']} (custom MongoDB)")
                    except Exception as e:
                        log.error(f"Clone DB failed for {c['clone_id']}, falling back to main: {e}")

                client = Client(f"clone_{c['clone_id']}", api_id=config.API_ID,
                                api_hash=config.API_HASH, bot_token=c["bot_token"],
                                workers=30, sleep_threshold=60)
                self._register_handlers(client, c["clone_id"])
                await client.start()
                _clients[c["clone_id"]] = client
                log.info(f"Clone loaded: @{c.get('bot_username','?')} ({c['clone_id']})")
            except Exception as e:
                log.error(f"Failed to load clone {c['clone_id']}: {e}")

    async def create_clone(self, token: str, mongo_uri: Optional[str] = None) -> Optional[dict]:
        """
        Create a new clone bot with optional separate MongoDB URI.
        Usage: /add_clone <BOT_TOKEN> [MONGODB_URI]
        """
        temp = Client("_val", api_id=config.API_ID, api_hash=config.API_HASH,
                      bot_token=token, in_memory=True)
        try:
            await temp.start()
            me = temp.me
            await temp.stop()
            cid = generate_clone_id()
            data = {
                "clone_id": cid, "bot_token": token,
                "bot_username": me.username or f"bot_{me.id}",
                "bot_id": me.id, "status": "active",
                "mongo_uri": mongo_uri,  # <-- stored for DB isolation
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            await repo.register_clone(data)

            # If custom URI provided, connect clone DB and ensure indexes
            if mongo_uri:
                try:
                    await database.connect(cid, uri=mongo_uri)
                    await repo.ensure_indexes(cid)
                    log.info(f"Clone {cid}: custom MongoDB connected and indexed")
                except Exception as e:
                    log.error(f"Clone {cid}: custom MongoDB connection failed: {e}")
                    # Still create the clone; it will use main DB as fallback

            client = Client(f"clone_{cid}", api_id=config.API_ID, api_hash=config.API_HASH,
                            bot_token=token, workers=30, sleep_threshold=60)
            self._register_handlers(client, cid)
            await client.start()
            _clients[cid] = client
            log.info(f"Clone created: @{me.username} ({cid})")
            return data
        except Exception as e:
            log.error(f"Create clone failed: {e}")
            return None

    async def stop_clone(self, cid: str):
        client = _clients.pop(cid, None)
        if client:
            try: await client.stop()
            except: pass

    async def restart_clone(self, cid: str) -> bool:
        await self.stop_clone(cid)
        await asyncio.sleep(1)
        cd = await repo.get_clone(cid)
        if cd:
            client = Client(f"clone_{cid}", api_id=config.API_ID, api_hash=config.API_HASH,
                            bot_token=cd["bot_token"], workers=30, sleep_threshold=60)
            self._register_handlers(client, cid)
            try:
                await client.start()
                _clients[cid] = client
                return True
            except: pass
        return False

    async def shutdown_all(self):
        for cid in list(_clients.keys()):
            await self.stop_clone(cid)
        if main_client:
            try: await main_client.stop()
            except: pass

clone_mgr = CloneManager()

# ═══════════════════════════════════════════════════════════════════════════════
# 13. ADMIN CALLBACK HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

async def admin_callback_handler(c: Client, q: CallbackQuery, clone_id: str):
    data = q.data
    msg = q.message
    uid = q.from_user.id
    is_owner = uid == config.OWNER_ID
    is_admin = is_owner or uid in config.ADMIN_IDS

    if not is_admin and not data.startswith("cancel_dl:"):
        await q.answer("❌ Unauthorized", show_alert=True)
        return

    # Dashboard
    if data == "admin_dashboard":
        await show_admin_dash(c, msg, True); return

    # Stats
    if data == "a_stats":
        db_name = await repo.get_db_name_for_clone(clone_id)
        tu = await repo.count_users(db_name=db_name)
        tf = await repo.count_files(db_name=db_name)
        tb = await repo.count_batches(db_name=db_name)
        tc = await repo.count_clones()
        ac = await repo.count_clones("active")
        storage = await repo.total_storage(db_name=db_name) + await repo.batch_total_storage(db_name=db_name)
        db_ok = await database.is_connected(db_name)
        cpu = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()
        up = fmt_time(int(time.time() - _start_time))
        await msg.edit_text(
            f"**📊 Stats**\n\n"
            f"👥 Users: `{tu}`\n📁 Files: `{tf}`\n📦 Batches: `{tb}`\n"
            f"🤖 Clones: `{ac}/{tc}` active\n💾 Storage: `{fmt_size(storage)}`\n"
            f"🗄 DB ({db_name}): {'✅' if db_ok else '❌'}\n"
            f"🖥 CPU: `{cpu}%` RAM: `{mem.percent}%`\n⏱ Uptime: `{up}`",
            reply_markup=kb.back()); return

    # Users
    if data == "a_users":
        db_name = await repo.get_db_name_for_clone(clone_id)
        users = await repo.get_all_users(db_name=db_name, limit=100)
        text = f"**👥 Users ({len(users)}):**\n\n"
        for u in users[:50]:
            text += f"• {u.get('first_name','?')} @{u.get('username','?')} `{u['user_id']}`\n"
        if len(users) > 50: text += f"\n... +{len(users)-50} more"
        await msg.edit_text(text, reply_markup=kb.back()); return

    # Files
    if data == "a_files":
        db_name = await repo.get_db_name_for_clone(clone_id)
        tf = await repo.count_files(db_name=db_name)
        storage = await repo.total_storage(db_name=db_name)
        await msg.edit_text(f"**📁 Files**\nTotal: `{tf}`\nStorage: `{fmt_size(storage)}`",
                            reply_markup=kb.back()); return

    # Batches list
    if data == "a_batches":
        db_name = await repo.get_db_name_for_clone(clone_id)
        batches = await repo.get_batches(db_name=db_name, limit=10)
        if not batches:
            await msg.edit_text("📦 No batches found.", reply_markup=kb.back()); return
        text = "**📦 Batches (recent 10):**\n\n"
        for b in batches:
            s = b.get("status","?")
            icon = {"active":"🟢","delivering":"🟡","completed":"✅","partial":"⚠️","cancelled":"❌","deleted":"🗑"}.get(s,"⚪")
            text += f"{icon} `{b['batch_id'][:16]}…` {b['total_files']} files [{s}]\n"
        text += "\n/details <batch_id> for full info"
        await msg.edit_text(text, reply_markup=kb.back()); return

    # Clones
    if data == "a_clones":
        clones = await repo.get_all_clones()
        text = f"**🤖 Clones ({len(clones)}):**\n\n"
        for c in clones:
            s = c.get("status","?")
            icon = "🟢" if s == "active" else "🔴"
            mu = " 🗄" if c.get("mongo_uri") else ""
            text += f"{icon} `{c['clone_id']}` — @{c.get('bot_username','?')} [{s}]{mu}\n"
        text += "\n`/add_clone <token> [mongo_uri]` — mu for separate DB"
        await msg.edit_text(text, reply_markup=kb.back()); return

    # Broadcast
    if data == "a_broadcast":
        await msg.edit_text("📢 **Broadcast**\n\n`/broadcast` (reply to a message) to send to all users.\n\nProgress shown in real-time.",
                            reply_markup=kb.back()); return

    
    # Settings
if data == "a_settings":
    await msg.edit_text(
        "⚙️ **Global Settings**\n\nConfigure via .env:\n"
        "`FORCE_SUB_CHANNELS`, `BACKUP_CHANNELS`\n"
        "`PROTECT_CONTENT`, `AUTO_DELETE_SECONDS`\n"
        "`RATE_LIMIT_*`, `SHORTENER_*`\n\n"
        "Clone-specific: `/set_start`, `/add_fsub`, etc.",
        reply_markup=kb.back()
    )
    return

if data == "conf_restart_all":
    await msg.edit_text("🔄 Restarting...")

    try:
        await clone_mgr.shutdown_all()
        await database.close()
    except Exception as e:
        log.error(f"Shutdown error: {e}")

    await asyncio.sleep(2)

    os._exit(0)
    return

if data == "cancel_restart_all":
    await show_admin_dash(c, msg, True)
    return

    # Backup
    if data == "a_backup":
        await msg.edit_text(
            "💾 **Backup**\n\n"
            "`/backup` to export file records to JSON.\n"
            "Sent to log channel if configured.\n\n"
            "GDrive: " + ("✅ configured" if config.GDRIVE_FOLDER_ID else "❌ not set"),
            reply_markup=kb.back()
        )
        return
    # Cancel delivery
    if data.startswith("cancel_dl:"):
        parts = data.split(":")
        bid, cl = parts[1], parts[2] if len(parts) > 2 else clone_id
        delivery_engine.cancel(cl, uid, bid)
        await q.answer("⏹ Delivery cancelled", show_alert=True); return

    await q.answer("Unknown action")


async def show_admin_dash(c: Client, msg: Message, edit: bool = False):
    global _start_time
    tu = await repo.count_users(); tf = await repo.count_files()
    tb = await repo.count_batches(); tc = await repo.count_clones()
    ac = await repo.count_clones("active")
    storage = await repo.total_storage() + await repo.batch_total_storage()
    db_ok = await database.is_connected("main")
    cpu = psutil.cpu_percent(interval=0.1)
    mem = psutil.virtual_memory()
    up = fmt_time(int(time.time() - _start_time))

    text = (
        f"**⚙️ Admin Dashboard**\n\n"
        f"👥 {tu}  📁 {tf}  📦 {tb}\n"
        f"🤖 {ac}/{tc} clones  💾 {fmt_size(storage)}\n"
        f"🗄 {'✅' if db_ok else '❌'}  🖥 CPU {cpu}% RAM {mem.percent}%\n"
        f"⏱ {up}\n\n"
        f"Select option:"
    )
    if edit:
        try: await msg.edit_text(text, reply_markup=kb.admin())
        except: pass
    else:
        await msg.reply(text, reply_markup=kb.admin())

# ═══════════════════════════════════════════════════════════════════════════════
# 14. MAIN BOT COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

async def admin_command(c: Client, m: Message):
    uid = m.from_user.id
    if uid != config.OWNER_ID and uid not in config.ADMIN_IDS:
        await m.reply("❌ Unauthorized")
        return
    await show_admin_dash(c, m)

async def stats_command(c: Client, m: Message):
    uid = m.from_user.id
    if uid != config.OWNER_ID and uid not in config.ADMIN_IDS: return
    tu = await repo.count_users(); tf = await repo.count_files()
    tb = await repo.count_batches(); tc = await repo.count_clones()
    ac = await repo.count_clones("active")
    storage = await repo.total_storage() + await repo.batch_total_storage()
    up = fmt_time(int(time.time() - _start_time))
    await m.reply(
        f"**📊 Stats**\n\n"
        f"👥 Users: {tu}\n📁 Files: {tf}\n📦 Batches: {tb}\n"
        f"🤖 Clones: {ac}/{tc}\n💾 Storage: {fmt_size(storage)}\n⏱ Uptime: {up}"
    )

async def broadcast_command(c: Client, m: Message):
    uid = m.from_user.id
    if uid != config.OWNER_ID and uid not in config.ADMIN_IDS: return
    if not m.reply_to_message:
        await m.reply("Reply to a message with /broadcast")
        return
    pm = await m.reply("📢 Broadcasting...")
    users = await repo.get_all_user_ids()
    total = len(users); success = 0; failed = 0
    for i, u in enumerate(users):
        try:
            await m.reply_to_message.copy(u)
            success += 1
        except: failed += 1
        if (i+1) % 50 == 0:
            try: await pm.edit_text(f"📢 {i+1}/{total} | ✅ {success} | ❌ {failed}")
            except: pass
        await asyncio.sleep(0.05)
    await pm.edit_text(f"✅ Broadcast done!\n✅ {success}\n❌ {failed}\n📊 {total}")

async def add_clone_command(c: Client, m: Message):
    """
    /add_clone <BOT_TOKEN> [MONGODB_URI]
    If MONGODB_URI is provided, the clone bot will use its own separate database.
    """
    if m.from_user.id != config.OWNER_ID: return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 2:
        await m.reply(
            "Usage: `/add_clone <BOT_TOKEN> [MONGODB_URI]`\n\n"
            "• `BOT_TOKEN` — BotFather से Token\n"
            "• `MONGODB_URI` (optional) — Clone के लिए अलग MongoDB URI\n"
            "  अगर नहीं देंगे तो Main bot का DB use होगा।\n\n"
            "**Examples:**\n"
            "`/add_clone 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`\n"
            "`/add_clone 123456:ABC-DEF... mongodb+srv://user:pass@clone.mongodb.net/CloneDB`"
        )
        return

    token = parts[1].strip()
    mongo_uri = parts[2].strip() if len(parts) > 2 else None

    result = await clone_mgr.create_clone(token, mongo_uri)
    if result:
        msg = f"✅ **Clone added!**\nID: `{result['clone_id']}`\n@ {result['bot_username']}"
        if result.get("mongo_uri"):
            msg += "\n🗄 **Isolated Database:** ✅ (अलग MongoDB URI)"
        else:
            msg += "\n🗄 **Database:** Main (shared)"
        await m.reply(msg)
    else:
        await m.reply("❌ Failed. Check token or logs.")

async def backup_command(c: Client, m: Message):
    if m.from_user.id != config.OWNER_ID: return
    pm = await m.reply("💾 Backing up...")
    try:
        files = await (await database.col("files", "main")).find({"deleted": False}).to_list(length=100000)
        path = f"/tmp/backup_{int(time.time())}.json"
        with open(path, "w") as f:
            json.dump([{k: str(v) if isinstance(v, datetime) else v for k,v in doc.items()} for doc in files], f, indent=2)
        if config.LOG_CHANNEL_ID:
            await c.send_document(config.LOG_CHANNEL_ID, path, caption=f"💾 Backup — {len(files)} files")
        await pm.edit_text(f"✅ Backup complete! {len(files)} files.")
    except Exception as e:
        await pm.edit_text(f"❌ Backup failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 15. MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    global _start_time
    _start_time = time.time()

    log.info("=" * 60)
    log.info("🤖 FILE STORE BOT STARTING")
    log.info("=" * 60)

    Config.validate()
    log.info("✅ Config OK")

    # Connect MAIN database
    await database.connect("main")
    await repo.ensure_indexes("main")
    log.info("✅ Main DB ready")

    # Init main bot
    main_client = await clone_mgr.init_main()
    main_client.add_handler(MessageHandler(admin_command, filters.command("admin") & filters.private))
    main_client.add_handler(MessageHandler(stats_command, filters.command("stats") & filters.private))
    main_client.add_handler(MessageHandler(broadcast_command, filters.command("broadcast") & filters.private))
    main_client.add_handler(MessageHandler(add_clone_command, filters.command("add_clone") & filters.private))
    main_client.add_handler(MessageHandler(backup_command, filters.command("backup") & filters.private))
    clone_mgr._register_handlers(main_client, "main")

    await main_client.start()
    log.info(f"✅ Main bot: @{config.BOT_USERNAME}")

    # Load all clones (each with its own DB if configured)
    await clone_mgr.load_all_clones()

    # Start services
    await delivery_engine.start(config.DELIVERY_WORKERS)
    await auto_del_sched.start()
    log.info("✅ Services started")

    log.info("=" * 60)
    log.info(f"🚀 BOT RUNNING | Owner: {config.OWNER_ID}")
    log.info("=" * 60)

    asyncio.create_task(_daily_stats_loop())
    await idle()

async def _daily_stats_loop():
    while True:
        try:
            for cid in list(_clients.keys()) + ["main"]:
                db_name = await repo.get_db_name_for_clone(cid)
                await repo.record_daily(cid, db_name)
            await asyncio.sleep(3600)
        except: await asyncio.sleep(3600)

async def shutdown():
    log.info("🛑 Shutting down...")
    await delivery_engine.stop()
    await auto_del_sched.stop()
    await clone_mgr.shutdown_all()
    await database.close()
    log.info("✅ Shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        asyncio.run(shutdown())
        sys.exit(0)
    except Exception as e:
        log.critical(f"Fatal: {e}")
        traceback.print_exc()
        sys.exit(1)
