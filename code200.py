#!/usr/bin/env python3
"""
Telegram File Store & Multi-Clone Bot
======================================
Pyrogram v2 + Motor (Async MongoDB)

Environment:
    API_ID       - Telegram API ID
    API_HASH     - Telegram API Hash
    BOT_TOKEN    - Main bot token
    MONGODB_URI  - Main MongoDB connection string
    ADMIN_IDS    - Comma-separated Telegram user IDs

Author: HackerAI
"""

import os
import sys
import asyncio
import logging
import string
import random
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

from pyrogram import Client, filters, enums
from pyrogram.handlers import MessageHandler
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    Message,
    BotCommand,
)
from pyrogram.errors import (
    UserNotParticipant,
    ChatAdminRequired,
    FloodWait,
    BadRequest,
    PeerIdInvalid,
    ChannelInvalid,
    UsernameNotOccupied,
)

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import (
    ConnectionFailure,
    OperationFailure,
    ServerSelectionTimeoutError,
    InvalidURI,
)

# ---------------------------------------------------------------------------
#  CONFIGURATION
# ---------------------------------------------------------------------------

API_ID: int = int(os.environ.get("API_ID", 0))
API_HASH: str = os.environ.get("API_HASH", "")
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
MONGODB_URI: str = os.environ.get("MONGODB_URI", "")
ADMIN_IDS: List[int] = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip()
]

AUTO_DELETE_SECS: int = 28800  # 8 hours

# Validation
_MISSING = []
if not API_ID or API_ID == 0:
    _MISSING.append("API_ID")
if not API_HASH:
    _MISSING.append("API_HASH")
if not BOT_TOKEN:
    _MISSING.append("BOT_TOKEN")
if not MONGODB_URI:
    _MISSING.append("MONGODB_URI")
if _MISSING:
    raise SystemExit(
        "❌ Missing environment variables: "
        + ", ".join(_MISSING)
    )

# ---------------------------------------------------------------------------
#  LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
LOGGER = logging.getLogger("FileStoreBot")

# ---------------------------------------------------------------------------
#  DATABASE CLASS
# ---------------------------------------------------------------------------

class Database:
    """Async MongoDB handler via Motor."""

    def __init__(self, uri: str) -> None:
        self._uri = uri
        self._client: Optional[AsyncIOMotorClient] = None
        self.db = None
        self.users = None
        self.batches = None
        self.clones = None
        self.fsub_channels = None

    async def connect(self) -> None:
        self._client = AsyncIOMotorClient(
            self._uri,
            serverSelectionTimeoutMS=10000,
        )
        await self._client.admin.command("ping")
        self.db = self._client.get_database("file_store_bot")
        self.users = self.db["users"]
        self.batches = self.db["batches"]
        self.clones = self.db["clones"]
        self.fsub_channels = self.db["fsub_channels"]

        # Unique indexes — NO TTL
        await self.users.create_index("user_id", unique=True)
        await self.batches.create_index("batch_id", unique=True)
        await self.clones.create_index("bot_token", unique=True)
        await self.fsub_channels.create_index("channel_id", unique=True)

        LOGGER.info("MongoDB connected and indexes ready.")

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()
            LOGGER.info("MongoDB connection closed.")

    @staticmethod
    async def test_uri(uri: str) -> Tuple[bool, str]:
        test_client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=6000)
        try:
            await test_client.admin.command("ping")
            return True, ""
        except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
            return False, f"Connection failure: {exc}"
        except OperationFailure as exc:
            return False, f"Authentication failure: {exc}"
        except InvalidURI as exc:
            return False, f"Invalid URI: {exc}"
        except Exception as exc:
            return False, f"Unexpected error: {exc}"
        finally:
            test_client.close()

    # ---- Users ----

    async def add_user(self, user_id: int, username: str = "", first_name: str = "") -> None:
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "username": username,
                        "first_name": first_name,
                        "last_active": datetime.utcnow(),
                    },
                    "$setOnInsert": {
                        "joined_at": datetime.utcnow(),
                        "total_files": 0,
                    },
                },
                upsert=True,
            )
        except Exception as exc:
            LOGGER.error("add_user(%s) failed: %s", user_id, exc)

    async def user_count(self) -> int:
        try:
            return await self.users.count_documents({})
        except Exception as exc:
            LOGGER.error("user_count failed: %s", exc)
            return 0

    async def all_users(self) -> List[dict]:
        try:
            cursor = self.users.find({})
            return await cursor.to_list(length=None)
        except Exception as exc:
            LOGGER.error("all_users failed: %s", exc)
            return []

    # ---- Batches ----

    async def create_batch(self, user_id: int, file_ids: List[str], media_type: str = "document") -> str:
        batch_id = _random_id(prefix="batch_")
        doc = {
            "batch_id": batch_id,
            "user_id": user_id,
            "file_ids": file_ids,
            "media_type": media_type,
            "file_count": len(file_ids),
            "created_at": datetime.utcnow(),
        }
        try:
            await self.batches.insert_one(doc)
            await self.users.update_one(
                {"user_id": user_id},
                {"$inc": {"total_files": len(file_ids)}},
            )
            return batch_id
        except Exception as exc:
            LOGGER.error("create_batch failed: %s", exc)
            raise

    async def get_batch(self, batch_id: str) -> Optional[dict]:
        try:
            return await self.batches.find_one({"batch_id": batch_id})
        except Exception as exc:
            LOGGER.error("get_batch(%s) failed: %s", batch_id, exc)
            return None

    async def batch_count(self) -> int:
        try:
            return await self.batches.count_documents({})
        except Exception as exc:
            LOGGER.error("batch_count failed: %s", exc)
            return 0

    async def total_stored_files(self) -> int:
        try:
            pipeline = [{"$group": {"_id": None, "total": {"$sum": "$file_count"}}}]
            cur = self.batches.aggregate(pipeline)
            result = await cur.to_list(length=1)
            return result[0]["total"] if result else 0
        except Exception as exc:
            LOGGER.error("total_stored_files failed: %s", exc)
            return 0

    # ---- Clones ----

    async def register_clone(self, bot_token: str, mongo_uri: str, owner_id: int) -> str:
        clone_id = _random_id(prefix="clone_", length=8)
        doc = {
            "clone_id": clone_id,
            "bot_token": bot_token,
            "mongo_uri": mongo_uri,
            "owner_id": owner_id,
            "status": "active",
            "created_at": datetime.utcnow(),
        }
        try:
            await self.clones.insert_one(doc)
            return clone_id
        except Exception as exc:
            LOGGER.error("register_clone failed: %s", exc)
            raise

    async def all_clones(self) -> List[dict]:
        try:
            cursor = self.clones.find({})
            return await cursor.to_list(length=None)
        except Exception as exc:
            LOGGER.error("all_clones failed: %s", exc)
            return []

    async def remove_clone(self, bot_token: str) -> bool:
        try:
            r = await self.clones.delete_one({"bot_token": bot_token})
            return r.deleted_count > 0
        except Exception as exc:
            LOGGER.error("remove_clone failed: %s", exc)
            return False

    # ---- FSUB Channels ----

    async def add_fsub_channel(self, channel_id: str, title: str = "") -> bool:
        try:
            existing = await self.fsub_channels.find_one({"channel_id": channel_id})
            if existing:
                return False
            await self.fsub_channels.insert_one({
                "channel_id": channel_id,
                "title": title or channel_id,
                "added_at": datetime.utcnow(),
            })
            return True
        except Exception as exc:
            LOGGER.error("add_fsub_channel failed: %s", exc)
            return False

    async def remove_fsub_channel(self, channel_id: str) -> bool:
        try:
            if channel_id == "all":
                r = await self.fsub_channels.delete_many({})
                return r.deleted_count > 0
            r = await self.fsub_channels.delete_one({"channel_id": channel_id})
            return r.deleted_count > 0
        except Exception as exc:
            LOGGER.error("remove_fsub_channel failed: %s", exc)
            return False

    async def get_all_fsub_channels(self) -> List[dict]:
        try:
            cursor = self.fsub_channels.find({})
            return await cursor.to_list(length=None)
        except Exception as exc:
            LOGGER.error("get_all_fsub_channels failed: %s", exc)
            return []

    async def get_fsub_channel(self, channel_id: str) -> Optional[dict]:
        try:
            return await self.fsub_channels.find_one({"channel_id": channel_id})
        except Exception:
            return None

    async def fsub_channel_count(self) -> int:
        try:
            return await self.fsub_channels.count_documents({})
        except Exception as exc:
            LOGGER.error("fsub_channel_count failed: %s", exc)
            return 0


# ---------------------------------------------------------------------------
#  HELPERS
# ---------------------------------------------------------------------------

def _random_id(prefix: str = "", length: int = 10) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return prefix + "".join(random.choices(alphabet, k=length))


def _extract_file_id(message: Message) -> Optional[str]:
    for attr in ("document", "video", "audio", "photo", "voice", "video_note", "sticker", "animation"):
        obj = getattr(message, attr, None)
        if obj is not None:
            return obj.file_id
    return None


def _media_type(message: Message) -> str:
    if message.document:
        return "document"
    if message.video:
        return "video"
    if message.audio:
        return "audio"
    if message.photo:
        return "photo"
    if message.voice:
        return "voice"
    return "document"


async def _send_file_by_type(client: Client, chat_id: int, file_id: str, media_type: str) -> Optional[Message]:
    kwargs = {"chat_id": chat_id, "protect_content": True}
    try:
        if media_type == "photo":
            return await client.send_photo(file_id=file_id, **kwargs)
        if media_type == "video":
            return await client.send_video(file_id=file_id, **kwargs)
        if media_type == "audio":
            return await client.send_audio(file_id=file_id, **kwargs)
        return await client.send_document(file_id=file_id, **kwargs)
    except FloodWait as e:
        LOGGER.warning("FloodWait %ds – sleeping", e.value)
        await asyncio.sleep(e.value)
        return await _send_file_by_type(client, chat_id, file_id, media_type)
    except Exception as exc:
        LOGGER.error("send_file(%s) failed: %s", file_id[:20], exc)
        return None


async def _delete_after_delay(client: Client, chat_id: int, message_id: int, delay: int = AUTO_DELETE_SECS) -> None:
    await asyncio.sleep(delay)
    try:
        await client.delete_messages(chat_id, message_id)
    except Exception as exc:
        LOGGER.debug("delete_message(%s) failed: %s", message_id, exc)


async def _resolve_channel_info(client: Client, channel_id: str) -> Tuple[str, str]:
    try:
        chat = await client.get_chat(channel_id)
        username = chat.username
        if username:
            link = f"https://t.me/{username}"
        elif chat.invite_link:
            link = chat.invite_link
        else:
            link = str(channel_id)
        return link, chat.title or channel_id
    except Exception:
        return channel_id, channel_id


# ---------------------------------------------------------------------------
#  GLOBAL DATABASE INSTANCE
# ---------------------------------------------------------------------------

db: Optional[Database] = None

# ---------------------------------------------------------------------------
#  MAIN PYROGRAM CLIENT — NO plugins={} !
# ---------------------------------------------------------------------------

app = Client(
    name="file_store_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="./sessions",
    in_memory=True,
)

# ---------------------------------------------------------------------------
#  ALBUM CACHE (2-second window — temporary only)
# ---------------------------------------------------------------------------

_album_cache: Dict[str, dict] = {}

# ---------------------------------------------------------------------------
#  FSUB CHECK
# ---------------------------------------------------------------------------

async def _get_fsub_channels() -> List[str]:
    channels_raw = await db.get_all_fsub_channels()
    return [c["channel_id"] for c in channels_raw if c.get("channel_id")]


async def _is_joined_all(client: Client, user_id: int) -> Tuple[bool, Optional[str]]:
    channels = await _get_fsub_channels()
    if not channels:
        return True, None
    for ch in channels:
        try:
            member = await client.get_chat_member(ch, user_id)
            if member.status in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED):
                return False, ch
        except UserNotParticipant:
            return False, ch
        except (ChatAdminRequired, BadRequest, PeerIdInvalid, ChannelInvalid) as exc:
            LOGGER.warning("FSUB check skip %s: %s", ch, exc)
            continue
        except Exception as exc:
            LOGGER.error("FSUB error %s: %s", ch, exc)
            continue
    return True, None


async def _send_fsub_prompt(client: Client, message: Message, failed_ch: Optional[str] = None) -> None:
    channels = await _get_fsub_channels()
    lines = ["⚠️ **Access Denied!**\n", "Please **join** the following channel(s) to use this bot:\n"]
    for ch in channels:
        link, title = await _resolve_channel_info(client, ch)
        marker = " 👈" if ch == failed_ch else ""
        lines.append(f"📢 **{title}** – [Join]({link}){marker}")
    lines.append("\nAfter joining, click the button below.")
    text = "\n".join(lines)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 I've Joined", callback_data="refresh_sub")],
    ])
    await message.reply_text(text, reply_markup=kb, disable_web_page_preview=True)


# ---------------------------------------------------------------------------
#  START COMMAND
# ---------------------------------------------------------------------------

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message) -> None:
    user = message.from_user
    if user is None:
        return
    uid = user.id

    await db.add_user(uid, user.username or "", user.first_name or "")

    # Batch link?
    if len(message.command) > 1 and message.command[1].startswith("batch_"):
        await _deliver_batch(client, message, message.command[1])
        return

    # FSUB check
    joined, failed_ch = await _is_joined_all(client, uid)
    if not joined:
        await _send_fsub_prompt(client, message, failed_ch)
        return

    total_users = await db.user_count()
    stored_files = await db.total_stored_files()

    text = (
        "👋 **Namaste File!**\n\n"
        "Main Telegram File Store Bot hoon. Aap yahan files store, batch links generate, "
        "aur dynamic URLs share kar sakte hain.\n\n"
        "📊 **Global Stats:**\n"
        f"👥 Total Users: {total_users}\n"
        f"📁 Stored Files: {stored_files}\n"
        "⏰ Auto-Delete Window: 8 Hours Active"
    )

    buttons = [
        [
            InlineKeyboardButton("🔍 Search Files", callback_data="search_files"),
            InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
        ],
        [
            InlineKeyboardButton("🤖 Create Clone", callback_data="create_clone"),
            InlineKeyboardButton("📊 My Stats", callback_data="my_stats"),
        ],
    ]

    if uid in ADMIN_IDS:
        buttons.append([InlineKeyboardButton("🛡️ Admin Panel", callback_data="admin_panel")])

    kb = InlineKeyboardMarkup(buttons)
    LOGGER.info(f"Start sent to user {uid}")
    await message.reply_text(text, reply_markup=kb)


# ---------------------------------------------------------------------------
#  BATCH DELIVERY
# ---------------------------------------------------------------------------

async def _deliver_batch(client: Client, message: Message, batch_id: str) -> None:
    batch = await db.get_batch(batch_id)
    if batch is None:
        await message.reply_text("❌ Batch not found or has been deleted.")
        return

    file_ids: List[str] = batch.get("file_ids", [])
    media_type: str = batch.get("media_type", "document")
    if not file_ids:
        await message.reply_text("❌ This batch contains no files.")
        return

    info_msg = await message.reply_text(
        f"📁 **Sending {len(file_ids)} file(s)...**\n\n"
        f"⚠️ These files will be auto-deleted after **8 hours**.\n"
        f"You cannot forward or save them directly."
    )

    sent_ok = 0
    for fid in file_ids:
        sent = await _send_file_by_type(client, message.chat.id, fid, media_type)
        if sent is not None:
            sent_ok += 1
            asyncio.create_task(_delete_after_delay(client, sent.chat.id, sent.id))
        await asyncio.sleep(0.3)

    await info_msg.edit_text(
        f"✅ **Sent {sent_ok}/{len(file_ids)} files successfully!**\n\n"
        f"⏰ These messages will be auto-deleted in 8 hours."
    )


# ---------------------------------------------------------------------------
#  MEDIA HANDLER
# ---------------------------------------------------------------------------

@app.on_message(filters.private & filters.media & ~filters.command("start"))
async def media_handler(client: Client, message: Message) -> None:
    user = message.from_user
    if user is None:
        return
    uid = user.id

    joined, failed_ch = await _is_joined_all(client, uid)
    if not joined:
        await _send_fsub_prompt(client, message, failed_ch)
        return

    await db.add_user(uid, user.username or "", user.first_name or "")

    file_id = _extract_file_id(message)
    if file_id is None:
        await message.reply_text("❌ Unsupported media type.")
        return

    mtype = _media_type(message)

    # Album (grouped media)
    if message.media_group_id:
        key = f"{uid}:{message.media_group_id}"
        entry = _album_cache.get(key)
        if entry is None:
            entry = {"file_ids": [], "task": None}
            _album_cache[key] = entry

        entry["file_ids"].append(file_id)

        if entry["task"] is not None and not entry["task"].done():
            entry["task"].cancel()

        async def _flush_album(k: str = key):
            await asyncio.sleep(2)
            cached = _album_cache.pop(k, None)
            if cached and cached["file_ids"]:
                await _persist_and_reply(client, message, cached["file_ids"], mtype)

        entry["task"] = asyncio.create_task(_flush_album())
        return

    # Single file
    await _persist_and_reply(client, message, [file_id], mtype)


async def _persist_and_reply(client: Client, reference: Message, file_ids: List[str], media_type: str) -> None:
    try:
        batch_id = await db.create_batch(reference.from_user.id, file_ids, media_type)
    except Exception as exc:
        LOGGER.error("_persist_and_reply: %s", exc)
        await reference.reply_text("❌ Failed to create batch. Please try again.")
        return

    me = await client.get_me()
    link = f"https://t.me/{me.username}?start={batch_id}"

    text = (
        f"✅ **Batch Created Successfully!**\n\n"
        f"📁 **Files:** {len(file_ids)}\n"
        f"🔗 **Link:**\n`{link}`\n\n"
        f"ℹ️ Anyone with this link can access the files.\n"
        f"⏰ Auto-delete after 8 hours."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Share Link", url=f"https://t.me/share/url?url={link}")],
    ])
    await reference.reply_text(text, reply_markup=kb, disable_web_page_preview=True)


# ---------------------------------------------------------------------------
#  /add_fsub (admin)
# ---------------------------------------------------------------------------

@app.on_message(filters.command("add_fsub") & filters.private)
async def add_fsub_cmd(client: Client, message: Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    if uid not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**")
        return

    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text("❌ Usage: `/add_fsub <CHANNEL_ID>`\nExample: `/add_fsub @my_channel`")
        return

    channel_id = parts[1].strip()

    try:
        chat = await client.get_chat(channel_id)
        resolved = f"@{chat.username}" if chat.username else str(chat.id)
        title = chat.title or resolved
    except (PeerIdInvalid, ChannelInvalid, UsernameNotOccupied) as exc:
        await message.reply_text(f"❌ **Invalid Channel:** `{exc}`\n\nMake sure the bot is an **admin** in the channel.")
        return
    except Exception as exc:
        await message.reply_text(f"❌ **Error:** `{exc}`")
        return

    ok = await db.add_fsub_channel(resolved, title)
    if ok:
        await message.reply_text(f"✅ **Force-Sub Channel Added!**\n\n📢 **{title}** (`{resolved}`)\n\nUsers must now join this channel to use the bot.")
    else:
        await message.reply_text(f"⚠️ Channel `{resolved}` is already in the FSUB list.")


# ---------------------------------------------------------------------------
#  /del_fsub (admin)
# ---------------------------------------------------------------------------

@app.on_message(filters.command("del_fsub") & filters.private)
async def del_fsub_cmd(client: Client, message: Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    if uid not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**")
        return

    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text("❌ Usage: `/del_fsub <CHANNEL_ID>` or `/del_fsub all`")
        return

    target = parts[1].strip()

    if target == "all":
        deleted = await db.remove_fsub_channel("all")
        await message.reply_text("✅ **All FSUB channels removed.**" if deleted else "⚠️ No FSUB channels to remove.")
        return

    removed = await db.remove_fsub_channel(target)
    if removed:
        await message.reply_text(f"✅ **Removed:** `{target}` from FSUB list.")
    else:
        await message.reply_text("❌ Channel not found in FSUB list.")


# ---------------------------------------------------------------------------
#  /add_clone (admin)
# ---------------------------------------------------------------------------

@app.on_message(filters.command("add_clone") & filters.private)
async def add_clone_cmd(client: Client, message: Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    if uid not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!** Admins only.")
        return

    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await message.reply_text("❌ Usage:\n`/add_clone <BOT_TOKEN> <MONGODB_URI>`\n\nExample:\n`/add_clone 123456:ABC-DEF mongodb+srv://user:pass@cluster.mongodb.net/db`")
        return

    _, bot_token, mongo_uri = parts

    status = await message.reply_text("🔄 **Testing MongoDB connection…**")
    ok, err = await Database.test_uri(mongo_uri)
    if not ok:
        await status.edit_text(f"❌ **MongoDB Error**\n\n`{err}`")
        return

    await status.edit_text("✅ MongoDB OK  •  Testing bot token…")

    try:
        temp = Client(name=f"_tmp_{bot_token[:8]}", api_id=API_ID, api_hash=API_HASH, bot_token=bot_token, in_memory=True)
        await temp.start()
        cloned_me = await temp.get_me()
        await temp.stop()
    except Exception as exc:
        await status.edit_text(f"❌ **Invalid Bot Token**\n\n`{exc}`")
        return

    try:
        cid = await db.register_clone(bot_token, mongo_uri, uid)
    except Exception as exc:
        await status.edit_text(f"❌ **Registration failed**\n\n`{exc}`")
        return

    try:
        await _start_one_clone({"bot_token": bot_token, "mongo_uri": mongo_uri})
    except Exception as exc:
        await db.remove_clone(bot_token)
        await status.edit_text(f"❌ **Clone start failed – rollback done**\n\n`{exc}`")
        return

    await status.edit_text(f"✅ **Clone Bot Created & Running!**\n\n🤖 **Bot:** @{cloned_me.username}\n🆔 **Clone ID:** `{cid}`\n🗄️ **Database:** Connected\n\nUsers can now interact with the clone independently.")


# ---------------------------------------------------------------------------
#  CLONE MANAGEMENT
# ---------------------------------------------------------------------------

_running_clones: Dict[str, Client] = {}

async def _start_one_clone(cfg: dict) -> None:
    token = cfg["bot_token"]
    mongo_uri = cfg["mongo_uri"]
    tag = token[:20]

    if tag in _running_clones:
        LOGGER.warning("Clone %s already running, skipping.", tag)
        return

    cdb = Database(mongo_uri)
    await cdb.connect()

    clone = Client(name=f"clone_{tag}", api_id=API_ID, api_hash=API_HASH, bot_token=token, workdir="./sessions", in_memory=True)
    clone._clone_db = cdb

    async def _clone_start(cl: Client, msg: Message):
        await _clone_start_handler(cl, msg, cdb)

    async def _clone_media(cl: Client, msg: Message):
        await _clone_media_handler(cl, msg, cdb)

    clone.add_handler(MessageHandler(_clone_start, filters.command("start")))
    clone.add_handler(MessageHandler(_clone_media, filters.private & filters.media))

    await clone.start()
    _running_clones[tag] = clone
    me = await clone.get_me()
    LOGGER.info("Clone @%s started (DB: %s…)", me.username, mongo_uri[:30])


async def _stop_all_clones() -> None:
    for tag, cl in list(_running_clones.items()):
        try:
            if hasattr(cl, "_clone_db") and cl._clone_db:
                await cl._clone_db.close()
            await cl.stop()
        except Exception as exc:
            LOGGER.error("Stop clone %s error: %s", tag, exc)
    _running_clones.clear()


# ---------------------------------------------------------------------------
#  CLONE HANDLERS
# ---------------------------------------------------------------------------

async def _clone_start_handler(cl: Client, msg: Message, cdb: Database) -> None:
    user = msg.from_user
    if user is None:
        return
    uid = user.id
    await cdb.add_user(uid, user.username or "", user.first_name or "")

    if len(msg.command) > 1 and msg.command[1].startswith("batch_"):
        await _clone_deliver_batch(cl, msg, msg.command[1], cdb)
        return

    u = await cdb.user_count()
    f = await cdb.total_stored_files()
    text = (
        "👋 **Namaste File!**\n\n"
        "Main Telegram File Store Bot hoon. Aap yahan files store, batch links generate, "
        "aur dynamic URLs share kar sakte hain.\n\n"
        "📊 **Global Stats:**\n"
        f"👥 Total Users: {u}\n"
        f"📁 Stored Files: {f}\n"
        "⏰ Auto-Delete Window: 8 Hours Active"
    )
    buttons = [
        [InlineKeyboardButton("🔍 Search Files", callback_data="search_files"), InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        [InlineKeyboardButton("🤖 Create Clone", callback_data="create_clone"), InlineKeyboardButton("📊 My Stats", callback_data="my_stats")],
    ]
    kb = InlineKeyboardMarkup(buttons)
    await msg.reply_text(text, reply_markup=kb)


async def _clone_deliver_batch(cl: Client, msg: Message, batch_id: str, cdb: Database) -> None:
    batch = await cdb.get_batch(batch_id)
    if batch is None:
        await msg.reply_text("❌ Batch not found.")
        return
    file_ids = batch.get("file_ids", [])
    mtype = batch.get("media_type", "document")
    if not file_ids:
        await msg.reply_text("❌ No files.")
        return
    info = await msg.reply_text(f"📁 **Sending {len(file_ids)} file(s)...**\n\n⚠️ Auto-delete after 8 hours.")
    ok = 0
    for fid in file_ids:
        s = await _send_file_by_type(cl, msg.chat.id, fid, mtype)
        if s:
            ok += 1
            asyncio.create_task(_delete_after_delay(cl, s.chat.id, s.id))
        await asyncio.sleep(0.3)
    await info.edit_text(f"✅ **Sent {ok}/{len(file_ids)} files!**")


async def _clone_media_handler(cl: Client, msg: Message, cdb: Database) -> None:
    user = msg.from_user
    if user is None:
        return
    uid = user.id
    await cdb.add_user(uid, user.username or "", user.first_name or "")
    fid = _extract_file_id(msg)
    if fid is None:
        return
    mtype = _media_type(msg)
    if msg.media_group_id:
        key = f"c:{uid}:{msg.media_group_id}"
        entry = _album_cache.get(key)
        if entry is None:
            entry = {"file_ids": [], "task": None}
            _album_cache[key] = entry
        entry["file_ids"].append(fid)
        if entry["task"] is not None and not entry["task"].done():
            entry["task"].cancel()
        async def _flush(k: str = key, c=cl, m=msg, d=cdb, t=mtype):
            await asyncio.sleep(2)
            cached = _album_cache.pop(k, None)
            if cached and cached["file_ids"]:
                await _clone_persist_and_reply(c, m, d, cached["file_ids"], t)
        entry["task"] = asyncio.create_task(_flush())
        return
    await _clone_persist_and_reply(cl, msg, cdb, [fid], mtype)


async def _clone_persist_and_reply(cl: Client, msg: Message, cdb: Database, file_ids: List[str], media_type: str) -> None:
    try:
        bid = await cdb.create_batch(msg.from_user.id, file_ids, media_type)
    except Exception:
        await msg.reply_text("❌ Failed to create batch.")
        return
    me = await cl.get_me()
    link = f"https://t.me/{me.username}?start={bid}"
    text = f"✅ **Batch Created!**\n\n📁 **Files:** {len(file_ids)}\n🔗 **Link:**\n`{link}`\n\n⏰ Auto-delete after 8 hours."
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Share Link", url=f"https://t.me/share/url?url={link}")]])
    await msg.reply_text(text, reply_markup=kb, disable_web_page_preview=True)


# ---------------------------------------------------------------------------
#  CALLBACK QUERY HANDLER
# ---------------------------------------------------------------------------

@app.on_callback_query()
async def callback_handler(client: Client, cb: CallbackQuery) -> None:
    data = cb.data
    uid = cb.from_user.id

    LOGGER.info(f"Callback: data={data} from user={uid}")

    # ==================== PUBLIC BUTTONS ====================

    if data == "search_files":
        await cb.answer("🔍 Search feature coming soon!", show_alert=True)
        return

    if data == "settings":
        await cb.answer()
        text = (
            "⚙️ **Settings**\n\n"
            f"🛡️ **Protect Content:** ✅ Enabled\n"
            f"⏰ **Auto-Delete:** {AUTO_DELETE_SECS // 3600} Hours\n"
            "📢 **Notifications:** Enabled\n\n"
            "Configured by bot admin via environment variables."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
        await cb.message.edit_text(text, reply_markup=kb)
        return

    if data == "create_clone":
        await cb.answer()
        text = (
            "🤖 **Create a Clone Bot**\n\n"
            "Use the command:\n\n"
            "`/add_clone <BOT_TOKEN> <MONGODB_URI>`\n\n"
            "**Steps:**\n"
            "1. Create a bot from @BotFather\n"
            "2. Get a MongoDB URI\n"
            "3. Run the command\n\n"
            "Each clone uses its own separate database."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
        if uid in ADMIN_IDS:
            await cb.message.edit_text(text, reply_markup=kb)
        else:
            await cb.answer("❌ Admins only!", show_alert=True)
        return

    if data == "my_stats":
        await cb.answer("📊 Fetching…")
        u = await db.user_count()
        b = await db.batch_count()
        text = (
            f"📊 **Your Stats**\n\n"
            f"👤 **User ID:** `{uid}`\n"
            f"👥 **Total Users:** {u}\n"
            f"📁 **Total Batches:** {b}\n"
            f"⏰ **Auto-Delete:** 8 Hours\n\n"
            "Send any file to create a batch link!"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
        await cb.message.edit_text(text, reply_markup=kb)
        return

    if data == "back_start":
        await cb.answer()
        uid_b = cb.from_user.id
        total_users = await db.user_count()
        stored_files = await db.total_stored_files()
        text = (
            "👋 **Namaste File!**\n\n"
            "Main Telegram File Store Bot hoon. Aap yahan files store, batch links generate, "
            "aur dynamic URLs share kar sakte hain.\n\n"
            "📊 **Global Stats:**\n"
            f"👥 Total Users: {total_users}\n"
            f"📁 Stored Files: {stored_files}\n"
            "⏰ Auto-Delete Window: 8 Hours Active"
        )
        buttons = [
            [InlineKeyboardButton("🔍 Search Files", callback_data="search_files"), InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
            [InlineKeyboardButton("🤖 Create Clone", callback_data="create_clone"), InlineKeyboardButton("📊 My Stats", callback_data="my_stats")],
        ]
        if uid_b in ADMIN_IDS:
            buttons.append([InlineKeyboardButton("🛡️ Admin Panel", callback_data="admin_panel")])
        kb = InlineKeyboardMarkup(buttons)
        await cb.message.edit_text(text, reply_markup=kb)
        return

    if data == "refresh_sub":
        await cb.answer("🔄 Checking…")
        joined, failed_ch = await _is_joined_all(client, uid)
        if joined:
            await cb.message.edit_text("✅ **Access Granted!** You are subscribed to all required channels.\n\nSend /start to continue.")
        else:
            await _send_fsub_prompt(client, cb.message, failed_ch)
        return

    # ==================== ADMIN-ONLY ====================

    if uid not in ADMIN_IDS:
        await cb.answer("⛔ Access Denied!", show_alert=True)
        return

    if data == "admin_panel":
        await cb.answer()
        text = "🛡️ **Admin Control Panel**\n\nSelect an option:"
        buttons = [
            [InlineKeyboardButton("📊 System Stats", callback_data="sys_stats"), InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
            [InlineKeyboardButton("👥 Manage Users", callback_data="manage_users"), InlineKeyboardButton("⚙️ Dynamic Settings", callback_data="dyn_settings")],
            [InlineKeyboardButton("🔄 Restart Bot", callback_data="restart_bot"), InlineKeyboardButton("❌ Close Panel", callback_data="close_panel")],
        ]
        kb = InlineKeyboardMarkup(buttons)
        await cb.message.edit_text(text, reply_markup=kb)
        return

    if data == "sys_stats":
        await cb.answer("📊 Loading…")
        uc = await db.user_count()
        bc = await db.batch_count()
        fc = await db.total_stored_files()
        cc = len(_running_clones)
        fsub_cnt = await db.fsub_channel_count()
        # psutil optional
        try:
            import psutil
            cpu = psutil.cpu_percent(interval=0.3)
            mem = psutil.virtual_memory()
            cpu_line = f"🖥️ **CPU:** {cpu}%"
            mem_line = f"💾 **RAM:** {mem.percent}% ({mem.used // (1024**2)}MB / {mem.total // (1024**2)}MB)"
        except ImportError:
            cpu_line = "🖥️ **CPU:** N/A (psutil not installed)"
            mem_line = "💾 **RAM:** N/A"
        text = (
            "📊 **System Statistics**\n\n"
            f"{cpu_line}\n"
            f"{mem_line}\n"
            f"👥 **Users:** {uc}\n"
            f"📁 **Batches:** {bc}\n"
            f"🗄️ **Stored Files:** {fc}\n"
            f"🤖 **Active Clones:** {cc}\n"
            f"📢 **FSUB Channels:** {fsub_cnt}\n"
            f"⏰ **Auto-Delete:** 8 Hours\n"
            f"🛡️ **Content Protection:** Enabled"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]])
        await cb.message.edit_text(text, reply_markup=kb)
        return

    if data == "broadcast":
        await cb.answer()
        text = (
            "📢 **Broadcast**\n\n"
            "Reply to this message with the content you want to broadcast to all users.\n"
            "Text, media, or any message type works.\n\n"
            "Send **/cancel** to cancel the broadcast."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]])
        prompt = await cb.message.edit_text(text, reply_markup=kb)

        # Store broadcast state
        _broadcast_state[uid] = {"prompt_id": prompt.id, "client": client}

    if data == "manage_users":
        await cb.answer()
        uc = await db.user_count()
        text = f"👥 **User Management**\n\n**Total Users:** {uc}\n\nUse **📢 Broadcast** to message all users."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]])
        await cb.message.edit_text(text, reply_markup=kb)
        return

    if data == "dyn_settings":
        await cb.answer()
        channels = await db.get_all_fsub_channels()
        lines = ["⚙️ **Dynamic Settings**\n"]
        lines.append(f"⏰ **Auto-Delete:** {AUTO_DELETE_SECS // 3600} Hours")
        lines.append(f"🛡️ **Protect Content:** ✅ Enabled\n")
        lines.append(f"📢 **Active FSUB Channels ({len(channels)}):**\n")
        if channels:
            for i, ch in enumerate(channels, 1):
                cid = ch.get("channel_id", "?")
                title = ch.get("title", cid)
                lines.append(f"{i}. **{title}** (`{cid}`)")
        else:
            lines.append("_(No channels configured)_")
        text = "\n".join(lines)

        buttons = []
        if channels:
            del_row = []
            for ch in channels:
                cid = ch.get("channel_id", "")
                del_row.append(InlineKeyboardButton(f"❌ {cid[:8]}…", callback_data=f"delfsub:{cid}"))
            buttons.append(del_row)
        buttons.append([
            InlineKeyboardButton("➕ Add via /add_fsub", callback_data="addfsub_prompt"),
            InlineKeyboardButton("🔙 Back", callback_data="admin_panel"),
        ])
        kb = InlineKeyboardMarkup(buttons)
        await cb.message.edit_text(text, reply_markup=kb)
        return

    if data == "addfsub_prompt":
        await cb.answer()
        text = "➕ **Add FSUB Channel**\n\nUse the command:\n`/add_fsub @channel` or `/add_fsub -1001234567890`\n\nMake sure the bot is an **admin** in the channel."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="dyn_settings")]])
        await cb.message.edit_text(text, reply_markup=kb)
        return

    if data.startswith("delfsub:"):
        cid = data[8:]
        removed = await db.remove_fsub_channel(cid)
        if removed:
            await cb.answer(f"✅ Removed `{cid}`", show_alert=False)
        else:
            await cb.answer(f"❌ Failed to remove", show_alert=True)
        # Refresh dyn_settings
        channels = await db.get_all_fsub_channels()
        lines = ["⚙️ **Dynamic Settings**\n"]
        lines.append(f"⏰ **Auto-Delete:** {AUTO_DELETE_SECS // 3600} Hours")
        lines.append(f"🛡️ **Protect Content:** ✅ Enabled\n")
        lines.append(f"📢 **Active FSUB Channels ({len(channels)}):**\n")
        if channels:
            for i, ch in enumerate(channels, 1):
                cid_ = ch.get("channel_id", "?")
                title = ch.get("title", cid_)
                lines.append(f"{i}. **{title}** (`{cid_}`)")
        else:
            lines.append("_(No channels configured)_")
        text = "\n".join(lines)
        buttons = []
        if channels:
            del_row = []
            for ch in channels:
                cid_ = ch.get("channel_id", "")
                del_row.append(InlineKeyboardButton(f"❌ {cid_[:8]}…", callback_data=f"delfsub:{cid_}"))
            buttons.append(del_row)
        buttons.append([
            InlineKeyboardButton("➕ Add via /add_fsub", callback_data="addfsub_prompt"),
            InlineKeyboardButton("🔙 Back", callback_data="admin_panel"),
        ])
        kb = InlineKeyboardMarkup(buttons)
        try:
            await cb.message.edit_text(text, reply_markup=kb)
        except Exception:
            pass
        return

    if data == "restart_bot":
        await cb.answer()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, Restart", callback_data="confirm_restart")],
            [InlineKeyboardButton("❌ No", callback_data="admin_panel")],
        ])
        await cb.message.edit_text("⚠️ **Restart Bot?**\n\nThis will stop the main bot + all clones and restart.", reply_markup=kb)
        return

    if data == "confirm_restart":
        await cb.answer("🔄 Restarting…")
        await cb.message.edit_text("🔄 **Bot is restarting…**")
        asyncio.create_task(_restart())
        return

    if data == "close_panel":
        await cb.answer("Panel closed.")
        try:
            await cb.message.delete()
        except Exception:
            pass
        return

    await cb.answer("⏳ Processing…")


# ---------------------------------------------------------------------------
#  BROADCAST STATE
# ---------------------------------------------------------------------------

_broadcast_state: Dict[int, dict] = {}


@app.on_message(filters.private & filters.text & filters.regex(r"^(?!/(cancel|start))") & filters.user(ADMIN_IDS))
async def broadcast_reply_handler(client: Client, message: Message) -> None:
    uid = message.from_user.id
    state = _broadcast_state.get(uid)
    if state is None:
        return
    if not message.reply_to_message or message.reply_to_message.id != state.get("prompt_id"):
        return

    del _broadcast_state[uid]
    LOGGER.info(f"Broadcast started by admin {uid}")

    status_msg = await message.reply_text("🔄 **Broadcasting…**")
    users = await db.all_users()
    ok = fail = 0
    for u in users:
        uid_ = u.get("user_id")
        if not uid_:
            continue
        try:
            if message.text and not message.media:
                await client.send_message(uid_, message.text)
            elif message.media:
                await message.copy(uid_)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.04)

    await status_msg.edit_text(f"✅ **Broadcast Complete**\n📨 Sent: {ok}\n❌ Failed: {fail}")


@app.on_message(filters.command("cancel") & filters.private & filters.user(ADMIN_IDS))
async def cancel_broadcast(client: Client, message: Message) -> None:
    uid = message.from_user.id
    if uid in _broadcast_state:
        del _broadcast_state[uid]
        await message.reply_text("❌ **Broadcast cancelled.**")
    else:
        await message.reply_text("⚠️ No active broadcast to cancel.")


# ---------------------------------------------------------------------------
#  RESTART
# ---------------------------------------------------------------------------

async def _restart() -> None:
    await _stop_all_clones()
    if db is not None:
        await db.close()
    LOGGER.info("Restarting…")
    os.execl(sys.executable, sys.executable, *sys.argv)


# ---------------------------------------------------------------------------
#  STARTUP / SHUTDOWN
# ---------------------------------------------------------------------------

@app.on_start()
async def on_start(client: Client) -> None:
    global db
    db = Database(MONGODB_URI)
    await db.connect()

    clones = await db.all_clones()
    for c in clones:
        try:
            await _start_one_clone(c)
        except Exception as exc:
            LOGGER.error("Failed to start clone %s…: %s", c["bot_token"][:10], exc)

    await client.set_bot_commands([
        BotCommand("start", "Start the bot"),
        BotCommand("add_clone", "Add a new clone bot (Admin)"),
        BotCommand("add_fsub", "Add a force-sub channel (Admin)"),
        BotCommand("del_fsub", "Remove a force-sub channel (Admin)"),
        BotCommand("cancel", "Cancel broadcast (Admin)"),
    ])

    me = await client.get_me()
    LOGGER.info("✅ Main bot @%s started", me.username)


@app.on_stop()
async def on_stop(**_kw: Any) -> None:
    await _stop_all_clones()
    if db is not None:
        await db.close()
    LOGGER.info("Bot stopped cleanly.")


# ---------------------------------------------------------------------------
#  ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    LOGGER.info("Launching File Store Bot…")
    app.run()
