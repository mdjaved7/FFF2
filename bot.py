#!/usr/bin/env python3
"""
Telegram File Store & Multi-Clone Bot
======================================
Pyrogram v2 + Motor (Async MongoDB)

Environment Variables:
    API_ID       - Telegram API ID
    API_HASH     - Telegram API Hash
    BOT_TOKEN    - Main bot token
    MONGODB_URI  - Main MongoDB connection string
    ADMIN_IDS    - Comma-separated Telegram user IDs
"""

import os
import sys
import asyncio
import logging
import string
import random
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

from dotenv import load_dotenv
load_dotenv()

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

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGODB_URI = os.environ.get("MONGODB_URI", "")
ADMIN_IDS = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip()
]
AUTO_DELETE_SECS = 28800

_missing = []
if not API_ID or API_ID == 0:
    _missing.append("API_ID")
if not API_HASH:
    _missing.append("API_HASH")
if not BOT_TOKEN:
    _missing.append("BOT_TOKEN")
if not MONGODB_URI:
    _missing.append("MONGODB_URI")
if _missing:
    raise SystemExit(
        "❌ Missing environment variables: " + ", ".join(_missing)
    )

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
LOGGER = logging.getLogger("FileStoreBot")

# ═══════════════════════════════════════════════════════════════════════════
# DATABASE CLASS
# ═══════════════════════════════════════════════════════════════════════════

class Database:
    def __init__(self, uri: str):
        self._uri = uri
        self._client = None
        self.db = None
        self.users = None
        self.batches = None
        self.clones = None
        self.fsub_channels = None

    async def connect(self):
        self._client = AsyncIOMotorClient(self._uri, serverSelectionTimeoutMS=10000)
        await self._client.admin.command("ping")
        self.db = self._client.get_database("file_store_bot")
        self.users = self.db["users"]
        self.batches = self.db["batches"]
        self.clones = self.db["clones"]
        self.fsub_channels = self.db["fsub_channels"]
        await self.users.create_index("user_id", unique=True)
        await self.batches.create_index("batch_id", unique=True)
        await self.clones.create_index("bot_token", unique=True)
        await self.fsub_channels.create_index("channel_id", unique=True)
        LOGGER.info("MongoDB connected and indexes ready.")

    async def close(self):
        if self._client:
            self._client.close()

    @staticmethod
    async def test_uri(uri: str):
        c = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=6000)
        try:
            await c.admin.command("ping")
            return True, ""
        except Exception as e:
            return False, str(e)
        finally:
            c.close()

    # Users
    async def add_user(self, uid, username="", first_name=""):
        try:
            await self.users.update_one(
                {"user_id": uid},
                {
                    "$set": {"username": username, "first_name": first_name, "last_active": datetime.utcnow()},
                    "$setOnInsert": {"joined_at": datetime.utcnow(), "total_files": 0},
                },
                upsert=True,
            )
        except Exception as e:
            LOGGER.error("add_user(%s): %s", uid, e)

    async def user_count(self):
        try:
            return await self.users.count_documents({})
        except Exception:
            return 0

    async def all_users(self):
        try:
            return await self.users.find({}).to_list(length=None)
        except Exception:
            return []

    # Batches
    async def create_batch(self, uid, file_ids, media_type="document"):
        bid = "batch_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
        await self.batches.insert_one({
            "batch_id": bid, "user_id": uid, "file_ids": file_ids,
            "media_type": media_type, "file_count": len(file_ids),
            "created_at": datetime.utcnow(),
        })
        await self.users.update_one({"user_id": uid}, {"$inc": {"total_files": len(file_ids)}})
        return bid

    async def get_batch(self, bid):
        return await self.batches.find_one({"batch_id": bid})

    async def batch_count(self):
        try:
            return await self.batches.count_documents({})
        except Exception:
            return 0

    async def total_stored_files(self):
        try:
            cur = self.batches.aggregate([{"$group": {"_id": None, "total": {"$sum": "$file_count"}}}])
            r = await cur.to_list(length=1)
            return r[0]["total"] if r else 0
        except Exception:
            return 0

    # Clones
    async def register_clone(self, token, mongo_uri, owner_id):
        cid = "clone_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        await self.clones.insert_one({
            "clone_id": cid, "bot_token": token, "mongo_uri": mongo_uri,
            "owner_id": owner_id, "status": "active", "created_at": datetime.utcnow(),
        })
        return cid

    async def all_clones(self):
        return await self.clones.find({}).to_list(length=None)

    async def remove_clone(self, token):
        r = await self.clones.delete_one({"bot_token": token})
        return r.deleted_count > 0

    # FSUB
    async def add_fsub_channel(self, ch_id, title=""):
        if await self.fsub_channels.find_one({"channel_id": ch_id}):
            return False
        await self.fsub_channels.insert_one({"channel_id": ch_id, "title": title or ch_id, "added_at": datetime.utcnow()})
        return True

    async def remove_fsub_channel(self, ch_id):
        if ch_id == "all":
            r = await self.fsub_channels.delete_many({})
            return r.deleted_count > 0
        r = await self.fsub_channels.delete_one({"channel_id": ch_id})
        return r.deleted_count > 0

    async def get_all_fsub_channels(self):
        return await self.fsub_channels.find({}).to_list(length=None)

    async def get_fsub_channel(self, ch_id):
        return await self.fsub_channels.find_one({"channel_id": ch_id})

    async def fsub_channel_count(self):
        try:
            return await self.fsub_channels.count_documents({})
        except Exception:
            return 0


db = None

# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def extract_file_id(msg):
    for a in ("document", "video", "audio", "photo", "voice", "video_note", "sticker", "animation"):
        o = getattr(msg, a, None)
        if o:
            return o.file_id
    return None

def media_type(msg):
    if msg.document: return "document"
    if msg.video: return "video"
    if msg.audio: return "audio"
    if msg.photo: return "photo"
    if msg.voice: return "voice"
    return "document"

async def send_file_by_type(cl, chat_id, file_id, mtype):
    kw = {"chat_id": chat_id, "protect_content": True}
    try:
        if mtype == "photo":
            return await cl.send_photo(file_id=file_id, **kw)
        if mtype == "video":
            return await cl.send_video(file_id=file_id, **kw)
        if mtype == "audio":
            return await cl.send_audio(file_id=file_id, **kw)
        return await cl.send_document(file_id=file_id, **kw)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await send_file_by_type(cl, chat_id, file_id, mtype)
    except Exception as e:
        LOGGER.error("send_file: %s", e)
        return None

async def delete_after(cl, chat_id, msg_id, delay=28800):
    await asyncio.sleep(delay)
    try:
        await cl.delete_messages(chat_id, msg_id)
    except Exception:
        pass

async def resolve_channel(cl, ch_id):
    try:
        chat = await cl.get_chat(ch_id)
        link = f"https://t.me/{chat.username}" if chat.username else (chat.invite_link or str(ch_id))
        return link, chat.title or str(ch_id)
    except Exception:
        return str(ch_id), str(ch_id)


async def get_fsub_list():
    return [c["channel_id"] for c in await db.get_all_fsub_channels() if c.get("channel_id")]

async def is_joined_all(cl, uid):
    channels = await get_fsub_list()
    if not channels:
        return True, None
    for ch in channels:
        try:
            m = await cl.get_chat_member(ch, uid)
            if m.status in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED):
                return False, ch
        except UserNotParticipant:
            return False, ch
        except (ChatAdminRequired, BadRequest, PeerIdInvalid, ChannelInvalid):
            continue
        except Exception:
            continue
    return True, None

async def fsub_prompt(cl, msg, failed_ch=None):
    channels = await get_fsub_list()
    lines = ["⚠️ **Access Denied!**\n", "Join these channel(s) first:\n"]
    for ch in channels:
        link, title = await resolve_channel(cl, ch)
        m = " 👈" if ch == failed_ch else ""
        lines.append(f"📢 **{title}** – [Join]({link}){m}")
    lines.append("\nThen click below.")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 I've Joined", callback_data="refresh_sub")]])
    await msg.reply_text("\n".join(lines), reply_markup=kb, disable_web_page_preview=True)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN CLIENT
# ═══════════════════════════════════════════════════════════════════════════

app = Client(
    name="file_store_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="./sessions",
    in_memory=True,
)

_album_cache = {}
_broadcast_state = {}

# ═══════════════════════════════════════════════════════════════════════════
# /start
# ═══════════════════════════════════════════════════════════════════════════

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(cl, msg):
    u = msg.from_user
    if not u:
        return
    await db.add_user(u.id, u.username or "", u.first_name or "")

    if len(msg.command) > 1 and msg.command[1].startswith("batch_"):
        await deliver_batch(cl, msg, msg.command[1])
        return

    joined, fc = await is_joined_all(cl, u.id)
    if not joined:
        await fsub_prompt(cl, msg, fc)
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

    btns = [
        [InlineKeyboardButton("🔍 Search Files", callback_data="search_files"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        [InlineKeyboardButton("🤖 Create Clone", callback_data="create_clone"),
         InlineKeyboardButton("📊 My Stats", callback_data="my_stats")],
    ]
    if u.id in ADMIN_IDS:
        btns.append([InlineKeyboardButton("🛡️ Admin Panel", callback_data="admin_panel")])

    await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(btns))

# ═══════════════════════════════════════════════════════════════════════════
# BATCH DELIVERY
# ═══════════════════════════════════════════════════════════════════════════

async def deliver_batch(cl, msg, bid):
    batch = await db.get_batch(bid)
    if not batch:
        await msg.reply_text("❌ Batch not found.")
        return
    fids = batch.get("file_ids", [])
    mt = batch.get("media_type", "document")
    if not fids:
        await msg.reply_text("❌ No files in batch.")
        return

    info = await msg.reply_text(f"📁 **Sending {len(fids)} file(s)...**\n\n⚠️ Auto-delete after 8 hours.")
    ok = 0
    for fid in fids:
        s = await send_file_by_type(cl, msg.chat.id, fid, mt)
        if s:
            ok += 1
            asyncio.create_task(delete_after(cl, s.chat.id, s.id))
        await asyncio.sleep(0.3)
    await info.edit_text(f"✅ **Sent {ok}/{len(fids)} files!**\n\n⏰ Auto-delete in 8 hours.")

# ═══════════════════════════════════════════════════════════════════════════
# MEDIA HANDLER
# ═══════════════════════════════════════════════════════════════════════════

@app.on_message(filters.private & filters.media & ~filters.command("start"))
async def media_handler(cl, msg):
    u = msg.from_user
    if not u:
        return
    joined, fc = await is_joined_all(cl, u.id)
    if not joined:
        await fsub_prompt(cl, msg, fc)
        return
    await db.add_user(u.id, u.username or "", u.first_name or "")

    fid = extract_file_id(msg)
    if not fid:
        await msg.reply_text("❌ Unsupported media.")
        return
    mt = media_type(msg)

    if msg.media_group_id:
        key = f"{u.id}:{msg.media_group_id}"
        e = _album_cache.get(key)
        if not e:
            e = {"file_ids": [], "task": None}
            _album_cache[key] = e
        e["file_ids"].append(fid)
        if e["task"] and not e["task"].done():
            e["task"].cancel()

        async def flush(k=key):
            await asyncio.sleep(2)
            c = _album_cache.pop(k, None)
            if c and c["file_ids"]:
                await persist_reply(cl, msg, c["file_ids"], mt)
        e["task"] = asyncio.create_task(flush())
        return

    await persist_reply(cl, msg, [fid], mt)

async def persist_reply(cl, msg, fids, mt):
    try:
        bid = await db.create_batch(msg.from_user.id, fids, mt)
    except Exception as e:
        LOGGER.error("persist_reply: %s", e)
        await msg.reply_text("❌ Failed to create batch.")
        return
    me = await cl.get_me()
    link = f"https://t.me/{me.username}?start={bid}"
    text = f"✅ **Batch Created!**\n\n📁 Files: {len(fids)}\n🔗 `{link}`\n\n⏰ Auto-delete after 8 hours."
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Share", url=f"https://t.me/share/url?url={link}")]])
    await msg.reply_text(text, reply_markup=kb, disable_web_page_preview=True)

# ═══════════════════════════════════════════════════════════════════════════
# /add_fsub  /del_fsub  /add_clone
# ═══════════════════════════════════════════════════════════════════════════

@app.on_message(filters.command("add_fsub") & filters.private)
async def add_fsub_cmd(cl, msg):
    if msg.from_user.id not in ADMIN_IDS:
        await msg.reply_text("❌ Access Denied!")
        return
    p = msg.text.strip().split(maxsplit=1)
    if len(p) < 2:
        await msg.reply_text("Usage: `/add_fsub @channel`")
        return
    try:
        chat = await cl.get_chat(p[1].strip())
        cid = f"@{chat.username}" if chat.username else str(chat.id)
        title = chat.title or cid
    except Exception as e:
        await msg.reply_text(f"❌ Invalid channel: {e}")
        return
    ok = await db.add_fsub_channel(cid, title)
    await msg.reply_text(f"✅ **Added** {title} (`{cid}`)" if ok else f"⚠️ Already exists.")

@app.on_message(filters.command("del_fsub") & filters.private)
async def del_fsub_cmd(cl, msg):
    if msg.from_user.id not in ADMIN_IDS:
        await msg.reply_text("❌ Access Denied!")
        return
    p = msg.text.strip().split(maxsplit=1)
    if len(p) < 2:
        await msg.reply_text("Usage: `/del_fsub @channel` or `/del_fsub all`")
        return
    t = p[1].strip()
    if t == "all":
        r = await db.remove_fsub_channel("all")
        await msg.reply_text("✅ All removed." if r else "⚠️ None to remove.")
        return
    r = await db.remove_fsub_channel(t)
    await msg.reply_text(f"✅ Removed `{t}`." if r else "❌ Not found.")

@app.on_message(filters.command("add_clone") & filters.private)
async def add_clone_cmd(cl, msg):
    if msg.from_user.id not in ADMIN_IDS:
        await msg.reply_text("❌ Access Denied!")
        return
    p = msg.text.strip().split(maxsplit=2)
    if len(p) < 3:
        await msg.reply_text("Usage: `/add_clone <BOT_TOKEN> <MONGODB_URI>`")
        return
    _, token, uri = p
    st = await msg.reply_text("🔄 Testing MongoDB…")
    ok, err = await Database.test_uri(uri)
    if not ok:
        await st.edit_text(f"❌ MongoDB: {err}")
        return
    await st.edit_text("✅ MongoDB OK. Testing bot token…")
    try:
        tmp = Client(name=f"_tmp_{token[:8]}", api_id=API_ID, api_hash=API_HASH, bot_token=token, in_memory=True)
        await tmp.start()
        me = await tmp.get_me()
        await tmp.stop()
    except Exception as e:
        await st.edit_text(f"❌ Invalid token: {e}")
        return
    try:
        cid = await db.register_clone(token, uri, msg.from_user.id)
    except Exception as e:
        await st.edit_text(f"❌ Registration failed: {e}")
        return
    try:
        await start_one_clone({"bot_token": token, "mongo_uri": uri})
    except Exception as e:
        await db.remove_clone(token)
        await st.edit_text(f"❌ Clone start failed, rollback done: {e}")
        return
    await st.edit_text(f"✅ Clone @{me.username} created & running! ID: `{cid}`")

# ═══════════════════════════════════════════════════════════════════════════
# CLONE SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

_running_clones = {}

async def start_one_clone(cfg):
    token = cfg["bot_token"]
    uri = cfg["mongo_uri"]
    tag = token[:20]
    if tag in _running_clones:
        return
    cdb = Database(uri)
    await cdb.connect()
    c = Client(name=f"clone_{tag}", api_id=API_ID, api_hash=API_HASH, bot_token=token, workdir="./sessions", in_memory=True)
    c._cdb = cdb

    async def h_start(cl2, msg):
        u = msg.from_user
        if not u: return
        await cdb.add_user(u.id, u.username or "", u.first_name or "")
        if len(msg.command) > 1 and msg.command[1].startswith("batch_"):
            b = await cdb.get_batch(msg.command[1])
            if b:
                fids = b.get("file_ids", [])
                mt = b.get("media_type", "document")
                info = await msg.reply_text(f"📁 Sending {len(fids)} file(s)...")
                ok = 0
                for fid in fids:
                    s = await send_file_by_type(cl2, msg.chat.id, fid, mt)
                    if s:
                        ok += 1
                        asyncio.create_task(delete_after(cl2, s.chat.id, s.id))
                    await asyncio.sleep(0.3)
                await info.edit_text(f"✅ Sent {ok}/{len(fids)} files!")
            else:
                await msg.reply_text("❌ Batch not found.")
            return
        ucnt = await cdb.user_count()
        fcnt = await cdb.total_stored_files()
        txt = (
            "👋 **Namaste File!**\n\nMain Telegram File Store Bot hoon.\n\n"
            f"📊 Global Stats:\n👥 Users: {ucnt}\n📁 Files: {fcnt}\n⏰ Auto-Delete: 8 Hours"
        )
        btns = [[InlineKeyboardButton("🔍 Search", callback_data="search_files"),
                 InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
                [InlineKeyboardButton("🤖 Create Clone", callback_data="create_clone"),
                 InlineKeyboardButton("📊 My Stats", callback_data="my_stats")]]
        await msg.reply_text(txt, reply_markup=InlineKeyboardMarkup(btns))

    async def h_media(cl2, msg):
        u = msg.from_user
        if not u: return
        await cdb.add_user(u.id, u.username or "", u.first_name or "")
        fid = extract_file_id(msg)
        if not fid: return
        mt = media_type(msg)
        if msg.media_group_id:
            key = f"c:{u.id}:{msg.media_group_id}"
            e = _album_cache.get(key)
            if not e:
                e = {"file_ids": [], "task": None}
                _album_cache[key] = e
            e["file_ids"].append(fid)
            if e["task"] and not e["task"].done():
                e["task"].cancel()
            async def flush2(k=key):
                await asyncio.sleep(2)
                c2 = _album_cache.pop(k, None)
                if c2 and c2["file_ids"]:
                    try:
                        bid2 = await cdb.create_batch(u.id, c2["file_ids"], mt)
                        me2 = await cl2.get_me()
                        link2 = f"https://t.me/{me2.username}?start={bid2}"
                        await msg.reply_text(f"✅ Batch Created!\n🔗 `{link2}`")
                    except Exception:
                        await msg.reply_text("❌ Failed.")
            e["task"] = asyncio.create_task(flush2())
            return
        try:
            bid = await cdb.create_batch(u.id, [fid], mt)
            me = await cl2.get_me()
            link = f"https://t.me/{me.username}?start={bid}"
            await msg.reply_text(f"✅ Batch Created!\n🔗 `{link}`")
        except Exception:
            await msg.reply_text("❌ Failed.")

    c.add_handler(MessageHandler(h_start, filters.command("start")))
    c.add_handler(MessageHandler(h_media, filters.private & filters.media))
    await c.start()
    _running_clones[tag] = c
    LOGGER.info("Clone @%s started", (await c.get_me()).username)

async def stop_all_clones():
    for tag, c in list(_running_clones.items()):
        try:
            if hasattr(c, "_cdb") and c._cdb:
                await c._cdb.close()
            await c.stop()
        except Exception:
            pass
    _running_clones.clear()

# ═══════════════════════════════════════════════════════════════════════════
# CALLBACK QUERY HANDLER
# ═══════════════════════════════════════════════════════════════════════════

@app.on_callback_query()
async def cb_handler(cl, cb):
    d = cb.data
    uid = cb.from_user.id

    if d == "search_files":
        await cb.answer("🔍 Coming soon!", show_alert=True)

    elif d == "settings":
        await cb.answer()
        t = (
            "⚙️ **Settings**\n\n"
            f"🛡️ Protect Content: ✅ Enabled\n"
            f"⏰ Auto-Delete: {AUTO_DELETE_SECS // 3600}h\n"
            "Configured via env variables."
        )
        await cb.message.edit_text(t, reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Back", callback_data="back_start")]]))

    elif d == "create_clone":
        await cb.answer()
        t = (
            "🤖 **Create Clone**\n\n"
            "`/add_clone <BOT_TOKEN> <MONGODB_URI>`\n\n"
            "Each clone has its own database."
        )
        await cb.message.edit_text(t, reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Back", callback_data="back_start")]]))

    elif d == "my_stats":
        await cb.answer("📊 Fetching…")
        uc = await db.user_count()
        bc = await db.batch_count()
        t = f"📊 **My Stats**\n\n👤 ID: `{uid}`\n👥 Total Users: {uc}\n📁 Batches: {bc}\n⏰ Auto-Delete: 8h"
        await cb.message.edit_text(t, reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Back", callback_data="back_start")]]))

    elif d == "back_start":
        await cb.answer()
        u = cb.from_user
        tu = await db.user_count()
        sf = await db.total_stored_files()
        txt = (
            "👋 **Namaste File!**\n\n Main Telegram File Store Bot hoon.\n\n"
            f"📊 Global Stats:\n👥 Users: {tu}\n📁 Files: {sf}\n⏰ Auto-Delete: 8 Hours"
        )
        btns = [[InlineKeyboardButton("🔍 Search", callback_data="search_files"),
                 InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
                [InlineKeyboardButton("🤖 Create Clone", callback_data="create_clone"),
                 InlineKeyboardButton("📊 My Stats", callback_data="my_stats")]]
        if u.id in ADMIN_IDS:
            btns.append([InlineKeyboardButton("🛡️ Admin Panel", callback_data="admin_panel")])
        await cb.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))

    elif d == "refresh_sub":
        await cb.answer("🔄 Checking…")
        joined, fc = await is_joined_all(cl, uid)
        if joined:
            await cb.message.edit_text("✅ **Access Granted!** Send /start")
        else:
            await fsub_prompt(cl, cb.message, fc)

    # Admin
    elif uid not in ADMIN_IDS:
        await cb.answer("⛔ Access Denied!", show_alert=True)

    elif d == "admin_panel":
        await cb.answer()
        t = "🛡️ **Admin Panel**"
        btns = [
            [InlineKeyboardButton("📊 System Stats", callback_data="sys_stats"),
             InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
            [InlineKeyboardButton("👥 Manage Users", callback_data="manage_users"),
             InlineKeyboardButton("⚙️ Dynamic Settings", callback_data="dyn_settings")],
            [InlineKeyboardButton("🔄 Restart", callback_data="restart_bot"),
             InlineKeyboardButton("❌ Close", callback_data="close_panel")],
        ]
        await cb.message.edit_text(t, reply_markup=InlineKeyboardMarkup(btns))

    elif d == "sys_stats":
        await cb.answer("📊 Loading…")
        uc = await db.user_count()
        bc = await db.batch_count()
        fc = await db.total_stored_files()
        cc = len(_running_clones)
        fsc = await db.fsub_channel_count()
        try:
            import psutil
            cpu = psutil.cpu_percent(interval=0.3)
            mem = psutil.virtual_memory()
            cpu_line = f"🖥️ CPU: {cpu}%"
            mem_line = f"💾 RAM: {mem.percent}%"
        except ImportError:
            cpu_line = "🖥️ CPU: N/A"
            mem_line = "💾 RAM: N/A"
        t = (
            "📊 **System Stats**\n\n"
            f"{cpu_line}\n{mem_line}\n"
            f"👥 Users: {uc}\n📁 Batches: {bc}\n🗄️ Files: {fc}\n"
            f"🤖 Clones: {cc}\n📢 FSUB: {fsc}\n⏰ Auto-Delete: 8h"
        )
        await cb.message.edit_text(t, reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))

    elif d == "broadcast":
        await cb.answer()
        t = "📢 **Broadcast**\n\nReply to this message with content to broadcast.\nSend `/cancel` to cancel."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]])
        prompt = await cb.message.edit_text(t, reply_markup=kb)
        _broadcast_state[uid] = {"prompt_id": prompt.id}

    elif d == "manage_users":
        await cb.answer()
        uc = await db.user_count()
        await cb.message.edit_text(f"👥 **Users:** {uc}\n\nUse Broadcast to message all.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))

    elif d == "dyn_settings":
        await cb.answer()
        chs = await db.get_all_fsub_channels()
        lines = [f"⚙️ **Dynamic Settings**\n\n⏰ Auto-Delete: {AUTO_DELETE_SECS // 3600}h\n"]
        lines.append(f"📢 **FSUB Channels ({len(chs)}):**\n")
        for ch in chs:
            lines.append(f"• **{ch.get('title','?')}** (`{ch.get('channel_id','')}`)")
        if not chs:
            lines.append("_(None)_")
        btns = []
        if chs:
            row = []
            for ch in chs:
                cid = ch.get("channel_id", "")
                row.append(InlineKeyboardButton(f"❌ Del", callback_data=f"delfsub:{cid}"))
            btns.append(row)
        btns.append([InlineKeyboardButton("➕ Add via /add_fsub", callback_data="noop"),
                     InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])
        await cb.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(btns))

    elif d.startswith("delfsub:"):
        cid = d[8:]
        r = await db.remove_fsub_channel(cid)
        await cb.answer("✅ Removed." if r else "❌ Failed.")
        chs = await db.get_all_fsub_channels()
        lines = [f"⚙️ **Dynamic Settings**\n\n⏰ Auto-Delete: {AUTO_DELETE_SECS // 3600}h\n"]
        lines.append(f"📢 **FSUB Channels ({len(chs)}):**\n")
        for ch in chs:
            lines.append(f"• **{ch.get('title','?')}** (`{ch.get('channel_id','')}`)")
        if not chs:
            lines.append("_(None)_")
        btns = []
        if chs:
            row = []
            for ch in chs:
                cid2 = ch.get("channel_id", "")
                row.append(InlineKeyboardButton(f"❌ Del", callback_data=f"delfsub:{cid2}"))
            btns.append(row)
        btns.append([InlineKeyboardButton("➕ Add via /add_fsub", callback_data="noop"),
                     InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])
        try:
            await cb.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(btns))
        except Exception:
            pass

    elif d == "restart_bot":
        await cb.answer()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes", callback_data="confirm_restart")],
            [InlineKeyboardButton("❌ No", callback_data="admin_panel")],
        ])
        await cb.message.edit_text("⚠️ **Restart?**", reply_markup=kb)

    elif d == "confirm_restart":
        await cb.answer("🔄 Restarting…")
        await cb.message.edit_text("🔄 Restarting…")
        asyncio.create_task(restart_bot())

    elif d == "close_panel":
        await cb.answer("Closed.")
        try:
            await cb.message.delete()
        except Exception:
            pass

    elif d == "noop":
        await cb.answer()

# ═══════════════════════════════════════════════════════════════════════════
# BROADCAST REPLY HANDLER
# ═══════════════════════════════════════════════════════════════════════════

@app.on_message(filters.private & filters.text & filters.user(ADMIN_IDS))
async def broadcast_handler(cl, msg):
    uid = msg.from_user.id
    state = _broadcast_state.get(uid)
    if not state:
        return
    if msg.text.startswith("/cancel"):
        del _broadcast_state[uid]
        await msg.reply_text("❌ Broadcast cancelled.")
        return
    if not msg.reply_to_message or msg.reply_to_message.id != state.get("prompt_id"):
        return
    del _broadcast_state[uid]
    st = await msg.reply_text("🔄 Broadcasting…")
    users = await db.all_users()
    ok = fail = 0
    for u in users:
        uid2 = u.get("user_id")
        if not uid2:
            continue
        try:
            if msg.media:
                await msg.copy(uid2)
            else:
                await cl.send_message(uid2, msg.text)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.04)
    await st.edit_text(f"✅ Done! Sent: {ok} | Failed: {fail}")

@app.on_message(filters.command("cancel") & filters.private & filters.user(ADMIN_IDS))
async def cancel_broadcast(cl, msg):
    if msg.from_user.id in _broadcast_state:
        del _broadcast_state[msg.from_user.id]
        await msg.reply_text("❌ Cancelled.")
    else:
        await msg.reply_text("⚠️ No active broadcast.")

# ═══════════════════════════════════════════════════════════════════════════
# RESTART
# ═══════════════════════════════════════════════════════════════════════════

async def restart_bot():

await stop_all_clones()

if db:

await db.close()

os.execl(sys.executable, sys.executable, *sys.argv)

# ═══════════════════════════════════════════════════════════════════════════
# STARTUP / SHUTDOWN
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# STARTUP LOGIC (no decorators — called directly in main)
# ═══════════════════════════════════════════════════════════════════════════

async def startup():
    global db
    db = Database(MONGODB_URI)
    await db.connect()
    for c in await db.all_clones():
        try:
            await start_one_clone(c)
        except Exception as e:
            LOGGER.error("Clone start failed: %s", e)
    await app.set_bot_commands([
        BotCommand("start", "Start the bot"),
        BotCommand("add_clone", "Add clone (Admin)"),
        BotCommand("add_fsub", "Add FSUB channel (Admin)"),
        BotCommand("del_fsub", "Remove FSUB channel (Admin)"),
        BotCommand("cancel", "Cancel broadcast (Admin)"),
    ])
    me = await app.get_me()
    LOGGER.info("✅ Bot @%s started", me.username)


async def shutdown():
    await stop_all_clones()
    if db:
        await db.close()
    LOGGER.info("Bot stopped cleanly.")


# ═══════════════════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    LOGGER.info("Launching File Store Bot…")

    async def main():
        await app.start()
        await startup()
        LOGGER.info("Bot is running. Press Ctrl+C to stop.")
        
        # Keep running until interrupted
        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await shutdown()
            await app.stop()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
