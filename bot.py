import os
import time
import asyncio
import logging
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import secrets

from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
from motor.motor_asyncio import AsyncIOMotorClient

# ═══════════════════════════════════════════════════════════════════════════════
# 1. CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("FileStoreEngine")

class Config:
    API_ID: int = int(os.getenv("API_ID", "123456"))          # Apna Telegram API ID dalein
    API_HASH: str = os.getenv("API_HASH", "YOUR_API_HASH")    # Apna API Hash dalein
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")  # Main Bot Token
    BOT_USERNAME: str = os.getenv("BOT_USERNAME", "YourBotUsername")
    
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_CHANNEL_ID: int = int(os.getenv("DB_CHANNEL_ID", "-1001234567890")) # Supergroup / Channel ID (-100 se shuru honi chahiye)
    
    OWNER_ID: int = int(os.getenv("OWNER_ID", "123456789"))
    ADMIN_IDS: List[int] = [OWNER_ID]
    
    FORCE_SUB_CHANNELS: List[str] = [] # e.g. ["https://t.me/YourChannelUsername"]
    BATCH_SEND_DELAY: float = 1.0       # Delay between sending files to avoid flood limit
    DELIVERY_WORKERS: int = 5

    @classmethod
    def validate(cls):
        if not cls.API_ID or not cls.BOT_TOKEN:
            raise ValueError("API_ID aur BOT_TOKEN config me set hona zaroori hai!")

config = Config()

# ═══════════════════════════════════════════════════════════════════════════════
# 2. DATABASE MANAGEMENT ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class Database:
    def __init__(self):
        self._clients: Dict[str, AsyncIOMotorClient] = {}

    async def connect(self, clone_id: str = "main", uri: Optional[str] = None):
        target_uri = uri or config.MONGO_URI
        if clone_id not in self._clients:
            client = AsyncIOMotorClient(target_uri)
            self._clients[clone_id] = client
            log.info(f"Database connected for clone: {clone_id}")

    def get_db(self, clone_id: str = "main", db_name: Optional[str] = None):
        client = self._clients.get(clone_id) or self._clients.get("main")
        name = db_name or f"filestore_{clone_id}"
        return client[name]

    async def col(self, collection_name: str, clone_id: str = "main", db_name: Optional[str] = None):
        db = self.get_db(clone_id, db_name)
        return db[collection_name]

database = Database()

class Repository:
    async def ensure_indexes(self, db_name: str = "main"):
        batches = await database.col("batches", db_name=db_name)
        await batches.create_index("batch_id", unique=True)
        files = await database.col("files", db_name=db_name)
        await files.create_index("file_id", unique=True)

    async def get_db_name_for_clone(self, clone_id: str) -> str:
        return f"filestore_{clone_id}"

    async def store_file(self, data: dict, db_name: str):
        col = await database.col("files", db_name=db_name)
        await col.insert_one(data)

    async def get_file_by_id(self, file_id: str, clone_id: str, db_name: str):
        col = await database.col("files", db_name=db_name)
        return await col.find_one({"file_id": file_id, "deleted": False})

    async def create_batch(self, data: dict, db_name: str):
        col = await database.col("batches", db_name=db_name)
        await col.insert_one(data)

    async def get_batch_by_id(self, batch_id: str, clone_id: str, db_name: str):
        col = await database.col("batches", db_name=db_name)
        return await col.find_one({"batch_id": batch_id, "deleted": False})

    async def add_user(self, uid: int, clone_id: str, username: str, fname: str, lname: str, db_name: str):
        col = await database.col("users", db_name=db_name)
        await col.update_one(
            {"user_id": uid},
            {"$set": {"user_id": uid, "username": username, "first_name": fname, "last_name": lname, "last_active": datetime.now(timezone.utc).isoformat()}},
            upsert=True
        )

    async def is_banned(self, uid: int, clone_id: str, db_name: str) -> bool:
        col = await database.col("banned", db_name=db_name)
        user = await col.find_one({"user_id": uid})
        return bool(user)

    async def get_settings(self, clone_id: str, db_name: str) -> dict:
        col = await database.col("settings", db_name=db_name)
        res = await col.find_one({"type": "config"})
        return res or {}

    async def get_all_clones(self) -> list:
        col = await database.col("clones", "main")
        return await col.find({"status": "active"}).to_list(length=1000)

    async def register_clone(self, data: dict):
        col = await database.col("clones", "main")
        await col.insert_one(data)

    async def delete_clone(self, clone_id: str):
        col = await database.col("clones", "main")
        await col.delete_one({"clone_id": clone_id})

repo = Repository()


# ═══════════════════════════════════════════════════════════════════════════════
# 3. HELPER UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def generate_token(length: int = 10) -> str:
    return secrets.token_urlsafe(length).replace("-", "").replace("_", "")[:length]

def generate_clone_id() -> str:
    return f"c_{secrets.token_hex(4)}"

def fmt_size(size_bytes: int) -> str:
    if not size_bytes: return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(units)-1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.2f} {units[i]}"

def fmt_time(seconds: int) -> str:
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    if d: return f"{d}d {h}h {m}m"
    if h: return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"

def extract_file_info(m: Message) -> Optional[dict]:
    media = m.document or m.video or m.audio or m.voice or m.photo
    if not media: return None
    file_id = getattr(media, "file_id", "")
    file_name = getattr(media, "file_name", "Media_File")
    file_size = getattr(media, "file_size", 0)
    if m.photo:
        file_name = f"Photo_{m.id}.jpg"
        file_size = m.photo.file_size
    return {"file_id": file_id, "file_name": file_name, "file_size": file_size}

async def check_all_subs(c: Client, uid: int, channels: List[str]) -> Tuple[bool, list, str]:
    unsubbed = []
    link = ""
    for ch in channels:
        try:
            ch_domain = ch.split("/")[-1].replace("@", "")
            member = await c.get_chat_member(ch_domain, uid)
            if member.status in ["kicked", "left"]:
                unsubbed.append(ch)
                link = ch
        except Exception:
            unsubbed.append(ch)
            link = ch
    return len(unsubbed) == 0, unsubbed, link

async def shorten_url_api(url: str) -> Optional[str]:
    return None # Yahan Shortener API add kar sakte hain


# ═══════════════════════════════════════════════════════════════════════════════
# 4. DELIVERY ENGINE & AUTO-DELETE SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

_start_time = time.time()
main_client: Optional[Client] = None
_clients: Dict[str, Client] = {}
_upload_buffers: Dict[str, Dict[str, Tuple[List[dict], float]]] = defaultdict(lambda: defaultdict(lambda: ([], 0.0)))
_delivery_queue: asyncio.Queue = asyncio.Queue()
_active_cancel_tokens: Dict[str, bool] = {}

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
        if not batch: batch = await repo.get_batch_by_id(batch_id, "main", "main")
        if not batch: return

        files = batch.get("files", []); total = len(files); sent = 0; failed = 0
        client = _clients.get(clone) or main_client
        if not client: return

        cancel_key = f"{uid}_{batch_id}"
        _active_cancel_tokens[cancel_key] = False
        sent_msg_ids = []
        delete_time = datetime.now(timezone.utc) + timedelta(hours=8)

        # Header Text
        banner_text = (
            "𝙷𝙸𝙽𝙳𝙸 𝚂𝚃𝙾𝚁𝚈\n"
            "❤️ 𝙷𝙴𝚈 𝙱𝚁𝙾 🇮🇳\n\n"
            "📂 𝙵𝙸𝙻𝙴𝚂 𝚆𝙸𝙻𝙻 𝙱𝙴 𝙳𝙴𝙻𝙴𝚃𝙴𝙳\n"
            "𝙰𝙵𝚃𝙴𝚁 𝟾 𝙷𝙾𝚄𝚁𝚂 𝙿𝙻𝙴𝙰𝚂𝙴\n"
            "𝚂𝙰𝚅𝙴 𝚃𝙷𝙴𝙼 𝚂𝙾𝙼𝙴𝚆𝙷𝙴𝚁𝙴 𝚂𝙰𝙵𝙴."
        )
        try:
            head_msg = await client.send_message(chat_id, banner_text)
            sent_msg_ids.append(head_msg.id)
        except Exception as e:
            log.error(f"Failed to send header: {e}")

        for idx, fm in enumerate(files):
            if _active_cancel_tokens.get(cancel_key, False):
                await client.send_message(chat_id, "🛑 **Sending Cancelled by User!**")
                break

            try:
                msg = await client.copy_message(
                    chat_id=chat_id, 
                    from_chat_id=config.DB_CHANNEL_ID, 
                    message_id=fm["db_msg_id"], 
                    caption=fm.get("caption", ""), 
                    protect_content=protect
                )
                sent += 1
                sent_msg_ids.append(msg.id)
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                msg = await client.copy_message(chat_id, config.DB_CHANNEL_ID, fm["db_msg_id"], caption=fm.get("caption", ""), protect_content=protect)
                sent += 1
                sent_msg_ids.append(msg.id)
            except Exception as e:
                log.error(f"Copy message error for msg_id {fm.get('db_msg_id')}: {e}")
                failed += 1
            await asyncio.sleep(config.BATCH_SEND_DELAY)

        # Register sent messages for 8-Hour Auto Delete
        if sent_msg_ids:
            col = await database.col("auto_delete", db_name)
            await col.insert_one({
                "chat_id": chat_id,
                "msg_ids": sent_msg_ids,
                "delete_at": delete_time.isoformat(),
                "clone_id": clone
            })

        _active_cancel_tokens.pop(cancel_key, None)

        try:
            await client.edit_message_text(chat_id, progress_msg_id, f"✅ **Delivery Complete!**\n\nSent: {sent}/{total}\nFailed: {failed}\n⏳ **Status:** Auto-delete set after 8 hours!")
        except: pass

delivery_engine = DeliveryEngine()

# ═══════════════════════════════════════════════════════════════════════════════
# 5. KEYBOARDS & UI HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

class KB:
    @staticmethod
    def main(is_owner: bool = False):
        b = [[InlineKeyboardButton("📚 Help", callback_data="help")]]
        if is_owner: b.append([InlineKeyboardButton("⚙️ Admin Dashboard", callback_data="admin_dashboard")])
        return InlineKeyboardMarkup(b)

    @staticmethod
    def sub_link(link: str):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Join Channel", url=link)],
            [InlineKeyboardButton("🔄 Verify Membership", callback_data="check_sub")],
        ])

    @staticmethod
    def cancel_batch(cancel_key: str, channel_link: Optional[str] = None):
        buttons = [[InlineKeyboardButton("🛑 Cancel Uploading", callback_data=f"stop_dl_{cancel_key}")]]
        if channel_link:
            buttons.append([InlineKeyboardButton("📢 Join Updates Channel", url=channel_link)])
        return InlineKeyboardMarkup(buttons)

kb = KB()

# ═══════════════════════════════════════════════════════════════════════════════
# 6. BOT HANDLER REGISTRATION
# ═══════════════════════════════════════════════════════════════════════════════

class CloneManager:
    async def init_main(self) -> Client:
        global main_client
        main_client = Client("main_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN, workers=50)
        return main_client

    def _register_handlers(self, client: Client, clone_id: str):
        async def _db(): return await repo.get_db_name_for_clone(clone_id)

        # /start command
        async def start_c(c: Client, m: Message):
            db_name = await _db(); user = m.from_user
            if not user: return
            await repo.add_user(user.id, clone_id, user.username or "", user.first_name or "", user.last_name or "", db_name)

            if await repo.is_banned(user.id, clone_id, db_name):
                await m.reply("❌ आप इस Bot पर बैन हैं।"); return

            # FORCE SUB MANDATORY CHECK
            settings = await repo.get_settings(clone_id, db_name)
            fsubs = settings.get("force_subs", []) or config.FORCE_SUB_CHANNELS
            fsub_link = None
            
            if fsubs:
                all_ok, _, link = await check_all_subs(c, user.id, fsubs)
                fsub_link = link
                if not all_ok:
                    msg = settings.get("force_sub_msg", "⚠️ **पहले Channel Join करें tabhi Files milengi!**")
                    await m.reply(msg, reply_markup=kb.sub_link(link)); return

            args = m.text.split(maxsplit=1)
            param = args[1].strip() if len(args) > 1 else ""

            # Fetch Single File
            if param.startswith("f_"):
                fid = param
                fd = await repo.get_file_by_id(fid, clone_id, db_name)
                if not fd: fd = await repo.get_file_by_id(fid, "main", "main")
                if not fd: await m.reply("❌ File नहीं मिली।"); return
                
                banner_text = "𝙷𝙸𝙽𝙳𝙸 𝚂𝚃𝙾𝚁𝚈\n❤️ 𝙷𝙴𝚈 𝙱𝚁𝙾 🇮🇳\n\n📂 𝙵𝙸𝙻𝙴𝚂 𝚆𝙸𝙻𝙻 𝙱𝙴 𝙳𝙴𝙻𝙴𝚃𝙴𝙳\n𝙰𝙵𝚃𝙴𝚁 𝟾 𝙷𝙾𝚄𝚁𝚂 𝙿𝙻𝙴𝙰𝚂𝙴\n𝚂𝙰𝚅𝙴 𝚃𝙷𝙴𝙼 𝚂𝙾𝙼𝙴𝚆𝙷𝙴𝚁𝙴 𝚂𝙰𝙵𝙴."
                h_msg = await c.send_message(m.chat.id, banner_text)
                f_msg = await c.copy_message(m.chat.id, config.DB_CHANNEL_ID, fd["db_msg_id"], caption=fd.get("caption",""), protect_content=settings.get("protect", True))
                
                delete_time = datetime.now(timezone.utc) + timedelta(hours=8)
                col = await database.col("auto_delete", db_name)
                await col.insert_one({"chat_id": m.chat.id, "msg_ids": [h_msg.id, f_msg.id], "delete_at": delete_time.isoformat(), "clone_id": clone_id})
                return

            # Fetch Batch Collection
            if param.startswith("b_"):
                bid = param
                batch = await repo.get_batch_by_id(bid, clone_id, db_name)
                if not batch: batch = await repo.get_batch_by_id(bid, "main", "main")
                if not batch: await m.reply("❌ Batch नहीं मिला।"); return
                
                cancel_key = f"{user.id}_{bid}"
                main_ch_link = fsub_link or (fsubs[0] if fsubs else None)
                
                info = await m.reply(
                    f"📦 **Batch Delivery Started!**\nFiles: {batch['total_files']} | Size: {fmt_size(batch['total_size'])}\n\n"
                    "⏳ *All sent files will automatically delete in 8 hours.*",
                    reply_markup=kb.cancel_batch(cancel_key, main_ch_link)
                )
                delivery_engine.enqueue(clone_id, user.id, m.chat.id, bid, info.id, settings.get("protect", True))
                return

            msg = settings.get("start_msg", "👋 Welcome!").format(first_name=user.first_name or "User")
            await m.reply(msg, reply_markup=kb.main(user.id == config.OWNER_ID or user.id in config.ADMIN_IDS))

        # /done command
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
                "deleted": False,
            }

            await repo.create_batch(data, db_name)
            del _upload_buffers[clone_id][key]

            bot_uname = config.BOT_USERNAME
            cd = await repo.get_clone(clone_id)
            if cd: bot_uname = cd.get("bot_username", bot_uname)

            link = f"https://t.me/{bot_uname}?start={batch_id}"

            await m.reply(
                f"✅ **Batch Upload Complete!**\n\n"
                f"📄 **कुल Files:** {len(files)}\n"
                f"📏 **कुल Size:** {fmt_size(total_size)}\n\n"
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
                await m.reply(f"❌ Channel copy error: {e}\n\n*Check karein ki Bot DB Channel me Admin hai ya nahi.*")
                return

            token = generate_token()
            fid = f"f_{token}"
            
            fd = {
                "file_id": fid, "clone_id": clone_id, "user_id": user.id,
                "file_name": info["file_name"], "file_size": info["file_size"],
                "db_msg_id": db_msg_id, "caption": m.caption or "",
                "created_at": datetime.now(timezone.utc).isoformat(),
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

        # Callback Handlers
        async def cb_c(c: Client, q: CallbackQuery):
            data = q.data; uid = q.from_user.id; db_name = await _db()
            await q.answer()

            if data == "close": 
                await q.message.delete()
            elif data.startswith("stop_dl_"):
                cancel_key = data.replace("stop_dl_", "")
                _active_cancel_tokens[cancel_key] = True
                await q.answer("🛑 Upload Cancelled!", show_alert=True)
                await q.message.edit_text("🛑 **Delivery Cancelled by User.**\n\n*Already sent files will still delete after 8 hours.*")

        client.add_handler(MessageHandler(start_c, filters.command("start") & filters.private))
        client.add_handler(MessageHandler(done_c, filters.command("done") & filters.private))
        client.add_handler(MessageHandler(cancel_c, filters.command("cancel") & filters.private))
        client.add_handler(MessageHandler(media_c, filters.private & ~filters.command(["start","done","cancel"])))
        client.add_handler(CallbackQueryHandler(cb_c))

clone_mgr = CloneManager()

# ═══════════════════════════════════════════════════════════════════════════════
# 7. AUTO-DELETE WORKER
# ═══════════════════════════════════════════════════════════════════════════════

async def auto_delete_messages_worker():
    while True:
        try:
            now = datetime.now(timezone.utc).isoformat()
            col = await database.col("auto_delete", "main")
            pending = await col.find({"delete_at": {"$lte": now}}).to_list(length=100)

            for item in pending:
                cid = item["chat_id"]
                mids = item["msg_ids"]
                clone_id = item.get("clone_id", "main")
                client = _clients.get(clone_id) or main_client

                if client:
                    try:
                        await client.delete_messages(chat_id=cid, message_ids=mids)
                    except Exception as err:
                        log.error(f"Auto delete error for chat {cid}: {err}")
                
                await col.delete_one({"_id": item["_id"]})
        except Exception as e:
            log.error(f"Auto delete loop error: {e}")

        await asyncio.sleep(30)

# ═══════════════════════════════════════════════════════════════════════════════
# 8. MAIN ENGINE BOOTSTRAPPER
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    global _start_time
    _start_time = time.time()

    log.info("Starting File Store System...")
    
    try:
        Config.validate()
        log.info("Connecting to MongoDB...")
        await database.connect("main")

        # Indexes ko background me run karein taaki startup block NA ho
        asyncio.create_task(repo.ensure_indexes("main"))

        log.info("Initializing Main Pyrogram Client...")
        mc = await clone_mgr.init_main()
        clone_mgr._register_handlers(mc, "main")

        log.info("Starting Pyrogram Main Client...")
        await mc.start()
        log.info(f"✅ Main Bot Active: @{config.BOT_USERNAME}")

        log.info("Loading Clones...")
        asyncio.create_task(clone_mgr.load_all_clones())

        log.info("Starting Delivery Engine Workers...")
        await delivery_engine.start(config.DELIVERY_WORKERS)
        
        log.info("Starting Auto-Delete Background Task...")
        asyncio.create_task(auto_delete_messages_worker())

        log.info("🚀 System Fully Active & Processing Link Requests!")
        await idle()

    except Exception as e:
        log.critical(f"❌ FATAL ERROR DURING STARTUP: {e}", exc_info=True)

            
