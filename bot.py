import os
import time
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    MessageHandler, 
    CommandHandler, 
    CallbackQueryHandler, 
    ContextTypes, 
    filters
)
from pymongo import MongoClient

# 🔑 Credentials ab sirf Railway Environment Variables se aayenge
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "") 
# Multiple Admin IDs Setup
ADMIN_IDS_RAW = os.getenv("ADMIN_ID", "0")
ADMIN_IDS = [int(aid.strip()) for aid in ADMIN_IDS_RAW.split(",") if aid.strip().isdigit()]
ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0
FORCE_SUB_CHANNEL = os.getenv("FORCE_SUB_CHANNEL", "")             
CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK", "") 
PRIVATE_STORE_ID = int(os.getenv("PRIVATE_STORE_ID", "0"))  

# MongoDB Setup
client = MongoClient(MONGO_URI)
db = client['bot_database']
batch_col = db['file_batches']
user_col = db['users']
delete_col = db['delete_queue'] 
history_col = db['user_history']  

user_queues = {}
backup_queues = {}
cancel_status = {}

# --- File Size Formatter ---
def get_readable_size(size_in_bytes):
    if not size_in_bytes:
        return "Unknown Size"
    for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} PB"

# --- Storage Checker ---
async def database_storage_checker(app):
    while True:
        try:
            stats_data = db.command("dbStats")
            storage_size_bytes = stats_data.get("storageSize", 0)
            storage_size_mb = storage_size_bytes / (1024 * 1024)
            
            if storage_size_mb >= 450.0:
                alert_text = (
                    f"⚠️ <b>MONGODB STORAGE WARNING!</b> ⚠️\n\n"
                    f"आपका डेटाबेस लगभग पूरा भरने वाला है!\n"
                    f"<b>Current Usage:</b> {storage_size_mb:.2f} MB / 512 MB\n\n"
                    f"कृपया कुछ पुराना डेटा डिलीट करें, अन्यथा बॉट नया डेटा सेव करना बंद कर देगा।"
                )
                try:
                    await app.bot.send_message(chat_id=ADMIN_ID, text=alert_text, parse_mode="HTML")
                except Exception as sms_err:
                    print(f"Alert sending failed: {sms_err}")
                    
        except Exception as e:
            print(f"Database storage check error: {e}")
            
        await asyncio.sleep(3600)

# --- Cancel Handler ---
async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    cancel_status[user_id] = True 
    try: 
        await query.message.delete()
    except: 
        pass
    await query.answer("❌ Files bhejna rok diya gaya hai.")

# --- Auto-Delete Monitor ---
async def auto_delete_monitor(app):
    while True:
        try:
            current_time = time.time()
            all_pending = delete_col.find({"delete_at": {"$lte": current_time}})
            for task in all_pending:
                chat_id = task['chat_id']
                for msg_id in task['message_ids']:
                    try: 
                        await app.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                    except: 
                        pass
                    await asyncio.sleep(0.1) 
                delete_col.delete_one({"_id": task['_id']})
        except Exception as e: 
            print(f"Auto-Delete Monitor Error: {e}")
        await asyncio.sleep(15)

async def run_post_init(application):
    asyncio.create_task(auto_delete_monitor(application))
    asyncio.create_task(database_storage_checker(application))

async def check_user_joined(context, user_id):
    try:
        member = await context.bot.get_chat_member(chat_id=FORCE_SUB_CHANNEL, user_id=user_id)
        return member.status in ['member', 'administrator', 'creator']
    except: 
        return False

# --- Send Files Logic ---
async def send_files_logic(update, context, batch_key):
    user = update.effective_user
    cancel_status[user.id] = False 
    batch = batch_col.find_one({"batch_key": batch_key})
    
    if not batch:
        await update.message.reply_text("❌ Yeh link amanaye (invalid) hai.")
        return

    try:
        history_col.insert_one({
            "user_id": user.id, 
            "first_name": user.first_name, 
            "username": user.username, 
            "action": "requested_files", 
            "batch_key": batch_key, 
            "time": datetime.now(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        })
    except:
        pass
    
    info_msg = await update.message.reply_text(
        "⏳ Sending files...", 
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("• Cancel", callback_data="cancel_action")],
            [InlineKeyboardButton("📟 UPDATE CHANNEL", url=CHANNEL_INVITE_LINK)]
        ])
    )
    
    sent_message_ids = [info_msg.message_id]
    is_cancelled = False
    
    for file in batch["files"]:
        if cancel_status.get(user.id): 
            is_cancelled = True
            break 

        try:
            sent_msg = None
            file_bytes = file.get('file_size', 0)
            readable_size = get_readable_size(file_bytes)
            
            custom_caption = (
                f">> JOIN > @AllstoryFM2 🔥\n"
                f"✅✨\n\n"
                f"👉 FILE SIZE :- {readable_size} 👑\n"
                f"🔥"
            )

            if file['file_type'] == 'document': 
                sent_msg = await context.bot.send_document(update.message.chat_id, file['file_id'], protect_content=True, caption=custom_caption)
            elif file['file_type'] == 'video': 
                sent_msg = await context.bot.send_video(update.message.chat_id, file['file_id'], protect_content=True, caption=custom_caption)
            elif file['file_type'] == 'photo': 
                sent_msg = await context.bot.send_photo(update.message.chat_id, file['file_id'], protect_content=True, caption=custom_caption)
            elif file['file_type'] == 'audio': 
                sent_msg = await context.bot.send_audio(update.message.chat_id, file['file_id'], protect_content=True, caption=custom_caption)

            if sent_msg: 
                sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.5) 
        except Exception as e:
            print(f"File send error: {e}")
            break

    if len(sent_message_ids) > 0:
        try:
            delete_col.insert_one({
                "chat_id": update.message.chat_id, 
                "message_ids": sent_message_ids, 
                "delete_at": time.time() + 28800 
            })
        except:
            pass

    try: 
        await context.bot.delete_message(chat_id=update.message.chat_id, message_id=info_msg.message_id)
    except: 
        pass

    alert_text = "𝙷𝙸𝙽𝙳𝙸 𝚂𝚃𝙾𝚁𝚈\n❤️ 𝙷𝙴𝚈 𝙱𝚁𝙾 🇮🇳 \n\n📂 𝙵𝙸𝙻𝙴𝚂 𝚆𝙸𝙻𝙻 𝙱𝙴 𝙳𝙴𝙻𝙴𝚃𝙴𝙳 \n𝙰𝙵𝚃𝙴𝚁 [ 𝟾 𝙷𝙾𝚄𝚁𝚂 ] 𝙿𝙻𝙴𝙰𝚂𝙴 \n𝚂𝙰𝚅𝙴 𝚃𝙷𝙴𝙼 𝚂𝙾𝙼𝙴𝚆𝙷𝙴𝚁𝙴 𝚂𝙰𝙵𝙴."
    if is_cancelled:
        alert_text += "\n\n⚠️ *Process was cancelled by user.*"

    try:
        final_msg = await update.message.reply_text(
            alert_text, 
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📟 UPDATE CHANNEL", url=CHANNEL_INVITE_LINK)]
            ])
        )
        delete_col.insert_one({
            "chat_id": update.message.chat_id, 
            "message_ids": [final_msg.message_id], 
            "delete_at": time.time() + 28800
        })
    except: 
        pass

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        if not user_col.find_one({"user_id": user.id}):
            user_col.insert_one({"user_id": user.id, "username": user.username, "first_name": user.first_name})
    except:
        pass
    args = context.args
    if args:
        if not await check_user_joined(context, user.id):
            await update.message.reply_text("⚠️ Files ke liye channel join karein:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 Join Channel", url=CHANNEL_INVITE_LINK)]]))
            return
        asyncio.create_task(send_files_logic(update, context, args[0]))
        return
    await update.message.reply_text("👋 Hello! I am a permanent batch file store bot.")

async def check_logs(update, context):
    if update.effective_user.id != ADMIN_ID: return
    try:
        logs = list(history_col.find().sort("_id", -1).limit(15))
        log_text = "📊 Recent Logs:\n\n" + "".join([f"👤 {e.get('first_name')}\n📥 {e.get('batch_key')}\n⏰ {e.get('time')}\n\n" for e in logs])
        await update.message.reply_text(log_text)
    except Exception as e:
        await update.message.reply_text(f"❌ Logs load karne me error: {e}")

async def stats(update, context):
    if update.effective_user.id == ADMIN_ID: 
        try:
            total_users = user_col.count_documents({})
            total_reqs = history_col.count_documents({})
            
            try:
                stats_cmd = db.command("dbStats")
                storage_bytes = stats_cmd.get("storageSize", stats_cmd.get("dataSize", 0))
                
                if storage_bytes < 1024 * 1024:
                    storage_kb = storage_bytes / 1024
                    storage_text = f"{storage_kb:.2f} KB / 512 MB"
                else:
                    storage_mb = storage_bytes / (1024 * 1024)
                    storage_text = f"{storage_mb:.2f} MB / 512 MB"
            except Exception as db_err:
                print(f"dbStats Error: {db_err}")
                storage_text = "Unavailable"

            await update.message.reply_text(
                f"👥 Total Users: {total_users}\n"
                f"📥 Total Requests: {total_reqs}\n"
                f"🗄️ DB Storage Used: {storage_text}"
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Stats calculation error: {e}")

async def broadcast(update, context):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args and not update.message.reply_to_message:
        await update.message.reply_text("❌ Kuch text likhein ya kisi message ko reply karein.")
        return
        
    await update.message.reply_text("📢 Broadcast shuru ho raha hai...")
    success = 0
    failed = 0
    for user in user_col.find():
        try:
            if update.message.reply_to_message: 
                await context.bot.copy_message(user['user_id'], update.message.chat_id, update.message.reply_to_message.message_id)
            else: 
                await context.bot.send_message(user['user_id'], " ".join(context.args))
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            if "Forbidden" in str(e): 
                try: user_col.delete_one({"user_id": user['user_id']})
                except: pass
    await update.message.reply_text(f"✅ Broadcast Complete!\n🟢 Success: {success}\n🔴 Failed/Blocked: {failed}")

async def get_link_manually(update, context):
    if update.effective_user.id != ADMIN_ID: return
    if ADMIN_ID not in backup_queues: 
        await update.message.reply_text("❌ Queue khali hai! Pehle files bhejein.")
        return
    batch_key = f"batch_{int(time.time())}"
    try:
        batch_col.insert_one({"batch_key": batch_key, "files": backup_queues[ADMIN_ID], "timestamp": time.time()})
        bot_info = await context.bot.get_me()
        await update.message.reply_text(f"🔗 Link: https://t.me/{bot_info.username}?start={batch_key}")
    except Exception as e:
        await update.message.reply_text(f"❌ Link generation error: {e}")

async def process_batch_queue(user_id, context, message):
    await asyncio.sleep(30)
    if user_id not in user_queues: return
    raw_files = user_queues.pop(user_id)
    saved_files = []
    
    for msg in raw_files:
        file_obj = msg.document or msg.video or (msg.photo[-1] if msg.photo else None) or msg.audio
        file_id = file_obj.file_id if file_obj else None
        file_size = file_obj.file_size if file_obj and hasattr(file_obj, 'file_size') else 0
        
        if file_id:
            while True:  # FloodWait आने पर ऑटो-रीटाय करने के लिए लूप
                try:
                    await context.bot.forward_message(PRIVATE_STORE_ID, msg.chat_id, msg.message_id)
                    saved_files.append({
                        "file_id": file_id, 
                        "file_size": file_size,
                        "file_type": 'document' if msg.document else ('video' if msg.video else ('photo' if msg.photo else 'audio'))
                    })
                    # 🛡️ Flood control से बचने के लिए डिले 1 सेकंड
                    await asyncio.sleep(1.0)
                    break
                except Exception as e:
                    error_str = str(e)
                    if "FloodWait" in error_str:
                        import re
                        seconds = int(re.search(r'\d+', error_str).group()) if re.search(r'\d+', error_str) else 5
                        print(f"⚠️ FloodWait detected! Sleeping for {seconds} seconds...")
                        await asyncio.sleep(seconds + 1)
                    else:
                        print(f"Forward error: {e}")
                        break

    backup_queues[user_id] = saved_files
    await message.reply_text("✅ Batch stored! Ab aap /getlink command use kar sakte hain.")

async def store_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_queues:
        user_queues[user_id] = []
        asyncio.create_task(process_batch_queue(user_id, context, update.message))
    user_queues[user_id].append(update.message)

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error_msg = str(context.error)
    if "httpx.ReadError" in error_msg or "NetworkError" in error_msg:
        print(f"🌐 [Network Alert]: Telegram Server connection lost temporarily. Retrying... Error: {error_msg}")
    else:
        print(f"⚠️ [Bot Error Handled]: An error occurred: {context.error}")

if __name__ == "__main__":
    req = HTTPXRequest(connect_timeout=60.0, read_timeout=60.0)
    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .request(req)
        .job_queue(None)
        .post_init(run_post_init)
        .build()
    )
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("logs", check_logs))
    app.add_handler(CommandHandler("getlink", get_link_manually))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CallbackQueryHandler(cancel_callback, pattern="cancel_action"))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.ALL & ~filters.COMMAND, store_file))
    
    app.add_error_handler(global_error_handler)
    
    print("🤖 Bot is running on Railway!")
    app.run_polling(drop_pending_updates=True)

    
