import asyncio
import os
import re
import logging
from collections import defaultdict
from telegram.ext import Application, MessageHandler, filters

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BOT_TOKEN = os.environ.get("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.environ.get("TARGET_CHANNEL_ID") 
SUMMARY_CHAT_ID = os.environ.get("SUMMARY_CHAT_ID")   

alerts_buffer = []

def parse_alert(message_text):
    patterns = {
        'action': r"🚨 (.*)",
        'symbol': r"Symbol: ([\w-]+)",
        'lots': r"LOTS: (\d+)",
    }
    data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, message_text)
        if match:
            data[key] = match.group(1).strip()
        else:
            return None
    try:
        data['lots'] = int(data['lots'])
        return data
    except:
        return None

async def message_handler(update, context):
    if update.message and update.message.text and str(update.message.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(update.message.text)
        if parsed:
            alerts_buffer.append(parsed)
            logger.info(f"Buffered: {parsed['symbol']}")

async def process_summary(context):
    global alerts_buffer
    if not alerts_buffer:
        return
    current = list(alerts_buffer)
    alerts_buffer.clear()
    msg = f"📊 **5-Min Summary** ({len(current)} trades)\n" + "\n".join([f"• {a['symbol']} ({a['lots']} lots)" for a in current[:5]])
    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')

async def main():
    if not all([BOT_TOKEN, TARGET_CHANNEL_ID, SUMMARY_CHAT_ID]):
        logger.error("Missing Environment Variables")
        return

    # 1. BUILD the application
    application = Application.builder().token(BOT_TOKEN).build()

    # 2. INITIALIZE manually to fix Python 3.13 weakref error
    await application.initialize()
    
    # 3. CONFIGURE JobQueue after initialization
    if application.job_queue:
        application.job_queue.run_repeating(process_summary, interval=300, first=10)
    
    # 4. ADD Handlers
    application.add_handler(MessageHandler(filters.Chat(chat_id=int(TARGET_CHANNEL_ID)), message_handler))

    # 5. START polling and the updater
    await application.start()
    await application.updater.start_polling()
    
    logger.info("Bot successfully started on Python 3.13!")
    
    try:
        while True:
            await asyncio.sleep(1)
    finally:
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
