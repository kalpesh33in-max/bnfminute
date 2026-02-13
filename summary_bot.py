import asyncio
import os
import re
import logging
from datetime import datetime
from collections import defaultdict
import telegram
from telegram.ext import Application, MessageHandler, filters

# Enable logging to see results in Railway
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BOT_TOKEN = "8520597591:AAE4eDmtssZEloon9UHCnNzYkqaaY8-3Bgw"
TARGET_CHANNEL_ID = "-1003665271298" 
SUMMARY_CHAT_ID = "-1003665271298" # Sending back to same channel for trial

# --- STATE ---
alerts_buffer = []

def parse_alert(message_text):
    patterns = {
        'action': r"🚨 (.*)",
        'symbol': r"Symbol: ([\w-]+)",
        'lots': r"LOTS: (\d+)",
        'oi_change': r"OI CHANGE\s+:\s*([+-]?[0-9,]+)",
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
        # Handle commas and signs in OI CHANGE
        oi_str = data['oi_change'].replace(',', '').replace('+', '')
        data['oi_change'] = int(oi_str)
        return data
    except Exception:
        return None

async def message_handler(update, context):
    # Log incoming messages for debugging
    if update.message and update.message.text:
        logger.info(f"Incoming from {update.message.chat_id}: {update.message.text[:50]}...")
    
    if update.message and update.message.text and str(update.message.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(update.message.text)
        if parsed:
            alerts_buffer.append(parsed)
            logger.info(f"Buffered: {parsed['symbol']}")

async def process_summary(context):
    global alerts_buffer
    if not alerts_buffer:
        logger.info("Summary job ran, but buffer is empty.")
        return
    current_batch = list(alerts_buffer)
    alerts_buffer.clear()
    msg = f"📊 **5-Minute Summary** ({len(current_batch)} trades)\n" + "\n".join([f"• {a['symbol']}: {a['lots']} lots" for a in current_batch])
    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')

async def main():
    if not all([BOT_TOKEN, TARGET_CHANNEL_ID, SUMMARY_CHAT_ID]):
        logger.error("Missing Configuration!")
        return

    # 1. Build application
    application = Application.builder().token(BOT_TOKEN).build()

    # 2. REQUIRED FOR PYTHON 3.13: Initialize first to fix the weakref error
    await application.initialize()

    # 3. Add handlers and jobs AFTER initialization
    application.add_handler(MessageHandler(filters.Chat(chat_id=int(TARGET_CHANNEL_ID)), message_handler))
    if application.job_queue:
        # Reduced interval to 30 seconds for trial
        application.job_queue.run_repeating(process_summary, interval=30, first=5)

    # 4. Start the bot
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
