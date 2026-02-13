import asyncio
import os
import re
import logging
from collections import defaultdict
from telegram.ext import Application, MessageHandler, filters

# Setup logging to see results in Railway Deploy Logs
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BOT_TOKEN = os.environ.get("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.environ.get("TARGET_CHANNEL_ID") 
SUMMARY_CHAT_ID = os.environ.get("SUMMARY_CHAT_ID")   

# --- STATE ---
alerts_buffer = []

# --- PARSING LOGIC ---
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
        data['oi_change'] = int(data['oi_change'].replace(',', ''))
        return data
    except Exception:
        return None

async def message_handler(update, context):
    # Only process if message is from the target channel
    if update.message and update.message.text and str(update.message.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(update.message.text)
        if parsed:
            alerts_buffer.append(parsed)
            logger.info(f"Buffered trade: {parsed['symbol']}")

async def process_summary(context):
    global alerts_buffer
    if not alerts_buffer:
        return
    
    current_batch = list(alerts_buffer)
    alerts_buffer.clear()
    
    msg = f"📊 **5-Minute Summary** ({len(current_batch)} trades)\n\n"
    for item in current_batch[:10]: # List top 10 trades
        msg += f"• {item['symbol']}: {item['lots']} lots\n"
        
    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')

async def main():
    if not all([BOT_TOKEN, TARGET_CHANNEL_ID, SUMMARY_CHAT_ID]):
        logger.error("CRITICAL: Missing environment variables (Token, Target ID, or Summary ID).")
        return

    # 1. Build application without starting it
    application = Application.builder().token(BOT_TOKEN).build()

    # 2. REQUIRED FOR PYTHON 3.13: Manually initialize before setting up Jobs
    # This prevents the "weak reference" error by locking the Application in memory.
    await application.initialize()

    # 3. CONFIGURE JobQueue & Handlers AFTER initialization
    if application.job_queue:
        # Runs every 300 seconds (5 mins), starts 10 seconds after bot is online
        application.job_queue.run_repeating(process_summary, interval=300, first=10)
        logger.info("Summary timer (5m) started.")
    
    # Listen only to the specific channel ID
    application.add_handler(MessageHandler(filters.Chat(chat_id=int(TARGET_CHANNEL_ID)), message_handler))

    # 4. Start the bot components
    await application.start()
    await application.updater.start_polling()
    
    logger.info("Summarizer Bot is active and running!")
    
    # Keep the script alive
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
