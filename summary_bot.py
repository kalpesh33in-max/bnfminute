import asyncio
import os
import re
import logging
from datetime import datetime
from collections import defaultdict
import telegram
from telegram.ext import Application, MessageHandler, filters

# Setup logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
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
        'strength': r"(🚀 BLAST 🚀|🌟 AWESOME|✅ VERY GOOD|👍 GOOD|🆗 OK)",
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
        elif key != 'strength':
            return None
    
    try:
        data['lots'] = int(data['lots'])
        data['oi_change'] = int(data['oi_change'].replace(',', ''))
        
        if data['symbol'].endswith('-I'):
            data['instrument'] = 'Future'
        else:
            data['instrument'] = 'Option'

        return data
    except Exception:
        return None

async def message_handler(update, context):
    if update.message and update.message.text and str(update.message.chat_id) == str(TARGET_CHANNEL_ID):
        parsed_data = parse_alert(update.message.text)
        if parsed_data:
            alerts_buffer.append(parsed_data)
            logger.info(f"Buffered: {parsed_data['symbol']}")

async def process_summary(context):
    global alerts_buffer
    if not alerts_buffer:
        return
    
    current_alerts = list(alerts_buffer)
    alerts_buffer.clear()
    
    summary = f"📊 **5-Minute Summary** ({len(current_alerts)} alerts)\n"
    for a in current_alerts[:5]:
        summary += f"• {a['symbol']}: {a['instrument']} ({a['lots']} lots)\n"
    
    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=summary, parse_mode='Markdown')

async def main():
    if not all([BOT_TOKEN, TARGET_CHANNEL_ID, SUMMARY_CHAT_ID]):
        logger.error("CRITICAL: Environment Variables Missing!")
        return

    # 1. Build the application correctly
    application = Application.builder().token(BOT_TOKEN).build()

    # 2. Add message handlers
    application.add_handler(MessageHandler(filters.Chat(chat_id=int(TARGET_CHANNEL_ID)), message_handler))

    # 3. INITIALIZE manually (This fixes the 'weak reference' error)
    await application.initialize()
    
    # 4. START the Job Queue for the 5-minute summary
    application.job_queue.run_repeating(process_summary, interval=300, first=10)
    
    # 5. START polling and the updater
    await application.start()
    await application.updater.start_polling()
    
    logger.info("Bot started successfully and is now polling.")
    
    # 6. Keep the loop running indefinitely
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        await application.stop()

if __name__ == "__main__":
    asyncio.run(main())
