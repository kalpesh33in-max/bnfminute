import os
import re
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# Logging setup
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN", "8537613424:AAFw7FN2KGIncULsgjuv_r3jF5OvIzFLcuM")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "-1003665271298") 
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID", "-1003665271298") 

alerts_buffer = []

def get_alert_details(message_text):
    patterns = {'action': r"🚨 (.*)", 'symbol': r"Symbol: ([\w-]+)", 'lots': r"LOTS: (\d+)"}
    data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, message_text)
        if match: data[key] = match.group(1).strip()
        else: return None
    try:
        data['lots'] = int(data['lots'])
        symbol = data['symbol'].upper()
        data['type'] = 'FUT' if any(x in symbol for x in ["-I", "FUT"]) else 'OPT'
        
        # Sentiment logic (simplified for brevity)
        action = data['action'].upper()
        if "WRITER" in action or "SHORT COVERING" in action:
            data['sentiment'], data['weight'] = 1, 4.0
        else:
            data['sentiment'], data['weight'] = 1, 1.0 # Default positive for test
            
        return data
    except: return None

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = get_alert_details(msg.text)
        if parsed:
            alerts_buffer.append(parsed)
            logger.info(f"Buffered: {parsed['symbol']}")

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        return
    
    # Process buffer and create message (Your existing logic)
    current_batch = list(alerts_buffer)
    alerts_buffer.clear()
    
    msg = f"📊 **BANK NIFTY SUMMARY**\nParsed {len(current_batch)} alerts."
    
    try:
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')
        logger.info("Summary sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send: {e}")

def main():
    # builder() + run_polling() is the stable way to avoid weakref errors
    application = Application.builder().token(BOT_TOKEN).build()

    # Add handler - filters.TEXT ensures it reads standard messages
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))

    # JobQueue setup
    if application.job_queue:
        application.job_queue.run_repeating(process_summary, interval=300, first=10)

    # run_polling handles initialize, start, and idle correctly
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
