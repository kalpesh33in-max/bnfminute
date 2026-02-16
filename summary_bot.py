import os
import re
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# Logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIG ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN", "8537613424:AAFw7FN2KGIncULsgjuv_r3jF5OvIzFLcuM")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "-1003665271298") 
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID", "-1003665271298") 

alerts_buffer = []

# (Keep your existing get_alert_details and message_handler functions here)

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        logger.info("Buffer empty. No summary to send.")
        return
    
    current_batch = list(alerts_buffer)
    alerts_buffer.clear()
    
    # (Keep your existing calculation logic here)
    msg = f"📊 **BANK NIFTY SUMMARY**\nParsed {len(current_batch)} alerts."

    try:
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')
        logger.info("Summary successfully sent.")
    except Exception as e:
        logger.error(f"Failed to send: {e}")

def main():
    # Use the builder to handle all memory setup correctly
    application = Application.builder().token(BOT_TOKEN).build()

    # Add handlers
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))

    # Set background timer (300 seconds = 5 minutes)
    if application.job_queue:
        application.job_queue.run_repeating(process_summary, interval=300, first=10)

    logger.info("Bot starting...")
    # This method is the ONLY stable way to run on Railway
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
