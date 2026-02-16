import os
import re
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# --- LOGGING ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIG (Railway Variables) ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID") 
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID") 

# (Keep your existing message_handler and calculation logic here)

def main():
    # builder() + run_polling() is the ONLY way to avoid the weakref crash in 3.13
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Standard message handler
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    
    # Initialize background timer (300 seconds = 5 minutes)
    if application.job_queue:
        application.job_queue.run_repeating(process_summary, interval=300, first=10)
    
    logger.info("Bot starting in stable polling mode...")
    
    # This blocks and keeps the bot online without the loop conflict
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
