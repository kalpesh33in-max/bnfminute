import os
import re
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# --- CONFIGURATION (Pulls from Railway Variables) ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID") 
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID") 

# (Keep your existing message_handler and calculation logic here)

def main():
    # builder() + run_polling() is the stable way to avoid the weakref crash
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Standard message handler
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    
    # Initialize background timer (300 seconds = 5 minutes)
    if application.job_queue:
        application.job_queue.run_repeating(process_summary, interval=300, first=10)
    
    # This blocks the code and keeps it alive on Railway correctly
    # It replaces initialize(), start(), and the while True loop
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
