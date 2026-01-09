import os
import asyncio
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# ENV VARIABLES (Railway)
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SOURCE_CHAT_ID = int(os.getenv("SOURCE_CHAT_ID"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID"))

if not BOT_TOKEN or not SOURCE_CHAT_ID or not TARGET_CHAT_ID:
    raise ValueError("Missing required environment variables")

# =========================
# MESSAGE HANDLER
# =========================
async def forward_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    chat_id = update.message.chat.id

    # Only listen to SOURCE channel
    if chat_id != SOURCE_CHAT_ID:
        return

    text = update.message.text
    if not text:
        return

    await context.bot.send_message(
        chat_id=TARGET_CHAT_ID,
        text=text
    )

# =========================
# MAIN APP
# =========================
async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, forward_message)
    )

    print("✅ Bot started. Listening for messages...")
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
