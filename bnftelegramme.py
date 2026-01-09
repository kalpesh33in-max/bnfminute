import os
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# ENV VARIABLES
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SOURCE_CHAT_ID = int(os.getenv("SOURCE_CHAT_ID"))
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID"))

print("✅ BOT STARTING")
print("SOURCE_CHAT_ID =", SOURCE_CHAT_ID)
print("TARGET_CHAT_ID =", TARGET_CHAT_ID)

# =========================
# HANDLER (ONE-WAY ONLY)
# =========================
async def forward_message(update: Update, context: ContextTypes.DEFAULT_TYPE):

    # Ignore anything that is NOT a channel post
    if not update.channel_post:
        return

    chat_id = update.channel_post.chat.id

    # 🚫 HARD BLOCK: ignore TARGET channel completely
    if chat_id == TARGET_CHAT_ID:
        return

    # ✅ ALLOW ONLY SOURCE channel
    if chat_id != SOURCE_CHAT_ID:
        return

    text = update.channel_post.text
    if not text:
        return

    await context.bot.send_message(
        chat_id=TARGET_CHAT_ID,
        text=text
    )

# =========================
# APP START
# =========================
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(
        MessageHandler(filters.ChatType.CHANNEL, forward_message)
    )

    print("🚀 BOT RUNNING (ONE-WAY MODE)")
    app.run_polling()

if __name__ == "__main__":
    main()
