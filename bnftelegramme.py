import os
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# SAFE ENV READ
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SOURCE_CHAT_ID = os.getenv("SOURCE_CHAT_ID")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")

# =========================
# HARD FAIL WITH MESSAGE
# =========================
if not BOT_TOKEN:
    raise RuntimeError("❌ TELEGRAM_BOT_TOKEN is missing in Railway variables")

if not SOURCE_CHAT_ID:
    raise RuntimeError("❌ SOURCE_CHAT_ID is missing in Railway variables")

if not TARGET_CHAT_ID:
    raise RuntimeError("❌ TARGET_CHAT_ID is missing in Railway variables")

SOURCE_CHAT_ID = int(SOURCE_CHAT_ID)
TARGET_CHAT_ID = int(TARGET_CHAT_ID)

print("✅ ENV LOADED")
print("SOURCE_CHAT_ID =", SOURCE_CHAT_ID)
print("TARGET_CHAT_ID =", TARGET_CHAT_ID)


# =========================
# HANDLER
# =========================
async def forward_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.channel_post:
        return

    if update.channel_post.chat.id != SOURCE_CHAT_ID:
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

    print("🚀 BOT STARTED – LISTENING FOR CHANNEL POSTS")
    app.run_polling()


if __name__ == "__main__":
    main()
