import os
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
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

# SOURCE: GDFL_RAW_ALERTS
SOURCE_CHAT_ID = int(os.environ["SOURCE_CHAT_ID"])

# TARGET: BNF_1MIN_AI_ALERTS
TARGET_CHAT_ID = int(os.environ["TARGET_CHAT_ID"])


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

    print("✅ Telegram Forward Bot is RUNNING")
    app.run_polling()


if __name__ == "__main__":
    main()
