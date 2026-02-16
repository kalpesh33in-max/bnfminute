import os
import re
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION (Railway Variables) ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID") 
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID") 

# --- STATE ---
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
        action = data['action'].upper()
        
        # Bullish/Bearish Logic
        bull_s = ["PUT WRITER", "SHORT COVERING (PE)", "SHORT COVERING ↗️"]
        bull_r = ["CALL BUY", "FUTURE BUY", "LONG BUILDUP"]
        bear_s = ["CALL WRITER", "SHORT BUILDUP"]
        bear_r = ["PUT BUY", "FUTURE SELL", "LONG UNWINDING (PE)", "LONG UNWINDING ↘️"]

        if any(k in action for k in bull_s): data['sentiment'], data['weight'] = 1, 4.0
        elif any(k in action for k in bull_r): data['sentiment'], data['weight'] = 1, 1.0
        elif any(k in action for k in bear_s): data['sentiment'], data['weight'] = -1, 4.0
        elif any(k in action for k in bear_r): data['sentiment'], data['weight'] = -1, 1.0
        else: data['sentiment'], data['weight'] = 0, 0
        return data
    except: return None

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = get_alert_details(msg.text)
        if parsed:
            alerts_buffer.append(parsed)
            logger.info(f"Buffered Alert: {parsed['symbol']}")

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        return
    
    current_batch = list(alerts_buffer)
    alerts_buffer.clear()
    
    # Simple summary logic for stability
    total_score = sum((a['sentiment'] * a['lots'] * a['weight']) for a in current_batch)
    trend = "🚀 STRONG BULLISH" if total_score > 1000 else "📈 BULLISH" if total_score > 200 else "🔥 STRONG BEARISH" if total_score < -1000 else "📉 BEARISH" if total_score < -200 else "↔️ NEUTRAL"
    
    msg = f"📊 **MARKET TREND SUMMARY**\nSentiment: **{trend}**\nAlerts Processed: {len(current_batch)}"

    try:
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')
        logger.info("Summary posted to Telegram.")
    except Exception as e:
        logger.error(f"Post failed: {e}")

def main():
    # builder() + run_polling() is the ONLY way to avoid the weak reference crash 
    application = Application.builder().token(BOT_TOKEN).build()

    # Add handler for alerts (Filtering out commands)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))

    # Setup the background timer (300 seconds = 5 minutes)
    if application.job_queue:
        application.job_queue.run_repeating(process_summary, interval=300, first=10)

    logger.info("Bot starting in stable polling mode...")
    # This method handles memory and loop management automatically
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
