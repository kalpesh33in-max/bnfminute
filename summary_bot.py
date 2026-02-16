import os
import re
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# --- LOGGING SETUP ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION (Pulls from Railway Variables) ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID") 
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID") 

# --- STATE ---
alerts_buffer = []

def get_alert_details(message_text):
    patterns = {
        'action': r"🚨 (.*)",
        'symbol': r"Symbol: ([\w-]+)",
        'lots': r"LOTS: (\d+)",
    }
    data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, message_text)
        if match:
            data[key] = match.group(1).strip()
        else:
            return None
    
    try:
        data['lots'] = int(data['lots'])
        action = data['action'].upper()
        symbol = data['symbol'].upper()
        
        # Determine Instrument Type
        data['type'] = 'FUT' if any(x in symbol for x in ["-I", "FUT"]) else 'OPT'

        # Sentiment & Weightage Logic
        bullish_smart = ["PUT WRITER", "SHORT COVERING (PE)", "SHORT COVERING ↗️"]
        bullish_retail = ["CALL BUY", "FUTURE BUY", "LONG BUILDUP"]
        bearish_smart = ["CALL WRITER", "SHORT BUILDUP"]
        bearish_retail = ["PUT BUY", "FUTURE SELL", "LONG UNWINDING (PE)", "LONG UNWINDING ↘️"]

        if any(k in action for k in bullish_smart):
            data['sentiment'], data['weight'] = 1, 4.0
        elif any(k in action for k in bullish_retail):
            data['sentiment'], data['weight'] = 1, 1.0
        elif any(k in action for k in bearish_smart):
            data['sentiment'], data['weight'] = -1, 4.0
        elif any(k in action for k in bearish_retail):
            data['sentiment'], data['weight'] = -1, 1.0
        else:
            data['sentiment'], data['weight'] = 0, 0
            
        return data
    except Exception:
        return None

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Support for both standard messages and channel posts
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = get_alert_details(msg.text)
        if parsed:
            alerts_buffer.append(parsed)
            logger.info(f"Buffered: {parsed['symbol']} | {parsed['action']}")

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        logger.info("Summary skipped: Buffer empty.")
        return
    
    current_batch = list(alerts_buffer)
    alerts_buffer.clear()
    
    pillars = {
        "BANKNIFTY": {"fut_score": 0, "opt_score": 0},
        "HDFCBANK": {"fut_score": 0, "opt_score": 0},
        "ICICIBANK": {"fut_score": 0, "opt_score": 0}
    }
    
    for a in current_batch:
        sym = a['symbol'].upper()
        target_pillar = next((p for p in pillars if p in sym), None)
        weighted_score = a['sentiment'] * a['lots'] * a['weight']

        if target_pillar:
            p = pillars[target_pillar]
            if a['type'] == 'FUT':
                p['fut_score'] += weighted_score
            else:
                p['opt_score'] += weighted_score

    total_market_score = sum((p['fut_score'] + p['opt_score']) for p in pillars.values())
    
    # Simple Trend Logic for Output
    if total_market_score > 1000: trend = "🚀 STRONG BULLISH"
    elif total_market_score > 200: trend = "📈 BULLISH"
    elif total_market_score < -1000: trend = "🔥 STRONG BEARISH"
    elif total_market_score < -200: trend = "📉 BEARISH"
    else: trend = "↔️ NEUTRAL"

    msg = f"📊 **BANK NIFTY MASTER TREND**\nSentiment: **{trend}**\n\n"
    msg += f"Processed {len(current_batch)} alerts in this cycle."

    try:
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')
        logger.info("Summary posted to Telegram.")
    except Exception as e:
        logger.error(f"Failed to post summary: {e}")

def main():
    """Stable entry point to prevent memory/weakref crashes."""
    # Build the application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add handler (filters for text and excludes bot commands)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    
    # Setup background timer (300 seconds = 5 minutes)
    if application.job_queue:
        application.job_queue.run_repeating(process_summary, interval=300, first=10)
    
    logger.info("Bot starting in stable polling mode...")
    # run_polling() is the stable method for Railway
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
