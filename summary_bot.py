import asyncio
import os
import re
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN", "8537613424:AAFw7FN2KGIncULsgjuv_r3jF5OvIzFLcuM")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "-1003665271298") 
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID", "-1003665271298") 

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
        data['type'] = 'FUT' if any(x in symbol for x in ["-I", "FUT"]) else 'OPT'

        # Sentiment Weights
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
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = get_alert_details(msg.text)
        if parsed:
            alerts_buffer.append(parsed)
            logger.info(f"Buffered: {parsed['symbol']} | {parsed['action']}")

async def manual_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Allows you to trigger the summary immediately using /summary"""
    await process_summary(context)

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        logger.info("Summary skipped: Buffer empty.")
        return
    
    current_batch = list(alerts_buffer)
    alerts_buffer.clear()
    
    pillars = {
        "BANKNIFTY": {"fut_score": 0, "opt_score": 0, "max_fut": None, "max_opt": None},
        "HDFCBANK": {"fut_score": 0, "opt_score": 0, "max_fut": None, "max_opt": None},
        "ICICIBANK": {"fut_score": 0, "opt_score": 0, "max_fut": None, "max_opt": None}
    }
    
    future_lots_total = option_lots_total = 0

    for a in current_batch:
        sym = a['symbol'].upper()
        target_pillar = next((p for p in pillars if p in sym), None)
        weighted_score = a['sentiment'] * a['lots'] * a['weight']

        if target_pillar:
            p = pillars[target_pillar]
            if a['type'] == 'FUT':
                p['fut_score'] += weighted_score
                future_lots_total += a['lots']
                if not p['max_fut'] or a['lots'] > p['max_fut']['lots']: p['max_fut'] = a
            else:
                p['opt_score'] += weighted_score
                option_lots_total += a['lots']
                if not p['max_opt'] or a['lots'] > p['max_opt']['lots']: p['max_opt'] = a

    total_market_score = sum((p['fut_score'] + p['opt_score']) for p in pillars.values())
    pillar_reports = []
    
    for name, data in pillars.items():
        score = data['fut_score'] + data['opt_score']
        status = "🚀 STRONG BULLISH" if score > 1000 else "✅ BULLISH" if score > 200 else "🔥 STRONG BEARISH" if score < -1000 else "❌ BEARISH" if score < -200 else "↔️ NEUTRAL"
        pillar_reports.append(f"• **{name}**: {status}\n  (Fut: {int(data['fut_score'])} | Opt: {int(data['opt_score'])})")

    trend = "🚀 STRONG BULLISH" if total_market_score > 1500 else "📈 BULLISH" if total_market_score > 300 else "🔥 STRONG BEARISH" if total_market_score < -1500 else "📉 BEARISH" if total_market_score < -300 else "↔️ NEUTRAL"

    total_participation = (future_lots_total + option_lots_total) or 1
    msg = f"📊 **BANK NIFTY MASTER TREND**\nSentiment: **{trend}**\n\n"
    msg += "🔹 **The Three Pillars:**\n" + "\n".join(pillar_reports) + "\n\n"
    msg += f"• ⚡ **Futures**: {int((future_lots_total/total_participation)*100)}% | 📊 **Options**: {int((option_lots_total/total_participation)*100)}%\n"

    try:
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')
        logger.info("Summary sent to Telegram.")
    except Exception as e:
        logger.error(f"Failed to send summary: {e}")

async def main():
    # Build application with JobQueue enabled
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Handlers
    application.add_handler(CommandHandler("summary", manual_summary))
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL | filters.TEXT, message_handler))
    
    # Faster 30-second interval for testing
    if application.job_queue:
        application.job_queue.run_repeating(process_summary, interval=30, first=10)
    
    async with application:
        await application.start()
        await application.updater.start_polling()
        logger.info("Bot started. Listening for alerts...")
        await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
