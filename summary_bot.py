import asyncio
import os
import re
import logging
from datetime import datetime
from collections import defaultdict
import telegram
from telegram.ext import Application, MessageHandler, filters

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN", "8537613424:AAFw7FN2KGIncULsgjuv_r3jF5OvIzFLcuM")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "-1003665271298") 
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID", "-1003665271298") 

# --- STATE ---
alerts_buffer = []

def get_sentiment(action_text):
    t = action_text.upper()
    bullish_keywords = ["PUT WRITER", "CALL BUY", "FUTURE BUY", "SHORT COVERING (PE)", "SHORT COVERING ↗️", "LONG BUILDUP"]
    bearish_keywords = ["PUT BUY", "CALL WRITER", "FUTURE SELL", "LONG UNWINDING (PE)", "LONG UNWINDING ↘️", "SHORT BUILDUP"]
    
    # Note: Short covering in CE is generally bullish (shorts exiting), 
    # but for simplicity we follow primary trend.
    if any(k in t for k in bullish_keywords): return 1
    if any(k in t for k in bearish_keywords): return -1
    return 0

def parse_alert(message_text):
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
        data['sentiment'] = get_sentiment(data['action'])
        return data
    except Exception:
        return None

async def message_handler(update, context):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            alerts_buffer.append(parsed)
            logger.info(f"Buffered: {parsed['symbol']} ({parsed['action']})")

async def process_summary(context):
    global alerts_buffer
    if not alerts_buffer:
        return
    
    current_batch = list(alerts_buffer)
    alerts_buffer.clear()
    
    # Weighted Scoring
    # HDFC + ICICI = ~50% weight. Index = ~50% weight.
    total_score = 0
    symbols_data = defaultdict(lambda: {"lots": 0, "bullish": 0, "bearish": 0})
    
    for a in current_batch:
        sym = a['symbol'].upper()
        weight = 0.5 # Default
        if "HDFCBANK" in sym: weight = 1.5
        elif "ICICIBANK" in sym: weight = 1.5
        elif "BANKNIFTY" in sym or "NIFTY" in sym: weight = 2.0
        
        score = a['sentiment'] * a['lots'] * weight
        total_score += score
        
        symbols_data[sym]["lots"] += a['lots']
        if a['sentiment'] > 0: symbols_data[sym]["bullish"] += a['lots']
        elif a['sentiment'] < 0: symbols_data[sym]["bearish"] += a['lots']

    # Determine Trend Label
    if total_score > 500: trend = "🚀 STRONG BULLISH"
    elif total_score > 100: trend = "📈 BULLISH"
    elif total_score < -500: trend = "🔥 STRONG BEARISH"
    elif total_score < -100: trend = "📉 BEARISH"
    else: trend = "↔️ NEUTRAL / RANGEBOUND"

    # Build Message
    msg = f"📊 **MARKET TREND: {trend}**\n"
    msg += f"Score: {int(total_score)} | Trades: {len(current_batch)}\n\n"
    
    # Check HDFC vs ICICI specifically
    hdfc_sent = sum(a['sentiment'] for a in current_batch if "HDFCBANK" in a['symbol'].upper())
    icici_sent = sum(a['sentiment'] for a in current_batch if "ICICIBANK" in a['symbol'].upper())
    
    msg += "🔹 **Key Drivers:**\n"
    msg += f"• HDFC Bank: {'✅ Bullish' if hdfc_sent > 0 else '❌ Bearish' if hdfc_sent < 0 else '➖ Neutral'}\n"
    msg += f"• ICICI Bank: {'✅ Bullish' if icici_sent > 0 else '❌ Bearish' if icici_sent < 0 else '➖ Neutral'}\n\n"
    
    msg += "🔹 **Top Activity (Lots):**\n"
    # Show top 5 symbols
    sorted_syms = sorted(symbols_data.items(), key=lambda x: x[1]['lots'], reverse=True)[:5]
    for s, d in sorted_syms:
        sentiment_icon = "🟢" if d['bullish'] > d['bearish'] else "🔴" if d['bearish'] > d['bullish'] else "⚪"
        msg += f"• {s}: {d['lots']} {sentiment_icon}\n"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')

async def main():
    application = Application.builder().token(BOT_TOKEN).build()
    await application.initialize()
    application.add_handler(MessageHandler(filters.ALL, message_handler))
    
    if application.job_queue:
        # 5 Minute summary as requested (300 seconds)
        application.job_queue.run_repeating(process_summary, interval=300, first=10)

    await application.start()
    await application.updater.start_polling()
    logger.info("Trend Summary Bot started.")
    try:
        while True: await asyncio.sleep(1)
    finally:
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
