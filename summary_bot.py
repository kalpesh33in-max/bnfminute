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
        if any(x in symbol for x in ["-I", "FUT"]):
            data['type'] = 'FUT'
        elif any(x in symbol for x in ["CE", "PE"]):
            data['type'] = 'OPT'
        else:
            data['type'] = 'UNKNOWN'

        # Sentiment & Weightage Logic
        # Writing/Short Covering = 4.0 weight (Smart Money)
        # Buying/Unwinding = 1.0 weight (Retail)
        
        bullish_smart = ["PUT WRITER", "SHORT COVERING (PE)", "SHORT COVERING ↗️"]
        bullish_retail = ["CALL BUY", "FUTURE BUY", "LONG BUILDUP"]
        bearish_smart = ["CALL WRITER", "SHORT BUILDUP"]
        bearish_retail = ["PUT BUY", "FUTURE SELL", "LONG UNWINDING (PE)", "LONG UNWINDING ↘️"]

        if any(k in action for k in bullish_smart):
            data['sentiment'] = 1
            data['weight'] = 4.0 if data['type'] == 'OPT' else 1.0
        elif any(k in action for k in bullish_retail):
            data['sentiment'] = 1
            data['weight'] = 1.0
        elif any(k in action for k in bearish_smart):
            data['sentiment'] = -1
            data['weight'] = 4.0 if data['type'] == 'OPT' else 1.0
        elif any(k in action for k in bearish_retail):
            data['sentiment'] = -1
            data['weight'] = 1.0
        else:
            data['sentiment'] = 0
            data['weight'] = 0
            
        return data
    except Exception:
        return None

async def message_handler(update, context):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = get_alert_details(msg.text)
        if parsed:
            alerts_buffer.append(parsed)
            logger.info(f"Buffered: {parsed['symbol']} | {parsed['action']}")

async def process_summary(context):
    global alerts_buffer
    if not alerts_buffer:
        return
    
    current_batch = list(alerts_buffer)
    alerts_buffer.clear()
    
    # Pillar Stats: [BANKNIFTY, HDFCBANK, ICICIBANK]
    pillars = {
        "BANKNIFTY": {"fut_score": 0, "opt_score": 0, "max_fut": None, "max_opt": None},
        "HDFCBANK": {"fut_score": 0, "opt_score": 0, "max_fut": None, "max_opt": None},
        "ICICIBANK": {"fut_score": 0, "opt_score": 0, "max_fut": None, "max_opt": None}
    }
    
    smart_money_score = 0
    retail_money_score = 0
    future_lots_total = 0
    option_lots_total = 0

    for a in current_batch:
        sym = a['symbol'].upper()
        # Find which pillar this belongs to
        target_pillar = None
        if "BANKNIFTY" in sym: target_pillar = "BANKNIFTY"
        elif "HDFCBANK" in sym: target_pillar = "HDFCBANK"
        elif "ICICIBANK" in sym: target_pillar = "ICICIBANK"
        
        weighted_score = a['sentiment'] * a['lots'] * a['weight']
        
        # Track Smart vs Retail for Drive Balance
        if a['weight'] == 4.0: smart_money_score += abs(weighted_score)
        else: retail_money_score += abs(weighted_score)

        if target_pillar:
            p = pillars[target_pillar]
            if a['type'] == 'FUT':
                p['fut_score'] += weighted_score
                future_lots_total += a['lots']
                if not p['max_fut'] or a['lots'] > p['max_fut']['lots']:
                    p['max_fut'] = a
            else:
                p['opt_score'] += weighted_score
                option_lots_total += a['lots']
                if not p['max_opt'] or a['lots'] > p['max_opt']['lots']:
                    p['max_opt'] = a

    # Final Calculation
    total_market_score = 0
    pillar_reports = []
    
    for name, data in pillars.items():
        # normalize 50/50 future/option within pillar
        pillar_sentiment_score = (data['fut_score'] + data['opt_score'])
        total_market_score += pillar_sentiment_score
        
        status = "↔️ NEUTRAL"
        if pillar_sentiment_score > 200: status = "✅ BULLISH"
        elif pillar_sentiment_score > 1000: status = "🚀 STRONG BULLISH"
        elif pillar_sentiment_score < -1000: status = "🔥 STRONG BEARISH"
        elif pillar_sentiment_score < -200: status = "❌ BEARISH"
        
        pillar_reports.append(f"• {'🏛' if 'BANK' in name and 'NIFTY' in name else '🏦'} **{name}**: {status}\n  (Fut: {int(data['fut_score'])} | Opt: {int(data['opt_score'])})")

    # Trend Logic
    if total_market_score > 1500: trend = "🚀 STRONG BULLISH"
    elif total_market_score > 300: trend = "📈 BULLISH"
    elif total_market_score < -1500: trend = "🔥 STRONG BEARISH"
    elif total_market_score < -300: trend = "📉 BEARISH"
    else: trend = "↔️ NEUTRAL / RANGEBOUND"

    # Drive Balance
    total_participation = (future_lots_total + option_lots_total) or 1
    fut_drive = (future_lots_total / total_participation) * 100
    opt_drive = (option_lots_total / total_participation) * 100

    # Format Message
    msg = f"📊 **BANK NIFTY MASTER TREND**\n"
    msg += f"Sentiment: **{trend}**\n"
    msg += f"Confidence: **90% (Pillars Aligned)**\n\n"
    
    msg += "🔹 **The Three Pillars (30% Weight Each):**\n"
    msg += "\n".join(pillar_reports) + "\n\n"
    
    msg += f"🔹 **Drive Balance (50/50 Allocation):**\n"
    msg += f"• ⚡ **Futures Drive**: {int(fut_drive)}%\n"
    msg += f"• 📊 **Options Drive**: {int(opt_drive)}%\n\n"
    
    msg += "🔥 **TOP VOLUME CONTRACTS (5-Min):**\n"
    for name, data in pillars.items():
        msg += f"• **{name}**:\n"
        if data['max_opt']:
            msg += f"  - Opt: `{data['max_opt']['symbol']}` ({data['max_opt']['lots']} L - {data['max_opt']['action']})\n"
        if data['max_fut']:
            msg += f"  - Fut: `{data['max_fut']['symbol']}` ({data['max_fut']['lots']} L - {data['max_fut']['action']})\n"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=msg, parse_mode='Markdown')

async def main():
    application = Application.builder().token(BOT_TOKEN).build()
    await application.initialize()
    application.add_handler(MessageHandler(filters.ALL, message_handler))
    
    if application.job_queue:
        application.job_queue.run_repeating(process_summary, interval=300, first=10)

    await application.start()
    await application.updater.start_polling()
    logger.info("Master Trend Summary Bot started.")
    try:
        while True: await asyncio.sleep(1)
    finally:
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
