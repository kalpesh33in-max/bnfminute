import asyncio
import os
import re
import logging
from datetime import datetime
from collections import defaultdict
import telegram
from telegram.ext import Application, MessageHandler, filters

# Enable logging to see what's happening in Railway Deploy Logs
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BOT_TOKEN = os.environ.get("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.environ.get("TARGET_CHANNEL_ID") 
SUMMARY_CHAT_ID = os.environ.get("SUMMARY_CHAT_ID")   

# --- STATE ---
alerts_buffer = []

# --- CONSTANTS ---
SUMMARY_INTERVAL_MINUTES = 5

# --- LOGIC ---

def parse_alert(message_text):
    """
    Parses the text of an alert message. Fixed SyntaxError on 'action'.
    """
    patterns = {
        'strength': r"(🚀 BLAST 🚀|🌟 AWESOME|✅ VERY GOOD|👍 GOOD|🆗 OK)",
        'action': r"🚨 (.*)", # Fixed: Removed the breaking newline
        'symbol': r"Symbol: ([\w-]+)",
        'lots': r"LOTS: (\d+)",
        'oi_change': r"OI CHANGE\s+:\s*([+-]?[0-9,]+)",
    }

    data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, message_text)
        if match:
            data[key] = match.group(1).strip()
        else:
            if key != 'strength':
                return None
    
    # Process numeric values
    try:
        data['lots'] = int(data['lots'])
        data['oi_change'] = int(data['oi_change'].replace(',', ''))
    except (ValueError, KeyError):
        return None

    # Instrument type logic
    if data['symbol'].endswith('-I'):
        data['instrument'] = 'Future'
        data['base_symbol'] = data['symbol'].replace('-I', '')
    else:
        data['instrument'] = 'Option'
        base_match = re.match(r'^([A-Z]+)', data['symbol'])
        data['base_symbol'] = base_match.group(1) if base_match else "UNKNOWN"

    # Sentiment Logic
    action = data['action'].upper()
    is_call = 'CE' in data['symbol'].upper()
    is_put = 'PE' in data['symbol'].upper()

    data['sentiment'] = 'neutral'
    if 'BUY' in action:
        data['sentiment'] = 'bullish'
    elif 'SELL' in action or 'WRITER' in action:
        data['sentiment'] = 'bearish'
    elif 'UNWINDING' in action:
        data['sentiment'] = 'bearish' if is_call else 'bullish'
    elif 'COVERING' in action:
        data['sentiment'] = 'bullish' if is_call else 'bearish'

    return data

async def message_handler(update, context):
    if not update.message or not update.message.text:
        return
        
    if str(update.message.chat_id) == str(TARGET_CHANNEL_ID):
        parsed_data = parse_alert(update.message.text)
        if parsed_data:
            alerts_buffer.append(parsed_data)
            logger.info(f"Buffered alert for {parsed_data['symbol']}")

async def process_summary(context: telegram.ext.ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        logger.info("No alerts in buffer. Skipping summary.")
        return

    processing_alerts = list(alerts_buffer)
    alerts_buffer.clear()

    future_sentiment = defaultdict(lambda: defaultdict(int))
    option_sentiment = defaultdict(int)
    top_trades = sorted(processing_alerts, key=lambda x: x['lots'], reverse=True)[:3]

    for alert in processing_alerts:
        if alert['instrument'] == 'Future':
            future_sentiment[alert['base_symbol']][alert['sentiment']] += alert['lots']
        elif alert['base_symbol'] == 'BANKNIFTY':
            option_sentiment[alert['sentiment']] += alert['lots']

    # Simple Market Narrative
    hdfc_net = future_sentiment['HDFCBANK']['bullish'] - future_sentiment['HDFCBANK']['bearish']
    icici_net = future_sentiment['ICICIBANK']['bullish'] - future_sentiment['ICICIBANK']['bearish']
    
    if hdfc_net > 0 and icici_net > 0:
        narrative = "BANKNIFTY is **bullish**, supported by HDFCBANK and ICICIBANK."
    elif hdfc_net < 0 and icici_net < 0:
        narrative = "BANKNIFTY is **bearish**, dragged down by HDFCBANK and ICICIBANK."
    else:
        narrative = "BANKNIFTY sentiment is mixed or diverging."

    summary_message = f"**--- 📊 5 Minute Market Summary ---**\n\n"
    summary_message += f"**Narrative:** {narrative}\n\n"
    summary_message += "**--- Futures ---**\n"
    
    for symbol, sents in future_sentiment.items():
        net = sents['bullish'] - sents['bearish']
        summary_message += f"• {symbol}: {'📈' if net > 0 else '📉'} ({net:+,} Lots)\n"

    summary_message += f"\n**--- BANKNIFTY Options ---**\n"
    summary_message += f"• Bullish: {option_sentiment['bullish']:,} | Bearish: {option_sentiment['bearish']:,}\n"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=summary_message, parse_mode='Markdown')

async def main():
    if not all([BOT_TOKEN, TARGET_CHANNEL_ID, SUMMARY_CHAT_ID]):
        logger.error("Missing Environment Variables!")
        return

    app = Application.builder().token(BOT_TOKEN).build()

    # Register handlers
    app.add_handler(MessageHandler(filters.Chat(chat_id=int(TARGET_CHANNEL_ID)), message_handler))

    # Schedule 5-minute job
    app.job_queue.run_repeating(process_summary, interval=SUMMARY_INTERVAL_MINUTES * 60, first=60)

    logger.info("Bot is running...")
    await app.run_polling()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
