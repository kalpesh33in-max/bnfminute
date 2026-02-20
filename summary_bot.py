import os
import re
import logging
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# Setup Logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

alerts_buffer = []
TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK"]

def format_indian_value(val):
    abs_val = abs(val)
    if abs_val >= 10000000:
        formatted = f"{val / 10000000:.2f} Cr"
    elif abs_val >= 100000:
        formatted = f"{val / 100000:.2f} Lakh"
    else:
        formatted = f"{val:,.0f}"
    return formatted

def parse_alert(text):
    text_upper = text.upper()
    symbol_match = re.search(r"SYMBOL:\s*([\w-]+)", text_upper)
    lot_match = re.search(r"LOTS:\s*(\d+)", text_upper)
    price_match = re.search(r"PRICE:\s*([\d.]+)", text_upper)
    oi_match = re.search(r"OI CHANGE:\s*([\d,]+)", text_upper)
    
    if not symbol_match or not lot_match: return None
    
    symbol = symbol_match.group(1)
    if symbol not in TRACK_SYMBOLS: return None
    
    lots = int(lot_match.group(1))
    price = float(price_match.group(1)) if price_match else 0
    oi_change = int(oi_match.group(1).replace(',', '')) if oi_match else 0

    category = "OTHERS"
    if "CALL WRITER" in text_upper: category = "CALL_WRITER"
    elif "PUT WRITER" in text_upper: category = "PUT_WRITER"
    elif "CALL BUYER" in text_upper: category = "CALL_BUYER"
    elif "PUT BUYER" in text_upper: category = "PUT_BUYER"
    elif "CALL SHORT COVERING" in text_upper: category = "CALL_SC"
    elif "PUT SHORT COVERING" in text_upper: category = "PUT_SC"
    elif "CALL UNWINDING" in text_upper: category = "CALL_UNW"
    elif "PUT UNWINDING" in text_upper: category = "PUT_UNW"
    elif "FUTURE BUY" in text_upper: category = "FUT_BUY"
    elif "FUTURE SELL" in text_upper: category = "FUT_SELL"
    elif "FUTURE SHORT COVERING" in text_upper: category = "FUT_SC"
    elif "FUTURE UNWINDING" in text_upper: category = "FUT_UNW"

    value = 0
    if "FUTURE" in category:
        value = lots * 100000
    else:
        value = price * oi_change

    return {"symbol": symbol, "category": category, "lots": lots, "value": value}

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.channel_post.chat_id) == TARGET_CHANNEL_ID:
        parsed = parse_alert(update.channel_post.text)
        if parsed:
            alerts_buffer.append(parsed)

async def send_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer: return

    current_data = alerts_buffer.copy()
    alerts_buffer = []

    summary = {s: defaultdict(lambda: {'value': 0, 'lots': 0}) for s in TRACK_SYMBOLS}
    for item in current_data:
        summary[item['symbol']][item['category']]['value'] += item['value']
        summary[item['symbol']][item['category']]['lots'] += item['lots']

    message = "📊 *VALUE SUMMARY REPORT*\n\n"
    total_bull_v, total_bear_v = 0, 0
    total_bull_l, total_bear_l = 0, 0

    for symbol, categories in summary.items():
        message += f"🔹 *{symbol}*\n"
        for cat, data in categories.items():
            if data['lots'] > 0:
                message += f"{cat.replace('_',' ')} : {format_indian_value(data['value'])} ({data['lots']} Lots)\n"
        
        # Sentiment logic
        bull_v = categories['PUT_WRITER']['value'] + categories['CALL_BUYER']['value'] + categories['CALL_SC']['value'] + \
                 categories['PUT_UNW']['value'] + categories['FUT_BUY']['value'] + categories['FUT_SC']['value']
        bull_l = categories['PUT_WRITER']['lots'] + categories['CALL_BUYER']['lots'] + categories['CALL_SC']['lots'] + \
                 categories['PUT_UNW']['lots'] + categories['FUT_BUY']['lots'] + categories['FUT_SC']['lots']
        
        bear_v = categories['CALL_WRITER']['value'] + categories['PUT_BUY']['value'] + categories['PUT_SC']['value'] + \
                 categories['CALL_UNW']['value'] + categories['FUT_SELL']['value'] + categories['FUT_UNW']['value']
        bear_l = categories['CALL_WRITER']['lots'] + categories['PUT_BUY']['lots'] + categories['PUT_SC']['lots'] + \
                 categories['CALL_UNW']['lots'] + categories['FUT_SELL']['lots'] + categories['FUT_UNW']['lots']
        
        total_bull_v += bull_v
        total_bear_v += bear_v
        total_bull_l += bull_l
        total_bear_l += bear_l
        message += "\n"

    net_v = total_bull_v - total_bear_v
    net_l = total_bull_l - total_bear_l
    bias = "🚀 Bullish Build-up" if net_v > 0 else "📉 Bearish Build-up" if net_v < 0 else "⚖️ Neutral"
    
    message += "📈 *NET VALUE VIEW*\n\n"
    message += f"Total Bullish : {format_indian_value(total_bull_v)} ({total_bull_l} Lots)\n"
    message += f"Total Bearish : {format_indian_value(total_bear_v)} ({total_bear_l} Lots)\n"
    message += f"Net Dominance : {format_indian_value(net_v)} ({abs(net_l)} Lots)\n\n"
    message += f"Bias: {bias}\n"
    message += "⏳ Validity: Next 2 Minutes Only" # Updated to reflect 120s interval

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message, parse_mode='Markdown')

def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Listen for messages in the source channel
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_message))
    
    # SCHEDULER: Changed interval from 60 to 120 seconds
    application.job_queue.run_repeating(send_summary, interval=120, first=10)
    
    application.run_polling()

if __name__ == "__main__":
    main()
