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
    """Formats numbers into Indian numbering system (Cr/Lakh)"""
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
    
    # Extracting core data fields
    symbol_match = re.search(r"SYMBOL:\s*([\w-]+)", text_upper)
    lot_match = re.search(r"LOTS:\s*(\d+)", text_upper)
    price_match = re.search(r"PRICE:\s*([\d.]+)", text_upper)
    oi_match = re.search(r"OI\s+CHANGE\s*:\s*([+-]?[\d,]+)", text_upper)

    if not (symbol_match and lot_match):
        return None

    symbol_val = symbol_match.group(1)
    lots = int(lot_match.group(1))
    price = float(price_match.group(1)) if price_match else 0
    
    oi_str = oi_match.group(1).replace(",", "").replace("+", "") if oi_match else "0"
    oi_qty = abs(int(oi_str))

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_val), None)
    if not base_symbol:
        return None

    is_option = "CE" in symbol_val or "PE" in symbol_val
    
    if is_option:
        # Options: OI Qty x Price
        final_value = oi_qty * price
    else:
        # Futures: Lots x 100,000
        final_value = lots * 100000

    # Categorization logic
    action_type = None
    if "CALL WRITER" in text_upper: action_type = "CALL_WRITER"
    elif "PUT WRITER" in text_upper: action_type = "PUT_WRITER"
    elif "CALL BUY" in text_upper: action_type = "CALL_BUY"
    elif "PUT BUY" in text_upper: action_type = "PUT_BUY"
    elif "SHORT COVERING" in text_upper:
        if "(CE)" in text_upper: action_type = "CALL_SC"
        elif "(PE)" in text_upper: action_type = "PUT_SC"
        else: action_type = "FUT_SC"
    elif "LONG UNWINDING" in text_upper:
        if "(CE)" in text_upper: action_type = "CALL_UNW"
        elif "(PE)" in text_upper: action_type = "PUT_UNW"
        else: action_type = "FUT_UNW"
    elif "FUTURE BUY" in text_upper: action_type = "FUT_BUY"
    elif "FUTURE SELL" in text_upper: action_type = "FUT_SELL"

    if not action_type: return None

    return {"symbol": base_symbol, "value": final_value, "action_type": action_type, "lots": lots}

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            alerts_buffer.append(parsed)

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        return

    current_batch, alerts_buffer = list(alerts_buffer), []
    # Nested dict to store both total value and total lots
    data = defaultdict(lambda: defaultdict(lambda: {"value": 0, "lots": 0}))
    
    for a in current_batch:
        sym = a["symbol"]
        act = a["action_type"]
        data[sym][act]["value"] += a["value"]
        data[sym][act]["lots"] += a["lots"]

    message = "📊 1 MINUTE VALUE SUMMARY\n\n"
    total_bull_v = total_bear_v = 0
    total_bull_l = total_bear_l = 0

    for symbol in TRACK_SYMBOLS:
        if symbol not in data: continue
        d = data[symbol]
        
        message += f"🔷 {symbol}\n"
        message += "---------------------------\nTYPE\n---------------------------\n"
        
        def get_line(label, key):
            val = d[key]["value"]
            lts = d[key]["lots"]
            return f"{label.ljust(12)}: {format_indian_value(val)} ({lts} Lots)\n"

        message += get_line("CALL WRITER", "CALL_WRITER")
        message += get_line("PUT WRITER", "PUT_WRITER")
        message += get_line("CALL BUY", "CALL_BUY")
        message += get_line("PUT BUY", "PUT_BUY")
        message += get_line("CALL SC", "CALL_SC")
        message += get_line("PUT SC", "PUT_SC")
        message += get_line("CALL UNW", "CALL_UNW")
        message += get_line("PUT UNW", "PUT_UNW")
        message += "---------------------------\n"
        message += get_line("FUT BUY", "FUT_BUY")
        message += get_line("FUT SELL", "FUT_SELL")
        message += get_line("FUT SC", "FUT_SC")
        message += get_line("FUT UNW", "FUT_UNW")
        message += "---------------------------\n\n"

        # Calculate Bullish vs Bearish aggregates
        bull_v = d['PUT_WRITER']['value'] + d['CALL_BUY']['value'] + d['CALL_SC']['value'] + d['PUT_UNW']['value'] + d['FUT_BUY']['value'] + d['FUT_SC']['value']
        bull_l = d['PUT_WRITER']['lots'] + d['CALL_BUY']['lots'] + d['CALL_SC']['lots'] + d['PUT_UNW']['lots'] + d['FUT_BUY']['lots'] + d['FUT_SC']['lots']
        
        bear_v = d['CALL_WRITER']['value'] + d['PUT_BUY']['value'] + d['PUT_SC']['value'] + d['CALL_UNW']['value'] + d['FUT_SELL']['value'] + d['FUT_UNW']['value']
        bear_l = d['CALL_WRITER']['lots'] + d['PUT_BUY']['lots'] + d['PUT_SC']['lots'] + d['CALL_UNW']['lots'] + d['FUT_SELL']['lots'] + d['FUT_UNW']['lots']
        
        total_bull_v += bull_v
        total_bull_l += bull_l
        total_bear_v += bear_v
        total_bear_l += bear_l

    net_v = total_bull_v - total_bear_v
    net_l = total_bull_l - total_bear_l
    bias = "🔥 Bullish Build-up" if net_v > 0 else "🔻 Bearish Build-up" if net_v < 0 else "⚖ Neutral"
    
    message += "📈 NET VALUE VIEW\n\n"
    message += f"Total Bullish : {format_indian_value(total_bull_v)} ({total_bull_l} Lots)\n"
    message += f"Total Bearish : {format_indian_value(total_bear_v)} ({total_bear_l} Lots)\n"
    message += f"Net Dominance : {format_indian_value(net_v)} ({abs(net_l)} Lots)\n\n"
    message += f"Bias: {bias}\n"
    message += "⏳ Validity: Next 1 Minute Only"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message)

def main():
    if not BOT_TOKEN:
        print("Error: SUMMARIZER_BOT_TOKEN not set.")
        return
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    if app.job_queue:
        app.job_queue.run_repeating(process_summary, interval=60, first=10)
    print("Bot is starting summary tracking...")
    app.run_polling()

if __name__ == "__main__":
    main()
