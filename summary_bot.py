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
    
    # Handle OI Change (removing commas and converting to absolute magnitude)
    oi_str = oi_match.group(1).replace(",", "").replace("+", "") if oi_match else "0"
    oi_qty = abs(int(oi_str))

    # Identify base symbol
    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_val), None)
    if not base_symbol:
        return None

    # Identify if it is an Option or Future
    is_option = "CE" in symbol_val or "PE" in symbol_val
    
    # --- NEW CALCULATION LOGIC ---
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

    return {"symbol": base_symbol, "value": final_value, "action_type": action_type}

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
    data = defaultdict(lambda: defaultdict(int))
    for a in current_batch:
        data[a["symbol"]][a["action_type"]] += a["value"]

    message = "📊 1 MINUTE VALUE SUMMARY\n\n"
    total_bull = total_bear = 0

    for symbol in TRACK_SYMBOLS:
        if symbol not in data: continue
        d = data[symbol]
        
        message += f"🔷 {symbol}\n"
        message += "---------------------------\nTYPE\n---------------------------\n"
        # Displaying formatted numbers with commas
        message += f"CALL WRITER : {d['CALL_WRITER']:,.0f}\n"
        message += f"PUT WRITER  : {d['PUT_WRITER']:,.0f}\n"
        message += f"CALL BUY    : {d['CALL_BUY']:,.0f}\n"
        message += f"PUT BUY     : {d['PUT_BUY']:,.0f}\n"
        message += f"CALL SC     : {d['CALL_SC']:,.0f}\n"
        message += f"PUT SC      : {d['PUT_SC']:,.0f}\n"
        message += f"CALL UNW    : {d['CALL_UNW']:,.0f}\n"
        message += f"PUT UNW     : {d['PUT_UNW']:,.0f}\n"
        message += "---------------------------\n"
        message += f"FUT BUY     : {d['FUT_BUY']:,.0f}\n"
        message += f"FUT SELL    : {d['FUT_SELL']:,.0f}\n"
        message += f"FUT SC      : {d['FUT_SC']:,.0f}\n"
        message += f"FUT UNW     : {d['FUT_UNW']:,.0f}\n"
        message += "---------------------------\n\n"

        bull = d['PUT_WRITER'] + d['CALL_BUY'] + d['CALL_SC'] + d['PUT_UNW'] + d['FUT_BUY'] + d['FUT_SC']
        bear = d['CALL_WRITER'] + d['PUT_BUY'] + d['PUT_SC'] + d['CALL_UNW'] + d['FUT_SELL'] + d['FUT_UNW']
        total_bull += bull
        total_bear += bear

    net = total_bull - total_bear
    bias = "🔥 Bullish Build-up" if net > 0 else "🔻 Bearish Build-up" if net < 0 else "⚖ Neutral"
    
    message += "📈 NET VALUE VIEW\n\n"
    message += f"Total Bullish : {total_bull:,.0f}\n"
    message += f"Total Bearish : {total_bear:,.0f}\n"
    message += f"Net Dominance : {net:,.0f}\n\n"
    message += f"Bias: {bias}\n"
    message += "⏳ Validity: Next 1 Minute Only"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message)

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    if app.job_queue:
        app.job_queue.run_repeating(process_summary, interval=60, first=10)
    app.run_polling()

if __name__ == "__main__":
    main()
