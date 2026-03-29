import os
import re
import logging
import sys
import pytz
from datetime import datetime, timedelta, time
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# --- CONFIGURATION ---
IST = pytz.timezone('Asia/Kolkata')
# Captures the exact moment the scanner/bot is started or updated
SESSION_START = datetime.now(IST) 

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout
)

BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

# Buffer stores (parsed_data, timestamp)
alerts_buffer = []

TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK", "AXISBANK", "SBIN"]

LOT_SIZES = {
    "BANKNIFTY": 30, # Corrected to 30 as per your instruction
    "HDFCBANK": 550,
    "ICICIBANK": 700,
    "AXISBANK": 625,
    "SBIN": 750
}

def format_money(value):
    if value >= 1e7: return f"{value/1e7:.1f}Cr"
    elif value >= 1e5: return f"{value/1e5:.1f}L"
    else: return f"{value:.0f}"

def classify_strike(strike, option_type, future_price):
    try:
        strike, future_price = float(strike), float(future_price)
        if option_type == "CE": return "ITM" if strike < future_price else "OTM"
        if option_type == "PE": return "ITM" if strike > future_price else "OTM"
    except: pass
    return None

def get_bias_label(net_lots):
    if net_lots > 500: return "🔥 VERY STRONG BULLISH"
    elif net_lots > 150: return "🚀 STRONG BULLISH"
    elif net_lots > 0: return "🟢 Mild Bullish"
    elif net_lots < -500: return "🔥 VERY STRONG BEARISH"
    elif net_lots < -150: return "📉 STRONG BEARISH"
    elif net_lots < 0: return "🔴 Mild Bearish"
    else: return "⚖ Neutral"

def parse_alert(text):
    text_upper = text.upper()
    
    symbol_match = re.search(r"SYMBOL:\s*([^\n\r]+)", text_upper)
    lot_match = re.search(r"LOTS:\s*(\d+)", text_upper)
    price_match = re.search(r"PRICE:\s*([\d.]+)", text_upper)
    future_match = re.search(r"FUTURE\s+PRICE:\s*([\d.]+)", text_upper)

    if not (symbol_match and lot_match): return None

    symbol_full = symbol_match.group(1).strip()
    lots = int(lot_match.group(1))
    price = float(price_match.group(1)) if price_match else None
    future_price = float(future_match.group(1)) if future_match else None

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_full), None)
    if not base_symbol: return None

    opt_match = re.search(r"(\d+)(CE|PE)$", symbol_full)
    zone, option_type = None, None

    if opt_match and future_price:
        strike = opt_match.group(1)
        option_type = opt_match.group(2)
        zone = classify_strike(strike, option_type, future_price)

    is_future = (opt_match is None)

    action_type = None
    if "WRITER" in text_upper:
        if option_type == "CE": action_type = "CALL_WRITER"
        elif option_type == "PE": action_type = "PUT_WRITER"
    elif "CALL BUY" in text_upper: action_type = "CALL_BUY"
    elif "PUT BUY" in text_upper: action_type = "PUT_BUY"
    elif "SHORT COVERING" in text_upper:
        if is_future: action_type = "FUTURE_SC"
        else: action_type = "CALL_SC" if option_type == "CE" else "PUT_SC"
    elif "LONG UNWINDING" in text_upper:
        if is_future: action_type = "FUTURE_UNW"
        else: action_type = "CALL_UNW" if option_type == "CE" else "PUT_UNW"
    elif "FUTURE BUY" in text_upper or "BUY (LONG)" in text_upper:
        action_type = "FUTURE_BUY"
    elif "FUTURE SELL" in text_upper or "SELL (SHORT)" in text_upper:
        action_type = "FUTURE_SELL"

    if not action_type: return None

    return {
        "symbol": base_symbol,
        "lots": lots,
        "zone": zone,
        "action_type": action_type,
        "future": future_price,
        "price": price
    }

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            alerts_buffer.append((parsed, datetime.now(IST)))

async def run_report(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    now = datetime.now(IST)
    
    # STRICT MARKET HOURS CHECK (9:15 AM to 3:45 PM IST)
    current_time_int = now.hour * 100 + now.minute
    if current_time_int < 915 or current_time_int > 1545:
        logging.info("⏳ Market Closed. Skipping Telegram report.")
        return

    # DAILY RESET / TODAY ONLY FILTER: 
    # Remove any alerts that are not from the current calendar day (Today)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    alerts_buffer = [a for a in alerts_buffer if a[1] >= today_start]
    
    if not alerts_buffer: 
        logging.info("📝 No alerts collected for today yet. Waiting...")
        return

    # Calculate duration and start time based on the oldest alert in the buffer
    oldest_time = min(a[1] for a in alerts_buffer)
    start_time_str = oldest_time.strftime("%I:%M %p")
    duration_mins = int((now - oldest_time).total_seconds() / 60)
    if duration_mins < 1: duration_mins = 1

    logging.info(f"🕒 Generating daily cumulative report ({duration_mins} mins).")

    # Process EVERYTHING currently in the buffer (which is now Today only)
    batch = [a[0] for a in alerts_buffer]
    
    opt_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    opt_turn = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    fut_data = defaultdict(lambda: defaultdict(int))
    fut_turn = defaultdict(lambda: defaultdict(float))
    last_future = {}

    for alert in batch:
        sym, act, zone, lots, price = alert["symbol"], alert["action_type"], alert["zone"], alert["lots"], alert["price"]
        lot_size = LOT_SIZES.get(sym, 1)
        if alert["future"]: last_future[sym] = alert["future"]

        if "FUTURE" not in act:
            z = zone if zone else "OTM"
            opt_data[sym][act][z] += lots
            if "WRITER" in act or "_SC" in act:
                opt_turn[sym][act][z] += (lots * 125000)
            else:
                if price: opt_turn[sym][act][z] += (lots * price * lot_size)
        else:
            fut_data[sym][act] += lots
            fut_turn[sym][act] += (lots * 175000)

    message = f"<pre>\n📊 DAY CUMULATIVE FLOW ({duration_mins} MINS)\n"
    message += f"Started At: {start_time_str}\n\n"

    for symbol in TRACK_SYMBOLS:
        if symbol not in opt_data and symbol not in fut_data: continue
        message += f"💎 {symbol} (FUT: {last_future.get(symbol,'N/A')})\n"
        
        if symbol in opt_data:
            message += "--- OPTIONS FLOW ---\n"
            message += f"{'TYPE':8}{'ITM':>14}{'OTM':>14}{'TOT':>14}\n"
            message += "-" * 50 + "\n"
            
            s_bull_lots, s_bear_lots = 0, 0
            s_bull_turnover, s_bear_turnover = 0, 0
            for act in opt_data[symbol]:
                itm_l, otm_l = opt_data[symbol][act]["ITM"], opt_data[symbol][act]["OTM"]
                itm_t, otm_t = opt_turn[symbol][act]["ITM"], opt_turn[symbol][act]["OTM"]
                tot_l, tot_t = itm_l + otm_l, itm_t + otm_t
                
                # ORIGINAL LOGIC UNCHANGED
                if act in ["PUT_WRITER", "CALL_BUY", "CALL_SC", "PUT_UNW"]: 
                    s_bull_lots += tot_l
                    s_bull_turnover += tot_t
                else: 
                    s_bear_lots += tot_l
                    s_bear_turnover += tot_t
                
                itm_s = f"{itm_l}({format_money(itm_t)})"
                otm_s = f"{otm_l}({format_money(otm_t)})"
                tot_s = f"{tot_l}({format_money(tot_t)})"
                display_act = act.replace("CALL_WRITER","CALL_WR").replace("PUT_WRITER","PUT_WR")
                message += f"{display_act:8}{itm_s:>14}{otm_s:>14}{tot_s:>14}\n"
            
            message += f"Option Bias: {get_bias_label(s_bull_lots - s_bear_lots)}\n"
            message += f"Bullish Turn: {format_money(s_bull_turnover)}\n"
            message += f"Bearish Turn: {format_money(s_bear_turnover)}\n\n"

        if symbol in fut_data:
            message += "---- FUTURES FLOW ----\n"
            f_bull_lots, f_bear_lots = 0, 0
            f_bull_turnover, f_bear_turnover = 0, 0
            for act in fut_data[symbol]:
                lots, turn = fut_data[symbol][act], fut_turn[symbol][act]
                if act in ["FUTURE_BUY", "FUTURE_SC"]: 
                    f_bull_lots += lots
                    f_bull_turnover += turn
                elif act in ["FUTURE_SELL", "FUTURE_UNW"]: 
                    f_bear_lots += lots
                    f_bear_turnover += turn
                message += f"{act:12} : {lots} lots ({format_money(turn)})\n"
            
            message += f"Future Bias: {get_bias_label(f_bull_lots - f_bear_lots)}\n"
        
        message += "========================================\n\n"

    message += f"Validity: Next 15 Min\n"
    message += "</pre>"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message, parse_mode="HTML")

def main():
    if not BOT_TOKEN:
        print("Error: SUMMARIZER_BOT_TOKEN not set.")
        return
        
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    
    if app.job_queue:
        # Triggers strictly every 15 minutes (900 seconds)
        app.job_queue.run_repeating(run_report, interval=900, first=900)
        
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
