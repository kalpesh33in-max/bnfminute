import os
import re
import logging
import sys
import pytz
import json
from datetime import datetime, timedelta, time
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# --- CONFIGURATION ---
IST = pytz.timezone('Asia/Kolkata')
BUFFER_FILE = "alerts_buffer.json"
TOTALS_FILE = "daily_totals.json"

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout
)

BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

# Buffer for RAW alerts (Cleared every 15 mins)
alerts_buffer = []

# Persistent Daily Totals
daily_totals = {
    "opt_data": {}, # {symbol: {action: {zone: lots}}}
    "opt_turn": {}, # {symbol: {action: {zone: turnover}}}
    "fut_data": {}, # {symbol: {action: lots}}
    "fut_turn": {}, # {symbol: {action: turnover}}
    "last_future": {},
    "start_time": None,
    "last_reset_date": None
}

def save_state():
    try:
        state = {"buffer": alerts_buffer, "totals": daily_totals}
        with open(TOTALS_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        logging.error(f"Error saving state: {e}")

def load_state():
    global alerts_buffer, daily_totals
    if os.path.exists(TOTALS_FILE):
        try:
            with open(TOTALS_FILE, "r") as f:
                state = json.load(f)
            
            now = datetime.now(IST)
            today_str = now.strftime("%Y-%m-%d")
            
            # Load Totals if they belong to today
            if state.get("totals", {}).get("last_reset_date") == today_str:
                daily_totals = state["totals"]
                logging.info("Loaded today's running totals from disk.")
            else:
                daily_totals["last_reset_date"] = today_str
                logging.info("Starting fresh running totals for today.")

            # Load Buffer if it contains today's alerts
            alerts_buffer = [a for a in state.get("buffer", []) if a[1].startswith(today_str)]
            logging.info(f"Loaded {len(alerts_buffer)} raw alerts into buffer.")
        except Exception as e:
            logging.error(f"Error loading state: {e}")

def update_daily_totals(new_batch):
    global daily_totals
    for alert in new_batch:
        sym, act, zone, lots, price = alert["symbol"], alert["action_type"], alert["zone"], alert["lots"], alert["price"]
        lot_size = LOT_SIZES.get(sym, 1)
        if alert["future"]: daily_totals["last_future"][sym] = alert["future"]
        
        # Initialize nested dicts if needed
        for category in ["opt_data", "opt_turn"]:
            if sym not in daily_totals[category]: daily_totals[category][sym] = {}
            if act not in daily_totals[category][sym]: daily_totals[category][sym][act] = {"ITM": 0, "OTM": 0}

        for category in ["fut_data", "fut_turn"]:
            if sym not in daily_totals[category]: daily_totals[category][sym] = {}
            if act not in daily_totals[category][sym]: daily_totals[category][sym][act] = 0

        if "FUTURE" not in act:
            z = zone if zone else "OTM"
            daily_totals["opt_data"][sym][act][z] += lots
            if "WRITER" in act or "_SC" in act:
                daily_totals["opt_turn"][sym][act][z] += (lots * 125000)
            else:
                if price: daily_totals["opt_turn"][sym][act][z] += (lots * price * lot_size)
        else:
            daily_totals["fut_data"][sym][act] += lots
            daily_totals["fut_turn"][sym][act] += (lots * 175000)

TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK", "AXISBANK", "SBIN"]

LOT_SIZES = {
    "BANKNIFTY": 30, 
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
    try:
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
    except Exception as e:
        logging.error(f"Error parsing alert: {e}")
        return None

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = update.channel_post or update.message
        if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
            parsed = parse_alert(msg.text)
            if parsed:
                alerts_buffer.append((parsed, datetime.now(IST).isoformat()))
                save_state()
    except Exception as e:
        logging.error(f"Error in message handler: {e}")

async def run_report(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer, daily_totals
    try:
        now = datetime.now(IST)
        
        # STRICT MARKET HOURS CHECK (9:15 AM to 3:45 PM IST)
        current_time_int = now.hour * 100 + now.minute
        if current_time_int < 915 or current_time_int > 1545:
            logging.info("⏳ Market Closed. Skipping Telegram report.")
            return

        # 1. Update Daily Totals with the NEW batch and CLEAR buffer
        if alerts_buffer:
            batch = [a[0] for a in alerts_buffer]
            
            # Record start time if this is the first alert of the day
            if not daily_totals["start_time"]:
                oldest_time_str = min(a[1] for a in alerts_buffer)
                daily_totals["start_time"] = datetime.fromisoformat(oldest_time_str).strftime("%I:%M %p")

            update_daily_totals(batch)
            alerts_buffer = [] # CLEAR MEMORY
            save_state()
            logging.info("📈 Added new alerts to running totals and cleared buffer.")
        
        if not daily_totals["opt_data"] and not daily_totals["fut_data"]:
            logging.info("📝 No data for today yet. Waiting...")
            return

        logging.info(f"🕒 Generating Day Cumulative Report.")

        message = f"<pre>\n📊 DAY CUMULATIVE FLOW\n"
        message += f"Started At: {daily_totals['start_time'] or 'N/A'}\n"
        message += f"Current Time: {now.strftime('%I:%M %p')}\n\n"

        for symbol in TRACK_SYMBOLS:
            if symbol not in daily_totals["opt_data"] and symbol not in daily_totals["fut_data"]: continue
            message += f"💎 {symbol} (FUT: {daily_totals['last_future'].get(symbol,'N/A')})\n"
            
            if symbol in daily_totals["opt_data"]:
                message += "--- OPTIONS FLOW ---\n"
                message += f"{'TYPE':8}{'ITM':>14}{'OTM':>14}{'TOT':>14}\n"
                message += "-" * 50 + "\n"
                
                s_bull_lots, s_bear_lots = 0, 0
                s_bull_turnover, s_bear_turnover = 0, 0
                for act in daily_totals["opt_data"][symbol]:
                    itm_l = daily_totals["opt_data"][symbol][act].get("ITM", 0)
                    otm_l = daily_totals["opt_data"][symbol][act].get("OTM", 0)
                    itm_t = daily_totals["opt_turn"][symbol][act].get("ITM", 0)
                    otm_t = daily_totals["opt_turn"][symbol][act].get("OTM", 0)
                    tot_l, tot_t = itm_l + otm_l, itm_t + otm_t
                    
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

            if symbol in daily_totals["fut_data"]:
                message += "---- FUTURES FLOW ----\n"
                f_bull_lots, f_bear_lots = 0, 0
                f_bull_turnover, f_bear_turnover = 0, 0
                for act in daily_totals["fut_data"][symbol]:
                    lots = daily_totals["fut_data"][symbol][act]
                    turn = daily_totals["fut_turn"][symbol][act]
                    if act in ["FUTURE_BUY", "FUTURE_SC"]: 
                        f_bull_lots += lots
                        f_bull_turnover += turn
                    elif act in ["FUTURE_SELL", "FUTURE_UNW"]: 
                        f_bear_lots += lots
                        f_bear_turnover += turn
                    message += f"{act:12} : {lots} lots ({format_money(turn)})\n"
                
                message += f"Future Bias: {get_bias_label(f_bull_lots - f_bear_lots)}\n"
            
            message += "========================================\n\n"

        message += f"Validity: Next 15 Min (Mem-Optimized)\n"
        message += "</pre>"

        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error in report generator: {e}")
        try:
            await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=f"⚠️ *Summary Bot Error:* {str(e)}")
        except: pass

def main():
    if not BOT_TOKEN:
        print("Error: SUMMARIZER_BOT_TOKEN not set.")
        return
        
    load_state()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    
    if app.job_queue:
        app.job_queue.run_repeating(run_report, interval=900, first=900)
        
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
