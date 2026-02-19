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

def format_rs_short(value):
    """Converts Rupee values to short string format (L/Cr) for table alignment"""
    if value == 0: return "0"
    abs_val = abs(value)
    if abs_val >= 10000000: return f"{value / 10000000:.2f}Cr"
    if abs_val >= 100000: return f"{value / 100000:.2f}L"
    if abs_val >= 1000: return f"{value / 1000:.1f}k"
    return str(int(value))

def get_moneyness(symbol, spot, action_side):
    """Identifies ITM/ATM/OTM by comparing strike to current Future Price"""
    strike_match = re.search(r"(\d{4,6})", symbol)
    if not strike_match or spot == 0: return "OTM"
    strike = float(strike_match.group(1))
    
    # Standard ATM threshold for BankNifty
    if abs(strike - spot) <= 50: return "ATM"
    
    if "CE" in symbol:
        return "ITM" if strike < spot else "OTM"
    else: # PE
        return "ITM" if strike > spot else "OTM"

def parse_alert(text):
    """Extracts data and calculates monetary weight"""
    if not text: return None
    text_upper = text.upper()
    
    symbol_match = re.search(r"SYMBOL:\s*([\w-]+)", text_upper)
    lot_match = re.search(r"LOTS:\s*(\d+)", text_upper)
    price_match = re.search(r"PRICE:\s*([\d.]+)", text_upper)
    oi_match = re.search(r"OI\s+CHANGE\s*:\s*([+-]?[\d,]+)", text_upper)
    spot_match = re.search(r"FUTURE PRICE:\s*([\d.]+)", text_upper)

    if not (symbol_match and lot_match): return None

    symbol_val = symbol_match.group(1)
    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_val), None)
    if not base_symbol: return None

    lots = int(lot_match.group(1))
    price = float(price_match.group(1)) if price_match else 0
    spot = float(spot_match.group(1)) if spot_match else 0
    oi_qty = abs(int(oi_match.group(1).replace(",", "").replace("+", ""))) if oi_match else 0

    # Valuation Math
    is_option = any(x in symbol_val for x in ["CE", "PE"])
    val = (oi_qty * price) if is_option else (lots * 100000)
    
    money_cat = get_moneyness(symbol_val, spot, symbol_val) if is_option else "TOT"

    # Action detection
    action = None
    if "CALL WRITER" in text_upper: action = "CALL WRITER"
    elif "PUT WRITER" in text_upper: action = "PUT WRITER"
    elif "CALL BUY" in text_upper: action = "CALL BUY"
    elif "PUT BUY" in text_upper: action = "PUT BUY"
    elif "SHORT COVERING" in text_upper:
        action = "CALL SC" if "(CE)" in text_upper else "PUT SC" if "(PE)" in text_upper else "FUT SC"
    elif "LONG UNWINDING" in text_upper:
        action = "CALL UNW" if "(CE)" in text_upper else "PUT UNW" if "(PE)" in text_upper else "FUT UNW"
    elif "FUTURE BUY" in text_upper: action = "FUT BUY"
    elif "FUTURE SELL" in text_upper: action = "FUT SELL"

    if not action: return None
    return {"symbol": base_symbol, "val": val, "cat": money_cat, "action": action, "spot": spot}

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer: return
    
    current_batch, alerts_buffer = list(alerts_buffer), []
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    spots = {}

    for a in current_batch:
        if a:
            data[a["symbol"]][a["action"]][a["cat"]] += a["val"]
            spots[a["symbol"]] = a["spot"]

    message = "📊 1-MIN VALUATION REPORT\n\n"
    total_bull = total_bear = 0

    for sym in TRACK_SYMBOLS:
        if sym not in data: continue
        d = data[sym]
        message += f"🔷 {sym} (FUT: {spots.get(sym, 0)})\n"
        message += "-----------------------------------------------\n"
        message += "TYPE           ITM      ATM      OTM      TOT\n"
        message += "-----------------------------------------------\n"
        
        opt_acts = ["CALL WRITER", "PUT WRITER", "CALL BUY", "PUT BUY", "CALL SC", "PUT SC", "CALL UNW", "PUT UNW"]
        for act in opt_acts:
            itm, atm, otm = d[act]["ITM"], d[act]["ATM"], d[act]["OTM"]
            tot = itm + atm + otm
            message += f"{act:<12}: {format_rs_short(itm):<8} {format_rs_short(atm):<8} {format_rs_short(otm):<8} {format_rs_short(tot)}\n"
        
        message += "-----------------------------------------------\n"
        for fact in ["FUT BUY", "FUT SELL", "FUT SC", "FUT UNW"]:
            message += f"{fact:<12}: {format_rs_short(d[fact]['TOT'])}\n"
        message += "-----------------------------------------------\n\n"

        total_bull += (d["PUT WRITER"]["TOT"] + d["CALL BUY"]["TOT"] + d["CALL SC"]["TOT"] + d["PUT UNW"]["TOT"] + d["FUT BUY"]["TOT"] + d["FUT SC"]["TOT"])
        total_bear += (d["CALL WRITER"]["TOT"] + d["PUT BUY"]["TOT"] + d["PUT SC"]["TOT"] + d["CALL UNW"]["TOT"] + d["FUT SELL"]["TOT"] + d["FUT UNW"]["TOT"])

    net = total_bull - total_bear
    message += f"📈 NET MONETARY VIEW\nBull: {format_rs_short(total_bull)} | Bear: {format_rs_short(total_bear)}\n"
    message += f"Dominance: {format_rs_short(net)}\n"
    message += f"Bias: {'🔥 Bullish' if net > 0 else '🔻 Bearish' if net < 0 else '⚖ Neutral'}"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=f"<code>{message}</code>", parse_mode="HTML")

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    # Fixed the lambda to prevent NoneType errors found in your logs
    async def safe_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.message and update.message.text:
            parsed = parse_alert(update.message.text)
            if parsed: alerts_buffer.append(parsed)

    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), safe_handler))
    if app.job_queue: app.job_queue.run_repeating(process_summary, interval=60)
    app.run_polling()

if __name__ == "__main__": main()
