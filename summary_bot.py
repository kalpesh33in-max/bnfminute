import os
import re
import logging
import sys
import pytz
from datetime import datetime, timedelta, time
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# ---------------- CONFIG ---------------- #

IST = pytz.timezone("Asia/Kolkata")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout
)

BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

alerts_buffer = []

TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK", "AXISBANK", "SBIN"]

LOT_SIZES = {
    "BANKNIFTY": 30,
    "HDFCBANK": 550,
    "ICICIBANK": 700,
    "AXISBANK": 625,
    "SBIN": 750
}

# ---------------- HELPERS ---------------- #

def format_money(value):
    if value >= 1e7:
        return f"{value/1e7:.2f}Cr"
    elif value >= 1e5:
        return f"{value/1e5:.2f}L"
    else:
        return f"{value:.0f}"


def classify_strike(strike, option_type, future_price):
    try:
        strike = float(strike)
        future_price = float(future_price)

        if option_type == "CE":
            return "ITM" if strike < future_price else "OTM"
        if option_type == "PE":
            return "ITM" if strike > future_price else "OTM"

    except:
        return None


def get_bias_label(net_lots):

    if net_lots > 1500:
        return "🔥 VERY STRONG BULLISH"
    elif net_lots > 500:
        return "🚀 STRONG BULLISH"
    elif net_lots > 0:
        return "🟢 Mild Bullish"
    elif net_lots < -1500:
        return "🔥 VERY STRONG BEARISH"
    elif net_lots < -500:
        return "📉 STRONG BEARISH"
    elif net_lots < 0:
        return "🔴 Mild Bearish"
    else:
        return "⚖ Neutral"


# ---------------- ALERT PARSER ---------------- #

def parse_alert(text):

    text_upper = text.upper()

    symbol_match = re.search(r"SYMBOL:\s*([\w-]+)", text_upper)
    lot_match = re.search(r"LOTS:\s*(\d+)", text_upper)
    price_match = re.search(r"PRICE:\s*([\d.]+)", text_upper)
    future_match = re.search(r"FUTURE\s*PRICE:\s*([\d.]+)", text_upper)

    if not (symbol_match and lot_match):
        return None

    symbol_full = symbol_match.group(1)
    lots = int(lot_match.group(1))

    price = float(price_match.group(1)) if price_match else None
    future_price = float(future_match.group(1)) if future_match else None

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_full), None)

    if not base_symbol:
        return None

    opt_match = re.search(r"(\d+)(CE|PE)$", symbol_full)

    zone = None
    option_type = None

    if opt_match and future_price:

        strike = opt_match.group(1)
        option_type = opt_match.group(2)

        zone = classify_strike(strike, option_type, future_price)

    action_type = None

    if "WRITER" in text_upper:

        if option_type == "CE":
            action_type = "CALL_WRITER"

        elif option_type == "PE":
            action_type = "PUT_WRITER"

    elif "CALL BUY" in text_upper:
        action_type = "CALL_BUY"

    elif "PUT BUY" in text_upper:
        action_type = "PUT_BUY"

    elif "FUTURE BUY" in text_upper:
        action_type = "FUTURE_BUY"

    elif "FUTURE SELL" in text_upper:
        action_type = "FUTURE_SELL"

    if not action_type:
        return None

    return {
        "symbol": base_symbol,
        "lots": lots,
        "zone": zone,
        "action": action_type,
        "price": price,
        "future": future_price
    }


# ---------------- TELEGRAM LISTENER ---------------- #

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    msg = update.channel_post or update.message

    if not msg:
        return

    if str(msg.chat_id) != str(TARGET_CHANNEL_ID):
        return

    now = datetime.now(IST).time()

    if not (time(9, 0) <= now <= time(15, 30)):
        return

    parsed = parse_alert(msg.text)

    if parsed:
        alerts_buffer.append({
            "data": parsed,
            "time": datetime.now(IST)
        })

        logging.info(f"Alert received {parsed}")


# ---------------- REPORT ENGINE ---------------- #

async def run_report(context: ContextTypes.DEFAULT_TYPE, minutes):

    global alerts_buffer

    now = datetime.now(IST)

    if now.time() < time(9,15) or now.time() > time(15,30):
        return

    cutoff = now - timedelta(minutes=minutes)

    batch = [a["data"] for a in alerts_buffer if a["time"] > cutoff]

    if not batch:
        return

    opt_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    opt_turn = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

    last_future = {}

    for alert in batch:

        sym = alert["symbol"]
        act = alert["action"]
        zone = alert["zone"]
        lots = alert["lots"]
        price = alert["price"]

        if alert["future"]:
            last_future[sym] = alert["future"]

        if zone:

            opt_data[sym][act][zone] += lots

            if price:
                opt_turn[sym][act][zone] += lots * price * LOT_SIZES.get(sym,1)

    message = f"<pre>\n📊 {minutes} MIN INSTITUTIONAL FLOW REPORT\n\n"

    for symbol in TRACK_SYMBOLS:

        if symbol not in opt_data:
            continue

        message += f"💎 {symbol} (FUT: {last_future.get(symbol,'N/A')})\n"

        message += "TYPE        ITM        OTM        TOTAL\n"
        message += "--------------------------------------\n"

        bull = 0
        bear = 0

        for act in opt_data[symbol]:

            itm = opt_data[symbol][act]["ITM"]
            otm = opt_data[symbol][act]["OTM"]

            tot = itm + otm

            message += f"{act:10} {itm:8} {otm:8} {tot:8}\n"

            if act in ["PUT_WRITER","CALL_BUY"]:
                bull += tot
            else:
                bear += tot

        message += "\n"
        message += f"Bias: {get_bias_label(bull-bear)}\n"
        message += "--------------------------------------\n\n"

    message += "</pre>"

    await context.bot.send_message(
        chat_id=SUMMARY_CHAT_ID,
        text=message,
        parse_mode="HTML"
    )


# ---------------- JOB WRAPPERS ---------------- #

async def report_15m(context):
    await run_report(context,15)

async def report_30m(context):
    await run_report(context,30)

async def report_60m(context):
    await run_report(context,60)

async def report_120m(context):
    await run_report(context,120)


# ---------------- MAIN ---------------- #

def main():

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(MessageHandler(filters.TEXT, message_handler))

    if app.job_queue:

        app.job_queue.run_repeating(report_15m, interval=900, first=60)
        app.job_queue.run_repeating(report_30m, interval=1800, first=120)
        app.job_queue.run_repeating(report_60m, interval=3600, first=180)
        app.job_queue.run_repeating(report_120m, interval=7200, first=240)

    logging.info("BOT STARTED")

    app.run_polling()


if __name__ == "__main__":
    main()
