import os
import re
import sys
import logging
import pytz
from datetime import datetime, timedelta, time
from collections import defaultdict

from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

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

TRACK_SYMBOLS = ["BANKNIFTY","HDFCBANK","ICICIBANK","AXISBANK","SBIN"]

LOT_SIZES = {
    "BANKNIFTY":30,
    "HDFCBANK":550,
    "ICICIBANK":700,
    "AXISBANK":625,
    "SBIN":750
}

def format_money(value):
    if value >= 1e7:
        return f"{value/1e7:.2f}Cr"
    elif value >= 1e5:
        return f"{value/1e5:.2f}L"
    else:
        return f"{value:.0f}"

def classify_strike(strike, option_type, future_price):
    strike=float(strike)
    future_price=float(future_price)

    if option_type=="CE":
        return "ITM" if strike < future_price else "OTM"
    else:
        return "ITM" if strike > future_price else "OTM"

def get_bias_label(net):

    if net > 1500:
        return "🔥 VERY STRONG BULLISH"
    elif net > 500:
        return "🚀 STRONG BULLISH"
    elif net > 0:
        return "🟢 Mild Bullish"
    elif net < -1500:
        return "🔥 VERY STRONG BEARISH"
    elif net < -500:
        return "📉 STRONG BEARISH"
    elif net < 0:
        return "🔴 Mild Bearish"
    return "⚖ Neutral"

def parse_alert(text):

    text=text.upper()

    symbol=re.search(r"SYMBOL:\s*([\w\-]+)",text)
    lots=re.search(r"LOTS:\s*(\d+)",text)
    price=re.search(r"PRICE:\s*([\d\.]+)",text)
    future=re.search(r"FUTURE\s*PRICE:\s*([\d\.]+)",text)

    if not symbol or not lots:
        return None

    symbol=symbol.group(1)
    lots=int(lots.group(1))
    price=float(price.group(1)) if price else None
    future=float(future.group(1)) if future else None

    base_symbol=next((s for s in TRACK_SYMBOLS if s in symbol),None)

    if not base_symbol:
        return None

    opt=re.search(r"(\d+)(CE|PE)$",symbol)

    zone=None
    option_type=None

    if opt and future:
        strike,opt_type=opt.groups()
        option_type=opt_type
        zone=classify_strike(strike,opt_type,future)

    action=None

    if "WRITER" in text:
        action="CALL_WRITE" if option_type=="CE" else "PUT_WRITER"

    elif "CALL BUY" in text:
        action="CALL_BUY"

    elif "PUT BUY" in text:
        action="PUT_BUY"

    elif "SHORT COVERING" in text:
        action="CALL_SC" if option_type=="CE" else "PUT_SC"

    elif "LONG UNWINDING" in text:
        action="CALL_UNW" if option_type=="CE" else "PUT_UNW"

    elif "FUTURE BUY" in text:
        action="FUTURE_BUY"

    elif "FUTURE SELL" in text:
        action="FUTURE_SELL"

    return {
        "symbol":base_symbol,
        "action":action,
        "zone":zone,
        "lots":lots,
        "price":price,
        "future":future
    }

async def message_handler(update:Update,context:ContextTypes.DEFAULT_TYPE):

    msg=update.channel_post or update.message

    if not msg or not msg.text:
        return

    if str(msg.chat_id)!=str(TARGET_CHANNEL_ID):
        return

    now=datetime.now(IST)

    if now.weekday()>4:
        return

    if not time(9,0)<=now.time()<=time(15,30):
        return

    parsed=parse_alert(msg.text)

    if parsed:

        alerts_buffer.append({
            "data":parsed,
            "time":datetime.now(IST)
        })

async def run_report(context,minutes):

    now=datetime.now(IST)

    if now.weekday()>4:
        return

    if now.time()<time(9,15) or now.time()>time(15,30):
        return

    cutoff=now-timedelta(minutes=minutes)

    batch=[a["data"] for a in alerts_buffer if a["time"]>cutoff]

    if not batch:
        return

    opt_data=defaultdict(lambda:defaultdict(lambda:defaultdict(int)))
    opt_turn=defaultdict(lambda:defaultdict(lambda:defaultdict(float)))

    fut_data=defaultdict(lambda:defaultdict(int))
    fut_turn=defaultdict(lambda:defaultdict(float))

    last_future={}

    for alert in batch:

        sym=alert["symbol"]
        act=alert["action"]
        zone=alert["zone"]
        lots=alert["lots"]
        price=alert["price"]

        if alert["future"]:
            last_future[sym]=alert["future"]

        if zone:

            opt_data[sym][act][zone]+=lots

            if "WRITE" in act or "_SC" in act:

                mult=100000 if zone=="ITM" else 50000
                opt_turn[sym][act][zone]+=lots*mult

            else:

                if price:
                    opt_turn[sym][act][zone]+=lots*price*LOT_SIZES.get(sym,1)

        else:

            fut_data[sym][act]+=lots
            fut_turn[sym][act]+=lots*100000

    label=f"{minutes} MIN" if minutes<60 else f"{minutes//60} HOUR"

    message=f"<pre>\n📊 {label} INSTITUTIONAL FLOW REPORT\n\n"

    for symbol in TRACK_SYMBOLS:

        if symbol not in opt_data and symbol not in fut_data:
            continue

        message+=f"💎 {symbol} (FUT: {last_future.get(symbol,'N/A')})\n"

        message+="--- OPTIONS FLOW ---\n"

        message+=f"{'TYPE':10}{'ITM':>15}{'OTM':>15}{'TOT':>15}\n"
        message+="-"*55+"\n"

        bull=0
        bear=0
        bull_turn=0
        bear_turn=0

        for act in sorted(opt_data[symbol].keys()):

            itm_l=opt_data[symbol][act]["ITM"]
            otm_l=opt_data[symbol][act]["OTM"]

            itm_t=opt_turn[symbol][act]["ITM"]
            otm_t=opt_turn[symbol][act]["OTM"]

            tot_l=itm_l+otm_l
            tot_t=itm_t+otm_t

            message+=f"{act[:10]:10}{f'{itm_l}({format_money(itm_t)})':>15}{f'{otm_l}({format_money(otm_t)})':>15}{f'{tot_l}({format_money(tot_t)})':>15}\n"

            if act in ["PUT_WRITER","CALL_BUY","CALL_SC","PUT_UNW"]:
                bull+=tot_l
                bull_turn+=tot_t
            else:
                bear+=tot_l
                bear_turn+=tot_t

        message+="-"*55+"\n"

        message+=f"Option Bias: {get_bias_label(bull-bear)}\n"
        message+=f"Bullish Turn: {format_money(bull_turn)}\n"
        message+=f"Bearish Turn: {format_money(bear_turn)}\n\n"

        if symbol in fut_data:

            message+="--- FUTURES FLOW ---\n"

            fbull=0
            fbear=0
            fbull_turn=0
            fbear_turn=0

            for act in fut_data[symbol]:

                lots=fut_data[symbol][act]
                turn=fut_turn[symbol][act]

                message+=f"{act:12} : {lots} lots ({format_money(turn)})\n"

                if act=="FUTURE_BUY":
                    fbull+=lots
                    fbull_turn+=turn
                else:
                    fbear+=lots
                    fbear_turn+=turn

            message+=f"Future Bias: {get_bias_label(fbull-fbear)}\n"
            message+=f"Bullish Turn: {format_money(fbull_turn)}\n"
            message+=f"Bearish Turn: {format_money(fbear_turn)}\n"

        message+="="*55+"\n\n"

    message+=f"Validity: Next {label}\n"
    message+="</pre>"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID,text=message,parse_mode="HTML")

async def report15(c): await run_report(c,15)
async def report30(c): await run_report(c,30)
async def report60(c): await run_report(c,60)
async def report120(c): await run_report(c,120)

def main():

    app=Application.builder().token(BOT_TOKEN).build()

    app.add_handler(MessageHandler(filters.TEXT,message_handler))

    if app.job_queue:

        app.job_queue.run_repeating(report15,interval=900,first=60)
        app.job_queue.run_repeating(report30,interval=1800,first=120)
        app.job_queue.run_repeating(report60,interval=3600,first=180)
        app.job_queue.run_repeating(report120,interval=7200,first=240)

    print("BOT STARTED")

    app.run_polling()

if __name__=="__main__":
    main()
