import os
import re
import logging
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

alerts_buffer = []

# Symbols to Track
TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK"]

# ==========================
# PARSE ALERT
# ==========================
def parse_alert(text):
    symbol_match = re.search(r"Symbol:\s*([\w-]+)", text)
    lot_match = re.search(r"LOTS:\s*(\d+)", text)

    if not (symbol_match and lot_match):
        return None

    symbol_full = symbol_match.group(1).upper()
    lots = int(lot_match.group(1))

    base_symbol = None
    for s in TRACK_SYMBOLS:
        if s in symbol_full:
            base_symbol = s
            break

    if not base_symbol:
        return None

    text_upper = text.upper()

    # Updated Action List Logic
    if "CALL SHORTCOVERING" in text_upper:
        action_type = "CALL_SC"
    elif "CALL LONG-WINDED" in text_upper:
        action_type = "CALL_LW"
    elif "PUT SHORTCOVERING" in text_upper:
        action_type = "PUT_SC"
    elif "PUT LONG-WINDED" in text_upper:
        action_type = "PUT_LW"
    elif "FUTURE SHORTCOVERING" in text_upper:
        action_type = "FUT_SC"
    elif "FUTURE LONG-WINDED" in text_upper:
        action_type = "FUT_LW"
    else:
        return None

    return {
        "symbol": base_symbol,
        "lots": lots,
        "action_type": action_type,
    }

# ==========================
# MESSAGE HANDLER
# ==========================
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            alerts_buffer.append(parsed)

# ==========================
# PROCESS SUMMARY
# ==========================
async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer

    if not alerts_buffer:
        return

    current_batch = list(alerts_buffer)
    alerts_buffer.clear()

    data = defaultdict(lambda: defaultdict(int))

    for alert in current_batch:
        symbol = alert["symbol"]
        action = alert["action_type"]
        lots = alert["lots"]
        data[symbol][action] += lots

    message = "📊 5 MIN FLOW BREAKDOWN\n\n"

    total_bull = 0
    total_bear = 0

    for symbol in TRACK_SYMBOLS:
        if symbol not in data:
            continue

        message += f"🔷 {symbol}\n\n"

        c_sc = data[symbol]["CALL_SC"]
        c_lw = data[symbol]["CALL_LW"]
        p_sc = data[symbol]["PUT_SC"]
        p_lw = data[symbol]["PUT_LW"]
        f_sc = data[symbol]["FUT_SC"]
        f_lw = data[symbol]["FUT_LW"]

        message += f"CALL SHORTCOVERING   : {c_sc} Lots\n"
        message += f"CALL LONG-WINDED     : {c_lw} Lots\n"
        message += f"PUT SHORTCOVERING    : {p_sc} Lots\n"
        message += f"PUT LONG-WINDED      : {p_lw} Lots\n"
        message += f"FUTURE SHORTCOVERING : {f_sc} Lots\n"
        message += f"FUTURE LONG-WINDED   : {f_lw} Lots\n"
        message += "\n---------------------------------\n\n"

        # Classification Logic:
        # Bullish: Call SC, Put LW, Future SC
        # Bearish: Call LW, Put SC, Future LW
        bull = c_sc + p_lw + f_sc
        bear = c_lw + p_sc + f_lw

        total_bull += bull
        total_bear += bear

    net = total_bull - total_bear

    message += "📈 NET VIEW (All Symbols Combined)\n\n"
    message += f"Bullish Activity  : {total_bull} Lots\n"
    message += f"Bearish Activity  : {total_bear} Lots\n"
    message += f"Net Dominance     : {net}\n\n"

    if net > 0:
        bias = "🔥 Bullish Build-up"
    elif net < 0:
        bias = "🔻 Bearish Build-up"
    else:
        bias = "⚖ Neutral"

    message += f"Bias: {bias}\n"
    message += "⏳ Validity: Next 5 Minutes Only"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message)

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))

    if app.job_queue:
        app.job_queue.run_repeating(process_summary, interval=300, first=10)

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
