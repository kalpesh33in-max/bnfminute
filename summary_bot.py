import os
import re
import logging
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# Setup Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

alerts_buffer = []
TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK"]

def parse_alert(text):
    symbol_match = re.search(r"Symbol:\s*([\w-]+)", text, re.IGNORECASE)
    lot_match = re.search(r"LOTS:\s*(\d+)", text, re.IGNORECASE)

    if not (symbol_match and lot_match):
        return None

    symbol_full = symbol_match.group(1).upper()
    lots = int(lot_match.group(1))

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_full), None)
    if not base_symbol:
        return None

    text_upper = text.upper()
    action_type = None

    # Logic to match alerts from your screenshots
    if "SHORT COVERING" in text_upper:
        action_type = "PUT_SC" if "(PE)" in text_upper else "CALL_SC" if "(CE)" in text_upper else "FUT_SC"
    elif "LONG UNWINDING" in text_upper:
        action_type = "CALL_LW" if "(CE)" in text_upper else "PUT_LW" if "(PE)" in text_upper else "FUT_LW"
    elif "FUTURE BUY" in text_upper:
        action_type = "FUT_BUY"
    elif "FUTURE SELL" in text_upper:
        action_type = "FUT_SELL"
    elif "CALL WRITER" in text_upper:
        action_type = "CALL_WRITER"
    elif "PUT WRITER" in text_upper:
        action_type = "PUT_WRITER"
    elif "CALL BUY" in text_upper:
        action_type = "CALL_BUY"
    elif "PUT BUY" in text_upper:
        action_type = "PUT_BUY"
    
    return {"symbol": base_symbol, "lots": lots, "action_type": action_type} if action_type else None

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            alerts_buffer.append(parsed)
            logging.info(f"Captured: {parsed}")

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        return

    current_batch, alerts_buffer = list(alerts_buffer), []
    data = defaultdict(lambda: defaultdict(int))
    for alert in current_batch:
        data[alert["symbol"]][alert["action_type"]] += alert["lots"]

    message = "📊 **1-MINUTE FLOW SUMMARY**\n\n"
    total_bull = total_bear = 0

    for symbol in TRACK_SYMBOLS:
        if symbol not in data: continue
        d = data[symbol]
        message += f"🔹 **{symbol}**\n"
        message += f"CALL SC: {d['CALL_SC']} | LW: {d['CALL_LW']}\n"
        message += f"PUT  SC: {d['PUT_SC']} | LW: {d['PUT_LW']}\n"
        message += f"FUT  BUY: {d['FUT_BUY']} | SELL: {d['FUT_SELL']}\n"
        message += "---------------------------------\n"

        total_bull += (d['CALL_SC'] + d['PUT_LW'] + d['PUT_WRITER'] + d['CALL_BUY'] + d['FUT_BUY'])
        total_bear += (d['CALL_LW'] + d['PUT_SC'] + d['CALL_WRITER'] + d['PUT_BUY'] + d['FUT_SELL'])

    net = total_bull - total_bear
    bias = "🔥 Bullish" if net > 0 else "🔻 Bearish" if net < 0 else "⚖ Neutral"
    message += f"\n📈 **NET VIEW**\nBullish: {total_bull} | Bearish: {total_bear}\n**Bias: {bias}**"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message, parse_mode="Markdown")

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))

    if app.job_queue:
        # UPDATED: interval=60 for 1-minute reports
        app.job_queue.run_repeating(process_summary, interval=60, first=10)

    logging.info("Bot started. Summarizing every 1 minute.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
