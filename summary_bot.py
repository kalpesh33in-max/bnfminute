import os
import re
import logging
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

# ---------- SYMBOL WEIGHT ----------
SYMBOL_WEIGHT = {
    "BANKNIFTY": 0.50,
    "HDFCBANK": 0.25,
    "ICICIBANK": 0.25,
}

# ---------- ACTION WEIGHT ----------
ACTION_WEIGHT = {
    "OPTION_BUY": 0.10,
    "OPTION_WRITE": 0.40,
    "FUTURE_BUY": 0.25,
    "FUTURE_SELL": 0.25,
}

def parse_alert(text):
    symbol_match = re.search(r"Symbol:\s*([\w-]+)", text)
    lot_match = re.search(r"LOTS:\s*(\d+)", text)
    action_match = re.search(r"🚨\s*(.*)", text)

    if not (symbol_match and lot_match and action_match):
        return None

    symbol = symbol_match.group(1).upper()
    lots = int(lot_match.group(1))
    action_text = action_match.group(1).upper()

    base_symbol = None
    for key in SYMBOL_WEIGHT.keys():
        if key in symbol:
            base_symbol = key
            break

    if not base_symbol:
        return None

    action_type = None
    sentiment = 0

    if "CALL WRITER" in action_text:
        action_type = "OPTION_WRITE"
        sentiment = -1
    elif "PUT WRITER" in action_text:
        action_type = "OPTION_WRITE"
        sentiment = 1
    elif "CALL BUY" in action_text or "PUT BUY" in action_text:
        action_type = "OPTION_BUY"
        sentiment = 1 if "CALL" in action_text else -1
    elif "FUTURE BUY" in action_text:
        action_type = "FUTURE_BUY"
        sentiment = 1
    elif "FUTURE SELL" in action_text:
        action_type = "FUTURE_SELL"
        sentiment = -1
    else:
        return None

    return {
        "symbol": base_symbol,
        "lots": lots,
        "action_type": action_type,
        "sentiment": sentiment,
        "action_text": action_text,
    }

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            alerts_buffer.append(parsed)

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        return

    current_batch = list(alerts_buffer)
    alerts_buffer.clear()

    bull_power = 0
    bear_power = 0

    top_call_writer = {"symbol": "", "lots": 0}
    top_put_writer = {"symbol": "", "lots": 0}
    top_future_buy = {"symbol": "", "lots": 0}
    top_future_sell = {"symbol": "", "lots": 0}

    for alert in current_batch:
        symbol_weight = SYMBOL_WEIGHT.get(alert["symbol"], 0)
        action_weight = ACTION_WEIGHT.get(alert["action_type"], 0)

        score = alert["lots"] * symbol_weight * action_weight

        if alert["sentiment"] > 0:
            bull_power += score
        else:
            bear_power += score

        # Track Top Activities
        if "CALL WRITER" in alert["action_text"]:
            if alert["lots"] > top_call_writer["lots"]:
                top_call_writer = alert
        if "PUT WRITER" in alert["action_text"]:
            if alert["lots"] > top_put_writer["lots"]:
                top_put_writer = alert
        if alert["action_type"] == "FUTURE_BUY":
            if alert["lots"] > top_future_buy["lots"]:
                top_future_buy = alert
        if alert["action_type"] == "FUTURE_SELL":
            if alert["lots"] > top_future_sell["lots"]:
                top_future_sell = alert

    net_strength = bull_power - bear_power

    if net_strength > 500:
        trade_plan = "📈 BUY CALL"
    elif net_strength < -500:
        trade_plan = "📉 BUY PUT"
    else:
        trade_plan = "⏸ NO TRADE"

    control = "WRITERS DOMINANT" if bear_power > bull_power else "BUYERS ACTIVE"

    message = f"""
📊 5 MIN MARKET FLOW ENGINE

🟢 Bull Power: {int(bull_power)}
🔴 Bear Power: {int(bear_power)}
⚖️ Net Strength: {int(net_strength)}

━━━━━━━━━━━━━━━━━━

🏆 TOP CALL WRITER
{top_call_writer['symbol']} → {top_call_writer['lots']} Lots

🏆 TOP PUT WRITER
{top_put_writer['symbol']} → {top_put_writer['lots']} Lots

🏆 TOP FUTURE BUYER
{top_future_buy['symbol']} → {top_future_buy['lots']} Lots

🏆 TOP FUTURE SELLER
{top_future_sell['symbol']} → {top_future_sell['lots']} Lots

━━━━━━━━━━━━━━━━━━

🔥 Market Control: {control}
🎯 Trade Plan: {trade_plan}
⏳ Validity: Next 5 Minutes Only
"""

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message)

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))

    if app.job_queue:
        app.job_queue.run_repeating(process_summary, interval=300, first=10)

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
