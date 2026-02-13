
import asyncio
import os
import re
from datetime import datetime, timedelta
from collections import defaultdict
import telegram
from telegram.ext import Application, MessageHandler, filters

# --- CONFIGURATION ---
# These will be loaded from environment variables in your Railway project
BOT_TOKEN = os.environ.get("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.environ.get("TARGET_CHANNEL_ID") # e.g., -100123456789
SUMMARY_CHAT_ID = os.environ.get("SUMMARY_CHAT_ID")   # Can be your personal ID or another channel ID

# --- STATE ---
# A thread-safe way to store alerts between summary intervals
alerts_buffer = []
last_summary_time = datetime.now()

# --- CONSTANTS ---
SUMMARY_INTERVAL_MINUTES = 5

# --- LOGIC ---

def parse_alert(message_text):
    """
    Parses the text of an alert message and extracts structured data.
    Returns a dictionary with the data or None if parsing fails.
    """
    patterns = {
        'strength': r"(🚀 BLAST 🚀|🌟 AWESOME|✅ VERY GOOD|👍 GOOD|🆗 OK)",
        'action': r"🚨 (.*?)
",
        'symbol': r"Symbol: ([\w-]+)",
        'lots': r"LOTS: (\d+)",
        'oi_change': r"OI CHANGE\s+:\s*([+-]?[0-9,]+)",
    }

    data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, message_text)
        if match:
            data[key] = match.group(1).strip()
        else:
            # Strength is optional, others are required
            if key != 'strength':
                return None
    
    # Post-processing
    data['lots'] = int(data['lots'])
    data['oi_change'] = int(data['oi_change'].replace(',', ''))

    # Determine instrument type
    if data['symbol'].endswith('-I'):
        data['instrument'] = 'Future'
        data['base_symbol'] = data['symbol'].replace('-I', '')
    else:
        data['instrument'] = 'Option'
        data['base_symbol'] = re.match(r'^([A-Z]+)', data['symbol']).group(1)

    # Determine sentiment
    action = data['action']
    is_call = 'CE' in data['symbol']
    is_put = 'PE' in data['symbol']
    oi_positive = data['oi_change'] > 0

    data['sentiment'] = 'neutral'
    if 'BUY' in action:
        data['sentiment'] = 'bullish'
    elif 'SELL' in action or 'WRITER' in action:
        data['sentiment'] = 'bearish'
    elif 'UNWINDING' in action:
        data['sentiment'] = 'bearish' if is_call else 'bullish' # Long Unwinding PE is bullish
    elif 'COVERING' in action:
        data['sentiment'] = 'bullish' if is_call else 'bearish' # Short Covering PE is bearish
        if 'SHORT COVERING ↗️' in action: # Futures short covering
             data['sentiment'] = 'bullish'


    return data


async def message_handler(update, context):
    """
    Handles incoming messages from the target channel.
    """
    if str(update.message.chat_id) == str(TARGET_CHANNEL_ID):
        parsed_data = parse_alert(update.message.text)
        if parsed_data:
            alerts_buffer.append(parsed_data)
            print(f"Added alert for {parsed_data['symbol']} to buffer.")


async def process_summary(context: telegram.ext.ContextTypes.DEFAULT_TYPE):
    """
    Analyzes the buffered alerts, generates a summary, and sends it.
    """
    global alerts_buffer
    if not alerts_buffer:
        print("No alerts in buffer. Skipping summary.")
        return

    # Create a copy and clear the buffer for the next interval
    processing_alerts = list(alerts_buffer)
    alerts_buffer.clear()

    print(f"Processing {len(processing_alerts)} alerts for summary...")

    # --- Analysis Logic ---
    future_sentiment = defaultdict(lambda: defaultdict(int))
    option_sentiment = defaultdict(lambda: defaultdict(int))
    top_trades = sorted(processing_alerts, key=lambda x: x['lots'], reverse=True)[:3]

    for alert in processing_alerts:
        if alert['instrument'] == 'Future':
            future_sentiment[alert['base_symbol']][alert['sentiment']] += alert['lots']
        else: # Option
            if alert['base_symbol'] == 'BANKNIFTY':
                option_sentiment[alert['sentiment']] += alert['lots']

    # --- Narrative Generation ---
    hdfc_net = future_sentiment['HDFCBANK']['bullish'] - future_sentiment['HDFCBANK']['bearish']
    icici_net = future_sentiment['ICICIBANK']['bullish'] - future_sentiment['ICICIBANK']['bearish']
    
    narrative = "Market narrative could not be determined."
    if hdfc_net > 0 and icici_net > 0:
        narrative = "BANKNIFTY is **bullish**, supported by strength in both HDFCBANK and ICICIBANK."
    elif hdfc_net < 0 and icici_net < 0:
        narrative = "BANKNIFTY is **bearish**, dragged down by weakness in both HDFCBANK and ICICIBANK."
    elif hdfc_net * icici_net < 0: # Divergence if one is positive and one is negative
        narrative = f"BANKNIFTY movement is **choppy due to divergence**.
"
        narrative += f"• HDFCBANK is showing **{'bullish' if hdfc_net > 0 else 'bearish'}** activity.
"
        narrative += f"• ICICIBANK is showing **{'bullish' if icici_net > 0 else 'bearish'}** activity."
    else:
        narrative = "BANKNIFTY sentiment is mixed, with no clear driver from key components."


    # --- Message Construction ---
    summary_message = f"**--- 📊 5 Minute Market Summary ---**

"
    summary_message += f"**Market Narrative:**
{narrative}

"
    summary_message += "**--- Futures Summary ---**
"
    
    if not future_sentiment:
        summary_message += "No significant future activity.

"
    else:
        for symbol, sentiments in future_sentiment.items():
            net_lots = sentiments['bullish'] - sentiments['bearish']
            mood = 'Bullish 📈' if net_lots > 0 else 'Bearish 📉'
            summary_message += f"• **{symbol}-I:** {mood} (Net Volume: {net_lots:+,} Lots)
"

    summary_message += "
**--- BANKNIFTY Options Summary ---**
"
    if not option_sentiment:
        summary_message += "No significant BANKNIFTY option activity.

"
    else:
        total_bull_lots = option_sentiment['bullish']
        total_bear_lots = option_sentiment['bearish']
        options_mood = 'Bullish' if total_bull_lots > total_bear_lots else 'Bearish'
        summary_message += f"• **Overall Sentiment:** {options_mood}
"
        summary_message += f"• **Bullish Volume:** {total_bull_lots:,} Lots
"
        summary_message += f"• **Bearish Volume:** {total_bear_lots:,} Lots
"

    summary_message += "
**--- Top 3 Largest Trades ---**
"
    if not top_trades:
        summary_message += "No trades in this interval.
"
    else:
        for i, trade in enumerate(top_trades):
            summary_message += f"{i+1}. {trade.get('strength','')} {trade['action']} on **{trade['symbol']}** ({trade['lots']:,} Lots)
"

    # Send the summary
    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=summary_message, parse_mode='Markdown')
    print("Summary sent successfully.")


async def main():
    """
    Starts the bot, registers handlers, and schedules the summary job.
    """
    if not all([BOT_TOKEN, TARGET_CHANNEL_ID, SUMMARY_CHAT_ID]):
        print("CRITICAL: One or more environment variables are missing.")
        print("Please set SUMMARIZER_BOT_TOKEN, TARGET_CHANNEL_ID, and SUMMARY_CHAT_ID.")
        return

    print("Starting Summarizer Bot...")
    
    app = Application.builder().token(BOT_TOKEN).build()

    # Register the message handler to listen to the raw alerts channel
    app.add_handler(MessageHandler(filters.Chat(chat_id=int(TARGET_CHANNEL_ID)), message_handler))

    # Schedule the summary job to run every 5 minutes
    job_queue = app.job_queue
    job_queue.run_repeating(process_summary, interval=SUMMARY_INTERVAL_MINUTES * 60, first=SUMMARY_INTERVAL_MINUTES * 60)

    print(f"Bot started. Listening to channel {TARGET_CHANNEL_ID}. Summaries will be sent to {SUMMARY_CHAT_ID} every {SUMMARY_INTERVAL_MINUTES} minutes.")
    
    # Run the bot
    await app.run_polling()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped manually.")

