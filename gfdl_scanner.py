
import asyncio
import websockets
import json
import time
import requests
from datetime import datetime, timedelta
import re
import functools
import os
import sys
import ssl
from zoneinfo import ZoneInfo

# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

# --- Environment Variable Loading ---
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# --- Validation ---
required_vars = {
    "API_KEY": API_KEY,
    "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
    "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
}
missing_vars = [key for key, value in required_vars.items() if value is None]
if missing_vars:
    print(f"❌ Critical Error: Missing required environment variables: {', '.join(missing_vars)}", flush=True)
    print("Please set these variables in your deployment environment and restart the application.", flush=True)
    sys.exit(1)

# --- API and Connection ---
WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# --- Symbol List (Options & Futures) ---
SYMBOLS_TO_MONITOR = [
    "BANKNIFTY24FEB2658900CE", "BANKNIFTY24FEB2658900PE", "BANKNIFTY24FEB2658800CE", "BANKNIFTY24FEB2658800PE",
    "BANKNIFTY24FEB2658700CE", "BANKNIFTY24FEB2658700PE", "BANKNIFTY24FEB2658600CE", "BANKNIFTY24FEB2658600PE",
    "BANKNIFTY24FEB2658500CE", "BANKNIFTY24FEB2658500PE", "BANKNIFTY24FEB2658400CE", "BANKNIFTY24FEB2658400PE",
    "BANKNIFTY24FEB2659000CE", "BANKNIFTY24FEB2659000PE", "BANKNIFTY24FEB2659100CE", "BANKNIFTY24FEB2659100PE",
    "BANKNIFTY24FEB2659200CE", "BANKNIFTY24FEB2659200PE", "BANKNIFTY24FEB2659300CE", "BANKNIFTY24FEB2659300PE",
    "BANKNIFTY24FEB2659400CE", "BANKNIFTY24FEB2659400PE",
    "HDFCBANK24FEB26930CE", "HDFCBANK24FEB26930PE", "HDFCBANK24FEB26925CE", "HDFCBANK24FEB26925PE",
    "HDFCBANK24FEB26920CE", "HDFCBANK24FEB26920PE", "HDFCBANK24FEB26915CE", "HDFCBANK24FEB26915PE",
    "HDFCBANK24FEB26910CE", "HDFCBANK24FEB26910PE", "HDFCBANK24FEB26905CE", "HDFCBANK24FEB26905PE",
    "HDFCBANK24FEB26935CE", "HDFCBANK24FEB26935PE", "HDFCBANK24FEB26940CE", "HDFCBANK24FEB26940PE",
    "HDFCBANK24FEB26945CE", "HDFCBANK24FEB26945PE", "HDFCBANK24FEB26950CE", "HDFCBANK24FEB26950PE",
    "HDFCBANK24FEB26955CE", "HDFCBANK24FEB26955PE",
    "SBIN24FEB261040CE", "SBIN24FEB261040PE", "SBIN24FEB261035CE", "SBIN24FEB261035PE",
    "SBIN24FEB261030CE", "SBIN24FEB261030PE", "SBIN24FEB261025CE", "SBIN24FEB261025PE",
    "SBIN24FEB261020CE", "SBIN24FEB261020PE", "SBIN24FEB261015CE", "SBIN24FEB261015PE",
    "SBIN24FEB261045CE", "SBIN24FEB261045PE", "SBIN24FEB261050CE", "SBIN24FEB261050PE",
    "SBIN24FEB261055CE", "SBIN24FEB261055PE", "SBIN24FEB261060CE", "SBIN24FEB261060PE",
    "SBIN24FEB261065CE", "SBIN24FEB261065PE",
    "ICICIBANK24FEB261350CE", "ICICIBANK24FEB261350PE", "ICICIBANK24FEB261340CE", "ICICIBANK24FEB261340PE",
    "ICICIBANK24FEB261330CE", "ICICIBANK24FEB261330PE", "ICICIBANK24FEB261320CE", "ICICIBANK24FEB261320PE",
    "ICICIBANK24FEB261310CE", "ICICIBANK24FEB261310PE", "ICICIBANK24FEB261300CE", "ICICIBANK24FEB261300PE",
    "ICICIBANK24FEB261360CE", "ICICIBANK24FEB261360PE", "ICICIBANK24FEB261370CE", "ICICIBANK24FEB261370PE",
    "ICICIBANK24FEB261380CE", "ICICIBANK24FEB261380PE", "ICICIBANK24FEB261390CE", "ICICIBANK24FEB261390PE",
    "ICICIBANK24FEB261400CE", "ICICIBANK24FEB261400PE",
    "ICICIBANK24FEB261410CE", "ICICIBANK24FEB261410PE",
    "ICICIBANK24FEB261420CE", "ICICIBANK24FEB261420PE",
    "BANKNIFTY-I", "HDFCBANK-I", "ICICIBANK-I", "SBIN-I", "AXISBANK-I", "KOTAKBANK-I"
]

# --- Logic & Thresholds ---
LOT_SIZES = {"AXISBANK": 625, "KOTAKBANK": 2000, "SBIN": 750, "ICICIBANK": 700,"HDFCBANK": 550, "BANKNIFTY": 30}
DEFAULT_LOT_SIZE = 75
OI_ROC_THRESHOLD = 2.0

# ============================== STATE & UTILITIES =============================
symbol_data_state = {symbol: {"price": 0, "price_prev": 0, "oi": 0, "oi_prev": 0} for symbol in SYMBOLS_TO_MONITOR}
future_prices = {"BANKNIFTY": 0, "HDFCBANK": 0, "ICICIBANK": 0, "SBIN": 0, "AXISBANK": 0, "KOTAKBANK": 0}

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_alert(msg: str):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg}
    try:
        await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))
    except Exception as e:
        print(f"⚠️ Telegram Log: {e}", flush=True)

# =============================== CORE LOGIC ===================================
def lots_from_oi_change(symbol, oi_change):
    lot_size = DEFAULT_LOT_SIZE
    base_symbol = symbol.split("-")[0].upper()
    lot_size = LOT_SIZES.get(base_symbol, DEFAULT_LOT_SIZE)
    if lot_size == 0: return 0
    return int(abs(oi_chg) / lot_size)

def lot_bucket(lots):
    if lots >= 200: return "EXTREME HIGH"
    elif lots >= 150: return "EXTRA HIGH"
    elif lots >= 100: return "HIGH"
    elif lots >= 75: return "MEDIUM"
    return "LOW" if lots >= 1 else "IGNORE"

def classify_option(oi_change, price_change):
    if price_change == 0:
        return "HEDGING" if oi_change > 0 else "REMOVE FROM HEDGE"
    elif oi_change > 0:
        return "BUYER(LONG)" if price_change > 0 else "WRITER(SHORT)"
    elif oi_change < 0:
        return "REMOVE FROM SHORT" if price_change > 0 else "REMOVE FROM LONG"
    return "Indecisive Movement"

def get_option_moneyness(symbol, future_prices):
    underlying = next((key for key in future_prices if key in symbol), None)
    if not underlying: return "N/A"
    future_price = future_prices.get(underlying, 0)
    if future_price == 0: return "OTM"
    try:
        match = re.search(r'(\d+)(CE|PE)$', symbol)
        strike_price = int(match.group(1))
        option_type = match.group(2)
        atm_band = future_price * 0.001
        if abs(future_price - strike_price) <= atm_band: return "ATM"
        is_itm = (option_type == 'CE' and strike_price < future_price) or \
                 (option_type == 'PE' and strike_price > future_price)
        return "ITM" if is_itm else "OTM"
    except (AttributeError, TypeError, ValueError):
        return "N/A"

def format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_prices, price_chg, price_chg_percent):
    price_dir = "↑" if price_chg > 0 else "↓" if price_chg < 0 else "↔"
    product_name = next((key for key in future_prices if key in symbol), "UNKNOWN")
    future_price = future_prices.get(product_name, 0)
    
    return (f"{product_name} | OPTION\n"
            f"STRIKE: {symbol.replace(product_name, '')} {moneyness}\n"
            f"ACTION: {action}\n"
            f"SIZE: {bucket} ({lots} lots)\n"
            f"OI Δ: {oi_chg}\n"
            f"OI RoC: {oi_roc:.2f}%\n"
            f"PRICE: {price_dir}\n"
            f"PRICE Chg: {price_chg:+.2f} ({price_chg_percent:+.2f}%)\n"
            f"TIME: {now()}\n"
            f"FUTURE PRICE: {future_price:.2f}\n"
            f"LAST PRICE: {state['price']:.2f}")

async def process_data(data):
    print(data)
    global symbol_data_state, future_prices
    
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state: return

    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")
    if new_price is None or new_oi is None: return

    state = symbol_data_state[symbol]
    
    if symbol.endswith("-I"):
        base_symbol = symbol.split("-")[0].upper()
        if new_price > 0: future_prices[base_symbol] = new_price
        
        prev_oi, prev_price = state.get("oi", 0), state.get("price", 0)
        state["price"], state["oi"] = new_price, new_oi

        if prev_oi == 0:
            print(f"🟢 [{now()}] {symbol}: First Data (P: {new_price}, OI: {new_oi})", flush=True)
            return
        
        oi_chg = new_oi - prev_oi
        if abs(oi_chg) == 0: return

        lots = lots_from_oi_change(symbol, oi_chg)
        if lots >= 50:
            price_chg = new_price - prev_price
            oi_roc = (oi_chg / prev_oi) * 100 if prev_oi != 0 else 0.0
            price_chg_percent = (price_chg / prev_price) * 100 if prev_price != 0 else 0.0

            direction_symbol = "↔️"
            if oi_chg > 0 and price_chg > 0: direction_symbol = "⬆️"
            elif oi_chg > 0 and price_chg < 0: direction_symbol = "⬇️"
            elif oi_chg < 0 and price_chg > 0: direction_symbol = "↗️"
            elif oi_chg < 0 and price_chg < 0: direction_symbol = "↘️"

            alert_msg = (f"🔔 FUTURE ALERT: {symbol} {direction_symbol}\n"
                         f"Existing OI: {prev_oi}\n"
                         f"OI Change: {oi_chg} ({lots} lots)\n"
                         f"OI RoC: {oi_roc:.2f}%\n"
                         f"Price: {new_price:.2f}\n"
                         f"Price Chg: {price_chg:+.2f} ({price_chg_percent:+.2f}%)\n"
                         f"Time: {now()}")
            await send_alert(alert_msg)
            print(f"🚀 Alert (Future): {symbol} Lot size >= 50 detected.", flush=True)
        return

    state["price_prev"], state["oi_prev"] = state.get("price", 0), state.get("oi", 0)
    state["price"], state["oi"] = new_price, new_oi

    if state["oi_prev"] == 0:
        print(f"ℹ️ [{now()}] {symbol}: Initializing option state.", flush=True)
        return

    oi_chg = state["oi"] - state["oi_prev"]
    if oi_chg == 0: return

    price_chg = state["price"] - state["price_prev"]
    oi_roc = (oi_chg / state["oi_prev"]) * 100 if state["oi_prev"] != 0 else 0.0

    if abs(oi_roc) > OI_ROC_THRESHOLD:
        lots = lots_from_oi_change(symbol, oi_chg)
        if lots > 100:
            price_chg_percent = (price_chg / state["price_prev"]) * 100 if state["price_prev"] != 0 else 0.0
            moneyness = get_option_moneyness(symbol, future_prices)
            action = classify_option(oi_chg, price_chg)
            bucket = lot_bucket(lots)
            alert_msg = format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_prices, price_chg, price_chg_percent)
            await send_alert(alert_msg)
            print(f"📊 [{now()}] {symbol}: {moneyness}, lots: {lots}. TRIGGERING ALERT.", flush=True)

# ============================ MAIN SCANNER LOOP ===============================
async def run_scanner():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                print(f"✅ [{now()}] Connected to WebSocket. Authenticating...", flush=True)
                auth_request = {"MessageType": "Authenticate", "Password": API_KEY}
                await websocket.send(json.dumps(auth_request))
                auth_response = json.loads(await websocket.recv())
                
                if not auth_response.get("Complete"):
                    print(f"❌ [{now()}] Authentication FAILED: {auth_response.get('Comment')}. Retrying in 30s.", flush=True)
                    await asyncio.sleep(30)
                    continue
                
                print(f"✅ [{now()}] Authentication successful. Subscribing to {len(SYMBOLS_TO_MONITOR)} symbols...", flush=True)
                for symbol in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({
                        "MessageType": "SubscribeRealtime", "Exchange": "NFO",
                        "Unsubscribe": "false", "InstrumentIdentifier": symbol
                    }))
                print(f"✅ [{now()}] Subscriptions sent. Scanner is now live.", flush=True)
                await send_alert("✅ GFDL Scanner is LIVE and monitoring the market.")

                async for message in websocket:
                    try:
                        data = json.loads(message)
                        if data.get("MessageType") == "RealtimeResult":
                            await process_data(data)                        
                    except json.JSONDecodeError:
                        print(f"⚠️ [{now()}] Warning: Received a non-JSON message.", flush=True)
                    except Exception as e:
                        print(f"❌ [{now()}] Error during message processing for {data}: {e}", flush=True)

        except Exception as e:
            print(f"❌ An unexpected error occurred in the main loop: {e}", flush=True)
            await asyncio.sleep(30)

if __name__ == "__main__":
    print("🚀 GFDL Scanner Starting...", flush=True)
    try:
        asyncio.run(run_scanner())
    except KeyboardInterrupt:
        print("\n🛑 Scanner stopped by user.", flush=True)
        asyncio.run(send_alert("🛑 GFDL Scanner was stopped manually."))
    except Exception as e:
        error_message = f"💥 GFDL Scanner CRASHED with a critical error: {e}"
        print(error_message, flush=True)
        asyncio.run(send_alert(None, error_message))
