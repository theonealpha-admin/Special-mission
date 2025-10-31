from datetime import datetime, timedelta
from data.crypto import crypto_historical_data, crypto_websocket_connect
import time

end_date = datetime.now()
start_date = end_date - timedelta(days=2)

# Historical data test
print("📊 Fetching historical data...")
data = crypto_historical_data("BTC", start_date, end_date, interval="5m")
if data:
    print(f"✅ Got {len(data)} candles")
    print(f"First: {data[0]['date']} | Close: ${data[0]['close']:.2f}")
    print(f"Last: {data[-1]['date']} | Close: ${data[-1]['close']:.2f}")

print("\n📡 Starting WebSocket...")
ws = crypto_websocket_connect(["BTC", "ETH", "SOL"], tick_callback=None)

# Wait for first ticks
print("⏳ Waiting for live data...")
time.sleep(5)

# Get live prices
btc_ltp = ws.get_ltp("BTC")
eth_ltp = ws.get_ltp("ETH")
sol_ltp = ws.get_ltp("SOL")

print(f"\n💰 Live Prices:")
print(f"BTC: ${btc_ltp:.2f}" if btc_ltp else "BTC: Waiting...")
print(f"ETH: ${eth_ltp:.2f}" if eth_ltp else "ETH: Waiting...")
print(f"SOL: ${sol_ltp:.2f}" if sol_ltp else "SOL: Waiting...")

# Keep running
print("\n✅ Running... Press Ctrl+C to stop")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    ws.stop()
    print("\n👋 Stopped!")