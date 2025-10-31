from datetime import datetime, timedelta
import time
import json
import threading
from binance.client import Client
import websocket
import os
from dotenv import load_dotenv
from config import RedisConnection

redis_conn = RedisConnection.get_instance()

load_dotenv()
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
client = Client(API_KEY, API_SECRET)


def crypto_historical_data(symbol, current, end_date, interval="5m", chunk_days=60, exchange="spot"):
    """
    Binance historical data fetcher
    
    Args:
        symbol: Trading pair like "BTCUSDT", "ETHUSDT" or just "BTC", "ETH"
        current: Start datetime
        end_date: End datetime
        interval: "1m", "5m", "15m", "1h", "4h", "1d" etc
        chunk_days: Days per chunk (max 1000 candles per request)
        exchange: "spot" or "futures" (not used, kept for compatibility)
    """
    all_data = []
    
    # Remove timezone if present
    if hasattr(current, 'tz') and current.tz is not None:
        current = current.tz_localize(None)
    if hasattr(end_date, 'tz') and end_date.tz is not None:
        end_date = end_date.tz_localize(None)
    
    # Convert symbol format if needed (BTC -> BTCUSDT)
    if not symbol.endswith('USDT'):
        symbol = symbol.upper() + 'USDT'
    
    # Validate symbol
    try:
        info = client.get_symbol_info(symbol)
        if not info:
            print(f"❌ Symbol {symbol} not found")
            return None
    except Exception as e:
        print(f"❌ Error validating symbol: {e}")
        return None
    
    # Interval mapping for compatibility
    interval_map = {
        "1minute": "1m", "5minute": "5m", "15minute": "15m",
        "1m": "1m", "5m": "5m", "15m": "15m",
        "1h": "1h", "4h": "4h", "1d": "1d"
    }
    binance_interval = interval_map.get(interval, interval)
    
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days-1), end_date)
        
        try:
            # Binance expects milliseconds
            start_ms = int(current.timestamp() * 1000)
            end_ms = int(chunk_end.timestamp() * 1000)
            
            klines = client.get_historical_klines(
                symbol=symbol,
                interval=binance_interval,
                start_str=start_ms,
                end_str=end_ms,
                limit=1000
            )
            
            if klines:
                # Convert to Kite-like format
                chunk_data = []
                for k in klines:
                    chunk_data.append({
                        'date': datetime.fromtimestamp(k[0]/1000),
                        'open': float(k[1]),
                        'high': float(k[2]),
                        'low': float(k[3]),
                        'close': float(k[4]),
                        'volume': float(k[5])
                    })
                
                all_data.extend(chunk_data)
            
            time.sleep(0.34)  # Rate limit protection
            
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(1)
        
        current = chunk_end + timedelta(days=1)
    
    return all_data if all_data else None


class BinanceWS:
    """Binance WebSocket handler - Simple & Stable"""
    
    def __init__(self, api_key, api_secret, redis_conn):
        self.api_key = api_key
        self.api_secret = api_secret
        self.redis = redis_conn
        self.symbols = []
        self.ws_threads = []
        self.tick_callback = None
        self.running = False
    
    def start(self, symbols, tick_callback=None):
        """
        Start WebSocket for symbols
        
        Args:
            symbols: List like ["BTCUSDT", "ETHUSDT"] or ["BTC", "ETH"]
            tick_callback: Optional callback(symbol, tick_data)
        """
        # Normalize symbols to USDT pairs
        self.symbols = [s.upper() + 'USDT' if not s.endswith('USDT') else s.upper() 
                       for s in symbols]
        self.tick_callback = tick_callback
        self.running = True
        
        # Start WebSocket thread for each symbol
        for symbol in self.symbols:
            thread = threading.Thread(target=self._run_websocket, args=(symbol,), daemon=True)
            thread.start()
            self.ws_threads.append(thread)
        
        print(f"✅ WebSocket started for {len(self.symbols)} symbols")
        return self
    
    def _run_websocket(self, symbol):
        """Run WebSocket connection for a symbol"""
        ws_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@ticker"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if data.get('e') == '24hrTicker':
                    # Kite-like tick structure
                    tick = {
                        'instrument_token': symbol,
                        'last_price': float(data['c']),
                        'volume': float(data['v']),
                        'change': float(data['p']),
                        'change_percent': float(data['P'])
                    }
                    
                    # Store in Redis
                    self.redis.set(f"ltp:{symbol}", tick['last_price'])
                    
                    # User callback
                    if self.tick_callback:
                        self.tick_callback(symbol, tick)
            except Exception as e:
                print(f"❌ Error processing message for {symbol}: {e}")
        
        def on_error(ws, error):
            if self.running:
                print(f"⚠️ WebSocket error for {symbol}: {error}")
        
        def on_close(ws, close_status_code, close_msg):
            if self.running:
                print(f"🔌 WebSocket closed for {symbol}, reconnecting...")
                time.sleep(2)
                if self.running:
                    self._run_websocket(symbol)
        
        def on_open(ws):
            print(f"📡 Connected: {symbol}")
        
        # Create WebSocket connection
        ws = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        
        ws.run_forever()
    
    def get_ltp(self, symbol):
        """Get last price from Redis"""
        if not symbol.endswith('USDT'):
            symbol = symbol.upper() + 'USDT'
        
        ltp = self.redis.get(f"ltp:{symbol}")
        return float(ltp) if ltp else None
    
    def stop(self):
        """Stop WebSocket"""
        print("🛑 Stopping WebSocket...")
        self.running = False


def crypto_websocket_connect(symbols, tick_callback=None):
    """
    Create and start Binance WebSocket
    
    Usage:
        ws = websocket_connect(["BTC", "ETH"], tick_callback=my_callback)
        ltp = ws.get_ltp("BTC")
    """
    ws = BinanceWS(API_KEY, API_SECRET, redis_conn)
    ws.start(symbols, tick_callback)
    return ws