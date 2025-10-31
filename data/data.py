from datetime import datetime, timedelta
import time
from kiteconnect import KiteConnect, KiteTicker
import os
from dotenv import load_dotenv
from config import RedisConnection
redis_conn = RedisConnection.get_instance()

load_dotenv()
API_KEY = os.getenv("API_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)


def get_historical_data(symbol, current, end_date, interval="5minute", chunk_days=60, exchange="NSE"):
    all_data = []
    if hasattr(current, 'tz') and current.tz is not None:
        current = current.tz_localize(None)
    
    try:
        instruments = kite.instruments(exchange)
        token = next((i['instrument_token'] for i in instruments 
                    if i['tradingsymbol'] == symbol and i['instrument_type'] == 'EQ'), None)
        if not token:
            print(f"❌ Symbol {symbol} not found")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days-1), end_date)
        
        try:
            chunk_data = kite.historical_data(
                instrument_token=token,
                from_date=current,
                to_date=chunk_end,
                interval=interval
            )
            if chunk_data:
                all_data.extend(chunk_data)
                # print(f"✓ {len(chunk_data):5d} candles | {current.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            
            time.sleep(0.34)  # 3 req/sec = 333ms minimum, so 340ms safe
            
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(1)
        
        current = chunk_end + timedelta(days=1)
    
    return all_data if all_data else None


# websocket_handler.py
class KiteWS:
    def __init__(self, api_key, access_token, redis_conn):
        self.api_key = api_key
        self.access_token = access_token
        self.redis = redis_conn
        self.tokens = {}
        self.symbols = []
        self.kws = None
        self.tick_callback = None
    
    def start(self, symbols, tick_callback=None):
        self.symbols = symbols
        self.tick_callback = tick_callback
        self.kws = KiteTicker(self.api_key, self.access_token)
        self.kws.on_ticks = self._on_ticks
        self.kws.on_connect = self._on_connect
        self.kws.connect(threaded=True)
        return self.kws
    
    def _on_ticks(self, ws, ticks):
        for tick in ticks:
            symbol = next((s for s in self.symbols 
                         if tick['instrument_token'] == self.tokens.get(s)), None)
            if symbol:
                self.redis.set(f"ltp:{symbol}", tick['last_price'])
                
                if self.tick_callback:
                    self.tick_callback(symbol, tick)
    
    def _on_connect(self, ws, response):
        kite = KiteConnect(api_key=self.api_key)
        kite.set_access_token(self.access_token)
        instruments = kite.instruments("NSE")
        
        self.tokens = {s: next((i['instrument_token'] for i in instruments 
                               if i['tradingsymbol'] == s and i['instrument_type'] == 'EQ'), None) 
                      for s in self.symbols}
        
        valid_tokens = [t for t in self.tokens.values() if t]
        if valid_tokens:
            ws.subscribe(valid_tokens)
            ws.set_mode(ws.MODE_LTP, valid_tokens)
    
    def get_ltp(self, symbol):
        ltp = self.redis.get(f"ltp:{symbol}")
        return float(ltp) if ltp else None
    
    def stop(self):
        if self.kws:
            self.kws.close()


def websocket(symbols, tick_callback=None):
    ws = KiteWS(API_KEY, ACCESS_TOKEN, redis_conn)
    ws.start(symbols, tick_callback)
    return ws
