from functools import partial
import io
import os
import time
import sqlite3
import traceback
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from multiprocessing import Pool, Lock, cpu_count
from spreads.cal import calculate_historical_spreads, calculate_live
import warnings
from config import RedisConnection
from healper import read_feather_from_redis, write_feather_to_redis
redis_conn = RedisConnection.get_instance()
warnings.filterwarnings('ignore', message='divide by zero')
warnings.filterwarnings('ignore', message='invalid value encountered')

# ----------------- Setup -----------------
load_dotenv()
db_lock = Lock()
DB_PATH = os.getenv("DB_PATH", "ohlcv_data.db")
PAIR_CSV = "pair.csv"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS"))
TABLE_NAME = os.getenv("TABLE_NAME", "OHLCV_DATA")
DATA_START = os.getenv("data_start_date", "2025-01-01")

# ----------------- DB Helpers -----------------
def save_df(pair, df):
    write_feather_to_redis(redis_conn, pair, df, key="spreads", live=False, spreads=True)

# ----------------- Pair & Data Helpers -----------------
def load_pairs():
    df = pd.read_csv(PAIR_CSV).dropna(subset=["pair"])
    return df

def get_data(pair, from_date):
    symbols = pair.split("_")
    dfs = {}
    for sym in symbols:
        df = read_feather_from_redis(redis_conn, sym, key="historical", lr=False)
        if from_date:
            mask = df['date'] < from_date
            lookback_df = df[mask].tail(LOOKBACK_DAYS)
            forward_df = df[df['date'] >= from_date]
            df = pd.concat([lookback_df, forward_df])
        dfs[sym] = df
    return dfs

def last_spread_info(pair):
    lastD = read_feather_from_redis(redis_conn, pair, key="spreads", lr=True)
    if lastD is not None:
        return lastD['datetime']
    else:
        return None

# ----------------- Historical -----------------
def process_historical(pair, loop):
    from_date = last_spread_info(pair)
    df = get_data(pair, from_date)
    spreads = calculate_historical_spreads(df, pair)
    if not spreads.empty:
        print("spreads", spreads)
        save_df(pair, spreads)

# def calculate_historical(loop):
#     pairs = load_pairs()
#     for pair in pairs['pair']:
#         process_historical(pair, loop=loop)
    

def calculate_historical(loop):
    print("Spreads Start")
    pairs = load_pairs()
    with Pool(processes=10) as pool:
        pool.map(partial(process_historical, loop=loop), pairs['pair'])

# ----------------- Live -----------------
def last_info(pair):
    lastD = read_feather_from_redis(redis_conn, pair, key="spreads", lr=True)
    if lastD is not None:
        return lastD['datetime'], lastD['Volume']
    else:
        return None

def write_live_spread_to_redis(redis_conn, symbol, live_df, key):
    key = f"{key}:{symbol}"
    new_close = float(live_df.iloc[-1]['close'])
    
    if redis_conn.exists(key):
        existing_bytes = redis_conn.get(key)
        existing_df = pd.read_feather(io.BytesIO(existing_bytes))   
        existing_df.iloc[-1, existing_df.columns.get_loc('close')] = new_close
        existing_df.iloc[-1, existing_df.columns.get_loc('high')] = max(
            float(existing_df.iloc[-1]['high']), 
            new_close
        )
        existing_df.iloc[-1, existing_df.columns.get_loc('low')] = min(
            float(existing_df.iloc[-1]['low']), 
            new_close
        )
        
        ohlc_df = existing_df 
    else:
        ohlc_df = pd.DataFrame([{
            'open': new_close,
            'high': new_close,
            'low': new_close,
            'close': new_close
        }])
    buffer = io.BytesIO()
    ohlc_df.to_feather(buffer)
    redis_conn.set(key, buffer.getvalue())
    last_row = ohlc_df.iloc[-1]
    # print(f"Redis WRITE {symbol} | O:{last_row['open']:.5f} H:{last_row['high']:.5f} L:{last_row['low']:.5f} C:{last_row['close']:.5f}")

def process_live(pairs):
    s1, s2 = pairs.split('_', 1)
    symbols=[s1, s2]
    ltp = {}
    for sym in symbols:
        val = redis_conn.get(f"ltp:{sym}")
        if val:
            ltp[sym] = float(val)
            s1, s2 = pairs.split('_', 1)
            if s1 in ltp and s2 in ltp:
                from_date, hr = last_info(pairs)
                s1_data = pd.DataFrame([{'close': ltp[s1]}])
                s2_data = pd.DataFrame([{'close': ltp[s2]}])
                live = calculate_live(pairs, s1_data, s2_data, hr, from_date)
                # print(f"spreads lpt :- {live.to_string(index=False, header=False)}")
                write_live_spread_to_redis(redis_conn, pairs, live, key="spreads")

def calculate_live_spread():
    pairs = load_pairs()
    for pair in pairs['pair']:
        process_live(pair)

def live_Spreads_loop():
    while True:
        calculate_live_spread()
        time.sleep(1)

# ----------------- Main -----------------
# if __name__ == "__main__":
#     calculate_historical(loop=False)
#     # live_Spreads_loop()