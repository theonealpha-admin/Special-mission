import pandas as pd, io
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
load_dotenv()
hist_intv=os.getenv("hist_intv")

def write_feather_to_redis(redis_conn, symbol, data, key, live, spreads):
    key = f"{key}:{symbol}"
    new_df = pd.DataFrame(data)
    
    if redis_conn.exists(key):
        existing_bytes = redis_conn.get(key)
        if existing_bytes:
            existing_df = pd.read_feather(io.BytesIO(existing_bytes))
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            date_col = next((col for col in ['datetime', 'timestamp', 'date'] if col in combined_df.columns), None)
            combined_df = combined_df.drop_duplicates(subset=[date_col] if date_col else None, keep='last').reset_index(drop=True)
        else:
            combined_df = new_df
    else:
        combined_df = new_df
    
    # print("combined_df", combined_df)
    buffer = io.BytesIO()
    combined_df.to_feather(buffer)
    redis_conn.set(key, buffer.getvalue())

def live_feather_to_redis(redis_conn, symbol, data, key, live, spreads):
    key = f"{key}:{symbol}"
    if live == True:
        new_df = pd.DataFrame(data['data']) 
    
    if redis_conn.exists(key):
        existing_bytes = redis_conn.get(key)
        if existing_bytes:
            existing_df = pd.read_feather(io.BytesIO(existing_bytes))
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            date_col = next((col for col in ['datetime', 'timestamp', 'date'] if col in combined_df.columns), None)
            combined_df = combined_df.drop_duplicates(subset=[date_col] if date_col else None, keep='last').reset_index(drop=True)
        else:
            combined_df = new_df
    else:
        combined_df = new_df
        
    buffer = io.BytesIO()
    combined_df.to_feather(buffer)
    redis_conn.set(key, buffer.getvalue())
    

def read_feather_from_redis(redis_conn, symbol, key, lr=False):
    t1 = time.perf_counter()
    feather_bytes = redis_conn.get(f"{key}:{symbol}")
    if feather_bytes is None:
        return None
    buffer = io.BytesIO(feather_bytes)
    df = pd.read_feather(buffer)
    t2 = time.perf_counter()

    # print(f"Redis READ for {symbol}: {t2-t1:.6f} seconds")

    if lr:
        if len(df) == 0:
            print(f"No Data found for this symbol {symbol}")
            return None
        return df.iloc[-1]
    else:
        return df
