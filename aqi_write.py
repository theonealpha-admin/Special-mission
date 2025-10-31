import os
import io
import time
import pandas as pd
import numpy as np
from pathlib import Path
from config import RedisConnection

AMIBROKER_ASCII_DIR = r"C:\Program Files\AmiBroker\ASCII"
AMIBROKER_TRIGGER = os.path.join(AMIBROKER_ASCII_DIR, "~refresh.now")

def read_feather_from_redis(redis_conn, symbol, key):
    try:
        feather_bytes = redis_conn.get(f"{key}:{symbol}")
        if feather_bytes is None:
            return None
        return pd.read_feather(io.BytesIO(feather_bytes))
    except:
        return None

def get_ascii_filepath(symbol):
    clean_symbol = "_".join([p.split(":")[-1] for p in symbol.split("_")])
    return os.path.join(AMIBROKER_ASCII_DIR, f"{clean_symbol}.aqi")

def write_symbol_to_aqi(symbol, key, redis_conn):
    try:
        df = read_feather_from_redis(redis_conn, symbol, key)
        
        if df is None or df.empty:
            return f"{symbol}: No data"
        
        # Handle both 'datetime' and 'date' columns
        datetime_col = None
        if 'datetime' in df.columns:
            datetime_col = 'datetime'
        elif 'date' in df.columns:
            datetime_col = 'date'
        else:
            return f"{symbol}: Missing datetime/date column"
        
        required_cols = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            return f"{symbol}: Missing columns"
        
        df = df.sort_values(datetime_col).dropna(subset=[datetime_col] + required_cols).reset_index(drop=True)
        
        if len(df) == 0:
            return f"{symbol}: No valid data"
        
        # Convert to numpy arrays
        dates = df[datetime_col].astype(str).values
        spreadO = df['open'].values.astype(float)
        spreadH = df['high'].values.astype(float)
        spreadL = df['low'].values.astype(float)
        spreadC = df['close'].values.astype(float)
        beta_val = df['volume'].fillna(0).values.astype(float) if 'volume' in df.columns else np.zeros(len(df))
        sqrt_Q = np.zeros(len(df))
        
        # Vectorized date extraction
        extract_yr = np.vectorize(lambda s: int(s[2:4]))
        extract_m = np.vectorize(lambda s: int(s[5:7]))
        extract_d = np.vectorize(lambda s: int(s[8:10]))
        extract_hr = np.vectorize(lambda s: int(s[11:13]))
        extract_mn = np.vectorize(lambda s: int(s[14:16]))
        
        yr, m, d, hr, mn = extract_yr(dates), extract_m(dates), extract_d(dates), extract_hr(dates), extract_mn(dates)
        
        # Format date and time
        format_ymd = np.vectorize(lambda y, mo, da: f"{y:02d}{mo:02d}{da:02d}")
        format_hm = np.vectorize(lambda h, mi: f"{h:02d}{mi:02d}")
        ymd, hm = format_ymd(yr, m, d), format_hm(hr, mn)
        
        # Format floats
        format_float = np.vectorize(lambda x: f"{x:.6f}")
        o_str = format_float(spreadO)
        h_str = format_float(spreadH)
        l_str = format_float(spreadL)
        c_str = format_float(spreadC)
        b_str = format_float(beta_val)
        q_str = format_float(sqrt_Q)
        
        # Build AQI lines
        lines = np.char.add(np.char.add(np.char.add(ymd, ','), hm), ',')
        lines = np.char.add(np.char.add(np.char.add(lines, o_str), ','), h_str)
        lines = np.char.add(np.char.add(np.char.add(lines, ','), l_str), ',')
        lines = np.char.add(np.char.add(np.char.add(lines, c_str), ','), b_str)
        lines = np.char.add(np.char.add(np.char.add(lines, ',0,'), q_str), ',0\n')
        
        # Write to file
        aqi_file = get_ascii_filepath(symbol)
        temp_file = aqi_file + ".tmp"
        
        with open(temp_file, 'w', buffering=1048576) as f:
            f.writelines(lines)
        
        os.replace(temp_file, aqi_file)
        return f"{symbol}: Wrote {len(df)} lines"
    
    except Exception as e:
        return f"{symbol}: Error - {e}"

def get_all_symbols_from_redis(redis_conn):
    result = []
    for key in ['spreads', 'historical']:
        cursor = 0
        while True:
            cursor, keys_batch = redis_conn.scan(cursor, match=f"{key}:*", count=1000)
            for full_key in keys_batch:
                symbol = full_key.decode('utf-8').split(':', 1)[1] if isinstance(full_key, bytes) else full_key.split(':', 1)[1]
                result.append((symbol, key))
            if cursor == 0:
                break
    return result 

def aqi_write():
    Path(AMIBROKER_ASCII_DIR).mkdir(parents=True, exist_ok=True)
    redis_conn = RedisConnection.get_instance()
    
    print("AQI Writer started. Press Ctrl+C to stop.")

    try:
        while True:
            symbols_data = get_all_symbols_from_redis(redis_conn)
            
            if symbols_data:                
                for symbol, key in symbols_data:
                    result = write_symbol_to_aqi(symbol, key, redis_conn)
                    if "Error" in result or "No data" in result:
                        print(result)
                with open(AMIBROKER_TRIGGER, 'w') as f:
                    f.write(str(pd.Timestamp.now()))
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("AQI Writer stopped")

if __name__ == "__main__":
    aqi_write()