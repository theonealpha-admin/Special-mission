import pandas as pd
import io
import redis 
import os
from dotenv import load_dotenv
import sqlite3

from config import RedisConnection
load_dotenv()
hist_intv = os.getenv("hist_intv")
redis_conn = RedisConnection.get_instance()

def trim_to_last_n_rows_and_export_older(redis_conn, base_key, db_path='historical_data.db', n_rows=10000):
    pattern = f"{base_key}:*"
    all_keys = redis_conn.keys(pattern)
    
    if not all_keys:
        print(f"No keys found matching pattern '{pattern}'")
        return
    
    conn = sqlite3.connect(db_path)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS historical (
        symbol TEXT,
        date date,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER
    )
    """
    conn.execute(create_table_sql)
    conn.commit()
    
    processed_count = 0
    exported_rows = 0
    for full_key in all_keys:
        symbol = full_key.decode('utf-8').split(':')[-1] if isinstance(full_key, bytes) else full_key.split(':')[-1]
        
        feather_bytes = redis_conn.get(full_key)
        if feather_bytes is None:
            print(f"No data found for key: {full_key}")
            continue
        
        buffer = io.BytesIO(feather_bytes)
        try:
            df = pd.read_feather(buffer)
            print("df", df)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            else:
                print(f"No 'date' column found for {symbol}, skipping.")
                continue
        except Exception as e:
            print(f"Error reading Feather for {full_key}: {e}")
            continue
        
        original_len = len(df)
        if original_len == 0:
            print(f"No data rows for {symbol}, skipping.")
            continue
        
        if original_len <= n_rows:
            print(f"Data for {symbol} has {original_len} rows, less than or equal to {n_rows}, keeping as is.")
            continue
        
        recent_df = df.iloc[-(n_rows):].reset_index(drop=True)
        older_df = df.iloc[:(original_len - n_rows)].copy()
        
        recent_len = len(recent_df)
        older_len = len(older_df)
        
        if older_len > 0:
            older_df_export = older_df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            older_df_export['symbol'] = symbol
            older_df_export.to_sql('historical', conn, if_exists='append', index=False)
            exported_rows += older_len
            print(f"Exported {older_len} older rows for {symbol} to SQLite.")
        
        buffer = io.BytesIO()
        recent_df.to_feather(buffer)
        redis_conn.set(full_key, buffer.getvalue())
        print(f"Kept {recent_len} recent rows for {symbol} in Redis.")
        
        processed_count += 1
        print(f"Processed {symbol}: from {original_len} to {recent_len} rows in Redis.")
    
    conn.close()
    print(f"Operation completed. Processed {processed_count} symbols. Exported {exported_rows} total older rows to {db_path}.")

trim_to_last_n_rows_and_export_older(redis_conn, "historical")