from datetime import datetime, timedelta
import json
import re
import threading
import time
import psutil
from multiprocessing import Process
import pandas as pd, io
from data.auth import auth_run
from data.crypto import crypto_historical_data, crypto_websocket_connect
from data.data import get_historical_data, websocket
import os
from dotenv import load_dotenv
from config import RedisConnection
from healper import write_feather_to_redis, read_feather_from_redis, live_feather_to_redis
from rm import monitor_process_usage
from spreads.spreads import calculate_historical, live_Spreads_loop
from aqi_write import aqi_write
import multiprocessing as mp
from data.aggregator import CandleAggregator
load_dotenv()
redis_conn = RedisConnection.get_instance()

data_startD = os.getenv("data_startD")
hist_intv=os.getenv("hist_intv")
symbols = pd.read_csv("pair.csv")['pair'].str.split('_').explode().unique().tolist()

num = int(re.findall(r'\d+', hist_intv)[0])
aggregator = CandleAggregator(interval_minutes=num)

def download_histD(symbols, exchange):
    print("Data Downloader")
    for sym in symbols:
        df = read_feather_from_redis(redis_conn, sym, key="historical", lr=True)
        if df is not None:
            start = pd.to_datetime(df.iloc[0])
        else:
            start = datetime.strptime(data_startD, "%Y-%m-%d %H:%M")
        final_end = datetime.now()
        if exchange == "nse":
            data = get_historical_data(sym, start, final_end, interval=hist_intv)
        else:
            data = crypto_historical_data(sym, start, final_end, interval=hist_intv)
            # print("data", data)
        write_feather_to_redis(redis_conn, sym, data, key="historical", live=False, spreads=True)
    print("Data Downloader Complete")

def on_tick(symbol, tick):
    aggregator.process_tick(symbol, tick)

def run_ws(symbols, exchange):
    if isinstance(symbols, str):
        symbols = [symbols]
    if exchange == "nse":
        ws_thread = threading.Thread(target=websocket,args=(symbols,),kwargs={'tick_callback': on_tick},daemon=True,name="WebSocket")
        ws_thread.start()
    else:
        ws_thread = threading.Thread(target=crypto_websocket_connect,args=(symbols,),kwargs={'tick_callback': on_tick},daemon=True,name="WebSocket")
        ws_thread.start()

    # live_Spreads = threading.Thread(target=live_Spreads_loop, daemon=True, name="LiveSpreads")
    # live_Spreads.start()
    
    interval_minutes = int(''.join(filter(str.isdigit, os.getenv("hist_intv", "5"))))
    while True:
        now = datetime.now()
        next_minute = (now.minute // interval_minutes + 1) * interval_minutes
        next_time = now.replace(second=0, microsecond=0)
        if next_minute >= 60:
            next_time = next_time.replace(minute=0) + timedelta(hours=1)
        else:
            next_time = next_time.replace(minute=next_minute)
        wait_seconds = (next_time - now).total_seconds()
        print("remain time to update ", int(wait_seconds))
        time.sleep(wait_seconds)
        start_time = datetime.now()
        for sym in symbols:
            candle = aggregator.get_candle(sym)
            live_feather_to_redis(redis_conn, sym, candle, key="historical", live=True, spreads=True)
            # print("candle for sym",sym, candle)

        end_time = datetime.now()
        elapsed_ms = (end_time - start_time).total_seconds() * 1000
        print(f"total time for candle Update: {elapsed_ms:.2f} ms")

def live_loop():
    interval_minutes = int(''.join(filter(str.isdigit, os.getenv("hist_intv", "5"))))
    while True:
        now = datetime.now()
        next_run = (now.minute // interval_minutes + 1) * interval_minutes
        wait_seconds = (next_run - now.minute) * 60 - now.second
        print(f"Waiting {wait_seconds}s...")
        time.sleep(wait_seconds + 5)
        # t1 = time.perf_counter()
        # download_histD(symbols)
        # t2 = time.perf_counter()
        # print(f"total data download in : {t2-t1:.6f} seconds for total symbols {len(symbols)}")
        calculate_historical(loop=False)

if __name__ == "__main__":
    # auth_run()
    download_histD(symbols,"crypto")
    calculate_historical(loop=False)
    p1 = mp.Process(target=run_ws, args=(symbols,"crypto",))
    # p2 = mp.Process(target=aqi_write)
    p3 = mp.Process(target=live_loop)
    p1.start()
    # p2.start()
    p3.start()
    allpid = {
        "run_ws PID": p1.pid,
        # "aqi_write PID": p2.pid,
        "live_loop PID": p3.pid
    }
    # print("allpid", allpid)
    monitor_process_usage(allpid)

