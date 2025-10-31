import numpy as np
import pandas as pd
import os
from dotenv import load_dotenv
from statsmodels.regression.rolling import RollingOLS
from spreads.spreads_resepy import calculate_hedge_ratios
load_dotenv()

LOOKBACK = int(os.getenv('LOOKBACK_DAYS'))

def calculate_historical_spreads(df, pair):
    hedge_ratios = calculate_hedge_ratios(df, pair)
    if hedge_ratios.empty:
        return pd.DataFrame()
    
    s1, s2 = pair.split('_', 1)
    
    df1 = df[s1].set_index('date')
    df2 = df[s2].set_index('date')
    
    # Align all data with hedge_ratios index
    df1_aligned = df1.reindex(hedge_ratios.index)
    df2_aligned = df2.reindex(hedge_ratios.index)
    
    o = np.log(df1_aligned['open']) - (hedge_ratios * np.log(df2_aligned['open']))
    c = np.log(df1_aligned['close']) - (hedge_ratios * np.log(df2_aligned['close']))
    
    df_spread = pd.DataFrame({
        'datetime': hedge_ratios.index,
        'symbol': pair,
        'open': o.values,
        'high': np.maximum(o.values, c.values),
        'low': np.minimum(o.values, c.values),
        'close': c.values,
        'Volume': hedge_ratios.values
    }).dropna()
    
    return df_spread

def calculate_live(pair, s1_data, s2_data, prev_hr, from_date):
    c = np.log(s1_data['close'].to_numpy()) - (prev_hr * np.log(s2_data['close'].to_numpy()))
    return pd.DataFrame({'symbol': pair,'close': c.flatten()})
