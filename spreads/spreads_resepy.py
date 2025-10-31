import numpy as np
import pandas as pd
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
import os
from dotenv import load_dotenv

load_dotenv()
LOOKBACK = int(os.getenv('LOOKBACK_DAYS'))

def calculate_hedge_ratios(df, pair):
    global LOOKBACK    
    s1, s2 = pair.split('_', 1)
    
    df1, df2 = df[s1], df[s2]
    
    if len(df1) < LOOKBACK:
        print(f"Skipping {pair}, not enough data ({len(df1)} < {LOOKBACK})")
        return pd.Series()
    
    close1 = df1.set_index('date')['close']
    close2 = df2.set_index('date')['close']
    
    # Align both series
    close1, close2 = close1.align(close2, join='inner')
    
    # Check aligned length
    if len(close1) < LOOKBACK:
        print(f"Skipping {pair}, not enough aligned data ({len(close1)} < {LOOKBACK})")
        return pd.Series()
    
    log1 = np.log(close1)
    log2 = np.log(close2)
    
    log2_with_const = sm.add_constant(log2)
    
    log1 = log1.reset_index(drop=True)
    log2_with_const = log2_with_const.reset_index(drop=True)
    
    model = RollingOLS(log1, log2_with_const, window=LOOKBACK)
    results = model.fit()
    
    hedge_ratios = results.params.iloc[:, 1]
    
    hedge_ratios.index = close1.index
    
    return hedge_ratios













# import numpy as np
# import pandas as pd
# from statsmodels.regression.rolling import RollingOLS
# import statsmodels.api as sm
# import os
# from dotenv import load_dotenv
# load_dotenv()
# LOOKBACK = int(os.getenv('LOOKBACK_DAYS'))

# def calculate_hedge_ratios(df, pair):
#     global LOOKBACK    
#     s1, s2 = pair.split('_', 1)
    
#     df1, df2 = df[s1], df[s2]
    
#     if len(df1) < LOOKBACK:
#         print(f"Skipping {pair}, not enough data ({len(df1)} < {LOOKBACK})")
#         return pd.Series()
    
#     close1 = df1.set_index('date')['close']
#     close2 = df2.set_index('date')['close']
    
#     log1 = np.log(close1)
#     log2 = np.log(close2)
    
#     model = RollingOLS(log1, log2, LOOKBACK)
#     results = model.fit()
    
#     return results.params.squeeze()