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
    
    log1 = np.log(close1)
    log2 = sm.add_constant(np.log(close2))
    
    model = RollingOLS(log1, log2, LOOKBACK)
    results = model.fit()
    
    return results.params.iloc[:, 1]














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