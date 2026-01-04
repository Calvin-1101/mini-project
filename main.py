#%%
import yfinance as yf
import pandas as pd
from datetime import datetime
#to run this script in terminal, its different from zsh that used in macbook 
#first, bypass Powershell policy blocking scripts -> Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#then, enter the venv in terminal. same as source venv/bin/activate as macbook -> .\.venv\Scripts\Activate.ps1


btc = yf.download(['BTC-USD'],start="2022-11-15",interval='1d')  #5m interval only up to last 60 days. adjust this for a larger timeframe/shorter interval

# %%
btc = btc.copy()
btc.columns.get_level_values(1)
# btc = btc.droplevel('Ticker',axis=1).reset_index()
#%% 



# %%

btc.head()
btc.columns.dtype

# %%

# %%
