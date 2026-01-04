#%%
import pandas as pd
import yfinance as yf 

##Manual schema building first, for learning. 
#Pydantic or other schema libraries can be used later on. 

OHLCV_COLUMNS = ['timestamp','close','high','low','open','volume']

def validate_schema(df):
    validate_columns(df)
    validate_timestamp(df)
    validate_numerical_values(df)

def validate_columns(df):
    """
    df will be a cleaned dataframe after being preprocessed in preprocess.py 
   """

    missing_cols = [cols for cols in OHLCV_COLUMNS if cols not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns {missing_cols}")
    extra_cols = [cols for cols in df.columns if cols not in OHLCV_COLUMNS]
    if extra_cols:
        raise ValueError(f"Extra columns {missing_cols} are found in df")

def validate_timestamp(df):
    """
    df passed in is from preprocess_df 
    Ensure that timestamp is forward moving (increasing), no duplicates, and is timezone aware 
    To Note: cleaning and processing is NOT done here. this is the schema validation,
    so it EXPECTS a datetime object here. YFinance provides date in str format, but that has to be processed elsewhere.
    """
    ts = df['timestamp']
    if not pd.api.types.is_datetime64_any_dtype(ts):
        raise ValueError(f"timestamp is not of datetime format")
    
    if not ts.is_monotonic_increasing:
        raise ValueError("Timestamp is not increasing")
    
    if not ts.is_unique:
        raise ValueError("Timestamp has duplicates")
    
def validate_numerical_values(df):
    """
    """
    num_cols = ['close','high','low','open','volume']
    for cols in num_cols:
        if df[cols].dtypes not in ["float64","int64"]:
            raise ValueError(f"{cols} has non float64 values")
        
        if (df[cols] < 0).any():
            raise ValueError(f"{cols} contains negative values")
        
# if __name__ == "__main__":
#     df = yf.download(['BTC-USD'])
#     df = df.droplevel('Ticker',axis=1).reset_index()
#     validate_schema(df)

