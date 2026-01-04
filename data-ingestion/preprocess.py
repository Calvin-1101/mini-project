#%%
import pandas as pd
from schema import validate_schema
from datetime import datetime
import yfinance as yf
"""
preprocess.py takes in a raw csv file, performs preprocessing and cleaning,
and outputs a cleaned DataFrame that passes the schema validation checks. 

We will be using the yfinance library to obtain data, the data will be saved to a csv file
and loaded into preprocess.py to mimic actual CSV reading. 

This will allow us to be flexible and read CSV data from other sources in the future.


To add: will adding timezone awareness to column "timestamp" improve or change anything? 
"""


class DataLoader():
    def __init__(self,csv_file):
        """
        Note: self.df should not be a class attribute, as this DataLoader can and should 
        be reused for multiple instances of csv datas. if we use self.df, there might be persistent data,
        or other issues. This DataLoader should be stateless. 
        """
        self.file_path = csv_file

    def _flatten_yahoo_columns(self,df):
        """
        Method to flatten dataframes with MultiIndex columns
        """
        df = df.copy()
        print("before",df.columns)
        if not isinstance(df.columns,pd.MultiIndex):
            return df
        
        if df.columns.nlevels != 2:
            raise ValueError(f"Expected MultiIndex column depth of 2, received {df.columns.nlevels}")
        if df.columns.get_level_values(1).unique() != 1: #retrieving the Ticker level
            raise ValueError("Expected single Ticker operation only")

        df.columns = df.columns.get_level_values(0)
        print("after",df.columns)

        return df 
    
    # def _normalize_index(self,df):
    #     df = df.copy()
    #     #since the Date is the index, we want to normalize the index and make the Date into a column
    #     df = df.reset_index()
    #     print(df.columns)
    #     return df 

    def _normalize_columns(self,df):
        df = df.copy()

        df = self._flatten_yahoo_columns(df)
        # df = self._normalize_index(df)

        df.columns = (df.columns.str.strip().str.lower())
        print("before",df.columns)
        df = df.rename(columns={
            "date":"timestamp",
            "datetime":"timestamp",
            "time":"timestamp"
        })

        return df 
    
    def _parse_timestamp(self,df):
        #converts timestamp to datetime object 
        df = df.copy()
        if 'timestamp' not in df.columns:
            raise ValueError("Timestamp not in df columns")
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors = "raise")
        return df 



    def _coerce_numeric(self,df):
        #enforces all numerical values to be numeric, musst raise errors 
        df = df.copy()

        num_cols = ['open','high','low','close','volume']

        for cols in num_cols:
            if cols not in df.columns:
                raise ValueError(f"{cols} not in df columns")
            
            df[cols] = pd.to_numeric(df[cols],errors="raise")

        return df 

    def _clean(self,df):
        #drops null values, sorts timestamp by increasing order, drops duplicates in timestamp 
        df = df.copy()
        df = df.dropna()
        df= df.sort_values("timestamp")
        df = df.drop_duplicates(subset=['timestamp'],keep='first')

        return df 



    def _save_processed(self,df):
        #save proccessed dataframe as a csv -> easier to debug and reproduce models in the future. this is just a good habit.
        df = df.copy()
        current = datetime.now()
        year = current.year
        day = current.day
        month = current.month
        hour = current.hour 
        timestamp = f"{year}/{month}/{day}/{hour}"
        saved_file_path = f"C:/Users/calvin/Documents/python/data science projects/trading/processed_csv/{year}_{month}_{day}_{hour}_clean_csv.csv"
        df.to_csv(saved_file_path, index=False)

    def load(self):
        df = pd.read_csv(self.file_path)
        df = self._normalize_columns(df)
        df = self._parse_timestamp(df)
        df = self._coerce_numeric(df)
        df = self._clean(df)
        self._save_processed(df)
        return df
        
#%%
if __name__ == "__main__":
    btc = yf.download(['BTC-USD'],start="2022-11-15",interval='1d',multi_level_index=False)  #5m interval only up to last 60 days. adjust this for a larger timeframe/shorter interval
    
    #%%
    btc.head()
    #%%
    btc.to_csv(f"C:/Users/calvin/Documents/python/data science projects/trading/raw_csv/btc.csv",index_label='Date')
#%%
    btc_file_path = "C:/Users/calvin/Documents/python/data science projects/trading/raw_csv/btc.csv"
#%%
    loader = DataLoader(btc_file_path)
    df = loader.load()

# %%    
    validate_schema(df)
# %%
