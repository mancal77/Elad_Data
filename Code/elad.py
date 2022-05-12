!pip install yfinance
!pip install pyarrow

import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

num_of_tickers = 504         #number of up to date stocks in S&P 500 for validation
# get tickers list from wikipedia page
try:
  tickers_list = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0].Symbol.to_list()
except Exception as ex:
        print(ex)
        tickers_list = []
        exit
if len(tickers_list) != num_of_tickers:      #validation
  print("Couldn't load full S&P 500 tickers list from wikipedia page")
  exit
tickers_list.remove('BRK.B')    
tickers_list.remove('BF.B')   
tickers_list.extend(['BRK-B','BF-B'])   # '-' required, instead of '.' in yfinance API    
#print(tickers_list)

# Make the API request
try:
  data = yf.download(  
        tickers = tickers_list,
        period = '5y'
        #,group_by = 'ticker'
    )
except Exception as ex:
        print(ex)
        exit

data = data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1) #get rid of multi-level columns

if len(data.columns) != num_of_tickers + 1:   #validation
  print("Couldn't get data for all tickers from yfinance API")
  exit

data.sort_index(inplace = True)   #sort by date
data = data[6:]     #clean the first day (5 years ago) to get rid of nulls
data['Year'] = data.index.year   #add Year partition column
data['Month'] = data.index.month_name()   #add Month partition column
#data.head(10)
#data.tail(10)
#data.sample(frac=0.001)

table_from_pandas = pa.Table.from_pandas(data)   #convert to pyArrow table

# save as parquet files with partitioning by year + month
try:
  pq.write_to_dataset(
    table_from_pandas,
    root_path='data/tickers_partitioned.parquet',
    partition_cols=['Year','Month']
  )
except Exception as ex:
  print(ex)
  print("Couldn't save all the data files")
