from datetime import datetime
import time
from pymongo import MongoClient
import pandas as pd

client_mongo = MongoClient('localhost', 27017)
db = client_mongo['MarketData']
close_list = list(db['BTCUSDT'].find({"timeframe": '5m'}, {'candles'}))[0]
close_list = close_list['candles']
df = pd.DataFrame()
data_candle_pd = {
    'time': [],
    'open': [],
    'high': [],
    'low': [],
    'close': []
}
for cndl_obj in close_list:
    value = datetime.utcfromtimestamp(cndl_obj['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    print(value)
    data_candle_pd['time'].append(value)
    data_candle_pd['open'].append(cndl_obj['open'])
    data_candle_pd['high'].append(cndl_obj['high'])
    data_candle_pd['low'].append(cndl_obj['low'])
    data_candle_pd['close'].append(cndl_obj['close'])
df = pd.DataFrame(data_candle_pd)
print(df)
