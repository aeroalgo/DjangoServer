import json
import pandas as pd
from pymongo import MongoClient

client_mongo = MongoClient('localhost', 27017)
db = client_mongo['MarketData']
list_coll = db.list_collection_names()
print(list_coll)
for i in list_coll:
    close_list = list(db[i].find({"timeframe": '5m'}))[0]
    del close_list['time'][-3:]
    del close_list['open'][-3:]
    del close_list['high'][-3:]
    del close_list['low'][-3:]
    del close_list['close'][-3:]
    print(close_list['_id'])
    db[i].replace_one({'_id': close_list['_id']}, close_list)

# candle = {
#     '_id': col['_id'],
#     'exchange': col['exchange'],
#     'spot or futures': col['spot or futures'],
#     'timeframe': col['timeframe'],
#     'time': [],
#     'open': [],
#     'high': [],
#     'low': [],
#     'close': []
# }
# try:
#     for cndl_obg in col['candles']:
#         candle['time'].append(cndl_obg['time'])
#         candle['open'].append(cndl_obg['open'])
#         candle['high'].append(cndl_obg['high'])
#         candle['low'].append(cndl_obg['low'])
#         candle['close'].append(cndl_obg['close'])
#     db[collection_obj].delete_one({'_id': collection_obj + '_' + '5m'})
#     db[collection_obj].insert_one(candle)
#     print(col['_id'])
# except:
#     continue
