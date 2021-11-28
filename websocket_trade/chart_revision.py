from pymongo import MongoClient

client_mongo = MongoClient('localhost', 27017)
db = client_mongo['MarketData']
close_list = db['BTCUSDT'].find({"timeframe": '5m'}, {'time'})
collections = list(db.list_collections())
# print(len(collections))
# ['TRXUSDT',]
x = list(close_list)[0]['time']
print(x[-6:])
for i in range(len(x)):
    range_ts = x[i + 1] - x[i]
    if range_ts > 300000:
        print(x[i - 2:i + 2])
        print(range_ts, x[i])
# x = [1, 2, 3, 4, 5, 6]
#
# for i in enumerate(x):
#     x.append(1)
#     print(i)
# timestamp = list(db['BTCUSDT'].find({'_id': 'BTCUSDT' + '_' + '5m'}))[0]
# try:
#     timestamp = timestamp['candles'][-1]['time']
#     print(timestamp)
# except:
#     timestamp = 0
# if res['k']['t'] != timestamp:
#     pass
