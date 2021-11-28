from pymongo import MongoClient
from datetime import datetime
import tzlocal


def get_last_update_db():
    client_mongo = MongoClient('localhost', 27017)
    db = client_mongo['History']
    data_db = list(db['BTCUSDT'].find({'timeframe': '5m'}))[0]
    x = data_db['candles'][-1]['time']
    unix_timestamp = x/1000
    utc_time = datetime.utcfromtimestamp(unix_timestamp)
    return utc_time