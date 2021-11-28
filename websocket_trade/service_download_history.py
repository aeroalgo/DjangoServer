import time
import redis
from binance.enums import HistoricalKlinesType
from pymongo import MongoClient
from datetime import timezone
from datetime import datetime
from binance.client import Client
from tqdm import tqdm
import copy


def load_history(ticker):
    start_date = '01.01.2021'
    client_mongo = MongoClient('localhost', 27017)
    db = client_mongo['MarketData']
    r1 = redis.Redis(host='127.0.0.1', port=6379, db=1, charset="utf-8", decode_responses=True)
    dt_obj_st = datetime.strptime(start_date + ' 00:00:00,00',
                                  '%d.%m.%Y %H:%M:%S,%f')
    start_date_ts = int(dt_obj_st.replace(tzinfo=timezone.utc).timestamp()) * 1000
    # try:
    client = Client('config.API_KEY', 'config.API_SECRET')
    stamp_interval = {
        '1m': 1 * 60 * 1000,
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '4H': 240 * 60 * 1000,
        '1D': 24 * 60 * 60 * 1000
    }
    timeframe_dict = {
        '1m': Client.KLINE_INTERVAL_1MINUTE,
        '5m': Client.KLINE_INTERVAL_5MINUTE,
        '15m': Client.KLINE_INTERVAL_15MINUTE,
        '1D': Client.KLINE_INTERVAL_1DAY
    }
    symbol_or = {
        'SPOT': HistoricalKlinesType.SPOT,
        'FUTURES': HistoricalKlinesType.FUTURES
    }
    try:
        for symbol in tqdm(ticker):
            now = datetime.now()
            now = int(time.mktime(now.timetuple())) * 1000
            try:
                timestamp_rdb = int(r1.hget(symbol, 't'))
            except:
                continue
            sec_sleep = timestamp_rdb / 1000 + 300 - now - 20
            if sec_sleep < 0 and now < timestamp_rdb:
                time.sleep(25)
            data_klines = {
                '_id': symbol + '_' + '5m',
                'exchange': 'Binance',
                'spot or futures': 'SPOT',
                'timeframe': '5m',
                'time': [],
                'open': [],
                'high': [],
                'low': [],
                'close': []
            }
            data_db = list(db[symbol].find({'_id': symbol + '_' + '5m'}))
            if not data_db:
                db[symbol].update_one({'_id': symbol + '_' + '5m'}, {'$set': data_klines}, True)
            data_last_kline = r1.hgetall(symbol)
            end_date_ts = int(data_last_kline['t'])
            start_date = start_date_ts - stamp_interval['5m']
            end_date = end_date_ts
            data_db = list(db[symbol].find({'_id': symbol + '_' + '5m'}))[0]
            data_db['time'].insert(0, start_date)
            data_db['time'].append(end_date)
            index = 0
            try:
                while index != len(data_db['time']):
                    insert_candle = {
                        'time': [],
                        'open': [],
                        'high': [],
                        'low': [],
                        'close': []
                    }
                    if data_db['time'][index + 1] - data_db['time'][index] > \
                            stamp_interval['5m']:
                        klines = client.get_historical_klines(symbol, timeframe_dict['5m'],
                                                              data_db['time'][index] +
                                                              stamp_interval[
                                                                  '5m'],
                                                              data_db['time'][index + 1],
                                                              klines_type=symbol_or['SPOT'])
                        for kline in klines:
                            insert_candle['time'].append(kline[0])
                            insert_candle['open'].append(kline[1])
                            insert_candle['high'].append(kline[2])
                            insert_candle['low'].append(kline[3])
                            insert_candle['close'].append(kline[4])

                        insert_candle['time'].pop(-1)
                        insert_candle['open'].pop(-1)
                        insert_candle['high'].pop(-1)
                        insert_candle['low'].pop(-1)
                        insert_candle['close'].pop(-1)
                        if len(insert_candle['time']) != 0:
                            # print('start', ' ', data_db['candles'][index]['time'] + stamp_interval['5m'])
                            # print('end', ' ', data_db['candles'][index + 1]['time'])
                            # print('pos', ' ', index)
                            # print(data_db['candles'][index - 2:index + 2])
                            # print(klines)
                            # print(insert_candle)
                            for candle_obj in insert_candle:
                                db[symbol].update_one({'_id': symbol + '_' + '5m'},
                                                      {"$push": {candle_obj: {'$each': insert_candle[candle_obj],
                                                                              '$position': index}}})
                        data_db = list(db[symbol].find({'_id': symbol + '_' + '5m'}))[0]
                        data_db['time'].insert(0, start_date)
                        data_db['time'].append(end_date)
                    index += 1
            except:
                continue

    except:
        time.sleep(300)
        load_history(ticker)

# x = load_history(['WAVESUSDT'])
