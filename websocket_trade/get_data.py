from pymongo import MongoClient
from datetime import datetime
import time
from tqdm import tqdm
from binance.client import Client
from datetime import timezone
from binance.enums import HistoricalKlinesType



def get_history(exchange, spot_or_futures, start_date: str, end_date: str, timeframe: str):
    client_mongo = MongoClient('localhost', 27017)
    db = client_mongo['History']
    dt_obj_st = datetime.strptime(start_date + ' 00:00:00,00',
                                  '%d.%m.%Y %H:%M:%S,%f')
    dt_obj_end = datetime.strptime(end_date + ' 00:00:00,00',
                                   '%d.%m.%Y %H:%M:%S,%f')
    start_date_ts = int(dt_obj_st.replace(tzinfo=timezone.utc).timestamp()) * 1000
    end_date_ts = int(dt_obj_end.replace(tzinfo=timezone.utc).timestamp()) * 1000
    timeframe_dict = {
        '1m': Client.KLINE_INTERVAL_1MINUTE,
        '5m': Client.KLINE_INTERVAL_5MINUTE,
        '15m': Client.KLINE_INTERVAL_15MINUTE,
        '4H': Client.KLINE_INTERVAL_4HOUR,
        '1D': Client.KLINE_INTERVAL_1DAY
    }
    stamp_interval = {
        '1m': 1 * 60 * 1000,
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '4H': 240 * 60 * 1000,
        '1D': 24 * 60 * 60 * 1000
    }

    client = Client('config_analitycal.API_KEY', 'config_analitycal.API_SECRET')
    prices_futures = client.futures_exchange_info()
    prices_spot = client.get_all_tickers()
    symbol_futures = [symbol['symbol'] for symbol in prices_futures['symbols']]
    symbol_spot = [symbol['symbol'] for symbol in prices_spot if symbol['symbol'][-4:] == 'USDT']
    symbol_or = {
        'SPOT': [symbol_spot, HistoricalKlinesType.SPOT],
        'FUTURES': [symbol_futures, HistoricalKlinesType.FUTURES]
    }
    for symbol in tqdm(symbol_or[spot_or_futures]):
        data_db = db[symbol].find({'timeframe': timeframe})
        if not list(data_db):
            klines = client.get_historical_klines(symbol, timeframe_dict[timeframe], start_date_ts, end_date_ts)
            data_klines = {
                '_id': symbol + '_' + timeframe,
                'exchange': exchange,
                'spot or futures': spot_or_futures,
                'timeframe': timeframe,
                'candles': []
            }
            for kline in klines:
                candle = {
                    'time': kline[0],
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                }
                data_klines['candles'].append(candle)
            db[symbol].insert_one(data_klines)

        data_db = list(db[symbol].find({'timeframe': timeframe}))[0]
        start_date_ts_dict = {
            'time': start_date_ts
        }
        end_date_ts_dict = {
            'time': end_date_ts
        }
        data_db['candles'].insert(0, start_date_ts_dict)
        data_db['candles'].append(end_date_ts_dict)
        index_count = -1
        for i in data_db['candles']:
            if index_count != len(data_db['candles']) * -1:
                if data_db['candles'][index_count]['time'] - data_db['candles'][index_count - 1]['time'] > \
                        stamp_interval[timeframe]:
                    if index_count == -1:
                        klines = client.get_historical_klines(symbol, timeframe_dict[timeframe],
                                                              data_db['candles'][index_count - 1]['time'] +
                                                              stamp_interval[
                                                                  timeframe],
                                                              data_db['candles'][index_count]['time'],
                                                              klines_type=symbol_or[spot_or_futures][1])
                        for kline in klines:
                            candle = {
                                'time': kline[0],
                                'open': float(kline[1]),
                                'high': float(kline[2]),
                                'low': float(kline[3]),
                                'close': float(kline[4]),
                            }
                            data_db['candles'].insert(index_count, candle)

                    if index_count != -1:
                        klines = client.get_historical_klines(symbol, timeframe_dict[timeframe],
                                                              data_db['candles'][index_count - 1]['time'] +
                                                              stamp_interval[timeframe],
                                                              data_db['candles'][index_count]['time'],
                                                              klines_type=symbol_or[spot_or_futures][1])
                        for i in range(len(klines) - 1):
                            candle = {
                                'time': klines[i][0],
                                'open': float(klines[i][1]),
                                'high': float(klines[i][2]),
                                'low': float(klines[i][3]),
                                'close': float(klines[i][4]),
                            }
                            data_db['candles'].insert(index_count, candle)
            index_count += -1
        data_db['candles'].pop(0)
        data_db['candles'].pop(-1)
        db[symbol].update_one({'_id': symbol + '_' + timeframe}, {"$set": data_db}, True)
    return 'Download ОК'
