from pymongo import MongoClient

from config import config


def get_name_trade(ma_num: list):
    client_mongo = MongoClient('localhost', 27017)
    db = client_mongo['OptimizedData']
    data_name_pair = {}
    ticker_name = set()
    for ma in ma_num:
        collection = 'Portfolios'
        data_pair = db[collection].find({'MA': config.ma_type[str(ma)]}, {'PortfolioNamePair'})
        data_pair = list(data_pair)[0]['PortfolioNamePair']
        data_name_pair[ma] = data_pair
        for name in data_pair:
            ticker_name.add(name[0])
            ticker_name.add(name[1])
    return {'data_name_pair': data_name_pair, 'ticker_name': ticker_name}
