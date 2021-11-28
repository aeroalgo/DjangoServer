def update_mongo_id(id_key, db, collection, dict, q_candle, position):
    if q_candle is None and position is not None:
        for data in dict:
            db[collection].update_one({'_id': id_key}, {'$push': {
                data: {'$each': dict[data], '$position': position}}}, True)
    elif position is None:
        for data in dict:
            db[collection].update_one({'_id': id_key}, {'$push': {
                data: {'$each': dict[data]}}}, True)
    else:
        for data in dict:
            db[collection].update_one({'_id': id_key}, {'$push': {
                data: {'$each': dict[data][-q_candle:], '$position': position}}}, True)
