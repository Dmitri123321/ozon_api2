import pymongo

bases = ['products', 'prices', 'stocks', 'categories', 'attributes', 'analytics', 'transaction', 'raiting']


def make_index(app):
    def check_index(_indexes, _index, result=False):
        if len(_indexes):
            for key in _indexes:
                try:
                    if key.count('1') == 1 and _index == _indexes[key]['key'][0][0]:
                        result = True
                        break
                    elif key.count('1') == 2 and _index == _indexes[key]['key']:
                        result = True
                        break
                except:
                    pass
        return result

    indexes_list = ['product_id',
                    'product_id',
                    'product_id',
                    'category_id',
                    [('date', pymongo.ASCENDING), ('product_id', pymongo.ASCENDING)],
                    'operation_id',
                    'product_id',
                    [('category_id', pymongo.ASCENDING), ('attribute_id', pymongo.ASCENDING)]
                    ]
    for ind, index in enumerate(indexes_list):
        if ind not in [0, 3, 4, 5, 7]:
            continue
        indexes = app.collections_list[ind].index_information()
        if not check_index(indexes, index):
            app.collections_list[ind].create_index(index, unique=True)
            app.info_(f'index >> {index} was created')
        else:
            app.info_(f'index >> {index} already exists')


def send_items(app, ind, items, user_id, company_id):
    for item in items:
        try:
            if 'product_id' in item:
                up_key = {'product_id': item['product_id']}
            else:
                up_key = {'category_id': item['category_id']}
            result = app.collections_list[ind].update_one(up_key, {'$set': item}, upsert=True)
            a = result.raw_result['updatedExisting']
            b = bool(result.modified_count)
            c = 'upserted' in result.raw_result
            app.info_(f"Existing:{a}, modified:{b}, upserted:{c}, up_key:{up_key}")
        except:
            app.error_(f"'up_key:'{up_key}, user_id:{user_id}, company_id:{company_id}")


def send_items_transactions(app, items):
    for item in items:
        try:
            result = app.collection_transaction.insert_one(item)
            # print(result.__dir__())
            d = bool(result.inserted_id)
            app.info_(f"inserted:{d}, obj:{item['operation_id']}")
        except:
            app.error_('operation_id:', item['operation_id'])


def insert_many(app, ind, items, user_id, company_id):
    try:
        result = app.collections_list[ind].insert_many(items, ordered=False)
        d = bool(result.inserted_ids)
        app.info_(f"inserted:{d}, base:{bases[ind]}, user_id:{user_id}, company_id:{company_id}")
    except:
        app.error_(f"user_id:{user_id}, company_id:{company_id}")


def bulk_write(app, ind, items, user_id, company_id):
    try:
        list_write = [pymongo.UpdateOne({"date": item['date'], "product_id": item['product_id']},
                                        {'$set': item},
                                        upsert=True)
                      for item in items]
        result = app.collections_list[ind].bulk_write(list_write)
        f = True if result.matched_count == len(items) else False
        g = True if result.upserted_count == len(items) else False
        app.info_(f"updated: {f}, upserted: {g}, base:{bases[ind]}, user_id:{user_id}, company_id:{company_id}")
    except:
        app.error_(f"user_id:{user_id}, company_id:{company_id}")
        pass


def send_stocks(app, items):
    for item in items:
        try:
            state = {
                "date": item["date"],
                "coming": item["coming"],
                "present": item["present"],
                "reserved": item["reserved"],
            }
            result = app.collection_stocks.update_one(
                {'product_id': item['product_id']},
                {'$push': {'state': state},
                 '$set': {'offer_id_short': item['offer_id_short']}},
                upsert=True)
            a = result.raw_result['updatedExisting']
            b = bool(result.modified_count)
            c = 'upserted' in result.raw_result
            app.info_(f"Existing:{a}, modified:{b}, upserted:{c}, product_id:{item['product_id']}")
        except:
            app.error_('product_id:', item['product_id'])


def send_prices(app, items):
    for item in items:
        try:
            state = {
                "date": item["date"],
                "marketing_price": item["marketing_price"],
                "min_ozon_price": item["min_ozon_price"],
                "old_price": item["old_price"],
                "premium_price": item["premium_price"],
                "price": item["price"]
            }
            result = app.collection_prices.update_one(
                {'product_id': item['product_id']},
                {'$push': {'state': state}, '$set': {'offer_id_short': item['offer_id_short']}},
                upsert=True)
            a = result.raw_result['updatedExisting']
            b = bool(result.modified_count)
            c = 'upserted' in result.raw_result
            app.info_(f"Existing:{a}, modified:{b}, upserted:{c}, product_id:{item['product_id']}")
        except:
            app.error_('product_id:', item['product_id'])
