import time
from datetime import datetime


def make_index(app):
    def check_index(_indexes, _index, result=False):
        if len(_indexes):
            for key in _indexes:
                try:
                    if _index == _indexes[key]['key'][0][0]:
                        result = True
                        break
                except:
                    pass
        return result

    # indexes_list = ['product_id', 'product_id', 'product_id', 'date', 'operation_id']
    indexes_list = ['product_id']
    app.collections_list = [app.collection_products, app.collection_prices, app.collection_stocks,
                        app.collection_analytics, app.collection_transaction]
    for ind, index in enumerate(indexes_list):
        indexes = app.collections_list[ind].index_information()
        if not check_index(indexes, index):
            app.collections_list[ind].create_index(index, unique=True)
            app.info_('index was created', index)
        else:
            app.info_('index already exists', index)


def send_items(app, items):
    for item in items:
        try:
            result = app.collection_products.update_one({'product_id': item['product_id']}, {'$set': item}, upsert=True)
            a = result.raw_result['updatedExisting']
            b = bool(result.modified_count)
            c = 'upserted' in result.raw_result
            app.info_(f"Existing:{a}, modified:{b}, upserted:{c}, product_id:{item['product_id']}")
        except:
            app.error_('product_id:', item['product_id'])


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
        app.info_(f"inserted:{d}, user_id:{user_id}, company_id:{company_id}")
    except:
        app.error_(f"user_id:{user_id}, company_id:{company_id}")


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
