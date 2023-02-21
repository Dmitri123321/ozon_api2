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
                    [('date', pymongo.ASCENDING), ('product_id', pymongo.ASCENDING)],
                    [('date', pymongo.ASCENDING), ('product_id', pymongo.ASCENDING)],
                    'category_id',
                    'attribute_id',
                    [('date', pymongo.ASCENDING), ('product_id', pymongo.ASCENDING)],
                    'product_id',
                    [('date', pymongo.ASCENDING), ('product_id', pymongo.ASCENDING)]
                    ]
    for ind, index in enumerate(indexes_list):
        if ind in [6]:
            continue
        indexes = app.collections_list[ind].index_information()
        if not check_index(indexes, index):
            app.collections_list[ind].create_index(index, unique=True)
            app.info_(f'index >> {index} was created')
        else:
            app.info_(f'index >> {index} already exists')


def insert_many(app, ind, items, user_id, company_id):
    try:
        result = app.collections_list[ind].insert_many(items, ordered=False)
        d = bool(result.inserted_ids)
        app.info_(f"inserted:{d}, base:{bases[ind]}, user_id:{user_id}, company_id:{company_id}")
    except:
        app.error_(f"user_id:{user_id}, company_id:{company_id}")


def bulk_write(app, ind, items, user_id, company_id):
    try:
        if ind == 3:
            # a=2
            list_write = [pymongo.UpdateOne({"category_id": item['category_id']},
                                            {'$set': item},
                                            upsert=True)
                          for item in items]
        elif ind == 4:
            # a=2
            list_write = [pymongo.UpdateOne({"attribute_id": item['attribute_id']},
                                            {'$set': item},
                                            upsert=True)
                          for item in items]
        elif ind in [0, 7]:
            list_write = [pymongo.UpdateOne({"product_id": item['product_id']},
                                            {'$set': item},
                                            upsert=True)
                          for item in items]
        else:
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
