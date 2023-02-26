import csv
import json
from copy import deepcopy


def join_json():
    result = []
    full_result = []
    path = '../results/items.json'
    with open(path, 'r', encoding='utf-8-sig') as f:
        items = json.load(f)
    print(len(items))
    path = '../results/items_of_stock.json'
    with open(path, 'r', encoding='utf-8-sig') as f:
        items_of_stock = json.load(f)
    print(len(items_of_stock))
    path = '../results/analytics.json'
    with open(path, 'r', encoding='utf-8-sig') as f:
        analytics = json.load(f)
    print(len(analytics))
    for a in analytics:
        count = 0
        for item in items:
            if a['id'] == item['FBO OZON SKU ID']:
                item_result = deepcopy(item)
                item_result['ordered'] = a['ordered']
                item_result['sold'] = a['sold']
                result.append(item_result)
                count = 1
                break
        if count == 0:
            item_result = {'FBO OZON SKU ID': a['id'], 'ordered': a['id'],
                           'sold': a['id'], 'Наименование товара': a['name']}
            result.append(item_result)
    for r in result:
        for item in items_of_stock:
            try:
                if r['Артикул'] == item['Артикул']:
                    for key in item:
                        if key not in r:
                            r[key] = item[key]
            except KeyError:
                pass
        full_result.append(r)
    print(len(full_result))
    return full_result


def csv_to_json(path):
    with open(path, "r", encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        csv_items = [','.join(line).strip() for line in reader]
    keys = [key.replace('"', "") for key in csv_items[0].split(';')]
    items = []
    for i, it in enumerate(csv_items):
        item: dict = {}
        if i == 0:
            continue
        values = [key.replace('"', "") for key in it.split(';')]
        for j, key in enumerate(keys):
            item[key] = values[j]
        item['Модель'] = item['Артикул'].split('/')[0]
        item['Размер'] = 0
        try:
            item['Размер'] = item['Артикул'].split('/')[1].split('-')[0]
        except:
            pass
        item['Цвет'] = 0
        try:
            item['Цвет'] = '-'.join(item['Артикул'].split('/')[1].split('-')[1:])
        except:
            pass
        items.append(item)
    return items


def analytisc_helper(analytisc_data):
    ordered_units = [order for order in analytisc_data if order['metrics'][0] > 0]
    cancellations = [order for order in analytisc_data if order['metrics'][1] > 0]
    returns = [order for order in analytisc_data if order['metrics'][2] > 0]
    sold = deepcopy(ordered_units)
    if cancellations:
        for cancellation in cancellations:
            for i, ordered in enumerate(sold):
                if cancellation['dimensions'][0]['id'] == ordered['dimensions'][0]['id']:
                    count_ordered = ordered['metrics'][0]
                    count_cancellation = cancellation['metrics'][1]
                    count_ordered = count_ordered - count_cancellation
                    if count_ordered == 0:
                        sold.pop(i)
                    break
    if returns:
        for ret in returns:
            for i, ordered in enumerate(sold):
                if ret['dimensions'][0]['id'] == ordered['dimensions'][0]['id']:
                    count_ordered = ordered['metrics'][0]
                    count_returns = ret['metrics'][2]
                    count_ordered = count_ordered - count_returns
                    if count_ordered == 0:
                        sold.pop(i)
                    break
    # pprint(ordered_units)
    items = []
    for ord_ in ordered_units:
        item = {'id': ord_['dimensions'][0]['id'], 'name': ord_['dimensions'][0]['name'],
                'ordered': ord_['metrics'][0]}
        items.append(item)
    for s in sold:
        for item in items:
            if s['dimensions'][0]['id'] == item['id']:
                item['sold'] = s['metrics'][0]
    clear_items = []
    ids = list(set([item['id'] for item in items]))
    for id_ in ids:
        new_item = {'id': id_}
        ordered = 0
        sold = 0
        for item in items:
            if id_ == item['id']:
                new_item['name'] = item['name']
                ordered += item['ordered']
                sold += item['sold']
        new_item['ordered'] = ordered
        new_item['sold'] = sold
        clear_items.append(new_item)
    # pprint(clear_items)
    return clear_items


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


def send_items_transactions(app, items):
    for item in items:
        try:
            result = app.collection_transaction.insert_one(item)
            # print(result.__dir__())
            d = bool(result.inserted_id)
            app.info_(f"inserted:{d}, obj:{item['operation_id']}")
        except:
            app.error_('operation_id:', item['operation_id'])


def send_items(app, ind, items, user_id, company_id, up_key=None):
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


def reform_analytics(self, analytics_data, products_data, metrics):
    list1 = ['offer_id_short', 'offer_id', 'product_id', 'category_name', 'name', 'color', 'size', 'brand',
             'vendor_size', 'common_card_id']
    list2 = ['coming', 'present', 'reserved']
    list3 = ['marketing_price', 'min_ozon_price', 'old_price', 'premium_price', 'price']
    x = {'date': str(self.today), 'user_id': self.user_id, 'company_id': self.company_id,
         'updated_at': self.updated_at}
    analytics = []
    anal_dict = {y['dimensions'][0]['id']: y['metrics'] for y in analytics_data}
    for product in products_data:
        analytic = {'updated_at': self.updated_at}
        for key in list1:
            analytic[key] = product[key] if key in product else None
        for key in list2:
            analytic[key] = product['stocks'][key] if key in product['stocks'] else 0
        for key in list3:
            analytic[key] = product['price'][key] if key in product['price'] else 0
        error_metrics = []
        for i, metric in enumerate(metrics):
            try:
                analytic[metric] = anal_dict[str(product['fbo_sku'])][i]
            except:
                error_metrics.append(metric)
                analytic[metric] = None
        if error_metrics:
            self.app.warn_('error metrics:', *error_metrics, product['product_id'], self.company_id)
        analytic.update(x)
        analytics.append(analytic)
    return analytics
