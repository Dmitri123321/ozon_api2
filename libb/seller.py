import csv
import datetime as dt
import requests
import json

import time
from copy import deepcopy


class Client:
    def __set_name__(self, owner, name):
        self.name = '_' + name

    def __get__(self, instance, owner):
        return getattr(instance, self.name)

    def __set__(self, instance, value):
        setattr(instance, self.name, value)


class Seller:
    client_id = Client()
    api_key = Client()

    def __init__(self, app, client_id, api_key, user_id, company_id):
        self.app = app
        self.client_id = client_id
        self.api_key = api_key
        self.user_id = user_id
        self.company_id = company_id
        self.headers = {
            "Client-Id": f"{self.client_id}",
            "Api-Key": f"{self.api_key}"
        }

    def get_product_ids(self, last_id=""):
        url = 'https://api-seller.ozon.ru/v2/product/list'
        all_items = []
        for _ in range(0, 100):
            data = {
                "limit": 1000,
                "last_id": last_id,
                "filter": {
                    "visibility": "ALL"
                }
            }
            items, total, last_id = get_helper1(connect1(self, url, self.headers, data))
            all_items.extend(items)
            if not last_id or len(items) < 1000:
                break
        product_ids = [item.get('product_id') for item in all_items]
        return product_ids

    def get_products_info(self, product_ids, n=1000):
        url = 'https://api-seller.ozon.ru/v2/product/info/list'
        items_info = []
        product_ids_part = [product_ids[i: i + n] for i in range(0, len(product_ids), n)]
        for part in product_ids_part:
            data = {
                "offer_id": [],
                "product_id": part,
                "sku": []
            }
            items, total, last_id = get_helper1(connect1(self, url, self.headers, data))
            items_info.extend(items)
        return items_info

    def reform_items_info(self, products_info):
        keys_for_price = ['marketing_price', 'min_ozon_price', 'old_price', 'premium_price', 'price']
        keys_for_stock = ['coming', 'present', 'reserved']
        removed_keys = ['commissions', 'recommended_price', 'min_price', 'sources', 'errors', 'price_index',
                        'service_type', 'sources', 'state', 'status', 'vat', 'visibility_details']
        updated_at = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        prices, products, stocks, categories = [], [], [], []
        for item in products_info:
            product_id = item.pop('id')
            price = {}
            for i in keys_for_price:
                try:
                    price[i] = float(item.get(i, 0))
                except:
                    price[i] = 0
            stock = {x: item.get('stocks', {}).get(x, 0) for x in keys_for_stock}
            offer_id_short = l[0] if isinstance(l := item['offer_id'].split('/'), list) and len(l) > 0 else ''
            y = {'product_id': product_id, 'offer_id_short': offer_id_short,
                 'date': self.app.today_time, 'updated_at': updated_at}
            price.update(y)
            stock.update(y)
            category = {'category_id': item['category_id'], 'title': self.get_category_name(item['category_id']),
                        'updated_at': updated_at}
            product = {'user_id': self.user_id, 'company_id': self.company_id}
            removed_keys.extend(keys_for_price)
            for key in removed_keys:
                if key in item:
                    del item[key]
            item.update(y)
            item.update(product)
            item.update(category)
            item['price'] = price
            item['stocks'] = stock
            prices.append(price)
            stocks.append(stock)
            products.append(item)
            categories.append(category) if category not in categories else None
        return prices, products, stocks, categories

    def get_category_name(self, cat_id):
        url = 'https://api-seller.ozon.ru/v2/category/tree'
        name = self.app.cat_ids.get(cat_id)
        if not name:
            data = {
                "category_id": cat_id,
                "language": "DEFAULT"
            }
            items, total, last_id = get_helper2(connect1(self, url, self.headers, data))
            cat = items[0]
            name = cat['title']
            self.app.cat_ids[cat_id] = name
        return name

    def get_attributes(self, categories):
        updated_at = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        categories_list = [x['category_id'] for x in categories]
        url = 'https://api-seller.ozon.ru/v3/category/attribute'
        data = {
            "attribute_type": "ALL",
            "category_id": categories_list,
            "language": "DEFAULT"
        }
        items, total, last_id = get_helper2(connect1(self, url, self.headers, data))
        new_attributes = []
        for category in items:
            category_id = category['category_id']
            attributes = category['attributes']
            for attribute in attributes:
                attribute['category_id'] = category_id
                attribute['attribute_id'] = attribute.pop('id')
                attribute['updated_at'] = updated_at
                new_attributes.append(attribute)
        return new_attributes

    def get_attribute_values(self, product_ids, last_id='', total=1001):
        url = 'https://api-seller.ozon.ru/v3/products/info/attributes'
        filter_ = {"product_id": product_ids} if product_ids else {"visibility": "ALL"}
        all_items = []
        while total >= 1000:
            data = {
                "filter": filter_,
                "limit": 1000,
                "last_id": last_id,
                "sort_dir": "ASC"
            }
            items, total, last_id = get_helper2(connect1(self, url, self.headers, data))
            all_items.extend(items)
            if not last_id or total < 1000:
                break
        return {x['id']: x for x in all_items}

    def get_stocks(self, items_list):
        url = 'https://api-seller.ozon.ru/v3/product/info/stocks'

        offers_id = [pair.get('offer_id') for pair in items_list]

        data = {
            "filter": {"offer_id": offers_id,
                       "product_id": [],
                       "visiblity": "ALL", },
            "limit": 1000
        }

        json_data = connect1(self, url, self.headers, data)
        stocks = json_data['result'].get('items')
        return stocks

    def analytisc_helper(self, analytisc_data):
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

    def get_analytics(self, metrics, period, period_step_back, count_ids, mode='raw'):
        metrics = ["ordered_units", "cancellations", "returns", "revenue",
                   "delivered_units"] if not metrics else metrics
        # metrics = ["hits_view_search", "hits_view_pdp", "hits_view", "hits_tocart_search", "position_category"]
        """
            metrics:
            hits_view_search — показы в поиске и в категории,
            hits_view_pdp — показы на карточке товара,
            hits_view — всего показов,
            hits_tocart_search — в корзину из поиска или категории,
            hits_tocart_pdp — в корзину из карточки товара,
            hits_tocart — всего добавлено в корзину,
            session_view_search — сессии с показом в поиске или в категории,
            session_view_pdp — сессии с показом на карточке товара,
            session_view — всего сессий,
            conv_tocart_search — конверсия в корзину из поиска или категории,
            conv_tocart_pdp — конверсия в корзину из карточки товара,
            conv_tocart — общая конверсия в корзину,
            revenue — заказано на сумму,
            returns — возвращено товаров,
            cancellations — отменено товаров,
            ordered_units — заказано товаров,
            delivered_units — доставлено товаров,
            adv_view_pdp — показы на карточке товара, спонсорские товары,
            adv_view_search_category — показы в поиске и в категории, спонсорские товары,
            adv_view_all — показы всего, спонсорские товары,
            adv_sum_all — всего расходов на рекламу,
            position_category — позиция в поиске и категории,
            postings — отправления,
            postings_premium — отправления с подпиской Premium.
        """
        url = 'https://api-seller.ozon.ru/v1/analytics/data'
        today = dt.datetime.today()
        date_to = dt.datetime.strftime(today - dt.timedelta(period_step_back), '%Y-%m-%d')
        date_from = dt.datetime.strftime(today - dt.timedelta(period_step_back + period - 1), '%Y-%m-%d')
        """ 
            dimension:
            sku — идентификатор товара,
            spu — идентификатор товара,
            day — день,
            week — неделя,
            month — месяц,
            year — год,
            category1 — категория первого уровня,
            category2 — категория второго уровня,
            category3 — категория третьего уровня,
            category4 — категория четвертого уровня,
            brand — бренд,
            modelID — модель.
        """
        analytisc_data = []
        offset = 0
        attempt = 0
        len_result = 1000
        data_limit = 1000
        limit = count_ids // data_limit + 1
        while attempt < limit and not len_result < data_limit:
            attempt += 1
            try:
                data = {
                    "date_from": date_from,
                    "date_to": date_to,
                    "metrics": metrics,
                    "filters": [],
                    "dimension": ["sku"],
                    "limit": data_limit,
                    "offset": offset
                }
                json_data = connect1(self, url, self.headers, data)
                result = json_data['result']['data'] if mode == 'raw' else self.analytisc_helper(
                    json_data['result']['data'])
                len_result = len(result)
                analytisc_data += result
                offset += data_limit
            except:
                self.app.error_(self.headers)
                self.app.stop('')
        return analytisc_data, metrics

    def reform_analytics_data(self, analytics_data, products_data, company_data, metrics):
        analytics = []
        for analytic_data in analytics_data:
            fbs_sku = analytic_data['dimensions'][0]['id']
            analytic = {'offer_id_short': '', 'product_id': 0}
            for product in products_data:
                if int(fbs_sku) == product['fbo_sku']:
                    analytic['offer_id_short'] = product['offer_id_short']
                    analytic['product_id'] = product['product_id']
                    # if analytic['product_id'] == 258372932:
                    #     a =1
                    analytic['coming'] = product.get('stocks', {}).get('coming', 0)
                    analytic['present'] = product.get('stocks', {}).get('present', 0)
                    analytic['reserved'] = product.get('stocks', {}).get('reserved', 0)
                    analytic['brand'] = product['brand']
                    analytic['category_name'] = product['category_name']
                    analytic['name'] = product['name']
                    analytic['marketing_price'] = product['price']['marketing_price']
                    analytic['min_ozon_price'] = product['price']['min_ozon_price']
                    analytic['old_price'] = product['price']['old_price']
                    analytic['premium_price'] = product['price']['premium_price']
                    analytic['price'] = product['price']['price']
                    analytic['color'] = product['color']
                    analytic['size'] = product['size']
                    break
            if not analytic['product_id']:
                continue
            for i, metric in enumerate(metrics):
                try:
                    analytic[metric] = analytic_data['metrics'][i]
                except:
                    self.app.error_(metric, self.company_id, self.client_id)
            analytic['date'] = self.app.today_time
            analytic['user_id'] = company_data.get('user_id')
            analytic['company_id'] = company_data.get('id')
            analytic['updated_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            analytics.append(analytic)
        return analytics

    def get_stocks_of_warehouse(self):
        url = 'https://api-seller.ozon.ru/v1/analytics/stock_on_warehouses'

        data = {
            "limit": 1000,
            "offset": 0
        }

        json_data = connect1(self, url, self.headers, data)
        stocks = json_data
        return stocks

    def get_items_info(self, item):
        url = 'https://api-seller.ozon.ru/v2/product/info'

        data = {
            "offer_id": item.get('offer_id'),
            "product_id": item.get('product_id'),
            "sku": 0
        }

        json_data = connect1(self, url, self.headers, data)
        item_info = json_data['result']
        return item_info

    def get_report_of_stock(self, file_name):
        url = 'https://api-seller.ozon.ru/v1/report/stock/create'

        data = {
            "language": 'DEFAULT'
        }

        json_data = connect1(self, url, self.headers, data)
        code = json_data['result']['code']
        print(code)
        self.get_file_of_report(code=code, file_name=file_name)

    def get_report_of_items(self, file_name, items_list):
        url = 'https://api-seller.ozon.ru/v1/report/products/create'

        offers_id = [pair.get('offer_id') for pair in items_list]

        data = {
            "language": 'DEFAULT',
            "offer_id": offers_id,
            "search": "",
            "sku": [],
            "visibility": "ALL"
        }

        json_data = connect1(self, url, self.headers, data)
        code = json_data['result']['code']
        print(code)
        self.get_file_of_report(code=code, file_name=file_name)

    def get_file_of_report(self, code, file_name):
        url = 'https://api-seller.ozon.ru/v1/report/info'

        data = {
            "code": f'{code}'
        }

        body = json.dumps(data)
        while True:
            res = requests.post(url=url, headers=self.headers, data=body)
            print(res.status_code)
            report_info = res.json()
            print(report_info)
            url_file = report_info['result']['file']
            if url_file != '':
                break
            time.sleep(10)
        res = requests.get(url=url_file, headers=self.headers)
        print(res.status_code)
        path = '../results/' + file_name
        with open(path, 'wb') as f:
            f.write(res.content)

    def csv_to_json(self, path):
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

    def join_json(self):
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



    def get_transaction_list(self, period, period_step_back, page_count=1, page=1, while_loop=0):
        """пока не понятно что с периодом , отдает больше чем за 3 месяца"""
        url = 'https://api-seller.ozon.ru/v3/finance/transaction/list'
        date_to = dt.datetime.strftime(self.app.today - dt.timedelta(period_step_back), '%Y-%m-%dT%H:%M:%S.%f')[
                  0:23] + 'Z'
        date_from = dt.datetime.strftime(self.app.today - dt.timedelta(period_step_back + period - 1),
                                         '%Y-%m-%dT%H:%M:%S.%fZ')[0:23] + 'Z'
        transaction_list = []
        while page <= page_count:
            while_loop += 1
            if while_loop > 1000 or while_loop > page_count + 1:
                self.app.stop('while in loop')
            data = {
                "filter": {
                    "date": {
                        "from": date_from,
                        "to": date_to
                    },
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all"
                },
                "page": page,
                "page_size": 1000
            }
            json_data = connect1(self, url, self.headers, data)
            page_count = json_data['result']['page_count']
            transaction_list.extend(json_data['result']['operations'])
            page += 1
        return transaction_list

    def reform_transaction_list(self, transactions, products):
        for transaction in transactions:
            if len(transaction['items']) > 0:
                fbs_sku = transaction['items'][0]['sku']
                for product in products:
                    if int(fbs_sku) == product['fbs_sku']:
                        transaction['offer_id_short'] = product['offer_id_short']
                        transaction['product_id'] = product['product_id']
                        break
            else:
                transaction['offer_id_short'] = None
                transaction['product_id'] = None
        return transactions

    def get_rating(self, reform_json):
        url = 'https://api-seller.ozon.ru/v1/product/rating-by-sku'
        ids = [item['product_id'] for item in reform_json]
        n = 100
        sku_ids_part = [ids[i: i + n] for i in range(0, len(ids), n)]
        ratings = []
        for part in sku_ids_part:
            data = {
                "skus": part
            }
            json_data = connect1(self, url, self.headers, data)
            ratings_part = json_data['products']
            ratings += ratings_part
        for rating in ratings:
            rating['product_id'] = rating.pop('sku')
        return ratings


def connect1(self, url, headers, data, attempt=1, res=None):
    while attempt < 5:
        attempt += 1
        try:
            time.sleep(2)
            body = json.dumps(data)
            res = requests.post(url=url, headers=headers, data=body)
            status_code = res.status_code
            self.app.info_(status_code, url) if self else print(status_code, url)
            if status_code == 200:
                break
            elif status_code == 429:
                time.sleep(10)
                continue
        except:
            time.sleep(10)
    try:
        json_data = res.json()
    except:
        json_data = {}
        self.app.error_('no json_data or json_data are corrupt')
    return json_data


def get_helper1(json_data):
    result = json_data.get('result', {})
    last_id = result.get('last_id', '')
    total = result.get('total', 0)
    items = result.get('items', [])
    return items, total, last_id


def get_helper2(json_data):
    items = json_data.get('result', [])
    last_id = json_data.get('last_id', '')
    total = json_data.get('total', 0)
    return items, total, last_id
