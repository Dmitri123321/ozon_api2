from datetime import date
import requests
import json
import time


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
        self.headers = {"Client-Id": f"{self.client_id}", "Api-Key": f"{self.api_key}"}
        self.updated_at = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        self.today = date.today()

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
        prices, products, stocks = [], [], []
        cats = {}
        for item in products_info:
            product_id = item.pop('id')
            price = {}
            for i in keys_for_price:
                try:
                    price[i] = float(item.get(i, 0)) if i in [1, 3, 4] else int(item.get(i, 0))
                except:
                    price[i] = 0
            stock = {x: item.get('stocks', {}).get(x, 0) for x in keys_for_stock}
            offer_id_short = l[0] if isinstance(l := item['offer_id'].split('/'), list) and len(l) > 0 else ''
            y = {'product_id': product_id, 'offer_id_short': offer_id_short,
                 'date': str(self.today), 'updated_at': self.updated_at}
            price.update(y)
            stock.update(y)
            if item['category_id'] not in cats:
                cats[item['category_id']] = self.get_category(item['category_id'])
            product = {'user_id': self.user_id, 'company_id': self.company_id}
            removed_keys.extend(keys_for_price)
            for key in removed_keys:
                if key in item:
                    del item[key]
            item.update(y)
            item.update(product)
            item.update(cats[item['category_id']])
            item['price'] = price
            item['stocks'] = stock
            prices.append(price)
            stocks.append(stock)
            products.append(item)
        categories = [cats[x] for x in cats]
        return prices, products, stocks, categories

    def get_category(self, cat_id):
        url = 'https://api-seller.ozon.ru/v2/category/tree'
        data = {
            "category_id": cat_id,
            "language": "DEFAULT"
        }
        items, total, last_id = get_helper2(connect1(self, url, self.headers, data))
        name = items[0].get('title') if len(items) > 0 and isinstance(items[0], dict) else None
        category = {'category_id': cat_id, 'category_name': name, 'updated_at': self.updated_at}
        return category

    def get_attributes(self, categories):
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
                attribute['updated_at'] = self.updated_at
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

    def get_analytics(self, count_ids, metrics, date_from, date_to, len_result=1000, offset=0):
        url = 'https://api-seller.ozon.ru/v1/analytics/data'
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
        while len_result >= 1000:
            data = {
                "date_from": date_from,
                "date_to": date_to,
                "metrics": metrics,
                "filters": [],
                "dimension": ["sku", "day"],
                "limit": 1000,
                "offset": offset
            }
            items, total, last_id = get_helper1(connect1(self, url, self.headers, data))
            len_result = len(items)
            analytisc_data.extend(items)
            offset += 1000
            if len_result < 1000:
                break
        return analytisc_data

    def reform_analytics(self, analytics_data, products_data, metrics):
        list1 = ['offer_id_short', 'offer_id', 'product_id', 'category_name', 'name', 'color', 'size', 'brand',
                 'vendor_size', 'common_card_id']
        list2 = ['coming', 'present', 'reserved']
        list3 = ['marketing_price', 'min_ozon_price', 'old_price', 'premium_price', 'price']
        product_dict = {}
        for product in products_data:
            productic = {'user_id': self.user_id, 'company_id': self.company_id, 'updated_at': self.updated_at}
            for key in list1:
                productic[key] = product[key] if key in product else None
            for key in list2:
                productic[key] = product['stocks'][key] if key in product['stocks'] else 0
            for key in list3:
                productic[key] = product['price'][key] if key in product['price'] else 0
            product_dict[str(product['fbo_sku'])] = productic
        analytics = []
        bad = []
        for analy in analytics_data:
            if len(analy.get('dimensions', [])) == 2:
                fbo_sku = analy['dimensions'][0]['id']
                analytic = {'date': analy['dimensions'][1]['id']}
                productic = product_dict.get(fbo_sku, {})
                if not productic:
                    bad.append(fbo_sku)
                    continue
                analytic.update(productic)
                error_metrics = []
                for i, metric in enumerate(metrics):
                    try:
                        analytic[metric] = analy['metrics'][i]
                    except:
                        error_metrics.append(metric)
                        analytic[metric] = None
                if error_metrics:
                    self.app.warn_('error metrics:', analytic['product_id'], self.company_id)
                analytics.append(analytic)
        del analytics_data, products_data, metrics, product_dict
        return analytics

    def get_transaction_list(self, date_from, date_to, page=1):
        """пока не понятно что с периодом , отдает больше чем за 3 месяца"""
        url = 'https://api-seller.ozon.ru/v3/finance/transaction/list'
        operations_list = []
        while True:
            data = {
                "filter": {
                    "date": {
                        "from": '{}{}'.format(date_from, 'T00:00:00.000Z'),
                        "to": '{}{}'.format(date_to, 'T00:00:00.000Z')
                    },
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all"
                },
                "page": page,
                "page_size": 1000
            }
            operations, page_count, row_count = get_helper3(connect1(self, url, self.headers, data))
            page_size = len(operations)
            operations_list.extend(operations)
            page += 1
            if page_size < 1000:
                break
        return operations_list

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
            transaction['updated_at'] = self.updated_at
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
            rating['updated_at'] = self.updated_at
        return ratings

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


def connect1(self, url, headers, data, attempt=1, res=None):
    while attempt < 5:
        attempt += 1
        try:
            time.sleep(1)
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
    items = result['items'] if 'items' in result else result['data'] if 'data' in result else []
    return items, total, last_id


def get_helper2(json_data):
    items = json_data.get('result', [])
    last_id = json_data.get('last_id', '')
    total = json_data.get('total', 0)
    return items, total, last_id


def get_helper3(json_data):
    result = json_data.get('result', {})
    operations = result.get('operations', [])
    page_count = result.get('last_id', 1)
    row_count = result.get('total', 1)
    return operations, page_count, row_count
