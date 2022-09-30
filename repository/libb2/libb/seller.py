import datetime as dt
from pprint import pprint
import requests, json
from libb.writers import CSV, XLSX, JSON_
import time
from copy import deepcopy
from config.config import *


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

    def __init__(self, client_id, api_key):
        self.client_id = client_id
        self.api_key = api_key
        self.headers = {
            "Client-Id": f"{self.client_id}",
            "Api-Key": f"{self.api_key}"
        }

    def get_items(self):
        url = 'https://api-seller.ozon.ru/v2/product/list'

        data = {
            "limit": 500,
            "last_id": "",
            "filter": {
                "visibility": "ALL"
            }
        }

        body = json.dumps(data)
        res = requests.post(url=url, headers=self.headers, data=body)
        print(res.status_code)
        items = res.json()['result'].get('items')
        return items

    def get_prices(self, items_list):
        url = 'https://api-seller.ozon.ru/v2/product/info/list'

        offers_id = [pair.get('offer_id') for pair in items_list]

        data = {
            "offer_id": offers_id,
            "product_id": [],
            "sku": []
        }

        body = json.dumps(data)
        res = requests.post(url=url, headers=self.headers, data=body)
        print(res.status_code)
        items = res.json()['result'].get('items')
        return items

    def get_stocks(self, items_list):
        url = 'https://api-seller.ozon.ru/v3/product/info/stocks'

        offers_id = [pair.get('offer_id') for pair in items_list]

        data = {
            "filter": {"offer_id": offers_id,
                       "product_id": [],
                       "visiblity": "ALL", },
            "limit": 1000
        }

        body = json.dumps(data)
        res = requests.post(url=url, headers=self.headers, data=body)
        print(res.status_code)
        stocks = res.json()['result'].get('items')
        return stocks

    def get_analytics(self, for_the_days=1, metrics=["ordered_units"]):
        """
            unknown_metric — неизвестная метрика,
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
        date_to = dt.datetime.strftime(today, '%Y-%m-%d')
        date_from = dt.datetime.strftime(today - dt.timedelta(for_the_days), '%Y-%m-%d')

        data = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": metrics,
            "filters": [],
            "dimension": [
                "sku",
                "day"
            ],
            "limit": 1000,
            "offset": 0
        }

        body = json.dumps(data)
        while True:
            try:
                res = requests.post(url=url, headers=self.headers, data=body)
                print(res.status_code)
                if res.status_code != 200:
                    raise Exception('Сервер не ответил')
                data = res.json()['result']['data']
                ordered_units = [order for order in data if order['metrics'][0] > 0]
                cancellations = [order for order in data if order['metrics'][1] > 0]
                returns = [order for order in data if order['metrics'][2] > 0]
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
                return ordered_units, sold
            except:
                print('error')
                time.sleep(5)

    def get_stocks_of_warehouse(self):
        url = 'https://api-seller.ozon.ru/v1/analytics/stock_on_warehouses'

        data = {
            "limit": 1000,
            "offset": 0
        }

        body = json.dumps(data)
        res = requests.post(url=url, headers=self.headers, data=body)
        print(res.status_code)
        # pprint(res.json())
        stocks = res.json()
        return stocks

    def get_report_of_stock(self, file_name):
        url = 'https://api-seller.ozon.ru/v1/report/stock/create'

        data = {
            "language": 'DEFAULT'
        }

        body = json.dumps(data)
        report_code = requests.post(url=url, headers=self.headers, data=body)
        code = report_code.json()['result']['code']
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

        body = json.dumps(data)
        report_code = requests.post(url=url, headers=self.headers, data=body)
        code = report_code.json()['result']['code']
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


def main(client):
    client_items = client.get_items()
    # print(len(client_items))
    # client_stocks = client.get_stocks(items_list=client_items)
    # writer.stock_xls(items=client_stocks, file_name='stocks.xlsx')
    # metrics = ["ordered_units", "cancellations", "returns"]
    # ordered, sold = amber.get_analytics(for_the_days=30, metrics=metrics)
    # writer.ordered_xls(items=ordered, file_name='ordered.xlsx')
    # writer.ordered_xls(items=sold, file_name='sold.xlsx')
    # stocks_of_warehouse = client.get_stocks_of_warehouse()
    # json_writer.write_json(items=stocks_of_warehouse, file_name='stocks_of_warehouse.json')
    client.get_report_of_stock(file_name='report_of_stock.csv')
    client.get_report_of_items(file_name='report_of_items.csv', items_list=client_items)


writer = XLSX()
json_writer = JSON_()

if __name__ == "__main__":
    amber = Seller(client_id='144679', api_key='692a9348-0ecb-4fb2-9f8a-7524ec5d8673')
    main(amber)

# writer = CSV()
# writer.write_csv(items=amber_prices, file_name='prices.results')


