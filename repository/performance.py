from config.config import *
import json, requests, time
from repository.Report import Report


class Client:
    def __set_name__(self, owner, name):
        self.name = '_' + name

    def __get__(self, instance, owner):
        return getattr(instance, self.name)

    def __set__(self, instance, value):
        setattr(instance, self.name, value)


class Performance:
    client_id = Client()
    secret_key = Client()

    def __init__(self, account_name):
        self.client_id = accounts.get(account_name).get('performance').get('client_id')
        self.secret_key = accounts.get(account_name).get('performance').get('secret_key')
        self.token = self.get_token()

    def get_token(self):
        url = 'https://performance.ozon.ru/api/client/token'
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        data = {
            "client_id": self.client_id,
            "client_secret": self.secret_key,
            "grant_type": "client_credentials"
        }
        body = json.dumps(data)
        res = requests.post(url=url, headers=headers, data=body)
        token = res.json()['access_token']
        return token


class PerformanceInfo:
    performance = Client()

    def __init__(self, performance):
        self.performance = performance
        # self.items = self.get_items_info()

    def get_items_info(self):
        campaign_id = '616940'
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/search_promo/products'
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.performance.token}"
        }
        data = {
            "page": 1,
            "pageSize": 100
        }
        body = json.dumps(data)
        res = requests.post(url=url, headers=headers, data=body)
        # print(res.json())
        items = res.json()['products']
        return items

    def get_analytic(self):
        url = 'https://performance.ozon.ru:443/api/client/statistics'
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.performance.token}"
        }
        data = {
            "campaigns": ["616940"],
            "from": "2022-07-01T00:00:00.000Z",
            "to": "2022-07-20T00:00:00.000Z",
            "groupBy": "DATE"
        }
        body = json.dumps(data)
        res = requests.post(url=url, headers=headers, data=body)
        uuid = res.json().get('UUID')
        return uuid

    def get_reports_list(self):
        url = 'https://performance.ozon.ru:443/api/client/statistics/list'
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.performance.token}"
        }
        res = requests.get(url=url, headers=headers)
        print(res.json())

    def get_phrases_analytic(self):
        url = 'https://performance.ozon.ru:443/api/client/statistics/phrases'
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.performance.token}"
        }
        data = {
            "campaigns": ["616940"],
            "objects": ["479797025"],
            "from": "2022-07-01T00:00:00.000Z",
            "to": "2022-07-20T00:00:00.000Z",
            "groupBy": "DATE"
        }
        body = json.dumps(data)
        res = requests.post(url=url, headers=headers, data=body)
        uuid = res.json().get('UUID')
        return uuid


emblem = Performance(account_name='EMBLEM')
emblem_info = PerformanceInfo(emblem)
print(emblem_info.get_items_info())
# uuid = emblem_info.get_phrases_analytic()
uuid = emblem_info.get_analytic()
report = Report(emblem)
time.sleep(10)
report.get_report(uuid)
# report.report_status(uuid)
# emblem_info.get_reports_list()
