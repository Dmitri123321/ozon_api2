import requests


class Report:
    def __init__(self, account):
        self.account = account

    def get_report(self, uuid, file_name):
        print(uuid)
        url = f'https://performance.ozon.ru/api/client/statistics/report?UUID={uuid}'
        # print(self.account.token)
        headers = {
            # "Host": "performance.ozon.ru:443",
            "Authorization": f"Bearer {self.account.token}"
        }

        res = requests.get(url=url, headers=headers)

        print(res.status_code)
        # print(res.text)
        with open(rf"{file_name}.xlsx", 'wb') as f:
            f.write(res.content)

    def report_status(self, uuid):
        print(uuid)
        url = f'https://performance.ozon.ru:443/api/client/statistics/{uuid}'
        print(self.account.token)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.account.token}"
        }

        res = requests.get(url=url, headers=headers)

        print(res.status_code)
        print(res.text)
