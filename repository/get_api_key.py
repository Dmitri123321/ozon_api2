import requests
import json
import time

url = 'https://performance.ozon.ru/api/client/token'
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}
data = {
    "client_id": "2597820-1659011323364@advertising.performance.ozon.ru",
    "client_secret": "fHhRkGQQTMZCWFt0CGG-QENnDmXE9kL81epPzg2G_ZJqAZJpeuYu36OZaYpgJ21IoCAqdq2_x20ApWLTaw",
    "grant_type": "client_credentials"
}
body = json.dumps(data)
res = requests.post(url=url, headers=headers, data=body)
print(res.status_code)
key = res.json()['access_token']
print(key)

url = 'https://performance.ozon.ru/api/client/statistics/phrases'
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {key}"
}

data = {
    "campaigns": ["1326429"],
    "objects": ["584839983", "584901042"],
    "from": "2022-06-01T00:00:00.000Z",
    "to": "2022-07-28T00:00:00.000Z",
    "dateFrom": "2022-06-01",
    "dateTo": "2022-07-28",
    "groupBy": "DATE"
}
# auth = {}

body = json.dumps(data)
res = requests.post(url=url, headers=headers, data=body)
print(res.status_code)
print(res.json())
uuid = res.json()['UUID']
print(uuid)


time.sleep(7)
url = f'https://performance.ozon.ru/api/client/statistics/report?UUID={uuid}'
headers = {
    "Host": "performance.ozon.ru:443",
    "Authorization": f"Bearer {key}"
}

res = requests.get(url=url, headers=headers)

print(res.status_code)
print(res.text)
with open(rf"file.xlsx", 'wb') as f:
    f.write(res.content)
