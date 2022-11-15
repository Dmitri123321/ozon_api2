# # import json
# # from pandas.io.json import json_normalize
# # import openpyxl
# #
# # # file = open('text.json', 'r')
# # # text = file.read()
# # # file.close()
# # # text = json.loads(text)
# # text = [{"writer": "Mark Ross",
# #       "nationality": "USA",
# #       "books": [
# #           {"title": "XML Cookbook", "price": 23.56},
# #           {"title": "Python Fundamentals", "price": 50.70},
# #           {"title": "The NumPy library", "price": 12.30}
# #       ]
# #       },
# #      {"writer": "Barbara Bracket",
# #       "nationality": "UK",
# #       "books": [
# #           {"title": "Java Enterprise", "price": 28.60},
# #           {"title": "HTML5", "price": 31.35},
# #           {"title": "Python for Dummies", "price": 28.00}
# #       ]
# #       }]
# #
# # frame = json_normalize(text, 'books', ['nationality', 'writer'])
# # frame.to_excel('test.xlsx')
# from pymongo import MongoClient
#
# def check_index(indexes, index, result=False):
#     if len(indexes):
#         for key in indexes:
#             try:
#                 if index == indexes[key]['key'][0][0]:
#                     result = True
#                     break
#             except:
#                 pass
#     return result
#
import datetime

import pymongo
from pymongo import MongoClient

mongo_base_my_local=  "mongodb://127.0.0.1:27017"
cluster = MongoClient(mongo_base_my_local)

# item = [{
#             "brand": None,
#             "user_id": 0,
#             "company_id": 0,
#             "offer_id_short": "2KTB-JNS-K338",
#             "size": "42",
#             "color": "",
#             "category_id": 17029728,
#             "category_name": "Джинсы мужские",
#             "date": "2022-09-26T19:44:27.362Z",
#             "id": 11,
#             "name": "Джинсы мужские",
#             "offer_id": "2KTB-JNS-K338/42",
#             "barcode": "2001594499261",
#             "buybox_price": "",
#             "created_at": "2022-03-24T09:35:21.180105Z",
#             "images": [
#                 "https://cdn1.ozone.ru/s3/multimedia-1/6360055441.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-d/6360055453.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-2/6360055442.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-a/6360055450.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-f/6360055455.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-0/6360055440.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-5/6360055445.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-7/6360055447.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-j/6357344443.jpg"
#             ],
#             "errors": [],
#             "vat": "0.0",
#             "visible": False,
#             "visibility_details": {
#                 "has_price": False,
#                 "has_stock": False,
#                 "active_product": False,
#                 "reasons": {}
#             },
#             "price_index": "1.09",
#             "images360": [],
#             "color_image": "",
#             "primary_image": "https://cdn1.ozone.ru/s3/multimedia-3/6360055443.jpg",
#             "status": {
#                 "state": "price_sent",
#                 "state_failed": "",
#                 "moderate_status": "approved",
#                 "decline_reasons": [],
#                 "validation_state": "success",
#                 "state_name": "Продается",
#                 "state_description": "",
#                 "is_failed": False,
#                 "is_created": True,
#                 "state_tooltip": "",
#                 "item_errors": [],
#                 "state_updated_at": "2022-07-06T10:53:05.216503Z"
#             },
#             "state": "",
#             "service_type": "IS_CODE_SERVICE",
#             "fbo_sku": 533183365,
#             "fbs_sku": 533183366,
#             "currency_code": "RUB",
#             "is_kgt": False,
#             "rating": "123fdsf"
#         },
#
# {
#             "brand": None,
#             "user_id": 0,
#             "company_id": 0,
#             "offer_id_short": "2KTB-JNS-K338",
#             "size": "42",
#             "color": "",
#             "category_id": 17029728,
#             "category_name": "Джинсы мужские",
#             "date": "2022-09-26T19:44:27.362Z",
#             "id": 22,
#             "name": "Джинсы мужские",
#             "offer_id": "2KTB-JNS-K338/42",
#             "barcode": "2001594499261",
#             "buybox_price": "",
#             "created_at": "2022-03-24T09:35:21.180105Z",
#             "images": [
#                 "https://cdn1.ozone.ru/s3/multimedia-1/6360055441.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-d/6360055453.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-2/6360055442.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-a/6360055450.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-f/6360055455.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-0/6360055440.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-5/6360055445.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-7/6360055447.jpg",
#                 "https://cdn1.ozone.ru/s3/multimedia-j/6357344443.jpg"
#             ],
#             "errors": [],
#             "vat": "0.0",
#             "visible": False,
#             "visibility_details": {
#                 "has_price": False,
#                 "has_stock": False,
#                 "active_product": False,
#                 "reasons": {}
#             },
#             "price_index": "1.09",
#             "images360": [],
#             "color_image": "",
#             "primary_image": "https://cdn1.ozone.ru/s3/multimedia-3/6360055443.jpg",
#             "status": {
#                 "state": "price_sent",
#                 "state_failed": "",
#                 "moderate_status": "approved",
#                 "decline_reasons": [],
#                 "validation_state": "success",
#                 "state_name": "Продается",
#                 "state_description": "",
#                 "is_failed": False,
#                 "is_created": True,
#                 "state_tooltip": "",
#                 "item_errors": [],
#                 "state_updated_at": "2022-07-06T10:53:05.216503Z"
#             },
#             "state": "",
#             "service_type": "IS_CODE_SERVICE",
#             "fbo_sku": 533183365,
#             "fbs_sku": 533183366,
#             "currency_code": "RUB",
#             "is_kgt": False,
#             "rating": "123fdsf"
#         }]
#
# collection = cluster.TTTTT.coins1
# indexes = collection.index_information()
# # if not check_index(indexes, 'operation_id'):
# #     collection.create_index("operation_id", unique = True)
# # else:
# #     print('index exist')
# # collection.update_one({'id': item['id']}, update= {'$set': item}, upsert=True)
# # collection.update_many({'id': {'$in': [ 11,22]}}, update= {'$set': item})
#
# #
#
#
# a = [{
#         "operation_id": 2312820897,
#         "operation_type": "OperationMarketplaceServicePremiumPromotion",
#         "operation_date": "2022-09-25 00:00:00",
#         "operation_type_name": "Услуга продвижения Premium",
#         "delivery_charge": 0,
#         "return_delivery_charge": 0,
#         "accruals_for_sale": 0,
#         "sale_commission": 0,
#         "amount": -83.68,
#         "type": "services",
#         "posting": {
#             "delivery_schema": "FBO",
#             "order_date": "2022-09-22 19:52:23",
#             "posting_number": "03941449-0265-1",
#             "warehouse_id": 15431806189000
#         },
#         "items": [
#             {
#                 "name": "Джинсы KATEBI",
#                 "sku": 541351548
#             }
#         ],
#         "services": [
#             {
#                 "name": "MarketplaceServicePremiumPromotion",
#                 "price": -83.68
#             }
#         ]
#     },
#     {
#         "operation_id": 2312822687,
#         "operation_type": "OperationItemReturn",
#         "operation_date": "2022-09-25 00:00:00",
#         "operation_type_name": "Доставка и обработка возврата, отмены, невыкупа",
#         "delivery_charge": 0,
#         "return_delivery_charge": 0,
#         "accruals_for_sale": 0,
#         "sale_commission": 0,
#         "amount": -60,
#         "type": "returns",
#         "posting": {
#             "delivery_schema": "FBO",
#             "order_date": "2022-09-19 13:45:38",
#             "posting_number": "46291235-0226-5",
#             "warehouse_id": 15431806189000
#         },
#         "items": [
#             {
#                 "name": "Джинсы KATEBI",
#                 "sku": 533181581
#             }
#         ],
#         "services": [
#             {
#                 "name": "MarketplaceServiceItemFulfillment",
#                 "price": 0
#             },
#             {
#                 "name": "MarketplaceServiceItemDirectFlowTrans",
#                 "price": 0
#             },
#             {
#                 "name": "MarketplaceServiceItemDirectFlowLogistic",
#                 "price": -60
#             }
#         ]
#     }
# ]
# def add_items_transactions(app, items):
#     for item in items:
#         try:
#             collection.insert_one(item)
#         except:
#             pass
# # add_items_transactions(a)
#
#
#
#
#
# a = [{
#             "offer_id_short": "2KTB-JNS-K338",
#
#             "product_id": 253607007,
#             "state": {
#                 "date": "2022-09-26T19:44:27.362Z",
#                 "marketing_price": "1980.0000",
#                 "min_ozon_price": "",
#                 "old_price": "7000.0000",
#                 "premium_price": "",
#                 "price": "2199.0000"
#                     }
#         },
#         {
#             "offer_id_short": "2KTB-JNS-K338",
#             "date": "2022-09-26T19:44:27.362Z",
#             "product_id": 253607009,
#             "state": {
#                 "date": "2022-09-26T19:44:27.362Z",
#                 "marketing_price": "1980.0000",
#                 "min_ozon_price": "",
#                 "old_price": "7000.0000",
#                 "premium_price": "",
#                 "price": "2199.0000"
#                     }
#         }]
# def send_prices( items):
#
#     for item in items:
#         try:
#             result = collection.update_one({'product_id': item['product_id']}, {'$push': {'state': item['state']}, '$set':{'offer_id_short':item['offer_id_short']}}, upsert=True)
#             a = result.raw_result['updatedExisting']
#             b = bool(result.modified_count)
#             c = 'upserted' in result.raw_result
#             print(a,b,c)
#         except Exception as e:
#             print(e)
#             print(item['product_id'])
# # send_prices(a)
# #
# items = [{'id':11, 'fd':5},{'id':22, 'fd':5}]
# # collection.update_many({'id': {'$in': [11,22]}}, {'$set':{'$mod': item}}, upsert=True)
#
# def send_items( items):
#     for item in items:
#         try:
#             result = collection.update_one({'id': item['id']}, {'$set': item}, upsert=True)
#             a = result.raw_result['updatedExisting']
#             b = bool(result.modified_count)
#             c = 'upserted' in result.raw_result
#             print(a,b,c)
#         except Exception as e:
#             print(e)
#             print(item['product_id'])
# send_items(items)

collection = cluster.QWQW.re
# index = [('date', pymongo.ASCENDING), ('product_id', pymongo.ASCENDING)]
# result = collection.create_index(index, unique=True)
ti = "2022-10-27 02:56:42.420Z"
date = datetime.datetime.strptime(ti, "%Y-%m-%d %H:%M:%S.%fZ")
# date = datetime.datetime.today()
item = {"date": date, "product_id": 12345, "qw": "qw2"}
collection.update_one({"date": date, "product_id" : 12345 }, {'$set': item}, upsert=True)