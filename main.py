from libb.functions import *
from libb.db import get_companies_data
from libb.seller import Seller
from libb.app import App
from libb.writers import XLSX


def main(app):
    try:
        companies_data = get_companies_data(app)
        # companies_data = [{}]
        for company_data in companies_data:
            client_id = company_data.get('ozon_client_id')
            api_key = company_data.get('ozon_api_key')
            user_id = company_data.get('user_id')
            company_id = company_data.get('id')
            if client_id and api_key and user_id and company_id:
                client = Seller(app, client_id=client_id, api_key=api_key, user_id=user_id, company_id=company_id)
                # client = Seller(app, client_id='406931', api_key='97f6530c-f8f1-4571-96d7-2f19a9b6b16b', user_id=0, company_id=0)
                client_items = client.get_items()
                # pprint(client_items)
                # print(len(client_items))

                # client_stocks = client.get_stocks(items_list=client_items)
                # writer.stock_xls(items=client_stocks, file_name='stocks.xlsx')
                # metrics = ["ordered_units", "cancellations", "returns"]

                """join json"""
                # metrics = ["revenue", "returns", "cancellations", "ordered_units", "delivered_units"]
                # analytics = client.get_analytics(for_the_days=30, metrics=metrics)
                # json_writer.write_json(items=analytics, file_name='analytics.json')

                # writer.ordered_xls(items=ordered, file_name='ordered.xlsx')
                # writer.ordered_xls(items=sold, file_name='sold.xlsx')
                # stocks_of_warehouse = client.get_stocks_of_warehouse()
                # json_writer.write_json(items=stocks_of_warehouse, file_name='stocks_of_warehouse.json')

                """join json"""
                # client.get_report_of_stock(file_name='report_of_stock.csv')
                # items_of_stock = client.csv_to_json(path='../results/report_of_stock.csv')
                # json_writer.write_json(items=items_of_stock, file_name='items_of_stock.json')
                # client.get_report_of_items(file_name='report_of_items.csv', items_list=client_items)
                # items = client.csv_to_json(path='../results/report_of_items.csv')
                # json_writer.write_json(items=items, file_name='items.json')
                # print(client_items)

                prices, reform_json, stocks = client.get_items_info_all(items_list=client_items)
                app.info_('receive items_info and prepare')
                metrics = ["ordered_units", "cancellations", "returns", "revenue", "delivered_units"]
                analytics_data = client.get_analytics(metrics=metrics)
                app.info_('receive analytics data')
                analytics = client.reform_analytics_data(analytics_data=analytics_data, products=reform_json)
                app.info_('prepare analytics data')
                transaction_list = client.get_transaction_list()
                app.info_('receive transaction data')
                transactions = client.reform_transaction_list(transactions=transaction_list, products=reform_json)
                app.info_('prepare transaction data')
                rating = client.get_rating(reform_json)
                app.info_('prepare rating data')
                if app.config['to'] == 'json':
                    app.write_json_all(items=reform_json, file_path='results/items_info.json', abs_path_=app.path)
                    app.write_json_all(items=prices, file_path='results/prices.json', abs_path_=app.path)
                    app.write_json_all(items=stocks, file_path='results/stocks.json', abs_path_=app.path)
                    app.write_json_all(items=analytics, file_path='results/daily_analytics.json', abs_path_=app.path)
                    app.write_json_all(items=transactions, file_path='results/transactions.json', abs_path_=app.path)
                elif app.config['to'] == 'xlsx':
                    writer = XLSX()
                    pass
                else:
                    make_index(app)
                    send_items(app, reform_json)
                    insert_many(app, 1, prices, user_id, company_id)
                    insert_many(app, 2, stocks, user_id, company_id)
                    insert_many(app, 3, analytics, user_id, company_id)
                    insert_many(app, 4, transactions, user_id, company_id)
            else:
                app.warn_('check client data with company_id:', company_id)

    except:
        app.sms(f"{app.config['bot_name'], app.my_node} has been stoped with an error")
        app.sms(files=['info.log', 'warning.log', 'error.log'])


if __name__ == '__main__':
    main(App())
