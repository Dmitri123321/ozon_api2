from libb.functions import *
from libb.seller import Seller
from libb.app import App
# from libb.writers import XLSX, CSV
from libb.threated_rabbit import ReconnectingRabbit

company_data = {'client_id': '406931',
                'api_key': '97f6530c-f8f1-4571-96d7-2f19a9b6b16b',
                'user_id': 1, 'id': 1}

comand_set = {'company_data': company_data,
              'scenario': {'product_ids': []}

              }


def process(app, comand_set):
    try:
        company_data = comand_set.get('company_data')
        scenario = comand_set.get('scenario')
        if not company_data or not scenario:
            app.warn_('wrong comand_set:', comand_set)
            raise
        else:
            for key, value in company_data.items():
                if not value:
                    app.warn_(f'check client data with {key}:', value)
                    return

            client = Seller(app, client_id=company_data['ozon_client_id'], api_key=company_data['api_key'],
                            user_id=company_data['user_id'], company_id=company_data['id'])
            product_ids = client.get_product_ids() if not scenario['product_ids'] else scenario['product_ids']
            prices, products_data, stocks = client.get_items_info_all(items_list=product_ids)
            app.info_('receive items_info and prepare')
            metrics = None
            analytics_data = client.get_analytics(metrics=metrics)
            app.info_('receive analytics data')
            analytics = client.reform_analytics_data(analytics_data=analytics_data, products_data=products_data,
                                                     company_data=company_data)
            app.info_('prepare analytics data')
            transaction_list = client.get_transaction_list()
            app.info_('receive transaction data')
            transactions = client.reform_transaction_list(transactions=transaction_list, products=products_data)
            app.info_('prepare transaction data')
            ratings = client.get_rating(products_data)
            app.info_('prepare rating data')
            if app.config['to'] == 'json':
                app.write_json_all(items=products_data, file_path='results/items_info.json', abs_path_=app.path)
                app.write_json_all(items=prices, file_path='results/prices.json', abs_path_=app.path)
                app.write_json_all(items=stocks, file_path='results/stocks.json', abs_path_=app.path)
                app.write_json_all(items=analytics, file_path='results/daily_analytics.json', abs_path_=app.path)
                app.write_json_all(items=transactions, file_path='results/transactions.json', abs_path_=app.path)
                app.write_json_all(items=ratings, file_path='results/ratings.json', abs_path_=app.path)
            elif app.config['to'] == 'xlsx':
                # writer = XLSX()
                pass
            elif app.config['to'] == 'csv':
                # writer = CSV()
                pass
            else:
                make_index(app)
                send_items(app, 0, products_data, company_data['user_id'], company_data['id'])
                insert_many(app, 1, prices, company_data['user_id'], company_data['id'])
                insert_many(app, 2, stocks, company_data['user_id'], company_data['id'])
                bulk_write(app, 3, analytics, company_data['user_id'], company_data['id'])
                insert_many(app, 4, transactions, company_data['user_id'], company_data['id'])
                send_items(app, 5, ratings, company_data['user_id'], company_data['id'])

    except:
        app.error_('')


def main(app):
    try:
        ReconnectingRabbit(app, process).run()
    except:
        app.sms(f"{app.config['bot_name'], app.my_node} has been stoped with an error")
        app.sms(files=['info.log', 'warning.log', 'error.log'])


if __name__ == '__main__':
    main(App())
