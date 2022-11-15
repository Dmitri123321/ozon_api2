from libb.functions import *
from libb.seller import Seller
from libb.app import App
# from libb.writers import XLSX, CSV
from libb.threated_rabbit import ReconnectingRabbit

comand_set = {'company_data': {'client_id': '406931',
                               'api_key': '97f6530c-f8f1-4571-96d7-2f19a9b6b16b',
                               'user_id': 1, 'id': 1},
              'scenario': {'product_ids': [],
                           'analytics': {'metric': [], 'period': 1, 'period_step_back': 1},
                           'transactions': {'period': 1, 'period_step_back': 1}
                           }
              }


def process(app, data_set):
    if app.config['test']:
        data_set = {'company_data': {'ozon_client_id': '406931',
                                     'api_key': '97f6530c-f8f1-4571-96d7-2f19a9b6b16b',
                                     'user_id': 1,
                                     'id': 1
                                     },
                    'scenario': {'product_ids': [],
                                 'analytics': {'metrics': [], 'period': 1, 'period_step_back': 1},
                                 'transactions': {'period': 1, 'period_step_back': 1},
                                 'ratings': True

                                 }
                    }
    try:
        analytics, transactions, ratings = [], [], []
        company_data = data_set.get('company_data')
        scenario = data_set.get('scenario')

        if company_data and isinstance(company_data, dict) and scenario and isinstance(scenario, dict):
            for key, value in company_data.items():
                if not value:
                    app.warn_(f'check client data with {key}:', value)
                    return

            client = Seller(app, client_id=company_data['ozon_client_id'], api_key=company_data['api_key'],
                            user_id=company_data['user_id'], company_id=company_data['id'])
            product_ids = client.get_product_ids() if not scenario.get('product_ids') else scenario['product_ids']
            prices, products_data, stocks = client.get_all_products_info(product_ids=product_ids)
            app.info_('received and prepared products_info')
            analytics_data = scenario.get('analytics')
            if analytics_data and isinstance(analytics_data, dict):
                metrics = analytics_data.get('metrics')
                period = analytics_data.get('period', 1)
                if not isinstance(period, int) or period > 720 or period < 1:
                    app.warn_(f'check period: {analytics_data}')
                    return
                period_step_back = analytics_data.get('period_step_back', 1)
                if not isinstance(period_step_back, int) and period_step_back > 720:
                    app.warn_(f'check period_step_back: {analytics_data}')
                    return
                if isinstance(metrics, list) and len(metrics) <= 14:
                    analytics_data = client.get_analytics(metrics, period, period_step_back)
                    app.info_('received analytics data')
                    analytics = client.reform_analytics_data(analytics_data=analytics_data, products_data=products_data,
                                                             company_data=company_data)
                    app.info_('prepared analytics data')
                else:
                    app.warn_(f'check metrics: {metrics}')
                    return
            transaction_data = scenario.get('transactions')
            if transaction_data and isinstance(transaction_data, dict):
                period = transaction_data.get('period', 1)
                if not isinstance(period, int) or period > 720 or period < 1:
                    app.warn_(f'check period: {transaction_data}')
                    return
                period_step_back = transaction_data.get('period_step_back', 1)
                if not isinstance(period_step_back, int) and period_step_back > 720:
                    app.warn_(f'check period_step_back: {transaction_data}')
                    return
                transaction_list = client.get_transaction_list(period, period_step_back)
                app.info_('received transaction data')
                transactions = client.reform_transaction_list(transactions=transaction_list, products=products_data)
                app.info_('prepared transaction data')
            if scenario.get('ratings'):
                ratings = client.get_rating(products_data)
                app.info_('received and prepared rating data')
            lists_to_write = [products_data, prices, stocks, analytics, transactions, ratings]
            if app.config['to'] == 'json':
                for list_to_write in lists_to_write:
                    if list_to_write:
                        app.write_json_all(items=list_to_write, file_path=f'results/{app.name(list_to_write)}.json', abs_path_=app.path)
            elif app.config['to'] == 'xlsx':
                # writer = XLSX()
                pass
            elif app.config['to'] == 'csv':
                # writer = CSV()
                pass
            elif app.config['to'] == 'mongo':
                make_index(app)
                operations = {0: send_items, 1: insert_many, 2: insert_many, 3: bulk_write, 4: insert_many, 5: send_items}
                for key in operations.keys():
                    operations[key](app, key, lists_to_write[key], company_data['user_id'], company_data['id'])
                # send_items(app, 0, products_data, company_data['user_id'], company_data['id'])
                # insert_many(app, 1, prices, company_data['user_id'], company_data['id'])
                # insert_many(app, 2, stocks, company_data['user_id'], company_data['id'])
                # bulk_write(app, 3, analytics, company_data['user_id'], company_data['id'])
                # insert_many(app, 4, transactions, company_data['user_id'], company_data['id'])
                # send_items(app, 5, ratings, company_data['user_id'], company_data['id'])
        else:
            app.warn_('wrong data_set:', data_set)
            return

    except:
        app.error_('')
    return


def main(app):
    try:
        ReconnectingRabbit(app, process).run()
    except:
        app.sms(f"{app.config['bot_name'], app.my_node} has been stoped with an error")
        app.sms(files=['info.log', 'warning.log', 'error.log'])


if __name__ == '__main__':
    main(App())
