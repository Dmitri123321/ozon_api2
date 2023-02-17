import json
from libb.functions import *
from libb.seller import Seller


# from libb.writers import XLSX, CSV


def process(app, str_data_set):
    try:
        data_set = json.loads(str_data_set)
    except:
        app.warn_(f'this is not correct json {str_data_set}')
        return
    try:
        analytics, transactions, ratings = [], [], []
        company_data = data_set.get('company_data')
        scenario = data_set.get('scenario')

        if company_data and isinstance(company_data, dict) and scenario and isinstance(scenario, dict):
            for key, value in company_data.items():
                if not value:
                    app.warn_(f'check client data with {key}:', value)
                    return

            # keys = ['prices', 'stocks', 'ratings', 'categories', 'attributes', 'attribute_values']

            client = Seller(app, client_id=company_data['ozon_client_id'], api_key=company_data['api_key'],
                            user_id=company_data['user_id'], company_id=company_data['id'])
            """получим id продуктов"""
            product_ids = client.get_product_ids()
            if not product_ids:
                app.warn_(f'seller with id: {company_data["id"]} does not have products yet')
                return
            """проверим id, если их передали"""
            product_ids = check_discrepancy(app, scenario, product_ids, company_data["id"])
            if not product_ids:
                return
            """получим первые данные"""
            products_info = client.get_products_info(product_ids=product_ids)
            """разобьем их на подгруппы"""
            prices, products, stocks, categories = client.reform_items_info(products_info=products_info)
            """оставим то что нужно"""
            prices = process_scenario(app, scenario, 'prices', prices)
            stocks = process_scenario(app, scenario, 'stocks', stocks)
            attributes = get_attributes(app, scenario, client, categories)
            categories = process_scenario(app, scenario, 'categories', categories)
            """дополним характеристиками"""
            products = get_attribute_values(app, scenario, products, client)

            """получим аналитические данные"""
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
                    analytics_data, metrics = client.get_analytics(metrics, period, period_step_back, len(product_ids))
                    app.info_('received analytics data')
                    analytics = client.reform_analytics_data(analytics_data=analytics_data, products_data=products,
                                                             company_data=company_data, metrics=metrics)
                    app.info_('prepared analytics data')
                else:
                    app.warn_(f'check metrics: {metrics}')
                    return
            else:
                analytics = []
            """получим транзакции данные"""
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
                transactions = client.reform_transaction_list(transactions=transaction_list, products=products)
                app.info_('prepared transaction data')
            else:
                transactions = []
            if scenario.get('ratings'):
                ratings = client.get_rating(products)
                app.info_('received and prepared rating data')
            lists_to_write = [products, prices, stocks, categories, analytics, transactions,
                              ratings, attributes]
            if app.config:
                if app.config['to'] == 'json':
                    for list_to_write in lists_to_write:
                        if list_to_write:
                            app.write_json_all(items=list_to_write, file_path=f'results/{app.name(list_to_write)}.json',
                                               abs_path_=app.path)
                elif app.config['to'] == 'mongo':
                    pass
            operations = {0: send_items, 1: insert_many, 2: insert_many, 3: send_items, 4: bulk_write, 5: insert_many,
                          6: send_items, 7: send_items
                          }
            for key in operations.keys():
                if lists_to_write[key]:
                    app.info_(app.name(lists_to_write[key]), 'try up to mongo')
                    operations[key](app, key, lists_to_write[key], company_data['user_id'], company_data['id'])

        else:
            app.warn_('wrong data_set:', data_set)
            return

    except:
        app.error_(f'is it correct data_set: {data_set}')
    return


def check_discrepancy(app, scenario, product_ids, seller):
    need_product_ids = scenario.get('product_ids', [])
    if isinstance(need_product_ids, list) and len(need_product_ids) > 0:
        bad = [num for num in need_product_ids if not isinstance(num, int)]
        if bad:
            app.warn_('this are incorrect product_ids:', *bad)
            need_product_ids = list(set(need_product_ids) - set(bad))
        discrepancy = list(set(need_product_ids) - set(product_ids))
        if discrepancy:
            app.warn_(f'seller with id: {seller} does not have these product_ids:', *discrepancy)
            product_ids = list(set(need_product_ids) - set(discrepancy))
    elif isinstance(need_product_ids, list) and len(need_product_ids) == 0:
        pass
    else:
        product_ids = []
        app.warn_('check product_ids:', need_product_ids)
    return product_ids


def process_scenario(app, scenario, key, mass):
    value = scenario.get(key)
    if value is True:
        result = mass
    elif value is False:
        result = []
    else:
        result = []
        app.warn_(f'check {key}: {value}')
    return result


def get_attributes(app, scenario, client, categories):
    if need_attributes := scenario.get('attributes', False) is True:
        attributes = client.get_attributes(categories=categories)
    elif need_attributes is False:
        attributes = []
    else:
        attributes = []
        app.warn_(f'check attributes: {need_attributes}')
    return attributes


def get_attribute_values(app, scenario, products, client):
    def get_value_by_id(attribute1, value=None):
        attribute_id = attribute1.get('attribute_id', 0)
        values = attribute1.get('attribute', [])
        if len(values) > 0 and isinstance(values[0], dict) and 'value' in values[0]:
            value = values[0]['value']
        return {attribute_id: value}

    need_attribute_values = scenario.get('attribute_values')
    if isinstance(need_attribute_values, list) and len(need_attribute_values) > 0:
        attribute_values = client.get_attribute_values(need_attribute_values)
    elif need_attribute_values is True:
        attribute_values = client.get_attribute_values([])
    elif not need_attribute_values:
        attribute_values = {}
    else:
        attribute_values = {}
        app.warn_(f'check attribute_values: {need_attribute_values}')
    keys = ["height", "depth", "width", "dimension_unit", "weight", "weight_unit", 'pdf_list']
    for product in products:
        attribute_value = attribute_values.get(product['product_id'], {})
        for key in keys:
            product[key] = attribute_value.get(key)
        attributes = attribute_values.get('attributes')
        product['attributes'] = attributes
        attr_dicts = {}
        for attribute in attributes:
            attr_dicts.update(get_value_by_id(attribute))
        product['brand'] = attr_dicts[31] if 31 in attr_dicts else attr_dicts[85] if 85 in attr_dicts else None
        product['color'] = attr_dicts[10096] if 10096 in attr_dicts else None
        product['size'] = attr_dicts[4295] if 4295 in attr_dicts else 0
        product['vendor_size'] = attr_dicts[9533] if 9533 in attr_dicts else 0
        product['common_card_id'] = attr_dicts[8292] if 8292 in attr_dicts else None
    return products
