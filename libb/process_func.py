import json
from libb.keys import *
from libb.functions import *
from libb.seller import Seller


def my_try(function):
    def wrapper(*args, **kwargs):
        app = args[0]
        key = kwargs['key']
        app.info_(f' --> {key}')
        try:
            result = function(*args, **kwargs)
            app.info_(f' [v] {key}')
        except:
            result = []
            app.info_(f' [x] {key}')
        return result

    return wrapper


def process(app, str_data_set):
    try:
        data_set = json.loads(str_data_set)
    except:
        app.warn_(f'this is not correct json {str_data_set}')
        return
    try:
        operations = {0: bulk_write, 1: bulk_write, 2: bulk_write, 3: bulk_write, 4: bulk_write, 5: bulk_write,
                      6: insert_many, 7: bulk_write}
        company_data = data_set.get('company_data')
        scenario = data_set.get('scenario')
        if company_data and isinstance(company_data, dict) and scenario and isinstance(scenario, dict):
            for key, value in company_data.items():
                if not value:
                    app.warn_(f'check client data with {key}:', value)
                    return
            scenario = check_scenario(scenario)
            client = Seller(app, client_id=company_data['ozon_client_id'], api_key=company_data['api_key'],
                            user_id=company_data['user_id'], company_id=company_data['id'])
            """получим id продуктов"""
            product_ids1 = client.get_product_ids()
            if not product_ids1:
                app.warn_(f'seller with id: {company_data["id"]} does not have products yet')
                return
            """проверим id, если их передали"""
            product_ids = check_discrepancy(app, scenario, product_ids1, company_data["id"])
            if not product_ids:
                return
            """получим первые данные"""
            products_info = client.get_products_info(product_ids=product_ids)
            """разобьем их на подгруппы"""
            prices, products, stocks, categories = client.reform_items_info(products_info=products_info)
            """оставим то что нужно"""
            prices = prices if scenario.get('prices') else []
            stocks = stocks if scenario.get('stocks') else []
            """дополним характеристиками если надо"""
            attributes = get_attributes(app, scenario, categories, client)
            categories = categories if scenario.get('categories') else []
            """получим аналитику транзации рейтинг если надо"""
            key_data = []
            func_key = {'products': get_attribute_values, 'analytic': get_analytics, 'transactions': get_transactions,
                        'ratings': get_ratings}
            for i, key in enumerate(func_key):
                app.info_(f' --> {key}')
                try:
                    key_data.append(func_key[key](app, scenario, products, client))
                    app.info_(f' [v] {key}')
                except:
                    key_data.append([])
                    app.info_(f' [x] {key}')
            lists_to_write = [key_data[0], prices, stocks, categories, attributes, key_data[1], key_data[2], key_data[3]]
            if app.config['to'] == 'json':
                for list_to_write in lists_to_write:
                    if list_to_write:
                        app.write_json_all(items=list_to_write, file_path=f'results/{app.name(list_to_write)}.json',
                                           abs_path_=app.path)
            elif app.config['to'] == 'mongo':
                for i, key in enumerate(operations):
                    if lists_to_write[key]:
                        app.info_('-->>try to upload to mongo:', lists_to_write1[i])
                        operations[key](app, key, lists_to_write[key], company_data['user_id'], company_data['id'])
                    else:
                        app.info_('-->> skipp:', app.name(lists_to_write[key]))
            else:
                pass
        else:
            app.warn_('wrong data_set:', data_set)
            return
    except:
        app.error_(f'is it correct data_set: {data_set}')
    return


def check_discrepancy(app, scenario, product_ids, seller):
    need_product_ids = scenario['product_ids']
    if len(need_product_ids) > 0:
        bad = [num for num in need_product_ids if not isinstance(num, int)]
        if bad:
            app.warn_('this are incorrect product_ids:', *bad)
            need_product_ids = list(set(need_product_ids) - set(bad))
        discrepancy = list(set(need_product_ids) - set(product_ids))
        if discrepancy:
            app.warn_(f'seller with id: {seller} does not have these product_ids:', *discrepancy)
            product_ids = list(set(need_product_ids) - set(discrepancy))
    return product_ids


def get_attributes(app, scenario, categories, client):
    attributes = client.get_attributes(categories=categories) if scenario.get('attributes') else []
    return attributes


def get_attribute_values(app, scenario, products, client):
    def get_value_by_id(attribute1, value=None):
        attribute_id = attribute1.get('attribute_id', 0)
        values = attribute1.get('values', [])
        if len(values) > 0 and isinstance(values[0], dict) and 'value' in values[0]:
            value = values[0]['value']
        return {attribute_id: value}

    if isinstance(need_attribute_values := scenario['attribute_values'], list):
        attribute_values = client.get_attribute_values(need_attribute_values)
        for product in products:
            attribute_value = attribute_values.get(product['product_id'], {})
            for key in attribute_keys:
                product[key] = attribute_value.get(key)
            attributes = product['attributes'] if isinstance(product['attributes'], list) else []
            attr_dicts = {}
            for attribute in attributes:
                attr_dicts.update(get_value_by_id(attribute))
            product['brand'] = attr_dicts[31] if 31 in attr_dicts else attr_dicts[85] if 85 in attr_dicts else None
            product['color'] = attr_dicts[10096] if 10096 in attr_dicts else None
            product['size'] = attr_dicts[4295] if 4295 in attr_dicts else 0
            product['vendor_size'] = attr_dicts[9533] if 9533 in attr_dicts else 0
            product['common_card_id'] = attr_dicts[8292] if 8292 in attr_dicts else None
    return products


def get_analytics(app, scenario, products, client):
    if need_analytics := scenario['analytics']:
        need_metrics = need_analytics.get('metrics', [])
        if isinstance(need_metrics, bool):
            if need_metrics:
                metrics = def_metrics
            else:
                return []
        elif isinstance(need_metrics, list):
            if len(need_metrics) != 0:
                bad_metrics = [x for x in need_metrics if x not in all_metrics]
                app.warn_(f'this are incorrect analytics metrics:', *bad_metrics) if bad_metrics else None
                metrics = list(set(need_metrics) - set(bad_metrics))
                if len(metrics) > 14:
                    app.warn_('there is too much analytics metrics:', metrics)
                    return []
            else:
                metrics = def_metrics
        else:
            app.warn_('there is no a valid list of metrics:', need_metrics)
            return []
        if not isinstance(period := need_analytics.get('period', 1), int) and period > 720 or period < 1:
            app.warn_(f'check analytics period: {period}')
            return []
        if not isinstance(period_step_back := need_analytics.get('period_step_back', 1),
                          int) and period_step_back > 720 or period_step_back < 1:
            app.warn_(f'check analytics period_step_back: {period_step_back}')
            return []
        m_data = {'metrics': metrics, 'period': period, 'period_step_back': period_step_back}
        analytics_data = client.get_analytics(len(products), **m_data)
        analytics = client.reform_analytics(analytics_data, products, m_data['metrics'])
    else:
        analytics = []
    return analytics


def get_transactions(app, scenario, products, client):
    if need_transactions := scenario['transactions']:
        if not isinstance(period := need_transactions.get('period', 1), int) and period > 720 or period < 1:
            app.warn_(f'check transactions period: {period}')
            return []
        if not isinstance(period_step_back := need_transactions.get('period_step_back', 1),
                          int) and period_step_back > 720 or period_step_back < 1:
            app.warn_(f'check transactions period_step_back: {period_step_back}')
            return []
        m_data = {'period': period, 'period_step_back': period_step_back}
        transaction_list = client.get_transaction_list(**m_data)
        transactions = client.reform_transaction_list(transactions=transaction_list, products=products)
    else:
        transactions = []
    return transactions


def get_ratings(app, scenario, products, client):
    ratings = client.get_rating(products) if scenario.get('ratings') else []
    return ratings
