import json
from datetime import datetime, timedelta

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
            app.error_('')
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
        company_data, scenario = data_set.get('company_data'), data_set.get('scenario')
        """проверим полученную информацию"""
        if company_data and isinstance(company_data, dict) and scenario and isinstance(scenario, dict):
            if not check_company_data(company_data):
                app.warn_(f'check client data:', company_data)
                return
            app.info_('try collected data for ozon_client_id:', company_data["company_id"], 'user_id:', company_data["user_id"])
            scenario = check_scenario(scenario)
            """подключимся"""
            client = Seller(app, **company_data)
            """получим id продуктов"""
            product_ids1 = client.get_product_ids()
            if not product_ids1:
                app.warn_(f'seller with id: {company_data["company_id"]} does not have products yet')
                return
            """проверим id, если их передали"""
            product_ids = check_discrepancy(app, scenario, product_ids1, company_data["company_id"])
            if not product_ids:
                return
            """получим первые данные"""
            products_info = client.get_products_info(product_ids=product_ids)
            """обработаем и разобьем их на подгруппы"""
            prices, products, stocks, categories = client.reform_items_info(products_info=products_info)
            """получим все необходимые данные"""
            # по хорошему надо бы поменять categories и attributes местами и все остальные связи
            all_data = {'products': get_attribute_values(app, scenario, products, client, key='products'),
                        'prices': prices if scenario.get('prices') else [],
                        'stocks': stocks if scenario.get('stocks') else [],
                        'categories': categories if scenario.get('categories') else [],
                        'attributes': get_attributes(app, scenario, categories, client, key='attributes'),
                        'analytic': get_analytics(app, scenario, products, client, key='analytic'),
                        'transactions': get_transactions(app, scenario, products, client, key='transactions'),
                        'ratings': get_ratings(app, scenario, products, client, key='ratings')
                        }
            """отправим полученную инфу куда надо"""
            for i, key in enumerate(all_data):
                if items := all_data[key]:
                    if app.config['to'] == 'mongo':
                        app.info_('-->>try to upload to mongo:', key)
                        operations[i](app, i, items, company_data['user_id'], company_data['company_id'])
                    elif app.config['to'] == 'json':
                        app.write_json_all(items=items, file_path=f'results/{key}.json', abs_path_=app.path)
                else:
                    app.info_('-->> skipp:', key)
        else:
            app.warn_('wrong data_set:', data_set)
            return
    except:
        app.error_(f'is it correct data_set:', data_set)
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


@my_try
def get_attributes(app, scenario, categories, client, key):
    if len(categories) == 0:
        app.warn_(key, 'categories is empty')
        return []
    attributes = client.get_attributes(categories=categories) if scenario.get('attributes') else []
    return attributes


@my_try
def get_attribute_values(app, scenario, products, client, key):
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
            product['vendor_size'] = str(attr_dicts[9533] if 9533 in attr_dicts else 0)
            product['common_card_id'] = attr_dicts[8292] if 8292 in attr_dicts else None
    else:
        app.info_(key)
    return products


@my_try
def get_analytics(app, scenario, products, client, key):
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
        if need_analytics.get('date_from') and need_analytics.get('date_to'):
            date_from, date_to = check_date(need_analytics['date_from']), check_date(need_analytics['date_to'])
            if not date_from or not date_to:
                app.warn_('there is no a valid dates:', date_from, date_to)
                return []
            if date_from >= date_to:
                app.warn_(date_from, "cant be more than ", date_to)
                return []
        elif need_analytics.get('period') and need_analytics.get('period_step_back'):
            period, period_step_back = check_period(need_analytics['period']), check_period(
                need_analytics['period_step_back'])
            if not period or not period_step_back:
                app.warn_('there is no a valid dates:', period, period_step_back)
                return []
            date_to = datetime.strftime(client.today - timedelta(period_step_back), '%Y-%m-%d')
            date_from = datetime.strftime(client.today - timedelta(period_step_back + period - 1), '%Y-%m-%d')
        else:
            app.warn_('there is no any dates:', need_analytics)
            return []
        m_data = {'metrics': metrics, 'date_from': date_from, 'date_to': date_to}
        analytics_data = client.get_analytics(len(products), **m_data)
        analytics = client.reform_analytics(analytics_data, products, m_data['metrics'])
    else:
        app.info_(key)
        analytics = []
    return analytics


@my_try
def get_transactions(app, scenario, products, client, key):
    if need_transactions := scenario['transactions']:
        if need_transactions.get('date_from') and need_transactions.get('date_to'):
            date_from, date_to = check_date(need_transactions['date_from']), check_date(need_transactions['date_to'])
            if not date_from or not date_to:
                app.warn_('there is no a valid dates:', date_from, date_to)
                return []
            if date_from >= date_to:
                app.warn_(date_from, "cant be more than ", date_to)
                return []
        elif need_transactions.get('period') and need_transactions.get('period_step_back'):
            period, period_step_back = check_period(need_transactions['period']), check_period(
                need_transactions['period_step_back'])
            if not period or not period_step_back:
                app.warn_('there is no a valid dates:', period, period_step_back)
                return []
            date_to = datetime.strftime(client.today - timedelta(period_step_back), '%Y-%m-%d')
            date_from = datetime.strftime(client.today - timedelta(period_step_back + period - 1), '%Y-%m-%d')
        else:
            app.warn_('there is no any dates:', need_transactions)
            return []
        m_data = {'date_from': date_from, 'date_to': date_to}
        transaction_list = client.get_transaction_list(**m_data)
        transactions = client.reform_transaction_list(transactions=transaction_list, products=products)
    else:
        app.info_(key)
        transactions = []
    return transactions


@my_try
def get_ratings(app, scenario, products, client, key):
    if len(products) == 0:
        app.warn_(key, 'categories is empty')
        return []
    ratings = client.get_rating(products) if scenario.get('ratings') else []
    return ratings


def check_date(date_, valid_date=False):
    try:
        valid_date = (datetime.strptime(date_, '%Y-%m-%d')).date()
    except:
        pass
    return valid_date


def check_period(period_, valid_period=False):
    if isinstance(period_, int) and 1 <= period_ < 720:
        valid_period = period_
    return valid_period
