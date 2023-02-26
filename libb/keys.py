all_metrics = ['hits_view_search', 'hits_view_pdp', 'hits_view', 'hits_tocart_search', 'hits_tocart_pdp',
               'hits_tocart', 'session_view_search', 'session_view_pdp', 'session_view', 'conv_tocart_search',
               'conv_tocart_pdp', 'conv_tocart', 'revenue', 'returns', 'cancellations', 'ordered_units',
               'delivered_units', 'adv_view_pdp', 'adv_view_search_category', 'adv_view_all', 'adv_sum_all',
               'position_category', 'postings', 'postings_premium']
def_metrics = ["ordered_units", "cancellations", "returns", "revenue", "delivered_units"]
attribute_keys = ["height", "depth", "width", "dimension_unit", "weight", "weight_unit", 'pdf_list', 'attributes']
lists_to_write1 = ['products', 'prices', 'stocks', 'categories', 'attributes', 'analytics', 'transactions', 'ratings']
keys_1 = ['prices', 'stocks', 'ratings', 'categories', 'attributes', 'attribute_values']
keys_3 = ['analytics', 'transactions']
all_keys = ['product_ids', 'prices', 'stocks', 'ratings', 'categories', 'attributes', 'attribute_values', 'analytics',
            'transactions']
company_data_keys = ["ozon_client_id", "api_key", "user_id", "id"]


def check_scenario(scenario):
    for key in all_keys:
        need = scenario.get(key)
        if key in keys_1:
            scenario[key] = False if not isinstance(need, bool) else scenario[key]
        elif key == 'product_ids':
            scenario[key] = scenario[key] if isinstance(need, list) else []
        elif key in keys_3:
            if isinstance(need, dict):
                pass
            elif isinstance(need, bool) and need:
                if key == 'analytics':
                    scenario[key] = {'metrics': [], 'period': 1, 'period_step_back': 1}
                else:
                    scenario[key] = {'period': 1, 'period_step_back': 1}
            else:
                scenario[key] = {}
    scenario['attribute_values'] = scenario['product_ids'] if scenario['attribute_values'] else False
    return scenario


# a = {'product_ids': [1212],
#      'prices': True,
#      'stocks': True,
#      'analytics': {},
#      'transactions': {'period': 1, 'period_step_back': 1},
#      'ratings': True,
#      'categories': True,
#      'attributes': True,
#      'attribute_values': []
#      }
# from pprint import pprint
#
# pprint(check_scenario(a), sort_dicts=False)

def check_company_data(company_data, result=True):
    for key in company_data_keys:
        if not company_data.get(key):
            result=False
    company_data['client_id'] = company_data.pop('ozon_client_id')
    company_data['company_id'] = company_data.pop('id')
    return result