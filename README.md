пример запроса: <br>
data_set = { 'company_data': {'ozon_client_id': '123456',
                                     'api_key': 'gjhvhjdfyt-tertghjf-tret-trt546',
                                     'user_id': 1,
                                     'id': 1
                                     },
                    'scenario': {'product_ids': [], 
                                 'prices': True,
                                 'stocks': True,
                                 'analytics': {'metrics': [], 'period': 1, 'period_step_back': 1},
                                 'transactions': {'period': 1, 'period_step_back': 1},
                                 'ratings': True }} <br>
где: <br> product_ids - список int, по умолчанию пусто <br>
если цены prices не нужны то ключ prices должен отсутствовать  или быть False <br>
если остатки stocks не нужны то ключ stocks должен отсутствовать или быть False <br>
analytics: metrics - список int, по умолчанию пусто <br>
analytics: period - количество дней int, за которые нужно собрать данные,  по умолчанию 1 <br>
analytics: period_step_back - количество дней int, которые нужно отступить от сегодняшнего, по умолчанию 1 <br>
данная комбинация позволяет взять  информацию за вчера <br>
если аналитика не нужна то ключ analytics должен отстуствовать или быть False <br>
transactions: period - количество дней int, за которые нужно собрать данные,  по умолчанию 1 <br>
transactions: period_step_back - количество дней int, которые нужно отступить от сегодняшнего, по умолчанию 1 <br>
данная комбинация позволяет взять  информацию за вчера <br>
если транзакции не нужны то ключ transactions должен отстуствовать или быть False <br>
если рейтинг не нужен то ключ ratings должен отсутствовать или быть False <br>
