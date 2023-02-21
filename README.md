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
                                 'ratings': True 
                                 'categories': True,
                                 'attributes': True,
                                 'attribute_values': True
                                  }
           } <br>
где: <br> product_ids - список int, по умолчанию пустой массив - это значит будут взяты все товары поставщика<br>
 <br> для ключей 'prices', 'stocks', 'ratings', 'categories', 'attributes', 'attribute_values' правило работает следующим 
образом: если ключ присуствует и его значение True то он выполняется , если отсуствует или его присутвует но его значение 
False  то он не выполняется <br> 
<br> ключ 'analytics' если присутствует и равен True то по умолчанию выполняется команда
{'metrics': [], 'period': 1, 'period_step_back': 1}, где
<br>period - количество дней int, за которые нужно собрать данные,  по умолчанию 1 <br>
<br>analytics: period_step_back - количество дней int, которые нужно отступить от сегодняшнего, по умолчанию 1 <br>
<br> 'metrics' ключ  если пустой массив или True  или отсутствует то умолчанию будут выполены следующие метрики 
 ["ordered_units", "cancellations", "returns", "revenue", "delivered_units"]
если ключ равен 'metrics' False  то ключ 'analytics' выполнятся не будет !
если ключ равен 'analytics' False  то ключ 'analytics' выполнятся не будет !
<br> ключ 'transactions' если присутствует и равен True то по умолчанию выполняется команда
{'period': 1, 'period_step_back': 1}, где
<br> period - количество дней int, за которые нужно собрать данные,  по умолчанию 1 <br>
<br> period_step_back - количество дней int, которые нужно отступить от сегодняшнего, по умолчанию 1 <br>
если ключ равен 'transactions' False  то ключ 'transactions' выполнятся не будет !

<br>
подключение: <br>
mongo: "mongodb://...." <br>
rabbit: "amqp://..." <br>
rabbit_queue: "some_queue_name" <br>