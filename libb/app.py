import inspect
import json
import logging
import os
import time
from datetime import date, datetime
# from datetime import timedelta
from urllib.parse import urlparse
import pymysql
from pymongo import MongoClient
import sys
import platform
import requests
from pathlib import Path
from functools import wraps

path = os.path.dirname(sys.modules['__main__'].__file__).replace('/libb', '')


def decorator1(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        sms = args[0].sms
        bot_name = args[0].bot_name
        current_node = args[0].current_node
        a, b = function(*args, **kwargs)
        if b == 1:
            sms(text=f"{bot_name, current_node}. There is same problem. See the log")
            sms(files=['debug.log', 'error.log'])
            sys.exit()
        elif b == 2:
            print('>>>>>>>>finished<<<<<<<<')
            sys.exit()
        return a

    return wrapper


def decorator2(function):
    def wrapper(*args, **kwargs):
        typ, func = function(args[0])
        file_path = inspect.stack()[1][1]
        if '\\' in file_path:
            file_name = file_path.split('\\')[-1]
        else:
            file_name = file_path.split('/')[-1]
        level = kwargs.get('l', 1)
        func_name = inspect.stack()[level][3]
        try:
            text = ''.join("{} {}".format(x, '') for x in args[1:])
            func(text, exc_info=True) if typ == 'error' else func(text)
            text = {"msg": f"{text:50.50}", "file": f"{file_name:20}",
                    "func": f"{func_name:25}", "level": f"{typ:7}"}
            text1 = json.dumps(text)
        except:
            text1 = 'can not evalute text'
        print(text1)
        del text1, text, func, file_path, file_name, func_name
        return

    return wrapper


def decorator3(function):
    def wrapper(*args, **kwargs):
        *text1, typ = function(*args, **kwargs)
        log_error = args[0].log_error
        log_debug = args[0].log_debug
        log_warn = args[0].log_warn
        log_info = args[0].log_info
        func_name = inspect.stack()[1][3]
        text = "{} {} {}".format('>>>', func_name, ''.join("{}, {}".format(x, '') for x in text1))
        if typ == 'info':
            log_info.info(text)
        elif typ == 'error':
            log_error.error(text, exc_info=True)
        elif typ == 'warning':
            log_warn.warning(text)
        elif typ == 'debug':
            log_debug.debug(text)
        del text, text1, func_name
        return

    return wrapper


class App:
    def __init__(self):
        self.path = path
        self.today = date.today()
        self.today_time = datetime.today().replace(minute=0, hour=0, second=0, microsecond=0)
        self.bot_name = 'ozon_api'
        self.version = 0.27
        check_log_folder(folder='log')
        self.my_node = ['Z1']
        self.current_node, self.platform = get_current_node()
        self.log_error = logger(name='error', mode='w')
        self.log_debug = logger(name='debug', mode='w')
        self.log_warn = logger(name='warning', mode='w')
        self.log_info = logger(name='info', mode='w')
        self.info_('logger enabled and writes', self.today)
        self.config = load_config(self)
        self.info_(f'bot name: {self.bot_name} version: {self.version}')
        self.my_node = get_my_node()
        self.rabbit_data = connect_rabbit(self)
        self.lock = None
        self.info_('try connect to mongo')
        cluster = detect_mongo_base(self)
        self.collection_products = cluster.OzonData.products
        self.collection_prices = cluster.OzonData.prices
        self.collection_stocks = cluster.OzonData.stocks
        self.collection_analytics = cluster.OzonData.analytics
        self.collection_transaction = cluster.OzonData.transaction
        self.collection_rating = cluster.OzonData.rating
        self.collections_list = [self.collection_products, self.collection_prices,
                                 self.collection_stocks, self.collection_analytics,
                                 self.collection_transaction, self.collection_rating]
        self.cat_ids = {}
        self.info_('load app finished, starting....')

    def sms(self, text=None, lang='en', files=None):
        files = [] if files is None else files
        telegram = {
            "enable": True,
            "token": "5434258681:AAHeQR0OFAepvo1nj6p0mPDXGKDjlerC844",
            "url": "https://api.telegram.org/bot",
            "channel_id": "-1001749223608"
        }
        if telegram['enable']:
            try:
                if telegram['token'] and telegram['channel_id']:
                    if text is not None:
                        if lang != 'en':
                            text = text.encode("cp1251").decode("utf-8-sig", 'ignore')
                        url = '{}{}{}'.format(telegram['url'], telegram['token'], "/sendMessage")
                        requests.post(url, data={"chat_id": telegram['channel_id'], "text": text})
                    if len(files) > 0:
                        for f in files:
                            file = {'document': open((path + "/log/" + f), 'rb')}
                            url = '{}{}{}'.format(telegram['url'], telegram['token'], "/sendDocument")
                            requests.post(url, files=file, data={"chat_id": telegram['channel_id']})
            except Exception as e:
                print(e)
                self.error_('error sending a message in telegram')

    def write_json_all(self, items, file_path, abs_path_=None, error=False):
        if abs_path_:
            file_path = abs_path_ + '/' + file_path
        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(items, file, indent=4, ensure_ascii=False, default=str)
            return True
        except Exception as e:
            if error:
                print('error in write_json', e)
            return False

    def read_json(self, path_, error=False):
        try:
            with open(path_, 'r', encoding='utf-8-sig') as f:
                items = json.load(f)
            return items
        except Exception as e:
            if error:
                print('error in read_json', e)
            return []

    def name(self, var):
        callers_local_vars = inspect.currentframe().f_back.f_locals.items()
        return [var_name for var_name, var_val in callers_local_vars if var_val is var][0]

    def stop(self, text):
        if self.lock:
            with self.lock:
                self.critical_(text)
                os._exit(0)
        else:
            self.critical_(text)
            sys.exit()

    @decorator2
    def info_(self):
        return 'info', self.log_info.info

    @decorator2
    def debug_(self):
        return 'debug', self.log_debug.debug

    @decorator2
    def warn_(self):
        return 'warning', self.log_warn.warning

    @decorator2
    def error_(self):
        return 'error', self.log_error.error

    @decorator2
    def critical_(self):
        return 'critical', self.log_error.error

    @decorator3
    def info_1(self):
        return 'info'

    @decorator3
    def debug_1(self):
        return 'debug'

    @decorator3
    def error_1(self):
        return 'error'

    @decorator3
    def warn_1(self):
        return 'warning'


@decorator1
def detect_mongo_base(self, enable_ssl=False, status=0):
    if self.config:
        if self.current_node in self.my_node:
            mongo_base = self.config['mongodb']['mongo_base_my_local']
            self.info_('my local base will be used')
        else:
            mongo_base = self.config['mongodb']['mongo_base_my_local']
            self.info_('global base base will be used')
        if '27017' not in mongo_base:
            enable_ssl = True
        mongo_data = {'host': mongo_base, 'tls': enable_ssl, 'tlsAllowInvalidCertificates': True}
    else:
        mongo_base = os.getenv('mongo')
        mongo_data = {'host': mongo_base}
    self.info_(f'mongo_base: {mongo_base}, enable_ssl: {enable_ssl}')
    cluster = MongoClient(**mongo_data)
    try:
        cluster.server_info()
        self.info_('mongo connection established')
    except:
        self.info_('mongo connection failed')
        status = 1
    return cluster, status


@decorator1
def detect_mysql_base(self, status=0, connection=None, limit=1000):
    def get_adr(base1, status1=True):
        keys = ['host', 'port', 'user', 'password', 'db_name']
        base2 = {'host': base1['host'], 'db_name': base1['db_name'], 'user': base1['user'],
                 'password': base1['password'], 'port': base1['port']}
        for key in keys:
            if not base2[key]:
                status1 = False
        return base2, status1

    mysql_base = {}
    if self.current_node in self.my_node:
        try:
            if self.config['mysql']['local']:
                mysql_base, s = get_adr(self.config['mysql']['mysql_base_local'])
                self.info_('my local mysql base will be used')
            else:
                mysql_base, s = get_adr(self.config['mysql']['mysql_base_global'])
                self.info_('global mysql base will be used')
            if not s:
                raise Exception
        except:
            self.error_('there are no mysql base data')
    else:
        try:
            base = os.getenv('mysql_base')
            self.info_(base)
            if base and '//' in base:
                url_parse = urlparse(base)
                base = {'host': url_parse.hostname, 'db_name': url_parse.path.lstrip('/'),
                        'user': url_parse.username, 'password': url_parse.password, 'port': url_parse.port}
                mysql_base, s = get_adr(base)
            else:
                mysql_base, s = get_adr(dict(base))
            if not s:
                raise Exception
        except:
            self.warn_('there are no mysql base data in env')
    if mysql_base is None:
        return None, 1
    else:
        self.info_(f'mysql: {mysql_base}')
    attempt = 1
    while attempt < limit:
        attempt += 1
        try:
            connection = pymysql.connect(host=mysql_base['host'], user=mysql_base['user'], password=mysql_base['password'],
                                         database=mysql_base['db_name'], port=mysql_base['port'])
            if connection.server_version:
                self.info_('mysql connection established')
            else:
                raise
            connection.autocommit(True)
            self.mysql_base = mysql_base
            break
        except:
            self.info_('mysql connection failed')
            time.sleep(10)
    else:
        status = 1
    return connection, status


@decorator1
def connect_rabbit(self, adress=None, queue=None, status=0):
    try:
        if self.config:
            adress = self.config['rabbit']['rabbit_dict']
            adress = self.config['rabbit']['rabbit_url']
            queue = self.config['rabbit']['queue']
        else:
            adress = os.getenv('rabbit')
            queue = os.getenv('rabbit_queue')
        if not adress:
            self.error_('no rabbit adress')
            raise
        if not queue:
            self.error_('no rabbit queue')
            raise
    except:
        self.error_('no rabbit data')
        status = 1
    return [adress, queue], status


def logger(name, mode='a'):
    log = logging.getLogger(name=name)
    handler = logging.FileHandler(f"{path}/log/{name}.log", mode=mode, encoding="UTF-8")
    # handler = logging.handlers.RotatingFileHandler(f"{path}/log/{name}.log", mode=mode, maxBytes=1000000, backupCount=10, encoding = "UTF-8")
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    if name == 'error':
        log.setLevel(logging.ERROR)
    elif name == 'debug':
        log.setLevel(logging.DEBUG)
    elif name == 'info':
        log.setLevel(logging.INFO)
    elif name == 'warning':
        log.setLevel(logging.WARNING)
    log.addHandler(handler)
    return log


def get_my_node():
    try:
        my_node = platform.uname().node.upper()
    except:
        my_node = 'AAA'
    return my_node


@decorator1
def load_config(self, status=0):
    config = {}
    if self.platform == 'win':
        self.info_('try to load config')
        abs_path_config = next(Path(path).rglob('config.json'))
        config = self.read_json(abs_path_config)
        if config:
            self.info_(f'config.json has been loaded')
        else:
            self.info_(f'there are no config.json')
            status = 1
    return config, status


def check_log_folder(folder):
    new_path = path + "/" + folder
    if not os.path.isdir(new_path):
        os.mkdir(new_path)
    else:
        for file in os.listdir(new_path):
            os.remove(new_path + "/" + file)


def print_(text, typ):
    if type(text) == str:
        print(text)
    elif type(text) == list:
        if len(text) == 3:
            text1 = {"msg": f"{text[0]}", "file": f"{text[1]}", "func": f"{text[2]}", "level": f"{typ}"}
            print(json.dumps(text1))


def list_to_text(text):
    return text[0] if type(text) == list else text


def get_current_node():
    try:
        my_node = platform.uname().node.upper()
        type_os = 'win' if 'win' in platform.system().lower() else 'lin'
    except:
        my_node = 'uncknown'
        type_os = 'uncknown'
    return my_node, type_os
