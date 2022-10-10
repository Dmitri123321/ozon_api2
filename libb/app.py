import inspect
import json
import logging
import os
from datetime import date, timedelta

import pymysql
from pymongo import MongoClient
import sys
import platform
import requests
from pathlib import Path
from functools import wraps

path = os.path.dirname(sys.modules['__main__'].__file__).replace('/libb', '')


# a = open('D:\\py\\group\\new\\ozon_api\\log\\debug.log', 'r')
# print(a)
#

def decorator1(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        sms = args[0].sms
        config = args[0].config
        my_node = args[0].my_node
        a, b = function(*args, **kwargs)
        if b == 1:
            sms(text=f"{config['bot_name'], my_node}. There is same problem. See the log")
            sms(files=['debug.log', 'error.log'])
            sys.exit()
        elif b == 2:
            print('>>>>>>>>finished<<<<<<<<')
            sys.exit()
        return a

    return wrapper


def decorator2(function):
    def wrapper(*args, **kwargs):
        *text1, typ, func = function(*args, **kwargs)
        file_path = inspect.stack()[1][1]
        if '\\' in file_path:
            file_name = file_path.split('\\')[-1]
        else:
            file_name = file_path.split('/')[-1]
        func_name = inspect.stack()[1][3]
        try:
            text = ''.join("{} {}".format(x, '') for x in text1)
            func(text, exc_info=True) if typ == 'error' else func(text)
            text = {"msg": f"{text:50.50}", "file": f"{file_name:20}",
                    "func": f"{func_name:25}", "level": f"{typ:7}"}
            text1 = json.dumps(text)
        except:
            pass
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
        check_log_folder(folder='log')
        self.log_error = logger(name='error', mode='w')
        self.log_debug = logger(name='debug', mode='w')
        self.log_warn = logger(name='warning', mode='w')
        self.log_info = logger(name='info', mode='w')
        self.info_('logger enabled and writes', self.today)
        self.info_('try to load config')
        self.config = load_config(self)
        self.info_('config has been loaded')
        self.info_(f'version: {self.config["version"]}')
        self.my_node = get_my_node()
        if self.config['to'] == 'mongo':
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
        elif self.config['to'] == 'json':
            pass
        else:
            self.info_('check config and try again')
            raise
        if self.config['from'] == 'mysql':
            self.info_('try connect to mysql base')
            self.connection = detect_mysql_base(self)
        else:
            self.info_('chek config "from" and try again')
            raise
        self.lock = None
        self.cat_ids = {}
        self.info_('load app finished, starting....')

    def sms(self, text=None, lang='en', files=None):
        files = [] if files is None else files
        if self.config['telegram']['enable']:
            try:
                token = self.config['telegram']['token']
                url = "https://api.telegram.org/bot"
                channel_id = self.config['telegram']['channel_id']
                url += token
                if token == '' or channel_id == '':
                    return
                else:
                    if text is not None:
                        if lang != 'en':
                            text = text.encode("cp1251").decode("utf-8-sig", 'ignore')
                        requests.post(url + "/sendMessage", data={"chat_id": channel_id, "text": text})
                    if len(files) > 0:
                        for f in files:
                            file = {'document': open((path + "/log/" + f), 'rb')}
                            requests.post(url + "/sendDocument", files=file, data={"chat_id": channel_id})
            except Exception as e:
                print(e)
                self.error_('error sending a message in telegram')

    def write_json_all(self, items, file_path, abs_path_=None, error=False):
        if abs_path_:
            file_path = abs_path_ + '/' + file_path
        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(items, file, indent=4, ensure_ascii=False)
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
    def info_(self, *texts):
        return *texts, 'info', self.log_info.info

    @decorator2
    def debug_(self, *texts):
        return *texts, 'debug', self.log_debug.debug

    @decorator2
    def warn_(self, *texts):
        return *texts, 'warning', self.log_warn.warning

    @decorator2
    def error_(self, *texts):
        return *texts, 'error', self.log_error.error

    @decorator2
    def critical_(self, *texts):
        return *texts, 'critical', self.log_error.error

    @decorator3
    def info_1(self, *texts):
        return *texts, 'info'

    @decorator3
    def debug_1(self, *texts):
        return *texts, 'debug'

    @decorator3
    def error_1(self, *texts):
        return *texts, 'error'

    @decorator3
    def warn_1(self, *texts):
        return *texts, 'warning'


@decorator1
def detect_mongo_base(self):
    enable_ssl = False
    status = 0
    if self.my_node in self.config['node']:
        try:
            with open(self.config['mongodb']['mongo_base_data'], 'r', encoding='utf-8-sig') as g:
                mongo_base = g.read().strip()
            if "mongodb" not in mongo_base:
                raise Exception
            else:
                self.info_('global base adress has been detected')
        except:
            mongo_base = self.config['mongodb']['mongo_base_my_local']
            self.info_('my local base will be used')
    else:
        try:
            try:
                mongo_base = os.getenv('mongo')
                if mongo_base is None:
                    raise Exception
            except:
                mongo_base = self.config['mongodb']['mongo_base_data']
                self.info_('global base adress has been detected')
                if "mongodb" not in mongo_base:
                    self.error_('mongo adress incorrect')
                    status = 1
        except:
            mongo_base = None
    if '27017' not in mongo_base:
        enable_ssl = True
    self.log_debug.debug(f'mongo_base: {mongo_base}, enable_ssl: {enable_ssl}')
    cluster = MongoClient(mongo_base, tls=enable_ssl, tlsAllowInvalidCertificates=True)
    try:
        cluster.server_info()
        self.info_('mongo connection established')
    except:
        self.info_('mongo connection failed')
        status = 1
    return cluster, status


@decorator1
def detect_mysql_base(self, enable_ssl=False, status=0, connection=None):
    def get_adr(adress, s=True):
        base = {'host': adress['host'],
                'database': adress['database'],
                'user': adress['user'],
                'password': adress['password'],
                'port': adress['port'],
                }
        for key in base:
            if not base[key]:
                s = False
        return base, s

    mysql_base = {}
    if self.my_node in self.config['node']:
        try:
            mysql_base, s = get_adr(self.config['mysql']['mysql_base_local'])
            if not s:
                raise Exception
            self.info_('my local mysql base will be used')
        except:
            mysql_base, s = get_adr(self.config['mysql']['mysql_base_global'])
            if not s:
                status = 1
            self.info_('global mysql base will be used')
    else:
        try:
            try:
                mysql_base_global = os.getenv('mysql_base')
                if mysql_base_global is None:
                    raise Exception
            except:
                self.info_('no mysql in env')
                mysql_base, s = get_adr(self.config['mysql']['mysql_base_global'])
                if not s:
                    raise
                else:
                    self.info_('global mysql base will be used')
        except:
            status = 1
    self.log_debug.debug(f'mysql: {mysql_base}, enable_ssl: {enable_ssl}')

    try:
        connection = pymysql.connect(host=mysql_base['host'], user=mysql_base['user'], password=mysql_base['password'],
                                     database=mysql_base['database'], port=mysql_base['port'])
        if connection.server_version:
            self.info_('mysql connection established')
        else:
            raise
        connection.autocommit(True)
    except:
        self.info_('mysql connection failed')
        status = 1
    return connection, status


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


def load_config(self):
    try:
        abs_path_config = next(Path(path).rglob('config.json'))
        config = self.read_json(abs_path_config)
        if config:
            self.info_(f'config.json loaded')
        else:
            raise
    except:
        self.error_('config.json not detected')
        sys.exit()
    return config


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
