# import functools
import os
import time
import threading
from typing import Union
from dotenv import load_dotenv
import pika

lock = threading.Lock()
index = 0
threads = []
load_dotenv("config.env")
is_messge = 0


def simple_rabbit():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', virtual_host='/', heartbeat=0))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        global index
        index += 1
        print(body)
        for i in range(0, 10):
            print(i * 10)
            time.sleep(10)
        print(index, 'done')
        if ch.is_open:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(on_message_callback=callback, queue='text1', auto_ack=False)
    channel.start_consuming()


class Rabbit:
    def __init__(self, app):
        self.app = app
        self._connection = None
        self._channel = None
        self._params = None
        self._status = 0
        self._consumer_tag = None
        # self._adress = os.getenv('rabbit') if os.getenv('rabbit') else self.app.config['rabbit']
        self._adress = 'amqp://admin:123@:5672/%2F'
        # self._queue = self.app.config['rabbit']['queue']
        self._queue = 'text1'

    def connect(self):
        self._adress: Union[str, dict]
        if type(self._adress) == str:
            self._params = pika.URLParameters(self._adress)
        elif type(self._adress) == dict:
            credentials = pika.PlainCredentials(self._adress['login'], self._adress['password'])
            self._params = pika.ConnectionParameters(host=self._adress['host'],
                                                     port=self._adress['port'],
                                                     virtual_host='/',
                                                     credentials=credentials,
                                                     heartbeat=30)
        return pika.BlockingConnection(self._params)

    def run(self):
        self._connection = self.connect()
        try:
            self._channel = self._connection.channel()
            # self.app.info_('rabbit connection established')
            self.on_open_callback()
        except Exception as e:
            print(e)
            # self.app.error_('rabbit connection failed')
            self._status = 1

    def on_open_callback(self):
        # self._channel.queue_declare(queue=self._queue, durable=True)
        self._channel.queue_declare(queue=self._queue)
        self.set_basic_qos()

    def set_basic_qos(self):
        self._channel.basic_qos(prefetch_count=1)
        self.on_basic_qos_ok()

    def on_basic_qos_ok(self):
        self.starting_consume()

    def starting_consume(self):
        self._consumer_tag = self._channel.basic_consume(queue=self._queue, on_message_callback=self.on_message)
        self._channel.start_consuming()

    def stop(self):
        print('<<<<<<')
        self._channel.close()
        self.close_connection()

    def close_connection(self):
        self._connection.close()

    def on_message(self, channel, method, properties, body):
        print(body)
        for i in range(0, 1):
            print(i * 10)
            time.sleep(10)
        print(index, 'done')
        if channel.is_open:
            self.acknowledge_message(delivery_tag=method.delivery_tag)
        else:
            self._channel.stop_consuming(self._consumer_tag)
            raise

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)


class Rabbit1:
    def __init__(self, app, func):
        self.app = app
        global lock
        self.app.lock = lock if not self.app.lock else self.app.lock
        self.func = func
        self._connection = None
        self._channel = None
        self._consumer_tag = None
        self._frames_received = 0
        self._frames_sent = 0
        self._is_running = False
        self._durable = True
        self._params = None
        if self.app:
            self._adress = self.app.rabbit_data[0]
            self._queue = self.app.rabbit_data[1]
        else:
            self._adress = os.getenv('rabbit')
            self._queue = os.getenv('rabbit_queue')

    def connect(self):
        self._adress: Union[str, dict]
        if self._adress and type(self._adress) == str:
            if 'heartbeat' in self._adress:
                self._adress = '{}{}'.format(self._adress.split('?heartbeat=')[0], '?heartbeat=10')
            else:
                self._adress = '{}{}'.format(self._adress, '?heartbeat=10')
            self._params = pika.URLParameters(self._adress)
        elif self._adress and type(self._adress) == dict:
            credentials = pika.PlainCredentials(self._adress['login'], self._adress['password'])
            self._params = pika.ConnectionParameters(host=self._adress['host'],
                                                     port=int(self._adress['port']),
                                                     virtual_host='/',
                                                     credentials=credentials,
                                                     heartbeat=30)
        else:
            self.app.stop('no connection data')
            raise
        return pika.BlockingConnection(self._params)

    def run(self):
        self._connection = self.connect()
        try:

            self._channel = self._connection.channel()
            self.app.info_('rabbit connection established')
            self._is_running = True
            self.on_open_callback()
        except:
            self.app.error_('rabbit connection failed')
            raise

    def on_open_callback(self):
        self._channel.queue_declare(queue=self._queue, durable=self._durable)
        self.app.info_(f'rabbit queue declare {self._queue} done')
        self.set_basic_qos()

    def set_basic_qos(self):
        self._channel.basic_qos(prefetch_count=1)
        self.on_basic_qos_ok()

    def on_basic_qos_ok(self):
        self.starting_consume()

    def starting_consume(self):
        self._consumer_tag = self._channel.basic_consume(queue=self._queue, on_message_callback=self.on_message)
        self.app.info_('ready to listen')
        self._channel.start_consuming()

    def stop(self):
        self.app.warn_('rabbit connection stoping')
        self._is_running = False
        # with self.app.lock:
        try:
            self._channel.close()
            self._connection.close()
            self.app.warn_('rabbit connection stoped')
        except:
            self.app.warn_('rabbit connection was broken yet')

    def do_work(self, *args):
        global is_messge
        delivery_tag = args[0]
        channel = args[1]
        body = args[2].decode()
        self.app.info_(f"started thread:{threading.current_thread()}")
        self.func(self.app, body)
        if self._frames_received <= self._connection._impl.frames_received:
            self._frames_received = self._connection._impl.frames_received
        else:
            self.app.warn_('frames_received not increase')
        if self._frames_sent <= self._connection._impl.frames_sent:
            self._frames_sent = self._connection._impl.frames_sent
        else:
            self.app.warn_('frames_sent not increase')
        self.app.info_(f'frames sent: {self._frames_sent}, frames received: {self._frames_received}')
        with lock:
            is_messge -= 1
            if channel.is_open:
                self.acknowledge_message(delivery_tag=delivery_tag)
            else:
                self.app.warn_('channel is closing ???')
                self._channel.stop_consuming(self._consumer_tag)

    def on_message(self, channel, method, properties, body):
        # if threads:
        #     del threads[0]
        global is_messge
        with self.app.lock:
            is_messge += 1
        t = Thread1(target=self.do_work, args=(method.delivery_tag, channel, body))
        t.daemon = True
        t.start()
        # threads.append(t)

    def acknowledge_message(self, delivery_tag):
        self.app.info_('send acknowledge')
        self._channel.basic_ack(delivery_tag)

    def do_nothing(self, **kwargs):
        _rabbit = kwargs['cls']
        while True:
            time.sleep(60)
            if _rabbit._is_running and not is_messge:
                _rabbit.app.info_('waiting for message')


class ReconnectingRabbit:

    def __init__(self, app, func):
        self.app = app
        self.func = func
        self._rabbit = Rabbit1(self.app, self.func)
        self._no_mess = None
        self._pause = 3
        self._timer_run = 0
        self._timer_stop = 0
        self._count = 0

    def run(self):
        while True:
            self._count += 1
            try:
                self._no_mess = Thread2(target=self._rabbit.do_nothing, args=(), _rabbit=self._rabbit).start()
                self._timer_run = time.time()
                self._rabbit.run()
                self.app.info_('starting rabbit')
                self._count = 0
            except:
                if not self._rabbit._is_running and self._count > 10 and self._timer_stop - self._timer_run < 5:
                    self.app.stop('no rabbit connection')
                elif self._rabbit._is_running and self._timer_stop - self._timer_run < 60:
                    self.app.stop('check rabbit server')
                self.app.info_(f'attempt {self._count} to connect ')
                self._timer_stop = time.time()
                time.sleep(self._pause)
                self.app.info_('reconnecting rabbit')
                self._reconnect()

    def _reconnect(self):
        if self._rabbit._is_running:
            self._rabbit.stop()
            self.app.info_('stoped rabbit')
        time.sleep(1)
        self._rabbit = Rabbit1(self.app, self.func)


class Thread1(threading.Thread):
    def __init__(self, target, args):
        threading.Thread.__init__(self, target=target, args=args)

    def wait_for_tstate_lock(self, block=True, timeout=-1):
        pass


class Thread2(threading.Thread):
    _instance = None
    _rabbit = None

    def __new__(cls, *args, **kwargs):
        Thread2._rabbit = kwargs['_rabbit']
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, target, args, _rabbit):
        threading.Thread.__init__(self, target=target, args=args, kwargs={'cls': self._rabbit}, daemon=True)


if __name__ == '__main__':
    r = ReconnectingRabbit(None, None)
    r.run()

# in do_work
# cb = functools.partial(self.acknowledge_message1, delivery_tag=delivery_tag)
# self.delivery_tag = delivery_tag
# self._connection.add_callback_threadsafe(callback=cb)

# def acknowledge_message1(self, delivery_tag):
#     self._channel.basic_ack(self.delivery_tag)
