import functools
import time
import traceback

from tests.async_push_test import Publisher


class Consumer(Publisher):
    def __init__(self, queue_name_, routing_key_, exchange_):
        super().__init__(queue_name_, routing_key_, exchange_)
        self._prefetch_count = 1

    @staticmethod
    def on_message(unused_channel, basic_deliver, properties, body):
        print("收到消息", body.decode())
        # print("收到消息", body)
        time.sleep(0.1)
        unused_channel.basic_ack(delivery_tag=basic_deliver.delivery_tag)

    def on_channel_open(self, channel):
        self._channel = channel
        try:

            self.setup_exchange()

        except (Exception,):
            print(traceback.format_exc(), ">>>")

    def setup_exchange(self):
        print("setup_exchange", self._exchange, self._exchange_type)
        cb = functools.partial(self.on_exchange_declareok)
        self._channel.exchange_declare(
            exchange=self._exchange,
            exchange_type=self._exchange_type,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame):
        print("on_exchange_declareok")
        self.setup_queue()

    def setup_queue(self):
        print("setup_queue", self._queue_name)
        cb = functools.partial(self.on_queue_declareok)
        self._channel.queue_declare(queue=self._queue_name, callback=cb)

    def on_queue_declareok(self, _unused_frame):
        print("on_queue_declareok")
        cb = functools.partial(self.on_bindok)
        self._channel.queue_bind(
            self._queue_name,
            self._exchange,
            routing_key=self._routing_key,
            callback=cb)

    def on_bindok(self, _unused_frame):
        self.set_qos()

    def set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        self.start_consuming()

    def start_consuming(self):
        self._channel.basic_consume(
            self._queue_name, self.on_message)


if __name__ == '__main__':
    print("[*] 测试异步消费 pull")

    queue_name_ = "async_test_queue_name_"
    routing_key_ = "async_test_routing_key_"
    exchange_ = "async_test_exchange_"

    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')


    # logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    def consumer():
        p = Consumer(queue_name_, routing_key_, exchange_)
        publisher_connect = p.connect()
        publisher_connect.ioloop.start()


    consumer()
