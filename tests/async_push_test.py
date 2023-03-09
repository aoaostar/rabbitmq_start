import pika
from pika.exchange_type import ExchangeType

import config
from RabbitMQ import RabbitMQ


class Publisher:
    def __init__(self, queue_name_, routing_key_, exchange_):
        self._channel = None
        self._connection = None
        self._queue_name = queue_name_
        self._routing_key = routing_key_
        self._exchange = exchange_
        self._exchange_type = ExchangeType.topic

    def connect(self):
        mq = RabbitMQ(
            host=config.RABBITMQ['host'],
            port=config.RABBITMQ['port'],
            username=config.RABBITMQ['username'],
            password=config.RABBITMQ['password'],
            vhost=config.RABBITMQ['virtual_host'],
        )
        self._connection = mq.async_connect(on_open_callback=self.on_open_callback)
        return self._connection

    def on_open_callback(self, conn):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self._channel = channel

        self.index = 0

        def cb(_):
            self.publish_message()

        self._channel.queue_declare(queue=self._queue_name, callback=cb)

    def publish_message(self):
        message = '测试消息 '
        self._channel.basic_publish(
            self._exchange,
            self._routing_key,
            message + self.index.__str__(),
            pika.BasicProperties(content_type='text/plain',
                                 delivery_mode=1)
        )
        print("发送消息", message + self.index.__str__())
        self.index += 1
        self.schedule_next_message()

    def schedule_next_message(self):
        self._connection.ioloop.call_later(1,
                                           self.publish_message)


if __name__ == '__main__':
    print("[*] 测试异步消费 push")
    queue_name_ = "async_test_queue_name_"
    # routing_key_ = "async_test_routing_key_"
    # exchange_ = "async_test_exchange_"
    routing_key_ = queue_name_
    exchange_ = ""


    # routing_key_ = "async_test_routing_key_"
    # exchange_ = "async_test_exchange_"

    def publisher():
        p = Publisher(queue_name_, routing_key_, exchange_)
        publisher_connect = p.connect()
        publisher_connect.ioloop.start()


    publisher()
