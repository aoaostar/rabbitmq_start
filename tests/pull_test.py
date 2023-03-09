import json
import time
from threading import Thread

import pika

from RabbitMQ import RabbitMQ


class PullTest:
    _queue_name = 'push_test'
    _exchange = ''

    def __init__(self):
        mq = RabbitMQ("127.0.0.1", 5672, 'guest', 'guest', '/')

        self._connection = mq.connect()

    def push(self, message_str):
        channel = self._connection.channel()
        channel.queue_declare(queue=self._queue_name)

        channel.basic_publish(
            exchange=self._exchange,
            routing_key=self._queue_name,
            body=message_str
        )

        print(" [x] 发送 " + message_str)

    def pull(self):
        if self._connection is None:
            return
        channel = self._connection.channel()
        channel.queue_declare(queue=self._queue_name, durable=False)

        while True:
            (method_frame, header_frame, body) = channel.basic_get(self._queue_name, auto_ack=True)
            if isinstance(method_frame, pika.spec.Basic.GetOk):
                print("[x] 收到 {0}".format(body))
            time.sleep(1)


if __name__ == '__main__':
    print("[*] 测试拉模式")


    def push():
        p = PullTest()
        i = 0
        while True:
            p.push(json.dumps({
                "orderId": 1000 + i
            }))
            time.sleep(1)
            i += 1


    Thread(target=push, daemon=True).start()

    PullTest().pull()
