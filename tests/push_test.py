import json
import time
from threading import Thread

from RabbitMQ import RabbitMQ


class PushTest:
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

        channel.basic_consume(self._queue_name, on_message_callback=self.callback, auto_ack=True)
        print('[*] 等待消息.')
        channel.start_consuming()

    @staticmethod
    def callback(ch, method, properties, message_str):
        print("[x] 收到 {0}".format(message_str))


if __name__ == '__main__':
    print("[*] 测试推模式")


    def push():
        p = PushTest()
        i = 0
        while True:
            p.push(json.dumps({
                "orderId": 1000 + i
            }))
            time.sleep(1)
            i += 1


    Thread(target=push, daemon=True).start()

    p = PushTest()
    p.pull()
