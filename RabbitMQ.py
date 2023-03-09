import pika


class RabbitMQ:
    def __init__(self, host, port, username, password, vhost):
        self._host = host  # broker IP
        self._port = port  # broker port
        self._vhost = vhost  # vhost
        self._credentials = pika.PlainCredentials(username, password)
        self._connection = None

    def connect(self):
        parameter = pika.ConnectionParameters(
            host=self._host, port=self._port, virtual_host=self._vhost,
            credentials=self._credentials)
        self._connection = pika.BlockingConnection(parameter)
        return self._connection

    def async_connect(self, on_open_callback):
        parameter = pika.ConnectionParameters(
            host=self._host, port=self._port, virtual_host=self._vhost,
            credentials=self._credentials)
        self._connection = pika.SelectConnection(parameter, on_open_callback=on_open_callback)
        return self._connection

    def close(self):
        if self._connection is not None:
            self._connection.close()
