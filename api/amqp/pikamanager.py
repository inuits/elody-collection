from rabbitmq_pika_flask import RabbitMQ
from rabbitmq_pika_flask.QueueParams import QueueParams


class PikaAmqpManager(RabbitMQ):
    def __init__(self):
        super().__init__(queue_params=QueueParams(False, False, False))
