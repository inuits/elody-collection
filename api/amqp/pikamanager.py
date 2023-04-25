import os

from rabbitmq_pika_flask import RabbitMQ
from rabbitmq_pika_flask.ExchangeParams import ExchangeParams


class PikaAmqpManager(RabbitMQ):
    def __init__(self):
        passive = os.getenv("PASSIVE_EXCHANGE", False) in [1, "1", "True", "true", True]
        durable = os.getenv("DURABLE_EXCHANGE", False) in [1, "1", "True", "true", True]
        super().__init__(
            exchange_params=ExchangeParams(passive=passive, durable=durable)
        )
