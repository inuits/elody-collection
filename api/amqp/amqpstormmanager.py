
from rabbitmq_pika_flask import RabbitMQ

#todo implement amqpstorm
class AmqpStormManager(RabbitMQ):
    def __init__(self):
        super().__init__()