from rabbitmq_pika_flask import RabbitMQ

class PikaAmqpManager(RabbitMQ):
    def __init__(self):
        super().__init__()