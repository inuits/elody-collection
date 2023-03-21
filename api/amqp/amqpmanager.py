import os

from amqp.amqpstormmanager import AmqpStormManager
from util import Singleton
from amqp.pikamanager import PikaAmqpManager


class AmqpManager(metaclass=Singleton):
    def __init__(self):
        self.amqp_manager_name = os.getenv("AMQP_MANAGER", "pika")
        self._init_amqp_managers()

    def get_amqp_manager(self):
        return self.amqp_manager

    def _init_amqp_managers(self):
        self.amqp_manager = {
            "pika": PikaAmqpManager,
            "amqpstorm": AmqpStormManager
        }.get(self.amqp_manager_name)()
