import os

from amqp.amqpstormmanager import AmqpStormManager
from amqp.pikamanager import PikaAmqpManager
from elody.util import Singleton


class AmqpManager(metaclass=Singleton):
    def __init__(self):
        self.amqp_manager_name = os.getenv("AMQP_MANAGER", "pika")
        self.__init_amqp_managers()

    def __init_amqp_managers(self):
        self.amqp_manager = {
            "pika": PikaAmqpManager,
            "amqpstorm": AmqpStormManager,
        }.get(self.amqp_manager_name)()

    def get_amqp_manager(self):
        return self.amqp_manager
