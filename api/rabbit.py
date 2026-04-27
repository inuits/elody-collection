from importlib import import_module
from json import loads
from os import getenv
from typing import Any

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from elody.loader import load_queues
from elody.util import CustomJSONEncoder, custom_json_dumps
from logging_elody.log import log

_rabbit = None


def init_rabbit(app):
    global _rabbit
    amqp_module = import_module(getenv("AMQP_MANAGER", "amqpstorm_flask"))
    auto_delete_exchange = getenv("AUTO_DELETE_EXCHANGE", False) in [
        1,
        "1",
        True,
        "True",
        "true",
    ]
    durable_exchange = getenv("DURABLE_EXCHANGE", True) in [
        1,
        "1",
        True,
        "True",
        "true",
    ]
    passive_exchange = getenv("PASSIVE_EXCHANGE", False) in [
        1,
        "1",
        True,
        "True",
        "true",
    ]

    ExchangeParams = (
        amqp_module.ExchangeParams
        if amqp_module.__name__ == "amqpstorm_flask"
        else amqp_module.ExchangeParams.ExchangeParams
    )
    _rabbit = amqp_module.RabbitMQ(
        exchange_params=ExchangeParams(
            auto_delete=auto_delete_exchange,
            durable=durable_exchange,
            passive=passive_exchange,
        ),
    )
    # FIXME: This is a temporary solution. The default amount of workers for
    # the backgroundscheduler are 10, but currently for DAMS for exapmle we are
    # at 12 queues, which means some queues don't start getting consumed.
    _rabbit.scheduler = BackgroundScheduler(
        executors={"default": ThreadPoolExecutor(20)},
    )
    load_queues(log)
    if amqp_module.__name__ == "amqpstorm_flask":
        _rabbit.init_app(
            app,
            "basic",
            loads,
            custom_json_dumps,
            json_encoder=CustomJSONEncoder,
        )
    else:
        _rabbit.init_app(app, "basic", loads, custom_json_dumps)


def get_rabbit() -> Any:
    global _rabbit
    return _rabbit
