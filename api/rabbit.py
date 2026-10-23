from importlib import import_module
from json import loads
from typing import Any

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from config_access import config
from elody.util import CustomJSONEncoder, custom_json_dumps
from logging_elody.log import log
from elody.observability import traced_rabbit
from plugins import get_plugins

_rabbit = None


def init_rabbit(app):
    global _rabbit
    apscheduler_enabled = config["AMQP_STORM_APSCHEDULER"]
    amqp_module = import_module(config["AMQP_MANAGER"])
    auto_delete_exchange = config["AUTO_DELETE_EXCHANGE"]
    durable_exchange = config["DURABLE_EXCHANGE"]
    passive_exchange = config["PASSIVE_EXCHANGE"]
    ExchangeParams = (
        amqp_module.ExchangeParams
        if amqp_module.__name__ == "amqpstorm_flask"
        else amqp_module.ExchangeParams.ExchangeParams
    )
    # before the queues are registered: their messages carry the trace
    _rabbit = traced_rabbit(
        amqp_module.RabbitMQ(
            exchange_params=ExchangeParams(
                auto_delete=auto_delete_exchange,
                durable=durable_exchange,
                passive=passive_exchange,
            ),
        )
    )
    # FIXME: This is a temporary solution. The default amount of workers for
    # the backgroundscheduler are 10, but currently for DAMS for example we are
    # at 12 queues, which means some queues don't start getting consumed.
    if apscheduler_enabled == 1:
        _rabbit.scheduler = BackgroundScheduler(
            executors={"default": ThreadPoolExecutor(20)},
        )
        load_queues()
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
    if apscheduler_enabled != 1:
        load_queues()


def load_queues():
    """Import the modules that register queue handlers, base first."""
    import_module("resources.queues")
    for plugin in get_plugins():
        plugin.register_queues()


def get_rabbit() -> Any:
    global _rabbit
    return _rabbit
