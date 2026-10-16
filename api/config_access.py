from importlib import import_module
from os import environ
from typing import TYPE_CHECKING

from elody.util import read_json_as_dict
from settings import BaseConfig, load_config

_bound = None


def get_config():
    try:
        from flask import current_app

        return current_app.config
    except RuntimeError:
        pass
    global _bound
    if _bound is None:
        _bound = build_config()  # before the app exists, or in tests
    return _bound


def init_config(app):
    global _bound
    app.config.update(build_config())
    _bound = app.config  # after boot, threads share the same object


def reset_config():  # for tests
    global _bound
    _bound = None


def build_config(env=environ) -> BaseConfig:
    config = load_config(env)

    apps = read_json_as_dict(env.get("APPS_MANIFEST"), None) or {}
    for app_name in apps:
        module_name = f"apps.{app_name}.settings"
        try:
            module = import_module(module_name)
        except ModuleNotFoundError as error:
            if error.name != module_name:
                raise  # the client's settings.py exists but has a broken import
            continue  # this client has no settings.py yet, which is fine
        config.update(module.load_config(env))

    return config


class _ConfigProxy:
    def __getitem__(self, key):
        return get_config()[key]

    def get(self, key, default=None):
        return get_config().get(key, default)

    def __contains__(self, key):
        return key in get_config()


if TYPE_CHECKING:
    config: BaseConfig
else:
    config = _ConfigProxy()
