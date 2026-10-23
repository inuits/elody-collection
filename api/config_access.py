from os import environ
from typing import TYPE_CHECKING

from plugins import configure_plugins, merged
from settings import BaseConfig, ConfigError, Env, load_config

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
    env = Env(env)
    config = load_config(env)

    configure_plugins(env.get("APPS_MANIFEST"))
    config.update(merged(lambda plugin: plugin.settings(env)))

    if env.missing or env.invalid:
        raise ConfigError(env.missing, env.invalid)
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
