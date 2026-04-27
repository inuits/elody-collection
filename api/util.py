import os
from importlib import import_module
from inspect import getmembers, isfunction

from elody import util
from werkzeug.exceptions import BadRequest


def encode_content_type_header(content_type: str, fallback_content_type=""):
    if not content_type:
        if not fallback_content_type:
            raise BadRequest("No Content-Type provided")
        content_type = fallback_content_type
    return content_type.replace("/", "").replace(".", "").replace("-", "")


def load_queues(logger):
    import_module("resources.queues")
    apps = util.read_json_as_dict(os.getenv("APPS_MANIFEST"), logger)
    for app in apps:
        try:
            module = import_module(f"apps.{app}.resources.queues")
            logger.info(f"Queues for {app} were loaded.")
            members = getmembers(module, isfunction)
            queues = [o for o in members if o[1].__module__ == module.__name__]
            for item in queues:
                logger.info(f"Imported {item}")
        except ModuleNotFoundError:
            logger.warning(f"Queues for {app} could not be loaded.")
