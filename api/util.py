import app
import json


class NonUniqueException(Exception):
    def __init__(self, message):
        super().__init__(message)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def read_json_as_dict(filename):
    try:
        with open(f"{filename}.json") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as ex:
        app.logger.error(f"Could not read {filename}.json as a dict: {ex}")
    return {}
