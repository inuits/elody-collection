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


def mediafile_is_public(mediafile):
    for item in mediafile.get("metadata", []):
        if item["key"] == "publication_status":
            return item["value"].lower() in ["beschermd", "expliciet", "publiek"]
    return False


def read_json_as_dict(filename):
    try:
        with open(filename) as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as ex:
        app.logger.error(f"Could not read {filename} as a dict: {ex}")
    return {}
