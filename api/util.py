import app
import json

from cloudevents.conversion import to_dict
from cloudevents.http import CloudEvent


class NonUniqueException(Exception):
    def __init__(self, message):
        super().__init__(message)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def __send_cloudevent(routing_key, data):
    attributes = {"type": routing_key, "source": "dams"}
    event = to_dict(CloudEvent(attributes, data))
    app.rabbit.send(event, routing_key=routing_key)


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


def signal_child_relation_changed(collection, id):
    data = {"parent_id": id, "collection": collection}
    __send_cloudevent("dams.child_relation_changed", data)


def signal_edge_changed(parent_ids_from_changed_edges):
    data = {
        "location": f'/entities?ids={",".join(parent_ids_from_changed_edges)}&skip_relations=1'
    }
    __send_cloudevent("dams.edge_changed", data)


def signal_entity_changed(entity):
    data = {
        "location": f'/entities/{entity["_key"]}',
        "type": entity["type"] if "type" in entity else "unspecified",
    }
    __send_cloudevent("dams.entity_changed", data)
