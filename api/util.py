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


def get_raw_id(item):
    return item.get("_key", item["_id"])


def get_item_metadata_value(item, key):
    for item in item.get("metadata", []):
        if item["key"] == key:
            return item["value"]
    return ""


def mediafile_is_public(mediafile):
    publication_status = get_item_metadata_value(mediafile, "publication_status")
    return publication_status.lower() in ["beschermd", "expliciet", "publiek"]


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
        "location": f"/entities/{get_raw_id(entity)}",
        "type": entity.get("type", "unspecified"),
    }
    __send_cloudevent("dams.entity_changed", data)


def signal_entity_deleted(entity):
    data = {"_id": get_raw_id(entity), "type": entity.get("type", "unspecified")}
    __send_cloudevent("dams.entity_deleted", data)


def signal_mediafile_changed(old_mediafile, mediafile):
    data = {"old_mediafile": old_mediafile, "mediafile": mediafile}
    __send_cloudevent("dams.mediafile_changed", data)


def signal_mediafile_deleted(mediafile, linked_entities):
    data = {"mediafile": mediafile, "linked_entities": linked_entities}
    __send_cloudevent("dams.mediafile_deleted", data)
