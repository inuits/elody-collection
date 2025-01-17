from configuration import get_object_configuration_mapper
from datetime import datetime, timezone
from elody.util import get_item_metadata_value, mediafile_is_public, get_raw_id
from logging_elody.log import log
from os import getenv
from rabbit import get_rabbit
from storage.storagemanager import StorageManager
from time import sleep


queue_prefix = getenv("QUEUE_PREFIX", "dams")
routing_key_prefix = getenv("ROUTING_KEY_PREFIX", "dams")


def __argument_wrapper(*, queue_name, routing_key):
    arguments = {"routing_key": routing_key}
    if getenv("AMQP_MANAGER", "amqpstorm_flask") == "amqpstorm_flask":
        arguments["queue_name"] = queue_name
    return arguments


def __is_malformed_message(data, fields):
    if not all(x in data for x in fields):
        log.error(f"Message malformed: missing one of {fields}")
        return True
    return False


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-update_parent_relation_values",
        routing_key=f"{routing_key_prefix}.child_relation_changed",
    )
)
def update_parent_relation_values(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["collection", "parent_id"]):
        return
    StorageManager().get_db_engine().update_parent_relation_values(
        data["collection"], data["parent_id"]
    )


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-add_entity_to_history",
        routing_key=f"{routing_key_prefix}.entity_changed",
    )
)
def add_entity_to_history(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["location", "type"]):
        return
    entity_id = data["location"].removeprefix("/entities/")
    storage = StorageManager().get_db_engine()
    entity = storage.get_item_from_collection_by_id("entities", entity_id)
    relations = storage.get_collection_item_relations("entities", entity_id, True)
    content = {
        "object": entity,
        "timestamp": datetime.now(timezone.utc),
        "collection": "entities",
        "relations": relations,
    }
    storage.save_item_to_collection("history", content)


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-add_scan_info_to_mediafile",
        routing_key=f"{routing_key_prefix}.file_scanned",
    )
)
def add_scan_info_to_mediafile(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["clamav_version", "infected", "mediafile_id"]):
        return
    storage = StorageManager().get_db_engine()
    content = {
        "scan_info": {
            "clamav_version": data["clamav_version"],
            "datetime": body["time"],
            "infected": data["infected"],
        }
    }
    if data["infected"]:
        metadata = storage.get_collection_item_sub_item(
            "mediafiles", data["mediafile_id"], "metadata", list()
        )
        for item in [x for x in metadata if x["key"] == "publication_status"]:
            item["value"] = "infected"
        content["metadata"] = metadata
    storage.patch_item_from_collection("mediafiles", data["mediafile_id"], content)


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-update_job",
        routing_key=f"{routing_key_prefix}.job_changed",
    )
)
def update_job(routing_key, body, message_id):
    try:
        id, collection = None, None
        if document_info_job_was_initiated_for := body["data"].get(
            "document_info_job_was_initiated_for"
        ):
            if document_info_job_was_initiated_for.get(
                "id"
            ) and document_info_job_was_initiated_for.get("type"):
                id = document_info_job_was_initiated_for["id"]
                type = document_info_job_was_initiated_for["type"]
                collection = (
                    get_object_configuration_mapper().get(type).crud()["collection"]
                )
        else:
            id = body["data"]["id"]
            collection = (
                get_object_configuration_mapper().get("job").crud()["collection"]
            )

        if id and collection:
            storage = StorageManager().get_db_engine()
            document = storage.get_item_from_collection_by_id(collection, id)
            if not document:
                sleep(5)
                document = storage.get_item_from_collection_by_id(collection, id)
            storage.patch_item_from_collection_v2(
                collection,
                document,
                body["data"]["patch"],
                "elody",
                run_post_crud_hook=False,
            )
    except Exception as exception:
        log.exception(
            f"{exception.__class__.__name__}: {exception}",
            info_labels={"mq_message": body},
            exc_info=exception,
        )


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-create_job",
        routing_key=f"{routing_key_prefix}.job_created",
    )
)
def create_job(routing_key, body, message_id):
    try:
        job = body["data"]
        job["computed_values"].update({"created_at": datetime.now(timezone.utc)})
        collection = get_object_configuration_mapper().get("job").crud()["collection"]
        StorageManager().get_db_engine().save_item_to_collection_v2(
            collection, job, run_post_crud_hook=False
        )
    except Exception as exception:
        log.exception(
            f"{exception.__class__.__name__}: {exception}",
            body["data"],
            exc_info=exception,
        )


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-handle_mediafile_status_change",
        routing_key=f"{routing_key_prefix}.mediafile_changed",
    )
)
def handle_mediafile_status_change(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["mediafile", "old_mediafile"]):
        return
    storage = StorageManager().get_db_engine()
    old_publication_status = get_item_metadata_value(
        data["old_mediafile"], "publication_status"
    )
    new_publication_status = get_item_metadata_value(
        data["mediafile"], "publication_status"
    )
    if old_publication_status == new_publication_status:
        return
    if mediafile_is_public(data["mediafile"]):
        return
    storage.handle_mediafile_status_change(data["mediafile"])
    storage.reindex_mediafile_parents(data["mediafile"])


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-handle_mediafiles_added_for_entity",
        routing_key=f"{routing_key_prefix}.mediafiles_added_for_entity",
    )
)
def handle_mediafiles_added_for_entity(routing_key, body, message_id):
    data = body["data"]
    mediafiles = data["mediafiles"]
    entity_id = get_raw_id(data["entity"])
    storage = StorageManager().get_db_engine()
    for mediafile in mediafiles:
        process_mediafile(storage, mediafile, entity_id)


def process_mediafile(storage, mediafile, entity_id):
    if entity_id:
        entity = storage.get_item_from_collection_by_id("entities", entity_id)
        entity_relations = entity.get("relations", [])

        if txt_relation := has_ocr_operation(entity_relations, "txt"):
            process_ocr([mediafile], txt_relation)
        if alto_relation := has_ocr_operation(entity_relations, "alto"):
            process_ocr([mediafile], alto_relation)
        if pdf_relation := has_ocr_operation(entity_relations, "pdf"):
            storage.delete_collection_item_relations(
                "entities", entity_id, [pdf_relation]
            )
            storage.delete_item_from_collection("mediafiles", pdf_relation.get("key"))
            process_ocr([mediafile], pdf_relation)


def process_ocr(mediafiles_data, operation_relation):
    body = create_ocr_body(mediafiles_data, operation_relation)
    get_rabbit().send(body, routing_key="dams.ocr_request")


def add_relation(storage, collection, body, item_id):
    payload = [
        {
            "key": body.get("id_new_mediafile"),
            "label": "hasMediafile",
            "type": "belongsTo",
            "is_ocr": True,
            "operation": body.get("operation"),
            "lang": body.get("lang"),
        }
    ]
    storage.patch_collection_item_relations(collection, item_id, payload)


def has_ocr_operation(relations, operation):
    for relation in relations:
        if (
            relation.get("type") == "belongsTo"
            and relation.get("is_ocr")
            and relation.get("operation") == operation
        ):
            return relation
    return ""


def create_ocr_body(mediafiles_data, relation):
    operation = relation.get("operation", "pdf")
    lang = relation.get("lang", "eng")
    return {
        "operation": operation,
        "lang": lang,
        "mediafile_image_data": mediafiles_data,
    }


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-handle_mediafile_deleted",
        routing_key=f"{routing_key_prefix}.mediafile_deleted",
    )
)
def handle_mediafile_deleted(routing_key, body, message_id):
    data = body["data"]
    deleted_mediafile = data["mediafile"]
    if __is_malformed_message(data, ["linked_entities", "mediafile"]):
        return
    storage = StorageManager().get_db_engine()

    ocr_keys = []
    for relation in deleted_mediafile.get("relations", []):
        if relation.get("is_ocr"):
            id = relation.get("key")
            ocr_keys.append(id)
            storage.delete_item_from_collection("mediafiles", id)

    for relation in deleted_mediafile.get("relations", []):
        if relation.get("type") == "belongsTo" and "is_ocr" not in relation:
            entity = storage.get_item_from_collection_by_id(
                "entities", relation.get("key")
            )
            if entity:
                entity_relations = entity.get("relations", [])
                if pdf_relation := has_ocr_operation(entity_relations, "pdf"):
                    storage.delete_collection_item_relations(
                        "entities", get_raw_id(entity), [pdf_relation]
                    )
                    storage.delete_item_from_collection(
                        "mediafiles", pdf_relation.get("key")
                    )
                    entity_mediafiles_ids = []
                    for relation in entity_relations:
                        if (
                            relation.get("type") == "hasMediafile"
                            and relation.get("label") == "hasMediafile"
                        ):
                            entity_mediafiles_ids.append(relation.get("key"))
                    if len(entity_mediafiles_ids) > 0:
                        mediafile_image_data = []
                        for id in entity_mediafiles_ids:
                            mediafile = storage.get_item_from_collection_by_id(
                                "mediafiles", id
                            )
                            mediafile_image_data.append(mediafile)
                        process_ocr(mediafile_image_data, pdf_relation)
                for entity_relation in entity_relations:
                    if entity_relation.get("key") in ocr_keys:
                        storage.delete_collection_item_relations(
                            "entities", get_raw_id(entity), [entity_relation]
                        )
    if "entity_id" in data["linked_entities"]:
        storage.handle_mediafile_deleted(data["linked_entities"])
        storage.reindex_mediafile_parents(parents=data["linked_entities"])
