from datetime import UTC, datetime
from os import getenv
from time import sleep

from configuration import get_object_configuration_mapper
from elody.job import handle_parent_job_finished
from elody.object_configurations.job_configuration import Status
from elody.util import (
    get_item_metadata_value,
    get_item_relation_key,
    get_raw_id,
    mediafile_is_public,
)
from logging_elody.log import log
from rabbit import get_rabbit
from storage.storagemanager import StorageManager

queue_prefix = getenv("QUEUE_PREFIX", "dams")
queue_type = getenv("QUEUE_TYPE")
routing_key_prefix = getenv("ROUTING_KEY_PREFIX", "dams")


def __argument_wrapper(*, queue_name, routing_key, single_active_consumer=False):
    arguments = {"routing_key": routing_key}
    if getenv("AMQP_MANAGER", "amqpstorm_flask") == "amqpstorm_flask":
        arguments["queue_name"] = queue_name
        queue_arguments = {}
        if queue_type:
            queue_arguments.update({"x-queue-type": queue_type})
        if single_active_consumer:
            queue_arguments.update({"x-single-active-consumer": True})
        if queue_arguments:
            arguments["queue_arguments"] = queue_arguments
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
    ),
)
def update_parent_relation_values(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["collection", "parent_id"]):
        return
    StorageManager().get_db_engine().update_parent_relation_values(
        data["collection"],
        data["parent_id"],
    )


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-add_entity_to_history",
        routing_key=f"{routing_key_prefix}.entity_changed",
    ),
)
def add_entity_to_history(routing_key, body, message_id):
    data = body["data"]
    entity_id = data["location"].removeprefix("/entities/")
    unchanged_entity = data.get("unchanged_entity")
    return add_item_to_history(entity_id, data, unchanged_entity)


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-add_mediafile_to_history",
        routing_key=f"{routing_key_prefix}.mediafile_changed",
    ),
)
def add_mediafile_to_history(routing_key, body, message_id):
    data = body["data"]
    mediafile_id = get_raw_id(data.get("mediafile"))
    unchanged_mediafile = data.get("old_mediafile")
    return add_item_to_history(
        mediafile_id,
        data,
        unchanged_mediafile,
        "mediafiles",
        ["mediafile", "old_mediafile"],
    )


def add_item_to_history(
    id,  # noqa: A002
    data,
    unchanged_item=None,
    collection="entities",
    required_fields=["location", "type"],  # noqa: B006
):
    storage = StorageManager().get_db_engine()
    if __is_malformed_message(data, required_fields):
        return

    item = storage.get_item_from_collection_by_id(collection, id)
    relations = storage.get_collection_item_relations(
        collection, id, True
    )  # noqa: FBT003
    content = {
        "object": item,
        "timestamp": datetime.now(UTC),
        "collection": collection,
        "relations": relations,
    }

    if unchanged_item:
        unchanged_item.pop("date_updated", None)
        unchanged_item.pop("date_created", None)

        item_date_updated = item.pop("date_updated", None)
        item_date_created = item.pop("date_created", None)
        if unchanged_item != item:
            item["date_updated"] = item_date_updated
            item["date_created"] = item_date_created
            content["entity"] = item
            storage.save_item_to_collection("history", content)
    else:
        storage.save_item_to_collection("history", content)


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-add_scan_info_to_mediafile",
        routing_key=f"{routing_key_prefix}.file_scanned",
    ),
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
        },
    }
    if data["infected"]:
        metadata = storage.get_collection_item_sub_item(
            "mediafiles",
            data["mediafile_id"],
            "metadata",
            [],
        )
        for item in [x for x in metadata if x["key"] == "publication_status"]:
            item["value"] = "infected"
        content["metadata"] = metadata
    storage.patch_item_from_collection("mediafiles", data["mediafile_id"], content)


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-update_job",
        routing_key=[
            f"{routing_key_prefix}.job_changed",
            f"{routing_key_prefix}.job_created",
        ],
        single_active_consumer=True,
    ),
)
def handle_job_change(routing_key, body, message_id):
    if not body["data"].get("patch"):
        create_job(routing_key, body, message_id)
    else:
        update_job(routing_key, body, message_id)


def update_job(routing_key, body, message_id):
    try:
        job_id = body["data"].get("id", body["data"].get("_id"))
        config = get_object_configuration_mapper().get("job")
        collection = config.crud()["collection"]

        if job_id and collection:
            storage = StorageManager().get_db_engine()
            document = storage.get_item_from_collection_by_id(collection, job_id)
            if not document:
                sleep(5)
                document = storage.get_item_from_collection_by_id(collection, job_id)
            if not document:
                return
            current_status = get_item_metadata_value(document, "status")
            new_status = get_item_metadata_value(body["data"]["patch"], "status")
            if current_status != "failed" or new_status == "failed":
                log.info(f"Updating job {job_id} with patch {body['data']['patch']}")
                storage.patch_item_from_collection_v2(
                    collection,
                    document,
                    body["data"]["patch"],
                    "elody",
                    run_post_crud_hook=False,
                )
                parent_job_id = get_item_relation_key(document, "hasParentJob")
                if parent_job_id and new_status and current_status != new_status:
                    _handle_status_update(
                        parent_job_id,
                        collection,
                        current_status,
                        new_status,
                    )

    except Exception as exception:  # noqa: BLE001
        log.exception(
            f"{exception.__class__.__name__}: {exception}",
            info_labels={"mq_message": body},
            exc_info=exception,
        )


def _handle_status_update(job_id, collection, current_status, new_status):
    if current_status == new_status:
        return

    storage = StorageManager().get_db_engine()
    parent_job = storage.get_item_from_collection_by_id(collection, job_id)
    if not parent_job:
        sleep(5)
        parent_job = storage.get_item_from_collection_by_id(collection, job_id)
    if not parent_job:
        return
    if not get_item_metadata_value(parent_job, "child_jobs"):
        return

    parent_job = storage.increment_metadata_values(
        id=job_id,
        collection=collection,
        metadata_key="child_jobs",
        increment_fields={
            current_status: -1,
            new_status: 1,
        },
    )

    parent_status = next(
        metadata_entry.get("value", None)
        for metadata_entry in parent_job.get("metadata", [{}])
        if metadata_entry.get("key") == "status"
    )

    finished, parent_child_status_value = _check_parent_children_status(parent_job)
    if finished:
        log.info(f"Finishing Parent Job: {job_id}")
        handle_parent_job_finished(
            job_id,
            parent_child_status_value,
            get_rabbit=get_rabbit,
        )
    elif parent_status == Status.FINISHED and new_status not in (
        Status.FINISHED,
        Status.WARNING,
        Status.FAILED,
    ):
        _handle_parent_wrong_status(job_id, collection)


def _check_parent_children_status(document) -> tuple[bool, dict[str, str]]:

    parent_children_status = next(
        metadata_entry
        for metadata_entry in document.get("metadata", [])
        if metadata_entry.get("key") == "child_jobs"
    )
    if parent_children_status is not None:
        parent_child_status_value = parent_children_status["value"]
        child_jobs_initiated = parent_child_status_value["initiated"]
        child_jobs_queued = parent_child_status_value["queued"]
        child_jobs_running = parent_child_status_value["running"]
        child_jobs_failed = parent_child_status_value["failed"]
        child_jobs_finished = parent_child_status_value["finished"]
        child_jobs_warning = parent_child_status_value["warning"]

        if (
            child_jobs_queued == 0
            and child_jobs_running == 0
            and (child_jobs_failed + child_jobs_finished + child_jobs_warning)
            == child_jobs_initiated
        ):
            return True, parent_child_status_value
        return False, parent_child_status_value
    return False, {}


def _handle_parent_wrong_status(parent_job_id, collection):
    storage = StorageManager().get_db_engine()
    config = get_object_configuration_mapper().get("job")
    collection = config.crud()["collection"]
    parent_job = storage.get_item_from_collection_by_id(
        collection=collection,
        id=parent_job_id,
    )
    if not parent_job:
        return

    finished, _ = _check_parent_children_status(
        parent_job,
    )  # checking again to make sure nothing changed in the meantime

    if not finished:
        log.info(f"Moving parent job back to running: {parent_job_id}")
        config.crud()["start_job"](parent_job_id, get_rabbit=get_rabbit)


def _attach_child(parent_job_id, collection):
    storage = StorageManager().get_db_engine()
    config = get_object_configuration_mapper().get("job")
    collection = config.crud()["collection"]
    parent_job = storage.get_item_from_collection_by_id(
        collection=collection,
        id=parent_job_id,
    )
    if not parent_job:
        sleep(5)
        parent_job = storage.get_item_from_collection_by_id(
            collection=collection,
            id=parent_job_id,
        )
    if not parent_job:
        return
    if parent_job and get_item_metadata_value(parent_job, "child_jobs"):
        parent_job = storage.increment_metadata_values(
            parent_job_id,
            collection,
            "child_jobs",
            {"initiated": 1, "queued": 1},
        )


def create_job(routing_key, body, message_id):
    try:
        job = body["data"]
        config = get_object_configuration_mapper().get("job")
        collection = config.crud()["collection"]
        storage = StorageManager().get_db_engine()
        storage.save_item_to_collection_v2(collection, job, run_post_crud_hook=False)

        parent_job_id = get_item_relation_key(job, "hasParentJob")
        if parent_job_id:
            _attach_child(parent_job_id, collection)

    except Exception as exception:  # noqa: BLE001
        log.exception(
            f"{exception.__class__.__name__}: {exception}",
            body["data"],
            exc_info=exception,
        )


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-handle_mediafile_status_change",
        routing_key=f"{routing_key_prefix}.mediafile_changed",
    ),
)
def handle_mediafile_status_change(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["mediafile", "old_mediafile"]):
        return
    storage = StorageManager().get_db_engine()
    old_publication_status = get_item_metadata_value(
        data["old_mediafile"],
        "publication_status",
    )
    new_publication_status = get_item_metadata_value(
        data["mediafile"],
        "publication_status",
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
    ),
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
                "entities",
                entity_id,
                [pdf_relation],
            )
            storage.delete_item_from_collection("mediafiles", pdf_relation.get("key"))
            process_ocr([mediafile], pdf_relation)


def process_ocr(mediafiles_data, operation_relation):
    body = create_ocr_body(mediafiles_data, operation_relation)
    get_rabbit().send(body, routing_key=f"{routing_key_prefix}.ocr_request")


def add_relation(storage, collection, body, item_id):
    payload = [
        {
            "key": body.get("id_new_mediafile"),
            "label": "hasMediafile",
            "type": "belongsTo",
            "is_ocr": True,
            "operation": body.get("operation"),
            "lang": body.get("lang"),
        },
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
    ),
)
def handle_mediafile_deleted(routing_key, body, message_id):  # noqa: PLR0912
    data = body["data"]
    deleted_mediafile = data["mediafile"]
    if __is_malformed_message(data, ["linked_entities", "mediafile"]):
        return
    storage = StorageManager().get_db_engine()
    ocr_keys = []
    for relation in deleted_mediafile.get("relations", []):
        if relation.get("is_ocr"):
            mediafile_id = relation.get("key")
            mediafile = storage.get_item_from_collection_by_id(
                "mediafiles",
                mediafile_id,
            )

            if mediafile and mediafile.get("technical_origin") != "original":
                ocr_keys.append(mediafile_id)
                storage.delete_item_from_collection("mediafiles", mediafile_id)

    for relation in deleted_mediafile.get("relations", []):
        if relation.get("type") == "belongsTo" and "is_ocr" not in relation:
            entity = storage.get_item_from_collection_by_id(
                "entities",
                relation.get("key"),
            )
            if entity:
                entity_relations = entity.get("relations", [])
                if pdf_relation := has_ocr_operation(entity_relations, "pdf"):
                    storage.delete_collection_item_relations(
                        "entities",
                        get_raw_id(entity),
                        [pdf_relation],
                    )
                    storage.delete_item_from_collection(
                        "mediafiles",
                        pdf_relation.get("key"),
                    )
                    entity_mediafiles_ids = []
                    for relation in entity_relations:  # noqa: PLW2901
                        if (
                            relation.get("type") == "hasMediafile"
                            and relation.get("label") == "hasMediafile"
                        ):
                            entity_mediafiles_ids.append(relation.get("key"))
                    if len(entity_mediafiles_ids) > 0:
                        mediafile_image_data = []
                        for mediafile_id in entity_mediafiles_ids:
                            mediafile = storage.get_item_from_collection_by_id(
                                "mediafiles",
                                mediafile_id,
                            )
                            mediafile_image_data.append(mediafile)
                        process_ocr(mediafile_image_data, pdf_relation)
                for entity_relation in entity_relations:
                    if entity_relation.get("key") in ocr_keys:
                        storage.delete_collection_item_relations(
                            "entities",
                            get_raw_id(entity),
                            [entity_relation],
                        )
    if "entity_id" in data["linked_entities"]:
        storage.handle_mediafile_deleted(data["linked_entities"])
        storage.reindex_mediafile_parents(parents=data["linked_entities"])


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-sync_entity_to_typesense",
        routing_key=f"{routing_key_prefix}.entity_changed",
    ),
)
def sync_entity_to_typesense(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["location", "type"]):
        return

    entity_id = data["location"].removeprefix("/entities/")
    entity_type = data.get("type", "")

    config = get_object_configuration_mapper().get(entity_type or "entities")
    ts_config = config.crud().get("typesense", {})
    if not ts_config.get("enabled"):
        return

    collection = config.crud().get("collection", "entities")
    storage = StorageManager().get_db_engine()
    entity = storage.get_item_from_collection_by_id(collection, entity_id)
    if not entity:
        return

    _index_entity_to_typesense(entity, ts_config, storage)


def _index_entity_to_typesense(entity, ts_config, storage):
    """Build the Typesense document for an entity (incl. denormalized reference
    fields) and upsert it. Shared by the direct sync handler and the reverse
    propagation that re-indexes referrers when a denormalized source changes."""
    from search.typesense_client import (  # noqa: PLC0415
        prepare_document_for_typesense,
        resolve_denormalized_fields,
        upsert_document,
    )

    doc = prepare_document_for_typesense(
        entity,
        ts_config.get("search_fields", []),
        facet_fields=ts_config.get("facet_fields", []),
    )
    denormalized_relations = ts_config.get("denormalized_relations", [])
    if denormalized_relations:
        doc.update(
            resolve_denormalized_fields(entity, denormalized_relations, storage)
        )
    upsert_document(ts_config.get("collection", "entities"), doc)


DENORMALIZED_REFERRER_RESYNC_CAP = 5000


def _iter_denormalized_source_relations(mapper, source_type):
    """Yield (ts_config, target_collection, relation) for every denormalized
    relation across all object configurations whose `source_type` matches
    `source_type`. Deduplicated by (target_collection, ref, as)."""
    seen = set()
    for config_cls in mapper.get_all().values():
        try:
            crud = config_cls().crud()
        except Exception:
            continue
        ts_config = crud.get("typesense", {})
        if not ts_config.get("enabled"):
            continue
        target_collection = crud.get("collection", "entities")
        for relation in ts_config.get("denormalized_relations", []):
            if relation.get("source_type") != source_type:
                continue
            key = (target_collection, relation.get("ref"), relation.get("as"))
            if key in seen:
                continue
            seen.add(key)
            yield ts_config, target_collection, relation


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-propagate_denormalized_source_change",
        routing_key=f"{routing_key_prefix}.entity_changed",
    ),
)
def propagate_denormalized_source_change(routing_key, body, message_id):
    """When an entity that is denormalized onto other documents (e.g. an author
    or SISO) changes, re-index the documents referencing it so their copied text
    stays fresh. Gated on the denormalized value actually changing, so an
    unrelated edit to a popular author does not re-index thousands of works."""
    from search.typesense_client import get_nested_value  # noqa: PLC0415

    data = body["data"]
    if __is_malformed_message(data, ["location", "type"]):
        return

    unchanged_entity = data.get("unchanged_entity")
    if not unchanged_entity:
        # Newly created source: no referrers exist yet, and any that are created
        # alongside it index themselves on their own save.
        return

    source_type = data.get("type", "")
    mapper = get_object_configuration_mapper()
    relations = list(_iter_denormalized_source_relations(mapper, source_type))
    if not relations:
        return

    source_id = data["location"].removeprefix("/entities/")
    storage = StorageManager().get_db_engine()
    source_entity = None
    for ts_config, target_collection, relation in relations:
        if source_entity is None:
            source_collection = relation.get("source_collection", "entities")
            source_entity = storage.get_item_from_collection_by_id(
                source_collection, source_id
            )
            if not source_entity:
                return
        target_field = relation["target_field"]
        if get_nested_value(unchanged_entity, target_field) == get_nested_value(
            source_entity, target_field
        ):
            continue  # denormalized text unchanged -> nothing to propagate
        _resync_denormalized_referrers(
            storage, source_entity, target_collection, relation, ts_config
        )


def _resync_denormalized_referrers(
    storage, source_entity, target_collection, relation, ts_config
):
    """Re-index every entity in `target_collection` whose reference field points
    at `source_entity` (matched on its _id or any of its identifiers)."""
    ids = [source_entity["_id"], *(source_entity.get("identifiers") or [])]
    ref_field = relation["ref"]
    page_size = 250
    skip = 0
    synced = 0
    while True:
        result = storage.get_items_from_collection(
            target_collection,
            skip=skip,
            limit=page_size,
            filters={ref_field: {"$in": ids}},
        )
        items = result.get("results", [])
        if not items:
            break
        for item in items:
            _index_entity_to_typesense(item, ts_config, storage)
            synced += 1
            if synced >= DENORMALIZED_REFERRER_RESYNC_CAP:
                log.warning(
                    f"Denormalized referrer resync hit cap "
                    f"({DENORMALIZED_REFERRER_RESYNC_CAP}) for source "
                    f"'{source_entity['_id']}' on '{ref_field}'; remaining "
                    f"referrers will refresh on the next full reindex."
                )
                return
        if len(items) < page_size:
            break
        skip += page_size
    if synced:
        log.info(
            f"Re-indexed {synced} referrer(s) after change to denormalized "
            f"source '{source_entity['_id']}' ({ref_field})."
        )


@get_rabbit().queue(
    **__argument_wrapper(
        queue_name=f"{queue_prefix}-delete_entity_from_typesense",
        routing_key=f"{routing_key_prefix}.entity_deleted",
    ),
)
def delete_entity_from_typesense(routing_key, body, message_id):
    from search.typesense_client import delete_document  # noqa: PLC0415

    data = body["data"]
    entity_id = data.get("entity_id") or data.get("_id")
    entity_type = data.get("type", "")
    if not entity_id:
        log.warning("delete_entity_from_typesense: no entity_id in message")
        return

    config = get_object_configuration_mapper().get(entity_type)
    ts_config = config.crud().get("typesense", {})
    if not ts_config.get("enabled"):
        return

    ts_collection = ts_config.get("collection", "entities")
    delete_document(ts_collection, entity_id)
