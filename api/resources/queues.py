import app
import util

from storage.storagemanager import StorageManager


def __is_malformed_message(data, fields):
    if not all(x in data for x in fields):
        app.logger.error(f"Message malformed: missing one of {fields}")
        return True
    return False


@app.rabbit.queue("dams.child_relation_changed")
def child_relation_changed(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["collection", "parent_id"]):
        return
    StorageManager().get_db_engine().update_parent_relation_values(
        data["collection"], data["parent_id"]
    )


@app.rabbit.queue("dams.file_scanned")
def handle_file_scanned(routing_key, body, message_id):
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
            "mediafiles", data["mediafile_id"], "metadata"
        )
        for item in [x for x in metadata if x["key"] == "publication_status"]:
            item["value"] = "infected"
        content["metadata"] = metadata
    storage.patch_item_from_collection("mediafiles", data["mediafile_id"], content)


@app.rabbit.queue("dams.job_changed")
def job_changed(routing_key, body, message_id):
    StorageManager().get_db_engine().patch_item_from_collection(
        "jobs",
        body["data"]["identifiers"][0],
        body["data"],
    )


@app.rabbit.queue("dams.job_created")
def job_created(routing_key, body, message_id):
    StorageManager().get_db_engine().save_item_to_collection("jobs", body["data"])


@app.rabbit.queue("dams.mediafile_changed")
def mediafile_changed(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["mediafile", "old_mediafile"]):
        return
    storage = StorageManager().get_db_engine()
    old_publication_status = util.get_item_metadata_value(
        data["old_mediafile"], "publication_status"
    )
    new_publication_status = util.get_item_metadata_value(
        data["mediafile"], "publication_status"
    )
    if old_publication_status == new_publication_status:
        return
    if util.mediafile_is_public(data["mediafile"]):
        return
    storage.handle_mediafile_status_change(data["mediafile"])
    storage.reindex_mediafile_parents(data["mediafile"])


@app.rabbit.queue("dams.mediafile_deleted")
def mediafile_deleted(routing_key, body, message_id):
    data = body["data"]
    if __is_malformed_message(data, ["linked_entities", "mediafile"]):
        return
    storage = StorageManager().get_db_engine()
    storage.handle_mediafile_deleted(data["linked_entities"])
    storage.reindex_mediafile_parents(parents=data["linked_entities"])
