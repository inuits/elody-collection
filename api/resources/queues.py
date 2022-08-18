import app

from storage.storagemanager import StorageManager


@app.rabbit.queue("dams.child_relation_changed")
def child_relation_changed(routing_key, body, message_id):
    data = body["data"]
    if "collection" not in data or "parent_id" not in data:
        app.logger.error("Message malformed: missing 'collection' or 'parent_id'")
        return
    StorageManager().get_db_engine().update_parent_relation_values(
        data["collection"], data["parent_id"]
    )


@app.rabbit.queue("dams.mediafile_changed")
def mediafile_changed(routing_key, body, message_id):
    data = body["data"]
    if "old_mediafile" not in data or "mediafile" not in data:
        app.logger.error("Message malformed: missing 'old_mediafile' or 'mediafile'")
        return
    StorageManager().get_db_engine().handle_mediafile_status_change(
        data["old_mediafile"], data["mediafile"]
    )
    StorageManager().get_db_engine().reindex_mediafile_parents(data["mediafile"])


@app.rabbit.queue("dams.mediafile_deleted")
def mediafile_deleted(routing_key, body, message_id):
    data = body["data"]
    if "mediafile" not in data or "linked_entities" not in data:
        app.logger.error("Message malformed: missing 'mediafile' or 'linked_entities'")
        return
    StorageManager().get_db_engine().handle_mediafile_deleted(data["linked_entities"])
    StorageManager().get_db_engine().reindex_mediafile_parents(
        parents=data["linked_entities"]
    )
