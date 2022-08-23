import app
import os

from cloudevents.conversion import to_dict
from cloudevents.http import CloudEvent
from flask import request
from flask_restful import Resource, abort
from storage.storagemanager import StorageManager
from validator import validate_json
from werkzeug.exceptions import BadRequest


class BaseResource(Resource):
    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        self.collection_api_url = os.getenv("COLLECTION_API_URL")
        self.image_api_url_ext = os.getenv("IMAGE_API_URL_EXT")
        self.storage_api_url = os.getenv("STORAGE_API_URL")
        self.storage_api_url_ext = os.getenv("STORAGE_API_URL_EXT")

    def get_request_body(self):
        try:
            request_body = request.get_json()
            invalid_input = request_body is None
        except BadRequest:
            invalid_input = True
        if invalid_input:
            abort(405, message="Invalid input")
        return request_body

    def abort_if_item_doesnt_exist(self, collection, id):
        item = self.storage.get_item_from_collection_by_id(collection, id)
        if not item:
            abort(
                404,
                message=f"Item with id {id} doesn't exist in collection {collection}",
            )
        return item

    def abort_if_not_valid_json(self, type, json, schema):
        validation_error = validate_json(json, schema)
        if validation_error:
            abort(
                400, message=f"{type} doesn't have a valid format. {validation_error}"
            )

    def abort_if_not_logged_in(self, token):
        if "email" not in token:
            abort(400, message="You must be logged in to access this feature")

    def _inject_api_urls_into_entities(self, entities):
        for entity in entities:
            if "primary_mediafile_location" in entity:
                entity[
                    "primary_mediafile_location"
                ] = f'{self.storage_api_url_ext}{entity["primary_mediafile_location"]}'
            if "primary_thumbnail_location" in entity:
                entity[
                    "primary_thumbnail_location"
                ] = f'{self.image_api_url_ext}{entity["primary_thumbnail_location"]}'
            if "primary_transcode_location" in entity:
                entity[
                    "primary_transcode_location"
                ] = f'{self.storage_api_url_ext}{entity["primary_transcode_location"]}'
        return entities

    def _inject_api_urls_into_mediafiles(self, mediafiles):
        for mediafile in mediafiles:
            if "original_file_location" in mediafile:
                mediafile[
                    "original_file_location"
                ] = f'{self.storage_api_url_ext}{mediafile["original_file_location"]}'
            if "thumbnail_file_location" in mediafile:
                mediafile[
                    "thumbnail_file_location"
                ] = f'{self.image_api_url_ext}{mediafile["thumbnail_file_location"]}'
            if "transcode_file_location" in mediafile:
                mediafile[
                    "transcode_file_location"
                ] = f'{self.storage_api_url_ext}{mediafile["transcode_file_location"]}'
        return mediafiles

    def __send_cloudevent(self, routing_key, data):
        attributes = {"type": routing_key, "source": "dams"}
        event = to_dict(CloudEvent(attributes, data))
        app.rabbit.send(event, routing_key=routing_key)

    def _signal_entity_changed(self, entity):
        data = {
            "location": f"/entities/{self._get_raw_id(entity)}",
            "type": entity["type"] if "type" in entity else "unspecified",
        }
        self.__send_cloudevent("dams.entity_changed", data)

    def _signal_entity_deleted(self, entity):
        data = {
            "_id": self._get_raw_id(entity),
            "type": entity["type"] if "type" in entity else "unspecified",
        }
        self.__send_cloudevent("dams.entity_deleted", data)

    def _signal_mediafile_changed(self, old_mediafile, mediafile):
        data = {"old_mediafile": old_mediafile, "mediafile": mediafile}
        self.__send_cloudevent("dams.mediafile_changed", data)

    def _signal_mediafile_deleted(self, mediafile, linked_entities):
        data = {"mediafile": mediafile, "linked_entities": linked_entities}
        self.__send_cloudevent("dams.mediafile_deleted", data)

    def _get_raw_id(self, item):
        return item["_key"] if "_key" in item else item["_id"]

    def _set_entity_mediafile_and_thumbnail(self, entity):
        mediafiles = self.storage.get_collection_item_mediafiles(
            "entities", self._get_raw_id(entity)
        )
        for mediafile in mediafiles:
            if "is_primary" in mediafile and mediafile["is_primary"] is True:
                entity["primary_mediafile"] = mediafile["filename"]
                entity["primary_mediafile_location"] = mediafile[
                    "original_file_location"
                ]
                if "transcode_file_location" in mediafile:
                    entity["primary_transcode"] = mediafile["transcode_filename"]
                    entity["primary_transcode_location"] = mediafile[
                        "transcode_file_location"
                    ]
                if "img_width" in mediafile and "img_height" in mediafile:
                    entity["primary_width"] = mediafile["img_width"]
                    entity["primary_height"] = mediafile["img_height"]
            if (
                "is_primary_thumbnail" in mediafile
                and mediafile["is_primary_thumbnail"] is True
            ):
                entity["primary_thumbnail_location"] = mediafile[
                    "thumbnail_file_location"
                ]
        return entity

    def _add_relations_to_metadata(self, entity, collection="entities", sort_by=False):
        relations = self.storage.get_collection_item_relations(
            collection, self._get_raw_id(entity), exclude_relations=["story_box_visits"]
        )
        if relations:
            if sort_by:
                sort = True
                for relation in relations:
                    if "order" not in relation:
                        sort = False
                if sort:
                    relations = sorted(relations, key=lambda tup: tup[sort_by])
            if "metadata" in entity:
                entity["metadata"] = entity["metadata"] + relations
            else:
                entity["metadata"] = relations
        return entity

    def _mediafile_is_public(self, mediafile):
        if "metadata" not in mediafile:
            return False
        for item in [
            x for x in mediafile["metadata"] if x["key"] == "publication_status"
        ]:
            return item["value"] == "publiek"
        return False

    def _get_mediafile_access(self, mediafile):
        if "metadata" not in mediafile:
            return "full"
        if not self._mediafile_is_public(mediafile):
            return "none"
        for item in [x for x in mediafile["metadata"] if x["key"] == "rights"]:
            if "in copyright" in item["value"].lower():
                return "limited"
            return "full"
        return "full"

    def _only_own_items(self, permissions=None):
        if not permissions:
            permissions = ["show-all"]
        else:
            permissions = [*permissions, *["show-all"]]
        for permission in permissions:
            if app.require_oauth.check_permission(permission):
                return False
        return True

    def abort_if_not_own_item(self, item, token):
        if "user" not in item or item["user"] != token["email"]:
            abort(403, message="Access denied")
