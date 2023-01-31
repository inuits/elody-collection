import app
import os
import util

from datetime import datetime
from flask import request
from flask_restful import Resource, abort
from storage.storagemanager import StorageManager
from validator import validate_json


class BaseResource(Resource):
    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        self.collection_api_url = os.getenv("COLLECTION_API_URL")
        self.image_api_url_ext = os.getenv("IMAGE_API_URL_EXT")
        self.storage_api_url = os.getenv("STORAGE_API_URL")
        self.storage_api_url_ext = os.getenv("STORAGE_API_URL_EXT")

    def _abort_if_item_doesnt_exist(self, collection, id):
        if item := self.storage.get_item_from_collection_by_id(collection, id):
            return item
        abort(
            404, message=f"Item with id {id} doesn't exist in collection {collection}"
        )

    def _abort_if_no_access(self, item, token, collection="entities"):
        app.logger.info(f"Checking if {token} has access to {item}")
        is_owner = self._is_owner_of_item(item, token)
        is_public = self._is_public(item)
        if not is_owner and not is_public:
            abort(403, message="Access denied")

    def _abort_if_not_logged_in(self, token):
        if "email" not in token:
            abort(401, message="You must be logged in to access this feature")

    def _abort_if_not_valid_json(self, type, json, schema):
        if validation_error := validate_json(json, schema):
            abort(
                400, message=f"{type} doesn't have a valid format. {validation_error}"
            )

    def _abort_if_not_valid_type(self, item, type):
        if type and item["type"] != type:
            abort(400, message=f"Item has the wrong type")

    def _add_relations_to_metadata(self, entity, collection="entities", sort_by=None):
        relations = self.storage.get_collection_item_relations(
            collection, util.get_raw_id(entity), exclude=["story_box_visits"]
        )
        if not relations:
            return entity
        if sort_by and all("order" in x for x in relations):
            relations = sorted(relations, key=lambda x: x[sort_by])
        entity["metadata"] = [*entity.get("metadata", []), *relations]
        return entity

    def _create_default_entity(self, user_id, type="asset"):
        content = {
            "type": type,
            "user": user_id,
            "date_created": str(datetime.now()),
            "version": 1,
        }

        entity = self.storage.save_item_to_collection("entities", content)
        util.signal_entity_changed(entity)
        return entity

    def _create_mediafile_for_entity(self, user_id, entity, filename):
        content = {
            "filename": filename,
            "user": user_id,
            "date_created": str(datetime.now()),
            "version": 1,
            "thumbnail_file_location": f"/iiif/3/{filename}/full/,150/0/default.jpg",
            "original_file_location": f"/download/{filename}",
        }
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        self.storage.add_mediafile_to_collection_item(
            "entities",
            util.get_raw_id(entity),
            mediafile["_id"],
            False,
        )
        util.signal_entity_changed(entity)
        return mediafile

    def _decorate_entity(self, entity, user_id):
        default_entity = {
            "type": "asset",
        }
        return {**default_entity, **entity}

    def _get_request_body(self):
        if (request_body := request.get_json(silent=True)) is not None:
            return request_body
        abort(400, message="Invalid input")

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

    def _is_owner_of_item(self, item, token):
        return "user" in item and item["user"] == token["email"]

    def _is_public(self, item):
        return "private" in item and not item["private"]

    def _only_own_items(self, permissions=None):
        if not permissions:
            permissions = ["show-all"]
        else:
            permissions.append("show-all")
        if app.require_oauth.check_permissions(permissions):
            return False
        return True

    def _set_entity_mediafile_and_thumbnail(self, entity):
        mediafiles = self.storage.get_collection_item_mediafiles(
            "entities", util.get_raw_id(entity)
        )
        for mediafile in mediafiles:
            if mediafile.get("is_primary", False):
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
            if mediafile.get("is_primary_thumbnail", False):
                entity["primary_thumbnail_location"] = mediafile[
                    "thumbnail_file_location"
                ]
        return entity
