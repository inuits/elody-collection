import app
import json
import os

from cloudevents.http import CloudEvent, to_json
from flask import request
from flask_restful import Resource, abort
from storage.storagemanager import StorageManager
from validator import validate_json
from werkzeug.exceptions import BadRequest


class BaseResource(Resource):
    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        self.collection_api_url = os.getenv(
            "COLLECTION_API_URL", "http://localhost:8000"
        )
        self.storage_api_url = os.getenv("STORAGE_API_URL", "http://localhost:8001")
        self.cantaloupe_api_url = os.getenv(
            "CANTALOUPE_API_URL", "http://localhost:8182"
        )

    def get_request_body(self):
        try:
            request_body = request.get_json()
            invalid_input = request_body is None
        except BadRequest:
            invalid_input = True
        if invalid_input:
            abort(
                405,
                message="Invalid input",
            )
        return request_body

    def abort_if_item_doesnt_exist(self, collection, id):
        item = self.storage.get_item_from_collection_by_id(collection, id)
        if item is None:
            abort(
                404,
                message="Item with id {} doesn't exist in collection {}".format(
                    id, collection
                ),
            )
        return item

    def abort_if_not_valid_json(self, type, json, schema):
        validation_error = validate_json(json, schema)
        if validation_error:
            abort(
                400,
                message="{} doesn't have a valid format\n{}".format(
                    type, validation_error
                ),
            )

    def _abort_if_incorrect_type(self, items, relation_type):
        for item in items:
            if item["type"] != relation_type:
                abort(
                    400,
                    message="Expected relation type '{}'. got '{}'".format(
                        item["type"], relation_type
                    ),
                )

    def _inject_api_urls_into_entities(self, entities):
        for entity in entities:
            if "primary_mediafile_location" in entity:
                entity[
                    "primary_mediafile_location"
                ] = f'{self.storage_api_url}{entity["primary_mediafile_location"]}'
            if "primary_thumbnail_location" in entity:
                entity[
                    "primary_thumbnail_location"
                ] = f'{self.cantaloupe_api_url}{entity["primary_thumbnail_location"]}'
            if "primary_transcode_location" in entity:
                entity[
                    "primary_transcode_location"
                ] = f'{self.storage_api_url}{entity["primary_transcode_location"]}'
        return entities

    def _inject_api_urls_into_mediafiles(self, mediafiles):
        for mediafile in mediafiles:
            if "original_file_location" in mediafile:
                mediafile[
                    "original_file_location"
                ] = f'{self.storage_api_url}{mediafile["original_file_location"]}'
            if "thumbnail_file_location" in mediafile:
                mediafile[
                    "thumbnail_file_location"
                ] = f'{self.cantaloupe_api_url}{mediafile["thumbnail_file_location"]}'
            if "transcode_file_location" in mediafile:
                mediafile[
                    "transcode_file_location"
                ] = f'{self.storage_api_url}{mediafile["transcode_file_location"]}'
        return mediafiles

    def _signal_entity_changed(self, entity):
        attributes = {"type": "dams.entity_changed", "source": "dams"}
        data = {
            "location": "/entities/{}".format(self._get_raw_id(entity)),
            "type": entity["type"] if "type" in entity else "unspecified",
        }
        event = CloudEvent(attributes, data)
        message = json.loads(to_json(event))
        app.rabbit.send(message, routing_key="dams.entity_changed")

    def _signal_entity_deleted(self, entity):
        pass

    def _signal_mediafile_changed(self, old_mediafile, mediafile):
        attributes = {"type": "dams.mediafile_changed", "source": "dams"}
        data = {"old_mediafile": old_mediafile, "mediafile": mediafile}
        event = CloudEvent(attributes, data)
        message = json.loads(to_json(event))
        app.rabbit.send(message, routing_key="dams.mediafile_changed")

    def _get_raw_id(self, item):
        return item["_key"] if "_key" in item else item["_id"]

    def _set_entity_mediafile_and_thumbnail(self, entity):
        mediafiles = self.storage.get_collection_item_mediafiles(
            "entities", self._get_raw_id(entity)
        )
        for mediafile in mediafiles:
            if "is_primary" in mediafile and mediafile["is_primary"] is True:
                entity["primary_mediafile_location"] = mediafile[
                    "original_file_location"
                ]
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
            collection, self._get_raw_id(entity)
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
        for metadata in mediafile["metadata"]:
            if metadata["key"] == "publication_status":
                return metadata["value"] == "publiek"
        return False

    def _get_mediafile_access(self, mediafile):
        if "metadata" not in mediafile or not self._mediafile_is_public(mediafile):
            return "none"
        for metadata in mediafile["metadata"]:
            if metadata["key"] == "rights":
                if metadata["value"].lower() == "in copyright":
                    return "limited"
                else:
                    return "full"
        return "none"
