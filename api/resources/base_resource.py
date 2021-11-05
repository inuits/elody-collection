import app
import json
import os
import uuid

from cloudevents.http import CloudEvent, to_json
from flask import request
from flask_restful import Resource, abort, reqparse
from storage.storagemanager import StorageManager
from validator import validate_json
from werkzeug.exceptions import BadRequest


class BaseResource(Resource):
    token_required = os.getenv("REQUIRE_TOKEN", "True").lower() in ["true", "1"]

    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        self.collection_api_url = os.getenv(
            "COLLECTION_API_URL", "http://localhost:8000"
        )
        self.storage_api_url = os.getenv("STORAGE_API_URL", "http://localhost:8001")
        self.cantaloupe_api_url = os.getenv(
            "CANTALOUPE_API_URL", "http://localhost:8182"
        )
        self.elastic_url = os.getenv("ELASTIC_URL", "es")
        self.upload_source = os.getenv("UPLOAD_SOURCE", "/mnt/media-import")
        self.req = reqparse.RequestParser()

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
                entity["primary_mediafile_location"] = (
                    self.storage_api_url + entity["primary_mediafile_location"]
                )
            if "primary_thumbnail_location" in entity:
                entity["primary_thumbnail_location"] = (
                    self.cantaloupe_api_url + entity["primary_thumbnail_location"]
                )
        return entities

    def _inject_api_urls_into_mediafiles(self, mediafiles):
        for mediafile in mediafiles:
            if "original_file_location" in mediafile:
                mediafile["original_file_location"] = (
                    self.storage_api_url + mediafile["original_file_location"]
                )
            if "thumbnail_file_location" in mediafile:
                mediafile["thumbnail_file_location"] = (
                    self.cantaloupe_api_url + mediafile["thumbnail_file_location"]
                )
        return mediafiles

    def _index_entity(self, entity_id):
        message_id = str(uuid.uuid4())
        attributes = {
            "id": message_id,
            "message_id": message_id,
            "type": "dams.index_start",
            "source": "dams",
        }
        data = {
            "elastic_url": self.elastic_url,
            "entities_url": "{}/entities/{}".format(self.collection_api_url, entity_id),
        }
        event = CloudEvent(attributes, data)
        message = json.loads(to_json(event))
        app.ramq.send(message, routing_key="dams.index_start", exchange_name="dams")
        return message

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

    def _add_relations_to_metadata(self, entity):
        relations = self.storage.get_collection_item_relations(
            "entities", self._get_raw_id(entity)
        )
        if relations:
            if "metadata" in entity:
                entity["metadata"].append({"relations": relations})
            else:
                entity["metadata"] = [{"relations": relations}]
        return entity
