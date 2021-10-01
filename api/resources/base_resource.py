import os

from flask import request
from flask_restful import Resource, abort, reqparse
from storage.storagemanager import StorageManager
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

    def abort_if_not_valid_json(self, validator, type, json):
        if not validator.validate(json):
            abort(400, message="{} doesn't have a valid format".format(type))

    def _inject_api_urls(self, mediafiles):
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
