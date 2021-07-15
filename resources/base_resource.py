import os

from flask_restful import Resource, abort
from flask import request
from storage.storagemanager import StorageManager
from werkzeug.exceptions import BadRequest


class BaseResource(Resource):
    token_required = os.getenv("REQUIRE_TOKEN", "True").lower() in ["true", "1"]

    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        self.storage_api_url = os.getenv("STORAGE_API_URL", "http://localhost:8001")

    def get_request_body(self):
        invalid_input = False
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
                message="Item {} doesn't exist in collection {}".format(id, collection),
            )
        return item
