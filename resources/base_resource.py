from flask_restful import Resource, abort
from flask import request
from storage.arangostore import ArangoStorageManager
from storage.mongostore import MongoStorageManager
from storage.storagemanager import StorageManager
import os

import app


class BaseResource(Resource):
    token_required = os.getenv("REQUIRE_TOKEN", "True").lower() in ["true", "1"]

    def __init__(self):
        self.storage = StorageManager().get_db_engine()

    def get_request_body(self):
        return request.get_json()

    def abort_if_item_doesnt_exist(self, collection, id):
        item = self.storage.get_item_from_collection_by_id(collection, id)
        if item is None:
            abort(
                404,
                message="Item {} doesn't exist in collection {}".format(id, collection),
            )
        return item
