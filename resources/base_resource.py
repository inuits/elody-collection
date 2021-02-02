from flask_restful import Resource, abort
from flask import request
from storage.arangostore import ArangoStorageManager
#from storage.mongostore import MongoStorageManager

import app

class BaseResource(Resource):
    def __init__(self):
        self.storage = ArangoStorageManager()
        #self.storage = MongoStorageManager()

    def get_request_body(self):
        return request.get_json(force=True)

    def abort_if_item_doesnt_exist(self, collection, id):
        item = self.storage.get_item_from_collection_by_id(collection, id)
        if item is None:
            abort(404, message="Item {} doesn't exist in collection {}".format(id, collection))
        return item

    def authorize_request(self):
        token = None
        if 'Authorization' in request.headers and request.headers['Authorization'].startswith('Bearer '):
            token = request.headers['Authorization'].split(None,1)[1].strip()
        validity = app.oidc.validate_token(token)
        if not validity:
            response_body = {'error': 'invalid_token',
                             'error_description': validity}
            abort(404, message=response_body)
