import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import Api

api_bp = Blueprint("entity", __name__)
api = Api(api_bp)


class EntitySixthCollectionId(CoghentBaseResource):
    @app.require_oauth("get-entity-sixth-collection-id")
    def get(self):
        return self.storage.get_sixth_collection_id()


api.add_resource(EntitySixthCollectionId, "/entities/sixthcollection/id")
