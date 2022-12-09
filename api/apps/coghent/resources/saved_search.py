import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import Api
from resources.saved_search import SavedSearch, SavedSearchDetail

api_bp = Blueprint("saved_search", __name__)
api = Api(api_bp)


class CoghentSavedSearch(CoghentBaseResource, SavedSearch):
    @app.require_oauth(permissions=["read-saved-search", "read-saved-search-all"])
    def get(self):
        return super().get()

    @app.require_oauth("create-saved-search")
    def post(self):
        return super().post()


class CoghentSavedSearchDetail(CoghentBaseResource, SavedSearchDetail):
    @app.require_oauth(permissions=["read-saved-search-detail", "read-saved-search-detail-all"])
    def get(self, id):
        return super().get(id)

    @app.require_oauth("update-saved-search")
    def put(self, id):
        return super().put(id)

    @app.require_oauth("patch-saved-search")
    def patch(self, id):
        return super().patch(id)

    @app.require_oauth("delete-saved-search")
    def delete(self, id):
        return super().delete(id)


api.add_resource(CoghentSavedSearch, "/saved_searches")
api.add_resource(CoghentSavedSearchDetail, "/saved_searches/<string:id>")
