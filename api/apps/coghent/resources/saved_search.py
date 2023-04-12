from app import policy_factory
from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint, request
from flask_restful import Api
from inuits_policy_based_auth import RequestContext
from resources.saved_search import SavedSearch, SavedSearchDetail

api_bp = Blueprint("saved_search", __name__)
api = Api(api_bp)


class CoghentSavedSearch(CoghentBaseResource, SavedSearch):
    @policy_factory.apply_policies(
        RequestContext(request, ["read-saved-search", "read-saved-search-all"])
    )
    def get(self):
        return super().get()

    @policy_factory.apply_policies(RequestContext(request, ["create-saved-search"]))
    def post(self):
        return super().post()


class CoghentSavedSearchDetail(CoghentBaseResource, SavedSearchDetail):
    @policy_factory.apply_policies(
        RequestContext(
            request, ["read-saved-search-detail", "read-saved-search-detail-all"]
        )
    )
    def get(self, id):
        return super().get(id)

    @policy_factory.apply_policies(RequestContext(request, ["update-saved-search"]))
    def put(self, id):
        return super().put(id)

    @policy_factory.apply_policies(RequestContext(request, ["patch-saved-search"]))
    def patch(self, id):
        return super().patch(id)

    @policy_factory.apply_policies(RequestContext(request, ["delete-saved-search"]))
    def delete(self, id):
        return super().delete(id)


api.add_resource(CoghentSavedSearch, "/saved_searches")
api.add_resource(CoghentSavedSearchDetail, "/saved_searches/<string:id>")
