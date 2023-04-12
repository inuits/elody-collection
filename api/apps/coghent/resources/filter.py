from app import policy_factory
from apps.coghent.resources.base_filter_resource import CoghentBaseFilterResource
from flask import Blueprint, request
from flask_restful import Api
from inuits_policy_based_auth import RequestContext
from resources.filter import (
    FilterEntities,
    FilterEntitiesBySavedSearchId,
    FilterMediafiles,
    FilterMediafilesBySavedSearchId,
)

api_bp = Blueprint("filter", __name__)
api = Api(api_bp)


class CoghentFilterEntities(CoghentBaseFilterResource, FilterEntities):
    @policy_factory.apply_policies(RequestContext(request, ["search-advanced"]))
    def post(self):
        return super().post()


class CoghentFilterEntitiesBySavedSearchId(
    CoghentBaseFilterResource, FilterEntitiesBySavedSearchId
):
    @policy_factory.apply_policies(RequestContext(request, ["search-advanced"]))
    def post(self, id):
        return super().post(id)


class CoghentFilterMediafiles(CoghentBaseFilterResource, FilterMediafiles):
    @policy_factory.apply_policies(RequestContext(request, ["search-advanced"]))
    def post(self):
        super().post()


class CoghentFilterMediafilesBySavedSearchId(
    CoghentBaseFilterResource, FilterMediafilesBySavedSearchId
):
    @policy_factory.apply_policies(RequestContext(request, ["search-advanced"]))
    def post(self, id):
        super().post(id)


api.add_resource(CoghentFilterEntities, "/entities/filter")
api.add_resource(CoghentFilterEntitiesBySavedSearchId, "/entities/filter/<string:id>")
api.add_resource(CoghentFilterMediafiles, "/mediafiles/filter")
api.add_resource(
    CoghentFilterMediafilesBySavedSearchId, "/mediafiles/filter/<string:id>"
)
