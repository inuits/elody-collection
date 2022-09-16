from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import Api
from resources.mediafile import MediafileAssets, MediafileDetail

api_bp = Blueprint("mediafile", __name__)
api = Api(api_bp)


class CoghentMediafileAssets(CoghentBaseResource, MediafileAssets):
    pass


class CoghentMediafileDetail(CoghentBaseResource, MediafileDetail):
    pass


api.add_resource(CoghentMediafileAssets, "/mediafiles/<string:id>/assets")
api.add_resource(CoghentMediafileDetail, "/mediafiles/<string:id>")
