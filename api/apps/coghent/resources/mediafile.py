import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import Api
from resources.mediafile import MediafileAssets, MediafileCopyright, MediafileDetail

api_bp = Blueprint("mediafile", __name__)
api = Api(api_bp)


class CoghentMediafileAssets(CoghentBaseResource, MediafileAssets):
    pass


class CoghentMediafileCopyright(CoghentBaseResource, MediafileCopyright):
    def get(self, id):
        ret_val, ret_code = super().get(id)
        if ret_val == "full":
            return ret_val, ret_code
        item = self.storage.get_item_from_collection_by_id("mediafiles", id)
        if self._has_access_to_item(item, "mediafiles"):
            return "full", 200
        return ret_val, ret_code


class CoghentMediafileDetail(CoghentBaseResource, MediafileDetail):
    pass


class MediafilePermissions(CoghentBaseResource):
    @app.require_oauth("get-mediafile-permissions")
    def get(self, id):
        return self._get_item_permissions(id, "mediafiles")


api.add_resource(CoghentMediafileAssets, "/mediafiles/<string:id>/assets")
api.add_resource(CoghentMediafileCopyright, "/mediafiles/<string:id>/copyright")
api.add_resource(CoghentMediafileDetail, "/mediafiles/<string:id>")
api.add_resource(MediafilePermissions, "/mediafiles/<string:id>/permissions")
