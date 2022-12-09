import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import Api
from resources.mediafile import (
    Mediafile,
    MediafileAssets,
    MediafileCopyright,
    MediafileDetail,
)

api_bp = Blueprint("mediafile", __name__)
api = Api(api_bp)


class CoghentMediafile(CoghentBaseResource, Mediafile):
    @app.require_oauth("read-mediafile")
    def get(self):
        return super().get()

    @app.require_oauth("create-mediafile")
    def post(self):
        return super().post()


class CoghentMediafileAssets(CoghentBaseResource, MediafileAssets):
    @app.require_oauth("get-mediafile-assets")
    def get(self, id):
        return super().get(id)


class CoghentMediafileCopyright(CoghentBaseResource, MediafileCopyright):
    @app.require_oauth("get-mediafile-copyright")
    def get(self, id):
        ret_val, ret_code = super().get(id)
        if ret_val == "full":
            return ret_val, ret_code
        item = self.storage.get_item_from_collection_by_id("mediafiles", id)
        if self._has_access_to_item(item, "mediafiles"):
            return "full", 200
        return ret_val, ret_code


class CoghentMediafileDetail(CoghentBaseResource, MediafileDetail):
    @app.require_oauth("read-mediafile")
    def get(self, id):
        return super().get(id)

    @app.require_oauth("update-mediafile")
    def put(self, id):
        return super().put(id)

    @app.require_oauth("patch-mediafile")
    def patch(self, id):
        return super().patch(id)

    @app.require_oauth("delete-mediafile")
    def delete(self, id):
        return super().delete(id)


class MediafilePermissions(CoghentBaseResource):
    @app.require_oauth("get-mediafile-permissions")
    def get(self, id):
        return self._get_item_permissions(id, "mediafiles")


api.add_resource(CoghentMediafile, "/mediafiles")
api.add_resource(CoghentMediafileAssets, "/mediafiles/<string:id>/assets")
api.add_resource(CoghentMediafileCopyright, "/mediafiles/<string:id>/copyright")
api.add_resource(CoghentMediafileDetail, "/mediafiles/<string:id>")
api.add_resource(MediafilePermissions, "/mediafiles/<string:id>/permissions")
