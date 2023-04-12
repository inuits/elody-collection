from app import policy_factory
from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint, request
from flask_restful import Api
from inuits_policy_based_auth import RequestContext
from resources.mediafile import (
    Mediafile,
    MediafileAssets,
    MediafileCopyright,
    MediafileDetail,
)

api_bp = Blueprint("mediafile", __name__)
api = Api(api_bp)


class CoghentMediafile(CoghentBaseResource, Mediafile):
    @policy_factory.apply_policies(RequestContext(request, ["read-mediafile"]))
    def get(self):
        return super().get()

    @policy_factory.apply_policies(RequestContext(request, ["create-mediafile"]))
    def post(self):
        return super().post()


class CoghentMediafileAssets(CoghentBaseResource, MediafileAssets):
    @policy_factory.apply_policies(RequestContext(request, ["get-mediafile-assets"]))
    def get(self, id):
        return super().get(id)


class CoghentMediafileCopyright(CoghentBaseResource, MediafileCopyright):
    @policy_factory.apply_policies(RequestContext(request, ["get-mediafile-copyright"]))
    def get(self, id):
        ret_val, ret_code = super().get(id)
        if ret_val == "full":
            return ret_val, ret_code
        item = self.storage.get_item_from_collection_by_id("mediafiles", id)
        if self._has_access_to_item(item, "mediafiles"):
            return "full", 200
        return ret_val, ret_code


class CoghentMediafileDetail(CoghentBaseResource, MediafileDetail):
    @policy_factory.apply_policies(RequestContext(request, ["read-mediafile"]))
    def get(self, id):
        return super().get(id)

    @policy_factory.apply_policies(RequestContext(request, ["update-mediafile"]))
    def put(self, id):
        return super().put(id)

    @policy_factory.apply_policies(RequestContext(request, ["patch-mediafile"]))
    def patch(self, id):
        return super().patch(id)

    @policy_factory.apply_policies(RequestContext(request, ["delete-mediafile"]))
    def delete(self, id):
        return super().delete(id)


class MediafilePermissions(CoghentBaseResource):
    @policy_factory.apply_policies(
        RequestContext(request, ["get-mediafile-permissions"])
    )
    def get(self, id):
        return self._get_item_permissions(id, "mediafiles")


api.add_resource(CoghentMediafile, "/mediafiles")
api.add_resource(CoghentMediafileAssets, "/mediafiles/<string:id>/assets")
api.add_resource(CoghentMediafileCopyright, "/mediafiles/<string:id>/copyright")
api.add_resource(CoghentMediafileDetail, "/mediafiles/<string:id>")
api.add_resource(MediafilePermissions, "/mediafiles/<string:id>/permissions")
