from flask import request
from inuits_policy_based_auth import RequestContext
from policy_factory import authenticate, get_user_context
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
)


class ShareLink(GenericObject):
    @authenticate(RequestContext(request))
    def get(self):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        filters = {}
        fields = {"type": "share_link"}
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        if title := request.args.get("title"):
            filters["title"] = title
        return super().get(
            "entities", skip=skip, limit=limit, fields=fields, filters=filters
        )

    @authenticate(RequestContext(request))
    def post(self):
        content = request.get_json()
        user = get_user_context().email or "default_uploader"
        return super().post("entities", content=content, type="share_link", user=user)


class ShareLinkDetail(GenericObjectDetail):
    @authenticate(RequestContext(request))
    def get(self, id):
        share_link = super().get("entities", id)
        self._abort_if_not_valid_type(share_link, "share_link")
        return share_link

    @authenticate(RequestContext(request))
    def put(self, id):
        content = request.get_json()
        return super().put(
            "entities",
            id,
            type="share_link",
            content=content,
        )

    @authenticate(RequestContext(request))
    def patch(self, id):
        content = request.get_json()
        return super().patch(
            "entities",
            id,
            type="share_link",
            content=content,
        )

    @authenticate(RequestContext(request))
    def delete(self, id):
        return super().delete("entities", id)
