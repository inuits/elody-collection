from app import policy_factory
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
)


class SavedSearch(GenericObject):
    @policy_factory.authenticate(RequestContext(request))
    def get(self):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        filters = {}
        fields = {"type": "saved_search"}
        if request.args.get("only_own", 0, int):
            filters["user_or_public"] = (
                policy_factory.get_user_context().email or "default_uploader"
            )
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        if title := request.args.get("title"):
            filters["title"] = title
        return super().get(
            "abstracts", skip=skip, limit=limit, fields=fields, filters=filters
        )

    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        content = request.get_json()
        user = policy_factory.get_user_context().email or "default_uploader"
        return super().post(
            "abstracts", content=content, type="saved_search", user=user
        )


class SavedSearchDetail(GenericObjectDetail):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        saved_search = super().get("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        return saved_search

    @policy_factory.authenticate(RequestContext(request))
    def put(self, id):
        content = request.get_json()
        return super().put(
            "abstracts",
            id,
            type="saved_search",
            content=content,
        )

    @policy_factory.authenticate(RequestContext(request))
    def patch(self, id):
        content = request.get_json()
        return super().patch(
            "abstracts",
            id,
            type="saved_search",
            content=content,
        )

    @policy_factory.authenticate(RequestContext(request))
    def delete(self, id):
        return super().delete("abstracts", id)
