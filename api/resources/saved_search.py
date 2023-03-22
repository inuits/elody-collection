import util

from app import policy_factory
from datetime import datetime
from flask import request
from resources.base_resource import BaseResource
from validator import saved_search_schema


class SavedSearch(BaseResource):
    @policy_factory.authenticate()
    def get(self):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        filters = {}
        fields = {"type": "saved_search"}
        if request.args.get("only_own", 0, int) or self._only_own_items(
            ["read-saved-search-all"]
        ):
            filters["user_or_public"] = (
                policy_factory.get_user_context().email or "default_uploader"
            )
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        if title := request.args.get("title"):
            filters["title"] = title
        saved_searches = self.storage.get_items_from_collection(
            "abstracts", skip, limit, fields, filters
        )
        saved_searches["limit"] = limit
        if skip + limit < saved_searches["count"]:
            saved_searches[
                "next"
            ] = f"/saved_searches?skip={skip + limit}&limit={limit}"
        if skip > 0:
            saved_searches[
                "previous"
            ] = f"/saved_searches?skip={max(0, skip - limit)}&limit={limit}"
        return saved_searches

    @policy_factory.authenticate()
    def post(self):
        content = request.get_json()
        self._abort_if_not_valid_json("Saved search", content, saved_search_schema)
        content["user"] = policy_factory.get_user_context().email or "default_uploader"
        content["date_created"] = str(datetime.now())
        content["version"] = 1
        try:
            saved_search = self.storage.save_item_to_collection("abstracts", content)
        except util.NonUniqueException as ex:
            return str(ex), 409
        return saved_search, 201


class SavedSearchDetail(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        if self._only_own_items(["read-saved-search-detail-all"]):
            self._abort_if_no_access(
                saved_search,
                policy_factory.get_user_context().auth_objects.get("token"),
            )
        return saved_search

    @policy_factory.authenticate()
    def put(self, id):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        content = request.get_json()
        self._abort_if_not_valid_json("Saved search", content, saved_search_schema)
        if self._only_own_items():
            self._abort_if_no_access(
                saved_search,
                policy_factory.get_user_context().auth_objects.get("token"),
            )
        content["date_updated"] = str(datetime.now())
        content["version"] = saved_search.get("version", 0) + 1
        try:
            saved_search = self.storage.update_item_from_collection(
                "abstracts", util.get_raw_id(saved_search), content
            )
        except util.NonUniqueException as ex:
            return str(ex), 409
        return saved_search, 201

    @policy_factory.authenticate()
    def patch(self, id):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        content = request.get_json()
        if self._only_own_items():
            self._abort_if_no_access(
                saved_search,
                policy_factory.get_user_context().auth_objects.get("token"),
            )
        content["date_updated"] = str(datetime.now())
        content["version"] = saved_search.get("version", 0) + 1
        try:
            saved_search = self.storage.patch_item_from_collection(
                "abstracts", util.get_raw_id(saved_search), content
            )
        except util.NonUniqueException as ex:
            return str(ex), 409
        return saved_search, 201

    @policy_factory.authenticate()
    def delete(self, id):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        if self._only_own_items():
            self._abort_if_no_access(
                saved_search,
                policy_factory.get_user_context().auth_objects.get("token"),
            )
        self.storage.delete_item_from_collection(
            "abstracts", util.get_raw_id(saved_search)
        )
        return "", 204
