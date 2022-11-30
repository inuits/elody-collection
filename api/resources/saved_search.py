import app

from datetime import datetime
from flask import request
from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource
from util import NonUniqueException
from validator import saved_search_schema


class SavedSearch(BaseResource):
    @app.require_oauth(permissions=["read-saved-search", "read-saved-search-all"])
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        filters = {}
        fields = {"type": "saved_search"}
        if int(request.args.get("only_own", 0)) or self._only_own_items(
            ["read-saved-search-all"]
        ):
            filters["user_or_public"] = dict(current_token).get(
                "email", "default_uploader"
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

    @app.require_oauth("create-saved-search")
    def post(self):
        content = self._get_request_body()
        self._abort_if_not_valid_json("Saved search", content, saved_search_schema)
        content["user"] = dict(current_token).get("email", "default_uploader")
        content["date_created"] = str(datetime.now())
        content["version"] = 1
        try:
            saved_search = self.storage.save_item_to_collection("abstracts", content)
        except NonUniqueException as ex:
            return str(ex), 409
        return saved_search, 201


class SavedSearchDetail(BaseResource):
    @app.require_oauth(
        permissions=["read-saved-search-detail", "read-saved-search-detail-all"]
    )
    def get(self, id):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        if self._only_own_items(["read-saved-search-detail-all"]):
            self._abort_if_no_access(saved_search, current_token)
        return saved_search

    @app.require_oauth("update-saved-search")
    def put(self, id):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        content = self._get_request_body()
        self._abort_if_not_valid_json("Saved search", content, saved_search_schema)
        if self._only_own_items():
            self._abort_if_no_access(saved_search, current_token)
        content["date_updated"] = str(datetime.now())
        content["version"] = saved_search.get("version", 0) + 1
        try:
            saved_search = self.storage.update_item_from_collection(
                "abstracts", self._get_raw_id(saved_search), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        return saved_search, 201

    @app.require_oauth("patch-saved-search")
    def patch(self, id):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(saved_search, current_token)
        content["date_updated"] = str(datetime.now())
        content["version"] = saved_search.get("version", 0) + 1
        try:
            saved_search = self.storage.patch_item_from_collection(
                "abstracts", self._get_raw_id(saved_search), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        return saved_search, 201

    @app.require_oauth("delete-saved-search")
    def delete(self, id):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        if self._only_own_items():
            self._abort_if_no_access(saved_search, current_token)
        self.storage.delete_item_from_collection(
            "abstracts", self._get_raw_id(saved_search)
        )
        return "", 204
