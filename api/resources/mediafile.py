import app
import util

from datetime import datetime
from flask import request
from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource
from validator import mediafile_schema


class Mediafile(BaseResource):
    @app.require_oauth("read-mediafile")
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        filters = {}
        if ids := request.args.get("ids", None):
            filters["ids"] = ids.split(",")
        if self._only_own_items():
            mediafiles = self.storage.get_items_from_collection(
                "mediafiles",
                skip,
                limit,
                {"user": dict(current_token).get("email", "default_uploader")},
                filters,
            )
        else:
            mediafiles = self.storage.get_items_from_collection(
                "mediafiles", skip, limit, filters=filters
            )
        mediafiles["limit"] = limit
        if skip + limit < mediafiles["count"]:
            mediafiles["next"] = f"/mediafiles?skip={skip + limit}&limit={limit}"
        if skip:
            mediafiles[
                "previous"
            ] = f"/mediafiles?skip={max(0, skip - limit)}&limit={limit}"
        mediafiles["results"] = self._inject_api_urls_into_mediafiles(
            mediafiles["results"]
        )
        return mediafiles

    @app.require_oauth("create-mediafile")
    def post(self):
        content = self._get_request_body()
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        content["user"] = dict(current_token).get("email", "default_uploader")
        content["date_created"] = str(datetime.now())
        content["version"] = content.get("version", 0) + 1
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        return mediafile, 201


class MediafileAssets(BaseResource):
    @app.require_oauth("get-mediafile-assets")
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if self._only_own_items():
            self._abort_if_no_access(mediafile, current_token, "mediafiles")
        entities = []
        for item in self.storage.get_mediafile_linked_entities(mediafile):
            entity = self.storage.get_item_from_collection_by_id(
                "entities", item["entity_id"].removeprefix("entities/")
            )
            entity = self._set_entity_mediafile_and_thumbnail(entity)
            entity = self._add_relations_to_metadata(entity)
            entities.append(self._inject_api_urls_into_entities([entity])[0])
        return entities, 200


class MediafileCopyright(BaseResource):
    @app.require_oauth("get-mediafile-copyright")
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if not self._only_own_items() or self._is_owner_of_item(
            mediafile, current_token
        ):
            return "full", 200
        if not util.mediafile_is_public(mediafile):
            return "none", 200
        for item in [x for x in mediafile["metadata"] if x["key"] == "rights"]:
            if "in copyright" in item["value"].lower():
                return "limited", 200
        return "full", 200


class MediafileDetail(BaseResource):
    @app.require_oauth("read-mediafile")
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if self._only_own_items() and not util.mediafile_is_public(mediafile):
            self._abort_if_no_access(mediafile, current_token, "mediafiles")
        if request.args.get("raw", None):
            return mediafile
        return self._inject_api_urls_into_mediafiles([mediafile])[0]

    @app.require_oauth("update-mediafile")
    def put(self, id):
        old_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        content = self._get_request_body()
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        if self._only_own_items():
            self._abort_if_no_access(old_mediafile, current_token, "mediafiles")
        content["date_updated"] = str(datetime.now())
        content["version"] = content.get("version", 0) + 1
        content["last_editor"] = dict(current_token).get("email", "default_uploader")
        mediafile = self.storage.update_item_from_collection(
            "mediafiles", self._get_raw_id(old_mediafile), content
        )
        self._signal_mediafile_changed(old_mediafile, mediafile)
        return mediafile, 201

    @app.require_oauth("patch-mediafile")
    def patch(self, id):
        old_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(old_mediafile, current_token, "mediafiles")
        content["date_updated"] = str(datetime.now())
        content["version"] = content.get("version", 0) + 1
        content["last_editor"] = dict(current_token).get("email", "default_uploader")
        mediafile = self.storage.patch_item_from_collection(
            "mediafiles", self._get_raw_id(old_mediafile), content
        )
        self._signal_mediafile_changed(old_mediafile, mediafile)
        return mediafile, 201

    @app.require_oauth("delete-mediafile")
    def delete(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if self._only_own_items():
            self._abort_if_no_access(mediafile, current_token, "mediafiles")
        linked_entities = self.storage.get_mediafile_linked_entities(mediafile)
        self.storage.delete_item_from_collection(
            "mediafiles", self._get_raw_id(mediafile)
        )
        self._signal_mediafile_deleted(mediafile, linked_entities)
        return "", 204
