import app

from flask import request
from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource
from validator import mediafile_schema


class Mediafile(BaseResource):
    @app.require_oauth("create-mediafile")
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        content["user"] = "default_uploader"
        if "email" in current_token:
            content["user"] = current_token["email"]
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        return mediafile, 201

    @app.require_oauth("read-mediafile")
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        if self._only_own_items():
            mediafiles = self.storage.get_items_from_collection(
                "entities", skip, limit, {"user": current_token["email"]}
            )
        else:
            mediafiles = self.storage.get_items_from_collection(
                "mediafiles", skip, limit
            )
        count = mediafiles["count"]
        mediafiles["limit"] = limit
        if skip + limit < count:
            mediafiles["next"] = f"/mediafiles?skip={skip + limit}&limit={limit}"
        if skip:
            mediafiles[
                "previous"
            ] = f"/mediafiles?skip={max(0, skip - limit)}&limit={limit}"
        mediafiles["results"] = self._inject_api_urls_into_mediafiles(
            mediafiles["results"]
        )
        return mediafiles


class MediafileDetail(BaseResource):
    @app.require_oauth("read-mediafile")
    def get(self, id):
        mediafile = self.abort_if_item_doesnt_exist("mediafiles", id)
        if self._only_own_items():
            self.abort_if_not_own_item(mediafile, current_token)
        if request.args.get("raw", None):
            return mediafile
        return self._inject_api_urls_into_mediafiles([mediafile])[0]

    @app.require_oauth("patch-mediafile")
    def patch(self, id):
        old_mediafile = self.abort_if_item_doesnt_exist("mediafiles", id)
        content = self.get_request_body()
        if self._only_own_items():
            self.abort_if_not_own_item(old_mediafile, current_token)
        mediafile = self.storage.patch_item_from_collection("mediafiles", id, content)
        self._signal_mediafile_changed(old_mediafile, mediafile)
        return mediafile, 201

    @app.require_oauth("update-mediafile")
    def put(self, id):
        old_mediafile = self.abort_if_item_doesnt_exist("mediafiles", id)
        content = self.get_request_body()
        if self._only_own_items():
            self.abort_if_not_own_item(old_mediafile, current_token)
        self.abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        mediafile = self.storage.update_item_from_collection("mediafiles", id, content)
        self._signal_mediafile_changed(old_mediafile, mediafile)
        return mediafile, 201

    @app.require_oauth("delete-mediafile")
    def delete(self, id):
        mediafile = self.abort_if_item_doesnt_exist("mediafiles", id)
        if self._only_own_items():
            self.abort_if_not_own_item(mediafile, current_token)
        linked_entities = self.storage.get_mediafile_linked_entities(mediafile)
        self.storage.delete_item_from_collection("mediafiles", id)
        self._signal_mediafile_deleted(mediafile, linked_entities)
        return "", 204


class MediafileCopyright(BaseResource):
    @app.require_oauth("get-mediafile-copyright")
    def get(self, id):
        mediafile = self.abort_if_item_doesnt_exist("mediafiles", id)
        return self._get_mediafile_access(mediafile), 200
