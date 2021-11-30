import app

from flask import request
from resources.base_resource import BaseResource
from validator import mediafile_schema


class Mediafile(BaseResource):
    @app.require_oauth()
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        return mediafile, 201

    @app.require_oauth()
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        mediafiles = self.storage.get_items_from_collection("mediafiles", skip, limit)
        count = mediafiles["count"]
        mediafiles["limit"] = limit
        if skip + limit < count:
            mediafiles["next"] = f"/mediafiles?skip={skip + limit}&limit={limit}"
        if skip:
            skip = max(0, skip - limit)
            mediafiles["previous"] = f"/mediafiles?skip={skip}&limit={limit}"
        mediafiles["results"] = self._inject_api_urls_into_mediafiles(
            mediafiles["results"]
        )
        return mediafiles


class MediafileDetail(BaseResource):
    @app.require_oauth()
    def get(self, id):
        mediafile = self.abort_if_item_doesnt_exist("mediafiles", id)
        if request.args.get("raw", None):
            return mediafile
        return self._inject_api_urls_into_mediafiles([mediafile])[0]

    @app.require_oauth()
    def patch(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        content = self.get_request_body()
        mediafile = self.storage.patch_item_from_collection("mediafiles", id, content)
        return mediafile, 201

    @app.require_oauth()
    def put(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        mediafile = self.storage.update_item_from_collection("mediafiles", id, content)
        return mediafile, 201

    @app.require_oauth()
    def delete(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        self.storage.delete_item_from_collection("mediafiles", id)
        return "", 204
