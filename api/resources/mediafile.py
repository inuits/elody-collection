import util

from app import policy_factory
from datetime import datetime
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource
from validator import mediafile_schema


class Mediafile(BaseResource):
    @policy_factory.authenticate()
    def get(self):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        filters = {}
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        if self._only_own_items():
            mediafiles = self.storage.get_items_from_collection(
                "mediafiles",
                skip,
                limit,
                {"user": policy_factory.get_user_context().email or "default_uploader"},
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

    @policy_factory.authenticate()
    def post(self):
        content = request.get_json()
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)

        content["user"] = policy_factory.get_user_context().email or "default_uploader"
        content["date_created"] = str(datetime.now())
        content["version"] = 1
        mediafile = self.storage.save_item_to_collection("mediafiles", content)

        accept_header = request.headers.get("Accept")
        if accept_header == "text/uri-list":
            response = f"{self.storage_api_url}/upload/{mediafile['filename'].strip()}?id={util.get_raw_id(mediafile)}"
        else:
            response = mediafile

        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class MediafileAssets(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if self._only_own_items():
            self._abort_if_no_access(
                mediafile,
                policy_factory.get_user_context().auth_objects[0],
                "mediafiles",
            )
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
    @policy_factory.apply_policies(RequestContext(request, ["get-mediafile-copyright"]))
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if not self._only_own_items() or self._is_owner_of_item(
            mediafile, policy_factory.get_user_context().auth_objects[0]
        ):
            return "full", 200
        if not util.mediafile_is_public(mediafile):
            return "none", 200
        for item in [x for x in mediafile["metadata"] if x["key"] == "rights"]:
            if "in copyright" in item["value"].lower():
                return "limited", 200
        return "full", 200


class MediafileDetail(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if self._only_own_items() and not util.mediafile_is_public(mediafile):
            self._abort_if_no_access(
                mediafile,
                policy_factory.get_user_context().auth_objects[0],
                "mediafiles",
            )
        if request.args.get("raw"):
            return mediafile
        return self._inject_api_urls_into_mediafiles([mediafile])[0]

    @policy_factory.authenticate()
    def put(self, id):
        user_context = policy_factory.get_user_context()
        old_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        content = request.get_json()
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        if self._only_own_items():
            self._abort_if_no_access(
                old_mediafile, user_context.auth_objects[0], "mediafiles"
            )
        content["date_updated"] = str(datetime.now())
        content["version"] = old_mediafile.get("version", 0) + 1
        content["last_editor"] = user_context.email or "default_uploader"
        mediafile = self.storage.update_item_from_collection(
            "mediafiles", util.get_raw_id(old_mediafile), content
        )
        util.signal_mediafile_changed(old_mediafile, mediafile)
        return mediafile, 201

    @policy_factory.authenticate()
    def patch(self, id):
        user_context = policy_factory.get_user_context()
        old_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        content = request.get_json()
        if self._only_own_items():
            self._abort_if_no_access(
                old_mediafile, user_context.auth_objects[0], "mediafiles"
            )
        content["date_updated"] = str(datetime.now())
        content["version"] = old_mediafile.get("version", 0) + 1
        content["last_editor"] = user_context.email or "default_uploader"
        mediafile = self.storage.patch_item_from_collection(
            "mediafiles", util.get_raw_id(old_mediafile), content
        )
        util.signal_mediafile_changed(old_mediafile, mediafile)
        return mediafile, 201

    @policy_factory.authenticate()
    def delete(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if self._only_own_items():
            self._abort_if_no_access(
                mediafile,
                policy_factory.get_user_context().auth_objects[0],
                "mediafiles",
            )
        linked_entities = self.storage.get_mediafile_linked_entities(mediafile)
        self.storage.delete_item_from_collection(
            "mediafiles", util.get_raw_id(mediafile)
        )
        util.signal_mediafile_deleted(mediafile, linked_entities)
        return "", 204
