from app import policy_factory, rabbit
from datetime import datetime, timezone
from elody.util import (
    get_raw_id,
    mediafile_is_public,
    signal_mediafile_changed,
    signal_mediafile_deleted,
)
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource
from validator import mediafile_schema


class Mediafile(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        fields = {}
        filters = {}
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        access_restricting_filters = (
            policy_factory.get_user_context().access_restrictions.filters
        )
        if isinstance(access_restricting_filters, dict):
            filters = {**filters, **access_restricting_filters}
        mediafiles = self.storage.get_items_from_collection(
            "mediafiles",
            skip=skip,
            limit=limit,
            fields=fields,
            filters=filters,
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

    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        content = self._get_content_according_content_type(request, "mediafile")
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        content["date_created"] = datetime.now(timezone.utc)
        content["version"] = 1
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        accept_header = request.headers.get("Accept")
        if accept_header == "text/uri-list":
            ticket_id = self._create_ticket(mediafile["filename"])
            response = f"{self.storage_api_url}/upload-with-ticket/{mediafile['filename'].strip()}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}"
        else:
            response = mediafile
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class MediafileAssets(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        entities = []
        for item in self.storage.get_mediafile_linked_entities(mediafile):
            entity = self.storage.get_item_from_collection_by_id(
                "entities", item["entity_id"].removeprefix("entities/")
            )
            entity = self._set_entity_mediafile_and_thumbnail(entity)
            entity = self._add_relations_to_metadata(entity)
            entities.append(entity)
        return self._inject_api_urls_into_entities(entities)


class MediafileCopyright(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if not mediafile_is_public(mediafile):
            return "none", 200
        for item in [x for x in mediafile["metadata"] if x["key"] == "rights"]:
            if "in copyright" in item["value"].lower():
                return "limited", 200
        return "full", 200


class MediafileDetail(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        if request.args.get("raw", 0, int):
            return mediafile
        return self._inject_api_urls_into_mediafiles([mediafile])[0]

    @policy_factory.authenticate(RequestContext(request))
    def put(self, id):
        old_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        content = self._get_content_according_content_type(request, "mediafile")
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        content["date_updated"] = datetime.now(timezone.utc)
        content["version"] = old_mediafile.get("version", 0) + 1
        content["last_editor"] = (
            policy_factory.get_user_context().email or "default_uploader"
        )
        mediafile = self.storage.update_item_from_collection(
            "mediafiles", get_raw_id(old_mediafile), content
        )
        signal_mediafile_changed(rabbit, old_mediafile, mediafile)
        return mediafile, 201

    @policy_factory.authenticate(RequestContext(request))
    def patch(self, id):
        old_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        content = self._get_content_according_content_type(request, "mediafile")
        content["date_updated"] = datetime.now(timezone.utc)
        content["version"] = old_mediafile.get("version", 0) + 1
        content["last_editor"] = (
            policy_factory.get_user_context().email or "default_uploader"
        )
        mediafile = self.storage.patch_item_from_collection(
            "mediafiles", get_raw_id(old_mediafile), content
        )
        signal_mediafile_changed(rabbit, old_mediafile, mediafile)
        return mediafile, 201

    @policy_factory.authenticate(RequestContext(request))
    def delete(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        linked_entities = self.storage.get_mediafile_linked_entities(mediafile)
        self.storage.delete_item_from_collection("mediafiles", get_raw_id(mediafile))
        signal_mediafile_deleted(rabbit, mediafile, linked_entities)
        return "", 204
