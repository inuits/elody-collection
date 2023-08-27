import elody.util as util
import os

from app import multitenancy_enabled, policy_factory, rabbit
from datetime import datetime, timezone
from flask import request
from flask_restful import abort
from resources.base_resource import BaseResource
from validator import mediafile_schema


class Mediafile(BaseResource):
    @policy_factory.authenticate()
    def get(self):
        user = self._get_user()
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        fields = {}
        filters = {}
        tenants_ids = None
        user_id = None
        if multitenancy_enabled:
            if self.is_admin(user):
                if request.args.get("tenant", None):
                    tenants_ids = self._get_tenant(
                        create_tenant=False,
                        tenant_requested=request.args.get("tenant", None),
                    )
            elif not (
                tenants_ids := self._get_tenant(
                    create_tenant=False,
                    tenant_requested=request.args.get("tenant", None),
                )
            ):
                abort(400, message="Tenant not found")
        elif request.args.get("only_own", 1, int) and not self.is_admin(user):
            user_id = user["email"]
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        mediafiles = self.storage.get_items_from_collection(
            "mediafiles",
            skip=skip,
            limit=limit,
            fields=fields,
            filters=filters,
            tenants_ids=tenants_ids,
            user_id=user_id,
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
        user = self._get_user()
        content = request.get_json()
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        if multitenancy_enabled:
            if not (
                tenants := self._get_tenant(
                    tenant_requested=request.args.get("tenant", None)
                )
            ):
                abort(400, message="Tenant not found")
        content["date_created"] = datetime.now(timezone.utc).isoformat()
        content["version"] = 1
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        accept_header = request.headers.get("Accept")
        if accept_header == "text/uri-list":
            user_id = user["email"] or "default_uploader"
            ticket_id = self._create_ticket(mediafile["filename"], user_id)
            response = f"{self.storage_api_url}/upload-with-ticket/{mediafile['filename'].strip()}?id={util.get_raw_id(mediafile)}&ticket_id={ticket_id}"
        else:
            response = mediafile
        user_relation = self.create_relation_dict(
            user["_id"], user["email"], "user", "hasUser"
        )
        self.storage.add_relations_to_collection_item(
            "mediafiles", mediafile["_id"], [user_relation]
        )
        if multitenancy_enabled:
            for tenant in tenants:
                self.storage.add_relations_to_collection_item(
                    "mediafiles", mediafile["_id"], [tenant]
                )
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class MediafileAssets(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        user = self._get_user()
        self._abort_if_no_access(mediafile, user, collection="mediafiles")
        entities = []
        for item in self.storage.get_mediafile_linked_entities(mediafile):
            entity = self.storage.get_item_from_collection_by_id(
                "entities", item["entity_id"].removeprefix("entities/")
            )
            if not self._has_access_to_item(entity, user):
                continue
            entity = self._set_entity_mediafile_and_thumbnail(entity)
            entity = self._add_relations_to_metadata(entity)
            entities.append(entity)
        return self._inject_api_urls_into_entities(entities)


class MediafileCopyright(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        user = self._get_user()
        if self._has_access_to_item(mediafile, user, collection="mediafiles"):
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
        user = self._get_user()
        self._abort_if_no_access(mediafile, user, collection="mediafiles")
        if request.args.get("raw", 0, int):
            return mediafile
        return self._inject_api_urls_into_mediafiles([mediafile])[0]

    @policy_factory.authenticate()
    def put(self, id):
        old_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        user = self._get_user()
        self._abort_if_no_access(old_mediafile, user, collection="mediafiles")
        content = request.get_json()
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        content["date_updated"] = datetime.now(timezone.utc).isoformat()
        content["version"] = old_mediafile.get("version", 0) + 1
        content["last_editor"] = user["email"] or "default_uploader"
        mediafile = self.storage.update_item_from_collection(
            "mediafiles", util.get_raw_id(old_mediafile), content
        )
        util.signal_mediafile_changed(rabbit, old_mediafile, mediafile)
        return mediafile, 201

    @policy_factory.authenticate()
    def patch(self, id):
        user = self._get_user()
        old_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        self._abort_if_no_access(old_mediafile, user, collection="mediafiles")
        content = request.get_json()
        content["date_updated"] = datetime.now(timezone.utc).isoformat()
        content["version"] = old_mediafile.get("version", 0) + 1
        content["last_editor"] = user["email"] or "default_uploader"
        mediafile = self.storage.patch_item_from_collection(
            "mediafiles", util.get_raw_id(old_mediafile), content
        )
        util.signal_mediafile_changed(rabbit, old_mediafile, mediafile)
        return mediafile, 201

    @policy_factory.authenticate()
    def delete(self, id):
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        user = self._get_user()
        self._abort_if_no_access(mediafile, user, collection="mediafiles")
        linked_entities = self.storage.get_mediafile_linked_entities(mediafile)
        self.storage.delete_item_from_collection(
            "mediafiles", util.get_raw_id(mediafile)
        )
        util.signal_mediafile_deleted(rabbit, mediafile, linked_entities)
        return "", 204
