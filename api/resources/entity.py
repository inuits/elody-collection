import mappers

from app import multitenancy_enabled, policy_factory, rabbit
from datetime import datetime, timezone
from elody.exceptions import NonUniqueException
from elody.util import (
    get_raw_id,
    mediafile_is_public,
    signal_entity_changed,
    signal_entity_deleted,
)
from flask import after_this_request, request
from flask_restful import abort
from resources.base_resource import BaseResource
from validator import entity_schema, mediafile_schema


class Entity(BaseResource):
    @policy_factory.authenticate()
    def get(self):
        user = self._get_user()
        accept_header = request.headers.get("Accept")
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        order_by = request.args.get("order_by", None)
        ascending = request.args.get("asc", 1, int)
        skip_relations = request.args.get("skip_relations", 0, int)
        filters = {}
        if item_type := request.args.get("type"):
            filters["type"] = item_type
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        access_restricting_filters = (
            policy_factory.get_user_context().access_restrictions.filters
        )
        if isinstance(access_restricting_filters, dict):
            filters = {**filters, **access_restricting_filters}
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
            user_id = user.get("email", "default_uploader")

        entities = self.storage.get_entities(
            skip,
            limit,
            skip_relations,
            filters,
            order_by,
            ascending,
            tenants_ids,
            user_id,
        )

        type_filter = f"type={item_type}&" if item_type else ""
        entities["limit"] = limit
        if skip + limit < entities["count"]:
            entities[
                "next"
            ] = f"/entities?{type_filter}skip={skip + limit}&limit={limit}&skip_relations={skip_relations}"
        if skip > 0:
            entities[
                "previous"
            ] = f"/entities?{type_filter}skip={max(0, skip - limit)}&limit={limit}&skip_relations={skip_relations}"
        entities["results"] = self._inject_api_urls_into_entities(
            entities["results"], user
        )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                entities,
                accept_header,
                "entities",
                fields,
            ),
            accept_header,
        )

    @policy_factory.authenticate()
    def post(self):
        user = self._get_user()
        content_type = request.content_type
        linked_data_request = self._is_rdf_post_call(content_type)
        create_mediafile = request.args.get(
            "create_mediafile", 0, int
        ) or request.args.get("create_mediafiles", 0, int)
        mediafile_filenames = [
            *request.args.getlist("mediafile_filename"),
            *request.args.getlist("mediafile_filename[]"),
        ]
        if create_mediafile and not mediafile_filenames:
            return "Mediafile can't be created without filename", 400
        if linked_data_request:
            content = self._create_linked_data(request, content_type)
        else:
            content = request.get_json()
        accept_header = request.headers.get("Accept")
        if multitenancy_enabled:
            if not (
                tenants := self._get_tenant(
                    tenant_requested=request.args.get("tenant", None)
                )
            ):
                abort(400, message="Tenant not found")
        entity = self._decorate_entity(content)
        if (
            entity["type"] == "tenant" or entity["type"] == "user"
        ) and not self.is_admin(user):
            abort(403, message="Non admin user can not create users or tenants")
        if not entity["type"] == "tenant" or not entity["type"] == "user":
            entity["date_created"] = datetime.now(timezone.utc).isoformat()
            entity["version"] = 1
        if not linked_data_request:
            self._abort_if_not_valid_json("Entity", entity, entity_schema)
        try:
            entity = self.storage.save_item_to_collection("entities", entity)
            if accept_header == "text/uri-list":
                response = ""
            else:
                response = entity
        except NonUniqueException as ex:
            return str(ex), 409
        user_relation = self.create_relation_dict(
            key=user["_id"],
            value=user.get("email", "default_uploader"),
            label="user",
            type="hasUser",
        )
        self.storage.add_relations_to_collection_item(
            "entities", entity["_id"], [user_relation]
        )
        if multitenancy_enabled:
            for tenant in tenants:
                self.storage.add_relations_to_collection_item(
                    "entities", entity["_id"], [tenant]
                )
        if create_mediafile:
            for mediafile_filename in mediafile_filenames:
                mediafile = self._create_mediafile_for_entity(
                    entity,
                    mediafile_filename,
                )
                if multitenancy_enabled:
                    for tenant in tenants:
                        self.storage.add_relations_to_collection_item(
                            "mediafiles", mediafile["_id"], [tenant]
                        )
                if accept_header == "text/uri-list":
                    user_id = user.get("email", "default_uploader")
                    ticket_id = self._create_ticket(mediafile_filename, user_id)
                    response += f"{self.storage_api_url}/upload-with-ticket/{mediafile_filename}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}\n"
        signal_entity_changed(rabbit, entity)
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class EntityDetail(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        entity = self._set_entity_mediafile_and_thumbnail(entity)
        if not request.args.get("skip_relations", 0, int):
            entity = self._add_relations_to_metadata(entity)
        accept_header = request.headers.get("Accept")
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                self._inject_api_urls_into_entities([entity], user)[0],
                accept_header,
                "entity",
                fields,
            ),
            accept_header,
        )

    @policy_factory.authenticate()
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        self._abort_if_not_valid_json("Entity", content, entity_schema)
        content["date_updated"] = datetime.now(timezone.utc).isoformat()
        content["version"] = entity.get("version", 0) + 1
        content["last_editor"] = user.get("email", "default_uploader")
        content["date_created"] = entity.get("date_created", content["date_updated"])
        try:
            entity = self.storage.update_item_from_collection(
                "entities", get_raw_id(entity), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        signal_entity_changed(rabbit, entity)
        return entity, 201

    @policy_factory.authenticate()
    def patch(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        content["date_updated"] = datetime.now(timezone.utc).isoformat()
        content["version"] = entity.get("version", 0) + 1
        content["last_editor"] = user.get("email", "default_uploader")
        try:
            entity = self.storage.patch_item_from_collection(
                "entities", get_raw_id(entity), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        signal_entity_changed(rabbit, entity)
        return entity, 201

    @policy_factory.authenticate()
    def delete(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        if request.args.get("delete_mediafiles", 0, int):
            mediafiles = self.storage.get_collection_item_mediafiles(
                "entities", get_raw_id(entity)
            )
            for mediafile in mediafiles:
                self._abort_if_no_access(mediafile, user, collection="mediafiles")
                self.storage.delete_item_from_collection(
                    "mediafiles", get_raw_id(mediafile)
                )
        self.storage.delete_item_from_collection("entities", get_raw_id(entity))
        signal_entity_deleted(rabbit, entity)
        return "", 204


class EntityMediafiles(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        mediafiles = dict()
        mediafiles["count"] = self.storage.get_collection_item_mediafiles_count(
            entity["_id"]
        )
        mediafiles_list = self.storage.get_collection_item_mediafiles(
            "entities", get_raw_id(entity), skip, limit
        )
        mediafiles["results"] = [
            mediafile
            for mediafile in mediafiles_list
            if self._has_access_to_item(mediafile, user, collection="mediafiles")
        ]
        mediafiles["limit"] = limit
        mediafiles["skip"] = skip
        if skip + limit < mediafiles["count"]:
            mediafiles[
                "next"
            ] = f"/entities/{id}/mediafiles?skip={skip + limit}&limit={limit}"
        if skip > 0:
            mediafiles[
                "previous"
            ] = f"/entities/{id}/mediafiles?skip={max(0, skip - limit)}&limit={limit}"
        self._inject_api_urls_into_mediafiles(mediafiles["results"], user)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return mediafiles

    @policy_factory.authenticate()
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        mediafiles = content if isinstance(content, list) else [content]
        accept_header = request.headers.get("Accept")
        if multitenancy_enabled:
            if not (
                tenants := self._get_tenant(
                    tenant_requested=request.args.get("tenant", None)
                )
            ):
                abort(400, message="Tenant not found")
        if accept_header == "text/uri-list":
            response = ""
        else:
            response = list()
        for mediafile in mediafiles:
            self._abort_if_not_valid_json("Mediafile", mediafile, mediafile_schema)
            if any(x in mediafile for x in ["_id", "_key"]):
                mediafile = self._abort_if_item_doesnt_exist(
                    "mediafiles", get_raw_id(mediafile)
                )
                self._abort_if_no_access(mediafile, user, collection="mediafiles")
            mediafile = self.storage.save_item_to_collection("mediafiles", mediafile)
            user_relation = self.create_relation_dict(
                user["_id"], user.get("email", "default_uploader"), "user", "hasUser"
            )
            self.storage.add_relations_to_collection_item(
                "mediafiles", mediafile["_id"], [user_relation]
            )
            mediafile = self.storage.add_mediafile_to_collection_item(
                "entities",
                get_raw_id(entity),
                mediafile["_id"],
                mediafile_is_public(mediafile),
            )
            if multitenancy_enabled:
                for tenant in tenants:
                    self.storage.add_relations_to_collection_item(
                        "mediafiles", mediafile["_id"], [tenant]
                    )
            if accept_header == "text/uri-list":
                response += f'{self.storage_api_url}/upload/{mediafile["filename"]}?id={get_raw_id(mediafile)}\n'
            else:
                response.append(mediafile)
        signal_entity_changed(rabbit, entity)
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class EntityMediafilesCreate(BaseResource):  # TODO add user
    @policy_factory.authenticate()
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        self._abort_if_no_access(entity)
        content = request.get_json()
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        content["original_file_location"] = f'/download/{content["filename"]}'
        content[
            "thumbnail_file_location"
        ] = f'/iiif/3/{content["filename"]}/full/,150/0/default.jpg'
        content["user"] = policy_factory.get_user_context().email or "default_uploader"
        content["date_created"] = datetime.now(timezone.utc).isoformat()
        content["version"] = 1
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        upload_location = f'{self.storage_api_url}/upload/{content["filename"]}?id={get_raw_id(mediafile)}'
        self.storage.add_mediafile_to_collection_item(
            "entities",
            get_raw_id(entity),
            mediafile["_id"],
            mediafile_is_public(mediafile),
        )
        signal_entity_changed(rabbit, entity)

        @after_this_request
        def add_header(response):
            response.headers["Warning"] = "299 - Deprecated API"
            return response

        return upload_location, 201


class EntityMetadata(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        metadata = self.storage.get_collection_item_sub_item(
            "entities", get_raw_id(entity), "metadata"
        )
        accept_header = request.headers.get("Accept")
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                metadata, accept_header, "metadata", fields
            ),
            accept_header,
        )

    @policy_factory.authenticate()
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        metadata = self.storage.add_sub_item_to_collection_item(
            "entities", get_raw_id(entity), "metadata", content
        )
        signal_entity_changed(rabbit, entity)
        return metadata, 201

    @policy_factory.authenticate()
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        metadata = self.storage.update_collection_item_sub_item(
            "entities", get_raw_id(entity), "metadata", content
        )
        signal_entity_changed(rabbit, entity)
        return metadata, 201

    @policy_factory.authenticate()
    def patch(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        metadata = self.storage.patch_collection_item_metadata(
            "entities", get_raw_id(entity), content
        )
        if not metadata:
            abort(400, message=f"Entity with id {id} has no metadata")
        signal_entity_changed(rabbit, entity)
        return metadata, 201


class EntityMetadataKey(BaseResource):
    @policy_factory.authenticate()
    def get(self, id, key):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        return self.storage.get_collection_item_sub_item_key(
            "entities", get_raw_id(entity), "metadata", key
        )

    @policy_factory.authenticate()
    def delete(self, id, key):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        self.storage.delete_collection_item_sub_item_key(
            "entities", get_raw_id(entity), "metadata", key
        )
        signal_entity_changed(rabbit, entity)
        return "", 204


class EntityRelations(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(
            "entities", get_raw_id(entity)
        )

    @policy_factory.authenticate()
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        self.tenat_user_relation_policy_check(content, user)
        relations = self.storage.add_relations_to_collection_item(
            "entities", get_raw_id(entity), content
        )
        signal_entity_changed(rabbit, entity)
        return relations, 201

    @policy_factory.authenticate()
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        self.tenat_user_relation_policy_check(content, user)
        relations = self.storage.update_collection_item_relations(
            "entities", get_raw_id(entity), content
        )
        signal_entity_changed(rabbit, entity)
        return relations, 201

    @policy_factory.authenticate()
    def patch(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        self.tenat_user_relation_policy_check(content, user)
        relations = self.storage.patch_collection_item_relations(
            "entities", get_raw_id(entity), content
        )
        signal_entity_changed(rabbit, entity)
        return relations, 201

    @policy_factory.authenticate()
    def delete(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        content = request.get_json()
        self.tenat_user_relation_policy_check(content, user)
        self.storage.delete_collection_item_relations(
            "entities", get_raw_id(entity), content
        )
        signal_entity_changed(rabbit, entity)
        return "", 204


class EntityRelationsAll(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(
            "entities", get_raw_id(entity), include_sub_relations=True
        )


class EntitySetPrimaryMediafile(BaseResource):
    @policy_factory.authenticate()
    def put(self, id, mediafile_id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        self._abort_if_no_access(mediafile, user, collection="mediafiles")
        if not mediafile_is_public(mediafile):
            abort(400, message=f"Mediafile with id {mediafile_id} is not public")
        self.storage.set_primary_field_collection_item(
            "entities", get_raw_id(entity), mediafile_id, "is_primary"
        )
        signal_entity_changed(rabbit, entity)
        return "", 204


class EntitySetPrimaryThumbnail(BaseResource):
    @policy_factory.authenticate()
    def put(self, id, mediafile_id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        user = self._get_user()
        self._abort_if_no_access(entity, user)
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        self._abort_if_no_access(mediafile, user, collection="mediafiles")
        if not mediafile_is_public(mediafile):
            abort(400, message=f"Mediafile with id {mediafile_id} is not public")
        self.storage.set_primary_field_collection_item(
            "entities", get_raw_id(entity), mediafile_id, "is_primary_thumbnail"
        )
        signal_entity_changed(rabbit, entity)
        return "", 204
