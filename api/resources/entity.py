import app
import mappers
import util

from datetime import datetime
from flask import after_this_request, request
from flask_restful import abort
from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource
from validator import entity_schema, mediafile_schema


class Entity(BaseResource):
    @app.require_oauth()
    def get(self):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        filters = {}
        if request.args.get("only_own", 0, int) or self._only_own_items(
            ["read-entity-all"]
        ):
            filters["user"] = dict(current_token).get("email", "default_uploader")
        if item_type := request.args.get("type"):
            filters["type"] = item_type
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        skip_relations = request.args.get("skip_relations", 0, int)
        type_filter = f"type={item_type}&" if item_type else ""
        entities = self.storage.get_entities(skip, limit, skip_relations, filters)
        entities["limit"] = limit
        if skip + limit < entities["count"]:
            entities[
                "next"
            ] = f"/entities?{type_filter}skip={skip + limit}&limit={limit}&skip_relations={skip_relations}"
        if skip > 0:
            entities[
                "previous"
            ] = f"/entities?{type_filter}skip={max(0, skip - limit)}&limit={limit}&skip_relations={skip_relations}"
        entities["results"] = self._inject_api_urls_into_entities(entities["results"])
        return entities

    @app.require_oauth()
    def post(self):
        create_mediafile = request.args.get(
            "create_mediafile", 0, int
        ) or request.args.get("create_mediafiles", 0, int)
        mediafile_filenames = [
            *request.args.getlist("mediafile_filename"),
            *request.args.getlist("mediafile_filename[]"),
        ]
        accept_header = request.headers.get("Accept")
        if create_mediafile and not mediafile_filenames:
            return "Mediafile can't be created without filename", 400
        content = self._get_request_body()
        user_id = dict(current_token).get("email", "default_uploader")
        entity = self._decorate_entity(content, user_id)
        content["date_created"] = str(datetime.now())
        content["version"] = 1
        self._abort_if_not_valid_json("Entity", entity, entity_schema)
        try:
            entity = self.storage.save_item_to_collection("entities", content)
            if accept_header == "text/uri-list":
                response = ""
            else:
                response = entity
        except util.NonUniqueException as ex:
            return str(ex), 409
        if create_mediafile:
            for mediafile_filename in mediafile_filenames:
                mediafile = self._create_mediafile_for_entity(
                    user_id, entity, mediafile_filename
                )
                if accept_header == "text/uri-list":
                    response += f"{self.storage_api_url}/upload/{mediafile_filename}?id={util.get_raw_id(mediafile)}\n"
        util.signal_entity_changed(entity)
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class EntityDetail(BaseResource):
    @app.require_oauth()
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-detail-all"]):
            self._abort_if_no_access(entity, current_token)
        entity = self._set_entity_mediafile_and_thumbnail(entity)
        if not request.args.get("skip_relations", 0, int):
            entity = self._add_relations_to_metadata(entity)
        return self._inject_api_urls_into_entities([entity])[0]

    @app.require_oauth()
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        self._abort_if_not_valid_json("Entity", content, entity_schema)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        content["date_updated"] = str(datetime.now())
        content["version"] = content.get("version", 0) + 1
        content["last_editor"] = dict(current_token).get("email", "default_uploader")
        try:
            entity = self.storage.update_item_from_collection(
                "entities", util.get_raw_id(entity), content
            )
        except util.NonUniqueException as ex:
            return str(ex), 409
        util.signal_entity_changed(entity)
        return entity, 201

    @app.require_oauth()
    def patch(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        content["date_updated"] = str(datetime.now())
        content["version"] = content.get("version", 0) + 1
        content["last_editor"] = dict(current_token).get("email", "default_uploader")
        try:
            entity = self.storage.patch_item_from_collection(
                "entities", util.get_raw_id(entity), content
            )
        except util.NonUniqueException as ex:
            return str(ex), 409
        util.signal_entity_changed(entity)
        return entity, 201

    @app.require_oauth()
    def delete(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        self.storage.delete_item_from_collection("entities", util.get_raw_id(entity))
        util.signal_entity_deleted(entity)
        return "", 204


class EntityMediafiles(BaseResource):
    @app.require_oauth()
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-mediafiles-all"]):
            self._abort_if_no_access(entity, current_token)
        mediafiles = self.storage.get_collection_item_mediafiles(
            "entities", util.get_raw_id(entity)
        )
        if not request.args.get("non_public"):
            mediafiles = [
                mediafile
                for mediafile in mediafiles
                if util.mediafile_is_public(mediafile)
            ]

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self._inject_api_urls_into_mediafiles(mediafiles)

    @app.require_oauth()
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        mediafiles = content if isinstance(content, list) else [content]
        accept_header = request.headers.get("Accept")
        if accept_header == "text/uri-list":
            response = ""
        else:
            response = list()
        for mediafile in mediafiles:
            self._abort_if_not_valid_json("Mediafile", mediafile, mediafile_schema)
            if mediafile.get("_id") or mediafile.get("_key"):
                mediafile = self._abort_if_item_doesnt_exist(
                    "mediafiles", util.get_raw_id(mediafile)
                )
            else:
                mediafile = self.storage.save_item_to_collection(
                    "mediafiles", mediafile
                )
            if self._only_own_items():
                self._abort_if_no_access(entity, current_token)
                self._abort_if_no_access(mediafile, current_token, "mediafiles")
            mediafile = self.storage.add_mediafile_to_collection_item(
                "entities",
                util.get_raw_id(entity),
                mediafile["_id"],
                util.mediafile_is_public(mediafile),
            )
            if accept_header == "text/uri-list":
                response += f'{self.storage_api_url}/upload/{mediafile["filename"]}?id={util.get_raw_id(mediafile)}\n'
            else:
                response.append(mediafile)
        util.signal_entity_changed(entity)
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class EntityMediafilesCreate(BaseResource):
    @app.require_oauth()
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        content["original_file_location"] = f'/download/{content["filename"]}'
        content[
            "thumbnail_file_location"
        ] = f'/iiif/3/{content["filename"]}/full/,150/0/default.jpg'
        content["user"] = dict(current_token).get("email", "default_uploader")
        content["date_created"] = str(datetime.now())
        content["version"] = 1
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        upload_location = f'{self.storage_api_url}/upload/{content["filename"]}?id={util.get_raw_id(mediafile)}'
        self.storage.add_mediafile_to_collection_item(
            "entities",
            util.get_raw_id(entity),
            mediafile["_id"],
            util.mediafile_is_public(mediafile),
        )
        util.signal_entity_changed(entity)

        @after_this_request
        def add_header(response):
            response.headers["Warning"] = "299 - Deprecated API"
            return response

        return upload_location, 201


class EntityMetadata(BaseResource):
    @app.require_oauth()
    def get(self, id):
        accept_header = request.headers.get("Accept")
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-metadata-all"]):
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.get_collection_item_sub_item(
            "entities", util.get_raw_id(entity), "metadata"
        )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(metadata, accept_header),
            accept_header,
        )

    @app.require_oauth()
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.add_sub_item_to_collection_item(
            "entities", util.get_raw_id(entity), "metadata", content
        )
        util.signal_entity_changed(entity)
        return metadata, 201

    @app.require_oauth("update-entity-metadata")
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.update_collection_item_sub_item(
            "entities", util.get_raw_id(entity), "metadata", content
        )
        util.signal_entity_changed(entity)
        return metadata, 201

    @app.require_oauth()
    def patch(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.patch_collection_item_metadata(
            "entities", util.get_raw_id(entity), content
        )
        if not metadata:
            abort(400, message=f"Entity with id {id} has no metadata")
        util.signal_entity_changed(entity)
        return metadata, 201


class EntityMetadataKey(BaseResource):
    @app.require_oauth()
    def get(self, id, key):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-metadata-key-all"]):
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.get_collection_item_sub_item_key(
            "entities", util.get_raw_id(entity), "metadata", key
        )
        return metadata

    @app.require_oauth()
    def delete(self, id, key):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        self.storage.delete_collection_item_sub_item_key(
            "entities", util.get_raw_id(entity), "metadata", key
        )
        util.signal_entity_changed(entity)
        return "", 204


class EntityRelations(BaseResource):
    @app.require_oauth()
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-relations-all"]):
            self._abort_if_no_access(entity, current_token)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(
            "entities", util.get_raw_id(entity)
        )

    @app.require_oauth()
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        relations = self.storage.add_relations_to_collection_item(
            "entities", util.get_raw_id(entity), content
        )
        util.signal_entity_changed(entity)
        return relations, 201

    @app.require_oauth()
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        relations = self.storage.update_collection_item_relations(
            "entities", util.get_raw_id(entity), content
        )
        util.signal_entity_changed(entity)
        return relations, 201

    @app.require_oauth()
    def patch(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        relations = self.storage.patch_collection_item_relations(
            "entities", util.get_raw_id(entity), content
        )
        util.signal_entity_changed(entity)
        return relations, 201

    @app.require_oauth()
    def delete(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        self.storage.delete_collection_item_relations(
            "entities", util.get_raw_id(entity), content
        )
        util.signal_entity_changed(entity)
        return "", 204


class EntityRelationsAll(BaseResource):
    @app.require_oauth()
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-relations-all"]):
            self._abort_if_no_access(entity, current_token)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(
            "entities", util.get_raw_id(entity), include_sub_relations=True
        )


class EntitySetPrimaryMediafile(BaseResource):
    @app.require_oauth()
    def put(self, id, mediafile_id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
            self._abort_if_no_access(mediafile, current_token, "mediafiles")
        if not util.mediafile_is_public(mediafile):
            abort(400, message=f"Mediafile with id {mediafile_id} is not public")
        self.storage.set_primary_field_collection_item(
            "entities", util.get_raw_id(entity), mediafile_id, "is_primary"
        )
        util.signal_entity_changed(entity)
        return "", 204


class EntitySetPrimaryThumbnail(BaseResource):
    @app.require_oauth()
    def put(self, id, mediafile_id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
            self._abort_if_no_access(mediafile, current_token, "mediafiles")
        if not util.mediafile_is_public(mediafile):
            abort(400, message=f"Mediafile with id {mediafile_id} is not public")
        self.storage.set_primary_field_collection_item(
            "entities", util.get_raw_id(entity), mediafile_id, "is_primary_thumbnail"
        )
        util.signal_entity_changed(entity)
        return "", 204
