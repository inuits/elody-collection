import app

from datetime import datetime
from exceptions import NonUniqueException
from flask import request, after_this_request
from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource
from validator import entity_schema, mediafile_schema


class Entity(BaseResource):
    @app.require_oauth(permissions=["read-entity", "read-entity-all"])
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        filters = {}
        if int(request.args.get("only_own", 0)) or self._only_own_items(
            ["read-entity-all"]
        ):
            filters["user"] = current_token["email"]
        if item_type := request.args.get("type", None):
            filters["type"] = item_type
        if ids := request.args.get("ids", None):
            filters["ids"] = ids.split(",")
        skip_relations = int(request.args.get("skip_relations", 0))
        type_filter = f"type={item_type}&" if item_type else ""
        entities = self.storage.get_entities(skip, limit, skip_relations, filters)
        count = entities["count"]
        entities["limit"] = limit
        if skip + limit < count:
            entities[
                "next"
            ] = f"/entities?{type_filter}skip={skip + limit}&limit={limit}&skip_relations={skip_relations}"
        if skip > 0:
            entities[
                "previous"
            ] = f"/entities?{type_filter}skip={max(0, skip - limit)}&limit={limit}&skip_relations={skip_relations}"
        entities["results"] = self._inject_api_urls_into_entities(entities["results"])
        return entities

    @app.require_oauth("create-entity")
    def post(self):
        content = self._get_request_body()
        self._abort_if_not_valid_json("Entity", content, entity_schema)
        content["user"] = "default_uploader"
        if "email" in current_token:
            content["user"] = current_token["email"]
        content["date_created"] = str(datetime.now())
        content["version"] = 1
        try:
            entity = self.storage.save_item_to_collection("entities", content)
        except NonUniqueException as ex:
            return str(ex), 409
        self._signal_entity_changed(entity)
        return entity, 201


class EntityDetail(BaseResource):
    @app.require_oauth(permissions=["read-entity-detail", "read-entity-detail-all"])
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-detail-all"]):
            self._abort_if_no_access(entity, current_token)
        entity = self._set_entity_mediafile_and_thumbnail(entity)
        entity = self._add_relations_to_metadata(entity)
        return self._inject_api_urls_into_entities([entity])[0]

    @app.require_oauth("update-entity")
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        self._abort_if_not_valid_json("Entity", content, entity_schema)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        content["date_updated"] = str(datetime.now())
        content["version"] = content.get("version", 0) + 1
        try:
            entity = self.storage.update_item_from_collection(
                "entities", self._get_raw_id(entity), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        self._signal_entity_changed(entity)
        return entity, 201

    @app.require_oauth("patch-entity")
    def patch(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        content["date_updated"] = str(datetime.now())
        content["version"] = content.get("version", 0) + 1
        try:
            entity = self.storage.patch_item_from_collection(
                "entities", self._get_raw_id(entity), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        self._signal_entity_changed(entity)
        return entity, 201

    @app.require_oauth("delete-entity")
    def delete(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        self.storage.delete_item_from_collection("entities", self._get_raw_id(entity))
        self._signal_entity_deleted(entity)
        return "", 204


class EntityMediafiles(BaseResource):
    @app.require_oauth(
        permissions=["read-entity-mediafiles", "read-entity-mediafiles-all"]
    )
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-mediafiles-all"]):
            self._abort_if_no_access(entity, current_token)
        mediafiles = self.storage.get_collection_item_mediafiles(
            "entities", self._get_raw_id(entity)
        )
        if not request.args.get("non_public"):
            mediafiles = [
                mediafile
                for mediafile in mediafiles
                if self._mediafile_is_public(mediafile)
            ]

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self._inject_api_urls_into_mediafiles(mediafiles)

    @app.require_oauth("add-entity-mediafiles")
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        self._abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        mediafile = self._abort_if_item_doesnt_exist(
            "mediafiles", self._get_raw_id(content)
        )
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
            self._abort_if_no_access(mediafile, current_token, "mediafiles")
        mediafile = self.storage.add_mediafile_to_collection_item(
            "entities",
            self._get_raw_id(entity),
            mediafile["_id"],
            self._mediafile_is_public(mediafile),
        )
        self._signal_entity_changed(entity)
        return mediafile, 201


class EntityMediafilesCreate(BaseResource):
    @app.require_oauth("create-entity-mediafile")
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
        content["user"] = "default_uploader"
        if "email" in current_token:
            content["user"] = current_token["email"]
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        upload_location = f'{self.storage_api_url}/upload/{content["filename"]}?id={self._get_raw_id(mediafile)}'
        self.storage.add_mediafile_to_collection_item(
            "entities",
            self._get_raw_id(entity),
            mediafile["_id"],
            self._mediafile_is_public(mediafile),
        )
        self._signal_entity_changed(entity)
        return upload_location, 201


class EntityMetadata(BaseResource):
    @app.require_oauth(permissions=["read-entity-metadata", "read-entity-metadata-all"])
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-metadata-all"]):
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.get_collection_item_sub_item(
            "entities", self._get_raw_id(entity), "metadata"
        )
        return metadata

    @app.require_oauth("add-entity-metadata")
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.add_sub_item_to_collection_item(
            "entities", self._get_raw_id(entity), "metadata", content
        )
        self._signal_entity_changed(entity)
        return metadata, 201

    @app.require_oauth("update-entity-metadata")
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.update_collection_item_sub_item(
            "entities", self._get_raw_id(entity), "metadata", content
        )
        self._signal_entity_changed(entity)
        return metadata, 201

    @app.require_oauth("patch-entity-metadata")
    def patch(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.patch_collection_item_metadata(
            "entities", self._get_raw_id(entity), content
        )
        self._signal_entity_changed(entity)
        return metadata, 201


class EntityMetadataKey(BaseResource):
    @app.require_oauth(
        permissions=["read-entity-metadata-key", "read-entity-metadata-key-all"]
    )
    def get(self, id, key):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-metadata-key-all"]):
            self._abort_if_no_access(entity, current_token)
        metadata = self.storage.get_collection_item_sub_item_key(
            "entities", self._get_raw_id(entity), "metadata", key
        )
        return metadata

    @app.require_oauth("delete-entity-metadata-key")
    def delete(self, id, key):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        self.storage.delete_collection_item_sub_item_key(
            "entities", self._get_raw_id(entity), "metadata", key
        )
        self._signal_entity_changed(entity)
        return "", 204


class EntityRelations(BaseResource):
    @app.require_oauth(
        permissions=["read-entity-relations", "read-entity-relations-all"]
    )
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-relations-all"]):
            self._abort_if_no_access(entity, current_token)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(
            "entities", self._get_raw_id(entity)
        )

    @app.require_oauth("add-entity-relations")
    def post(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        relations = self.storage.add_relations_to_collection_item(
            "entities", self._get_raw_id(entity), content
        )
        self._signal_entity_changed(entity)
        return relations, 201

    @app.require_oauth("update-entity-relations")
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        relations = self.storage.update_collection_item_relations(
            "entities", self._get_raw_id(entity), content
        )
        self._signal_entity_changed(entity)
        return relations, 201

    @app.require_oauth("patch-entity-relations")
    def patch(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        relations = self.storage.patch_collection_item_relations(
            "entities", self._get_raw_id(entity), content
        )
        self._signal_entity_changed(entity)
        return relations, 201

    @app.require_oauth("delete-entity-relations")
    def delete(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
        self.storage.delete_collection_item_relations(
            "entities", self._get_raw_id(entity), content
        )
        self._signal_entity_changed(entity)
        return "", 204


class EntityRelationsAll(BaseResource):
    @app.require_oauth(
        permissions=["read-entity-relations", "read-entity-relations-all"]
    )
    def get(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        if self._only_own_items(["read-entity-relations-all"]):
            self._abort_if_no_access(entity, current_token)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(
            "entities", self._get_raw_id(entity), include_sub_relations=True
        )


class EntitySetPrimaryMediafile(BaseResource):
    @app.require_oauth("set-entity-primary-mediafile")
    def put(self, id, mediafile_id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
            self._abort_if_no_access(mediafile, current_token)
        self.storage.set_primary_field_collection_item(
            "entities", self._get_raw_id(entity), mediafile_id, "is_primary"
        )
        return "", 204


class EntitySetPrimaryThumbnail(BaseResource):
    @app.require_oauth("set-entity-primary-thumbnail")
    def put(self, id, mediafile_id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        if self._only_own_items():
            self._abort_if_no_access(entity, current_token)
            self._abort_if_no_access(mediafile, current_token)
        self.storage.set_primary_field_collection_item(
            "entities", self._get_raw_id(entity), mediafile_id, "is_primary_thumbnail"
        )
        return "", 204
