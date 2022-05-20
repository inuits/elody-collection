import app
import os

from flask import request, after_this_request
from flask_restful import abort
from inuits_jwt_auth.authorization import current_token
from job_helper.job_helper import JobHelper
from pyArango.theExceptions import CreationError
from resources.base_resource import BaseResource
from validator import entity_schema, mediafile_schema

job_helper = JobHelper(
    job_api_base_url=os.getenv("JOB_API_BASE_URL"),
    static_jwt=os.getenv("STATIC_JWT", False),
)


class Entity(BaseResource):
    @app.require_oauth("create-entity")
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json("Entity", content, entity_schema)
        if "Email" in current_token:
            content["user"] = current_token["Email"]
        else:
            content["user"] = "default_uploader"
        try:
            entity = self.storage.save_item_to_collection("entities", content)
        except CreationError as ex:
            return ex.errors["errorMessage"], 409
        self._signal_entity_changed(entity)
        return entity, 201

    @app.require_oauth("read-entity")
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        item_type = request.args.get("type", None)
        skip_relations = int(request.args.get("skip_relations", 0))
        type_var = "type={}&".format(item_type) if item_type else ""
        ids = request.args.get("ids", None)
        if ids:
            ids = ids.split(",")
        entities = self.storage.get_entities(
            skip, limit, item_type, ids, skip_relations
        )
        count = entities["count"]
        entities["limit"] = limit
        if skip + limit < count:
            entities["next"] = "/{}?{}skip={}&limit={}&skip_relations={}".format(
                "entities", type_var, skip + limit, limit, 1 if skip_relations else 0
            )
        if skip > 0:
            entities["previous"] = "/{}?{}skip={}&limit={}&skip_relations={}".format(
                "entities",
                type_var,
                max(0, skip - limit),
                limit,
                1 if skip_relations else 0,
            )
        entities["results"] = self._inject_api_urls_into_entities(entities["results"])
        return entities


class EntityDetail(BaseResource):
    @app.require_oauth("read-entity")
    def get(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        entity = self._set_entity_mediafile_and_thumbnail(entity)
        entity = self._add_relations_to_metadata(entity)
        return self._inject_api_urls_into_entities([entity])[0]

    @app.require_oauth("update-entity")
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("Entity", content, entity_schema)
        entity = self.storage.update_item_from_collection("entities", id, content)
        self._signal_entity_changed(entity)
        return entity, 201

    @app.require_oauth("patch-entity")
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        entity = self.storage.patch_item_from_collection("entities", id, content)
        self._signal_entity_changed(entity)
        return entity, 201

    @app.require_oauth("delete-entity")
    def delete(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        self.storage.delete_item_from_collection("entities", id)
        self._signal_entity_deleted(entity)
        return "", 204


class EntitySetPrimaryMediafile(BaseResource):
    @app.require_oauth("set-entity-primary-mediafile")
    def put(self, id, mediafile_id):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.set_primary_field_collection_item(
            "entities", id, mediafile_id, "is_primary"
        )
        return 204


class EntitySetPrimaryThumbnail(BaseResource):
    @app.require_oauth("set-entity-primary-thumbnail")
    def put(self, id, mediafile_id):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.set_primary_field_collection_item(
            "entities", id, mediafile_id, "is_primary_thumbnail"
        )
        return 204


class EntityMetadata(BaseResource):
    @app.require_oauth("read-entity-metadata")
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        metadata = self.storage.get_collection_item_sub_item("entities", id, "metadata")
        return metadata

    @app.require_oauth("add-entity-metadata")
    def post(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        metadata = self.storage.add_sub_item_to_collection_item(
            "entities", id, "metadata", content
        )
        self._signal_entity_changed(entity)
        return metadata, 201

    @app.require_oauth("update-entity-metadata")
    def put(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        metadata = self.storage.update_collection_item_sub_item(
            "entities", id, "metadata", content
        )
        self._signal_entity_changed(entity)
        return metadata, 201


class EntityMetadataKey(BaseResource):
    @app.require_oauth("read-entity-metadata")
    def get(self, id, key):
        self.abort_if_item_doesnt_exist("entities", id)
        metadata = self.storage.get_collection_item_sub_item_key(
            "entities", id, "metadata", key
        )
        return metadata

    @app.require_oauth("delete-entity-metadata")
    def delete(self, id, key):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        self.storage.delete_collection_item_sub_item_key(
            "entities", id, "metadata", key
        )
        self._signal_entity_changed(entity)
        return "", 204


class EntityMediafiles(BaseResource):
    @app.require_oauth("read-entity-mediafiles")
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        mediafiles = self.storage.get_collection_item_mediafiles("entities", id)
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
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        mediafile_id = content["_id"]
        mediafile = self.storage.add_mediafile_to_collection_item(
            "entities", id, mediafile_id, self._mediafile_is_public(content)
        )
        return mediafile, 201


class EntityMediafilesCreate(BaseResource):
    @app.require_oauth("create-entity-mediafile")
    def post(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        if "filename" not in content:
            abort(
                405,
                message="Invalid input",
            )
        job = job_helper.create_new_job("Create mediafile", "mediafile")
        filename = content["filename"]
        mediafile = {
            "filename": filename,
            "original_file_location": f"/download/{filename}",
            "thumbnail_file_location": f"/iiif/3/{filename}/full/,150/0/default.jpg",
        }
        if "metadata" in content:
            mediafile["metadata"] = content["metadata"]
        mediafile = self.storage.save_item_to_collection("mediafiles", mediafile)
        upload_location = (
            f"{self.storage_api_url}/upload/{filename}?id={self._get_raw_id(mediafile)}"
        )
        job_helper.progress_job(job)
        try:
            self.storage.add_mediafile_to_collection_item(
                "entities", id, mediafile["_id"], self._mediafile_is_public(mediafile)
            )
            job_helper.finish_job(job)
        except Exception as ex:
            job_helper.fail_job(job, str(ex))
            return str(ex), 400
        self._signal_entity_changed(entity)
        return upload_location, 201


class EntityRelationsAll(BaseResource):
    @app.require_oauth("read-entity-relations")
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(
            "entities", id, include_sub_relations=True
        )


class EntityRelations(BaseResource):
    @app.require_oauth("read-entity-relations")
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations("entities", id)

    @app.require_oauth("add-entity-relations")
    def post(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        relations = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        self._signal_entity_changed(entity)
        return relations, 201

    @app.require_oauth("update-entity-relations")
    def put(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        relations = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        self._signal_entity_changed(entity)
        return relations, 201

    @app.require_oauth("patch-entity-relations")
    def patch(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        relations = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        self._signal_entity_changed(entity)
        return relations, 201

    @app.require_oauth("delete-entity-relations")
    def delete(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self.storage.delete_collection_item_relations("entities", id, content)
        self._signal_entity_changed(entity)
        return "", 204
