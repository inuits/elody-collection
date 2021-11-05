import app
import os
from flask import request, after_this_request
from flask_restful import abort
from inuits_jwt_auth.authorization import current_token
from job_helper.job_helper import JobHelper
from resources.base_resource import BaseResource
from validator import entity_schema, mediafile_schema

job_helper = JobHelper(
    job_api_base_url=os.getenv("JOB_API_BASE_URL", "http://localhost:8000")
)


class Entity(BaseResource):
    @app.require_oauth()
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json("Entity", content, entity_schema)
        if "Email" in current_token:
            content["user"] = current_token["Email"]
        else:
            content["user"] = "default_uploader"
        entity = self.storage.save_item_to_collection("entities", content)
        self._index_entity(self._get_raw_id(entity))
        return entity, 201

    @app.require_oauth()
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        item_type = request.args.get("type", None)
        with_relations = request.args.get("with_relations", None)
        type_var = "type={}&".format(item_type) if item_type else ""
        ids = request.args.get("ids")
        if ids:
            ids = ids.split(",")
            return self.storage.get_entities(skip, limit, ids=ids)
        entities = self.storage.get_entities(skip, limit, item_type)
        count = entities["count"]
        entities["limit"] = limit
        if skip + limit < count:
            entities["next"] = "/{}?{}skip={}&limit={}".format(
                "entities", type_var, skip + limit, limit
            )
        if skip > 0:
            entities["previous"] = "/{}?{}skip={}&limit={}".format(
                "entities", type_var, max(0, skip - limit), limit
            )
        if with_relations:
            for entity in entities["results"]:
                self._add_relations_to_metadata(entity)
        entities["results"] = self._inject_api_urls_into_entities(entities["results"])
        return entities


class EntityDetail(BaseResource):
    @app.require_oauth()
    def get(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        entity = self._set_entity_mediafile_and_thumbnail(entity)
        entity = self._add_relations_to_metadata(entity)
        return self._inject_api_urls_into_entities([entity])[0]

    @app.require_oauth()
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("Entity", content, entity_schema)
        entity = self.storage.update_item_from_collection("entities", id, content)
        self._index_entity(id)
        return entity, 201

    @app.require_oauth()
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        entity = self.storage.patch_item_from_collection("entities", id, content)
        self._index_entity(id)
        return entity, 201

    @app.require_oauth()
    def delete(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.delete_item_from_collection("entities", id)
        return "", 204


class EntitySetPrimaryMediafile(BaseResource):
    @app.require_oauth()
    def put(self, id, mediafile_id):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.set_primary_mediafile_for_entity("entities", id, mediafile_id)
        return 204


class EntitySetPrimaryThumbnail(BaseResource):
    @app.require_oauth()
    def put(self, id, mediafile_id):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.set_primary_mediafile_for_entity(
            "entities", id, mediafile_id, thumbnail=True
        )
        return 204


class EntityMetadata(BaseResource):
    @app.require_oauth()
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        metadata = self.storage.get_collection_item_sub_item("entities", id, "metadata")
        return metadata

    @app.require_oauth()
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        metadata = self.storage.add_sub_item_to_collection_item(
            "entities", id, "metadata", content
        )
        return metadata, 201

    @app.require_oauth()
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        metadata = self.storage.update_collection_item_sub_item(
            "entities", id, "metadata", content
        )
        return metadata, 201


class EntityMetadataKey(BaseResource):
    @app.require_oauth()
    def get(self, id, key):
        self.abort_if_item_doesnt_exist("entities", id)
        metadata = self.storage.get_collection_item_sub_item_key(
            "entities", id, "metadata", key
        )
        return metadata

    @app.require_oauth()
    def delete(self, id, key):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.delete_collection_item_sub_item_key(
            "entities", id, "metadata", key
        )
        return "", 204


class EntityMediafiles(BaseResource):
    @app.require_oauth()
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        mediafiles = self.storage.get_collection_item_mediafiles("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self._inject_api_urls_into_mediafiles(mediafiles)

    @app.require_oauth()
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("Mediafile", content, mediafile_schema)
        mediafile_id = content["_id"]
        mediafile = self.storage.add_mediafile_to_collection_item(
            "entities", id, mediafile_id
        )
        return mediafile, 201


class EntityMediafilesCreate(BaseResource):
    @app.require_oauth()
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
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
            "original_file_location": "/download/{}".format(filename),
            "thumbnail_file_location": "/iiif/3/{}/full/,150/0/default.jpg".format(
                filename
            ),
        }
        if "metadata" in content:
            mediafile["metadata"] = content["metadata"]
        mediafile = self.storage.save_item_to_collection("mediafiles", mediafile)
        upload_location = "{}/upload/{}?url={}/mediafiles/{}&action=postMD5".format(
            self.storage_api_url,
            filename,
            self.collection_api_url,
            self._get_raw_id(mediafile),
        )
        job_helper.progress_job(job)
        try:
            self.storage.add_mediafile_to_collection_item(
                "entities", id, mediafile["_id"]
            )
            job_helper.finish_job(job)
        except Exception as ex:
            job_helper.fail_job(job, str(ex))
            return str(ex), 400
        return upload_location, 201


class EntityRelations(BaseResource):
    @app.require_oauth()
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations("entities", id)

    @app.require_oauth()
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        relations = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return relations, 201

    @app.require_oauth()
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        relations = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return relations, 201

    @app.require_oauth()
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        relations = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return relations, 201


class EntityComponents(BaseResource):
    @app.require_oauth()
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_components("entities", id)

    @app.require_oauth()
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "components")
        components = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return components, 201

    @app.require_oauth()
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "components")
        components = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @app.require_oauth()
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "components")
        components = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return components, 201


class EntityParent(BaseResource):
    @app.require_oauth()
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_components("entities", id)

    @app.require_oauth()
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "parent")
        components = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return components, 201

    @app.require_oauth()
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "parent")
        components = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @app.require_oauth()
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "parent")
        components = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return components, 201


class EntityTypes(BaseResource):
    @app.require_oauth()
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_types("entities", id)

    @app.require_oauth()
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "isTypeOf")
        components = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return components, 201

    @app.require_oauth()
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "isTypeOf")
        components = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @app.require_oauth()
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "isTypeOf")
        components = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return components, 201


class EntityUsage(BaseResource):
    @app.require_oauth()
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_usage("entities", id)

    @app.require_oauth()
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "isUsedIn")
        components = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return components, 201

    @app.require_oauth()
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "isUsedIn")
        components = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @app.require_oauth()
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content, "isUsedIn")
        components = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return components, 201
