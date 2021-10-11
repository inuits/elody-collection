import json
import sys

import app
import os

from flask import g, request, after_this_request
from flask_restful import abort
from job_helper.job_helper import JobHelper
from resources.base_resource import BaseResource
from validator import Validator, entity_schema, mediafile_schema

entity_validator = Validator(entity_schema)
mediafile_validator = Validator(mediafile_schema)

job_helper = JobHelper(
    job_api_base_url=os.getenv("JOB_API_BASE_URL", "http://localhost:8000")
)


def _set_entity_mediafile_and_thumbnail(entity, storage):

    mediafiles = storage.get_collection_item_mediafiles("entities", entity["_key"])
    for mediafile in mediafiles:
        if "is_primary" in mediafile and mediafile["is_primary"] is True:
            entity["primary_mediafile_location"] = mediafile["original_file_location"]
        if "is_primary_thumbnail" in mediafile and mediafile["is_primary_thumbnail"] is True:
            entity["primary_thumbnail_location"] = mediafile["thumbnail_file_location"]
    return entity


class Entity(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json(entity_validator, "Entity", content)
        if hasattr(g, "oidc_token_info"):
            content["user"] = g.oidc_token_info["email"]
        else:
            content["user"] = "default_uploader"
        entity = self.storage.save_item_to_collection("entities", content)
        return entity, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        item_type = request.args.get("type", None)
        type_var = "type={}&".format(item_type) if item_type else ""
        ids = request.args.get("ids")
        if ids:
            ids = ids.split(",")
            return self.storage.get_items_from_collection_by_ids("entities", ids)
        entities = self.storage.get_items_from_collection(
            "entities", skip, limit, item_type
        )
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
        updated_entities = list()
        for entity in entities["results"]:
            updated_entities.append(_set_entity_mediafile_and_thumbnail(entity, self.storage))
        entities["results"] = updated_entities
        return entities


class EntityDetail(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        entity = self.abort_if_item_doesnt_exist("entities", id)
        return _set_entity_mediafile_and_thumbnail(entity, self.storage)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json(entity_validator, "Entity", content)
        entity = self.storage.update_item_from_collection("entities", id, content)
        return entity, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        entity = self.storage.patch_item_from_collection("entities", id, content)
        return entity, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.delete_item_from_collection("entities", id)
        return "", 204


class EntityMetadata(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        metadata = self.storage.get_collection_item_sub_item("entities", id, "metadata")
        return metadata

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        metadata = self.storage.add_sub_item_to_collection_item(
            "entities", id, "metadata", content
        )
        return metadata, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        metadata = self.storage.update_collection_item_sub_item(
            "entities", id, "metadata", content
        )
        return metadata, 201


class EntityMetadataKey(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id, key):
        self.abort_if_item_doesnt_exist("entities", id)
        metadata = self.storage.get_collection_item_sub_item_key(
            "entities", id, "metadata", key
        )
        return metadata

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id, key):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.delete_collection_item_sub_item_key(
            "entities", id, "metadata", key
        )
        return "", 204


class EntityMediafiles(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        mediafiles = self.storage.get_collection_item_mediafiles("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self._inject_api_urls(mediafiles)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json(mediafile_validator, "Mediafile", content)
        mediafile_id = content["_id"]
        mediafile = self.storage.add_mediafile_to_collection_item(
            "entities", id, mediafile_id
        )
        return mediafile, 201


class EntityMediafilesCreate(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
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
        mediafile_id = mediafile["_id"]
        mediafile_raw_id = mediafile["_key"] if "_key" in mediafile else mediafile_id
        upload_location = "{}/upload/{}?url={}/mediafiles/{}&action=postMD5".format(
            self.storage_api_url, filename, self.collection_api_url, mediafile_raw_id
        )
        job_helper.progress_job(job)
        try:
            self.storage.add_mediafile_to_collection_item("entities", id, mediafile_id)
            job_helper.finish_job(job)
        except Exception as ex:
            job_helper.fail_job(job, str(ex))
            return str(ex), 400
        return upload_location, 201


class EntityRelations(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations("entities", id)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        relations = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return relations, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        relations = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return relations, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        relations = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return relations, 201


class EntityComponents(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_components("entities", id)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return components, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @staticmethod
    def _abort_if_incorrect_type(items):
        for item in items:
            if item["type"] != "components":
                abort(400, message="Invalid relation type: '" + item["type"] + "'")


class EntityParent(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_components("entities", id)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return components, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @staticmethod
    def _abort_if_incorrect_type(items):
        for item in items:
            if item["type"] != "parent":
                abort(400, message="Invalid relation type: '" + item["type"] + "'")


class EntityTypes(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_types("entities", id)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return components, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @staticmethod
    def _abort_if_incorrect_type(items):
        for item in items:
            if item["type"] != "isTypeOf":
                abort(400, message="Invalid relation type: '" + item["type"] + "'")


class EntityUsage(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def get(self, id):
        self.abort_if_item_doesnt_exist("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_usage("entities", id)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.add_relations_to_collection_item(
            "entities", id, content
        )
        return components, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.update_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        content = self.get_request_body()
        self._abort_if_incorrect_type(content)
        components = self.storage.patch_collection_item_relations(
            "entities", id, content
        )
        return components, 201

    @staticmethod
    def _abort_if_incorrect_type(items):
        for item in items:
            if item["type"] != "isUsedIn":
                abort(400, message="Invalid relation type: '" + item["type"] + "'")
