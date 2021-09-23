import datetime
import json
import os
import sys

import app
import uuid
import requests
from flask import g, request, after_this_request
from flask_restful import abort
from resources.base_resource import BaseResource
from validator import EntityValidator, MediafileValidator
from job_helper.job_helper import JobHelper

entity_validator = EntityValidator()
mediafile_validator = MediafileValidator()
job_helper = JobHelper(
    job_api_base_url=os.getenv("JOB_API_BASE_URL", "http://localhost:8000")

)

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
        ids = request.args.get("ids")
        if ids:
            ids = ids.split(",")
            return self.storage.get_items_from_collection_by_ids("entities", ids)
        entities = self.storage.get_items_from_collection("entities", skip, limit)
        count = entities["count"]
        entities["limit"] = limit
        if skip + limit < count:
            entities["next"] = "/{}?skip={}&limit={}".format(
                "entities", skip + limit, limit
            )
        if skip > 0:
            entities["previous"] = "/{}?skip={}&limit={}".format(
                "entities", max(0, skip - limit), limit
            )
        return entities


class EntityDetail(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        return self.abort_if_item_doesnt_exist("entities", id)

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

        return mediafiles

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
        file_id = str(uuid.uuid4())
        filename = content["filename"]
        file_id = "{}-{}".format(file_id, filename)
        mediafile = {
            "filename": filename,
            "original_file_location": "{}/download/{}".format(
                self.storage_api_url, file_id
            ),
            "thumbnail_file_location": "{}/iiif/3/{}/full/,150/0/default.jpg".format(
                self.cantaloupe_api_url, file_id
            ),
        }
        if "metadata" in content:
            mediafile["metadata"] = content["metadata"]
        mediafile = self.storage.save_item_to_collection("mediafiles", mediafile)
        mediafile_id = mediafile["_id"]
        upload_location = "{}/upload/{}".format(self.storage_api_url, file_id)
        job_helper.progress_job(job)
        try:
            self.storage.add_mediafile_to_collection_item("entities", id, mediafile_id)
            job_helper.finish_job(job)
        except Exception as ex:
            job_helper.fail_job(job, str(ex))
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
            if item["type"] != "isTYpeOf":
                abort(400, message="Invalid relation type: '" + item["type"] + "'")