import json
import mappers
import os

from app import auto_create_tenants, multitenancy_enabled, policy_factory, rabbit
from datetime import datetime, timezone, timedelta
from elody.util import get_raw_id, signal_entity_changed
from flask import Response
from flask_restful import Resource, abort
from storage.storagemanager import StorageManager
from validator import validate_json


class BaseResource(Resource):
    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        self.collection_api_url = os.getenv("COLLECTION_API_URL")
        self.image_api_url_ext = os.getenv("IMAGE_API_URL_EXT")
        self.storage_api_url = os.getenv("STORAGE_API_URL")
        self.storage_api_url_ext = os.getenv("STORAGE_API_URL_EXT")

    def _abort_if_item_doesnt_exist(self, collection, id):
        if item := self.storage.get_item_from_collection_by_id(collection, id):
            return item
        abort(
            404, message=f"Item with id {id} doesn't exist in collection {collection}"
        )

    def _abort_if_no_access(self, item, user, collection="entities"):
        if not self._has_access_to_item(item, user, collection):
            abort(403, message="Access denied")

    def _abort_if_not_valid_json(self, type, json, schema):
        if validation_error := validate_json(json, schema):
            abort(
                400, message=f"{type} doesn't have a valid format. {validation_error}"
            )

    def _abort_if_not_valid_type(self, item, type):
        if type and item["type"] != type:
            abort(400, message=f"Item has the wrong type")

    def _add_relations_to_metadata(self, entity, collection="entities", sort_by=None):
        relations = self.storage.get_collection_item_relations(
            collection, get_raw_id(entity), exclude=["story_box_visits"]
        )
        if not relations:
            return entity
        if sort_by and all("order" in x for x in relations):
            relations = sorted(relations, key=lambda x: x[sort_by])
        entity["metadata"] = [*entity.get("metadata", []), *relations]
        return entity

    def _create_linked_data(self, request, content_type):
        content = request.get_data(as_text=True)
        try:
            data = mappers.map_data_to_ldjson(content, content_type)
            rdf_data = json.loads(data)
        except Exception as ex:
            abort(
                400,
                message="The request failed during mapping the data to ldjson. Check if the given RDF format is valid.",
            )
        return {"data": rdf_data}

    def _create_mediafile_for_entity(
        self,
        entity,
        filename,
    ):
        content = {
            "filename": filename,
            "date_created": datetime.now(timezone.utc).isoformat(),
            "version": 1,
            "thumbnail_file_location": f"/iiif/3/{filename}/full/,150/0/default.jpg",
            "original_file_location": f"/download/{filename}",
        }
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        self.storage.add_mediafile_to_collection_item(
            "entities",
            get_raw_id(entity),
            mediafile["_id"],
            False,
        )
        signal_entity_changed(rabbit, entity)
        return mediafile

    def _create_response_according_accept_header(
        self, response_data, accept_header=None, status_code=200
    ):
        match accept_header:
            case "application/json":
                return response_data, status_code
            case "application/ld+json":
                return Response(
                    response_data, status=status_code, mimetype="application/ld+json"
                )
            case "application/n-triples":
                return Response(
                    response_data, status=status_code, mimetype="application/n-triples"
                )
            case "application/rdf+xml":
                return Response(
                    response_data, status=status_code, mimetype="application/rdf+xml"
                )
            case "text/csv":
                return Response(response_data, status=status_code, mimetype="text/csv")
            case "text/turtle":
                return Response(
                    response_data, status=status_code, mimetype="text/turtle"
                )
            case "text/uri-list":
                return Response(
                    response_data, status=status_code, mimetype="text/uri-list"
                )
            case _:
                return response_data, status_code

    def _decorate_entity(self, entity):
        default_entity = {
            "type": "asset",
        }
        return default_entity | entity

    def _get_tenant(self, create_tenant=True, tenant_requested=None):
        tenants = policy_factory.get_user_context().tenant
        if not tenants:
            return None
        if not auto_create_tenants or not create_tenant:
            tenants_ids = []
            if not tenant_requested:
                for tenant in tenants:
                    tenants_ids.append(
                        self.storage.get_item_from_collection_by_id("entities", tenant)[
                            "_id"
                        ]
                    )
            elif tenant_requested in tenants:
                tenants_ids.append(
                    self.storage.get_item_from_collection_by_id(
                        "entities", tenant_requested
                    )["_id"]
                )
            else:
                abort(403, message="Requested tenant is not one of yours tenants.")
            return tenants_ids
        if not tenant_requested:
            tenant_return = []
            for tenant in tenants:
                if tenant_item := self.storage.get_item_from_collection_by_id(
                    "entities", tenant
                ):
                    tenant_dict = self.create_relation_dict(
                        tenant_item["_id"], tenant_item["tenant"], "tenant", "hasTenant"
                    )
                    tenant_return.append(tenant_dict)
                elif auto_create_tenants:
                    tenant_save = self.storage.save_item_to_collection(
                        "entities",
                        {"tenant": tenant, "identifiers": [tenant], "type": "tenant"},
                    )
                    tenant_dict = self.create_relation_dict(
                        tenant_save["_id"], tenant_save["tenant"], "tenant", "hasTenant"
                    )
                    tenant_return.append(tenant_dict)
                else:
                    abort(
                        403,
                        message="Tenant is not stored. Creat the tenant entity first to add a relation to this tenant.",
                    )
            return tenant_return
        elif tenant_requested in tenants:
            if tenant_item := self.storage.get_item_from_collection_by_id(
                "entities", tenant_requested
            ):
                tenant_dict = self.create_relation_dict(
                    tenant_item["_id"], tenant_item["tenant"], "tenant", "hasTenant"
                )
                return [tenant_dict]
            elif auto_create_tenants:
                tenant_save = self.storage.save_item_to_collection(
                    "entities",
                    {
                        "tenant": tenant_requested,
                        "identifiers": [tenant_requested],
                        "type": "tenant",
                    },
                )
                tenant_dict = self.create_relation_dict(
                    tenant_save["_id"], tenant_save["tenant"], "tenant", "hasTenant"
                )
                return [tenant_dict]
            else:
                abort(
                    403,
                    message="Tenant is not stored. Creat the tenant entity first to add a relation to this tenant.",
                )
        else:
            abort(403, message="Requested tenant is not one of yours tenants.")

    def _get_user(self, create_user=True):
        token = policy_factory.get_user_context().auth_objects.get("token")
        token_user_name = token.get("preferred_username")
        token_first_name = token.get("given_name")
        token_last_name = token.get("family_name")
        token_email = token.get("email")
        token_tenants = token.get("institutions", [])
        token_roles = policy_factory.get_user_context().roles
        token_scopes = policy_factory.get_user_context().scopes
        token_list = [
            token_user_name,
            token_first_name,
            token_last_name,
            token_tenants,
            token_roles,
            token_scopes,
        ]
        if user := self.storage.get_item_from_collection_by_id("entities", token_email):
            if not all(x in user.values() for x in token_list):
                self.storage.update_user(
                    user["_id"],
                    token_user_name,
                    token_first_name,
                    token_last_name,
                    token_tenants,
                    token_roles,
                    token_scopes,
                )
                user = self.storage.get_item_from_collection_by_id(
                    "entities", token_email
                )
        elif create_user:
            user = self.storage.save_item_to_collection(
                "entities",
                {
                    "email": token_email,
                    "username": token_user_name,
                    "first_name": token_first_name,
                    "last_name": token_last_name,
                    "tenants": token_tenants,
                    "roles": token_roles,
                    "identifiers": [token_email],
                    "type": "user",
                    "scopes": token_scopes,
                },
            )
            if multitenancy_enabled:
                tenants = self._get_tenant()
                for tenant in tenants:
                    self.storage.add_relations_to_collection_item(
                        "entities", user["_id"], [tenant]
                    )
        else:
            abort(403, message="User does not exist.")
        return user

    def _has_access_to_item(self, item, user, collection="entities"):
        if self.is_admin(user):
            return True
        if (email := user["email"]) and any(
            relation["value"] == email for relation in item["relations"]
        ):
            return True
        if (
            multitenancy_enabled
            and (tenants_ids := self._get_tenant(create_tenant=False))
            and any(tenant["key"] in tenants_ids for tenant in item["relations"])
        ):
            return True
        return False

    def _inject_api_urls_into_entities(self, entities):
        for entity in entities:
            if "primary_mediafile_location" in entity:
                entity[
                    "primary_mediafile_location"
                ] = f'{self.storage_api_url_ext}{entity["primary_mediafile_location"]}'
            if "primary_thumbnail_location" in entity:
                entity[
                    "primary_thumbnail_location"
                ] = f'{self.image_api_url_ext}{entity["primary_thumbnail_location"]}'
            if "primary_transcode_location" in entity:
                entity[
                    "primary_transcode_location"
                ] = f'{self.storage_api_url_ext}{entity["primary_transcode_location"]}'
        return entities

    def _inject_api_urls_into_mediafiles(self, mediafiles):
        for mediafile in mediafiles:
            if "original_file_location" in mediafile:
                mediafile[
                    "original_file_location"
                ] = f'{self.storage_api_url_ext}{mediafile["original_file_location"]}'
            if "thumbnail_file_location" in mediafile:
                mediafile[
                    "thumbnail_file_location"
                ] = f'{self.image_api_url_ext}{mediafile["thumbnail_file_location"]}'
            if "transcode_file_location" in mediafile:
                mediafile[
                    "transcode_file_location"
                ] = f'{self.storage_api_url_ext}{mediafile["transcode_file_location"]}'
        return mediafiles

    def _is_rdf_post_call(self, content_type):
        return content_type in [
            "application/ld+json",
            "application/n-triples",
            "application/rdf+xml",
            "text/turtle",
        ]

    def _set_entity_mediafile_and_thumbnail(self, entity):
        mediafiles = self.storage.get_collection_item_mediafiles(
            "entities", get_raw_id(entity)
        )
        for mediafile in mediafiles:
            if mediafile.get("is_primary", False):
                entity["primary_mediafile"] = mediafile["filename"]
                entity["primary_mediafile_location"] = mediafile[
                    "original_file_location"
                ]
                if "transcode_file_location" in mediafile:
                    entity["primary_transcode"] = mediafile["transcode_filename"]
                    entity["primary_transcode_location"] = mediafile[
                        "transcode_file_location"
                    ]
                if "img_width" in mediafile and "img_height" in mediafile:
                    entity["primary_width"] = mediafile["img_width"]
                    entity["primary_height"] = mediafile["img_height"]
            if mediafile.get("is_primary_thumbnail", False):
                entity["primary_thumbnail_location"] = mediafile[
                    "thumbnail_file_location"
                ]
        return entity

    def _create_ticket(self, filename: str, user_id: str) -> str:
        content = {
            "location": filename,
            "exp": (datetime.now(tz=timezone.utc) + timedelta(minutes=1)).timestamp(),
            "user": user_id,
            "type": "ticket",
        }
        ticket_id = self.storage.save_item_to_collection(
            "abstracts", content, only_return_id=True, create_sortable_metadata=False
        )
        return ticket_id

    def check_entity_relations_tenant(self, content):
        if not (tenants := self._get_tenant()):
            abort(400, message="Tenant not found")
        for item in content:
            if item["type"] == "tenant":
                if not any(item["key"] == tenant["key"] for tenant in tenants):
                    abort(
                        403,
                        message="Tenant in relations is not one of yours tenants.",
                    )

    def create_relation_dict(self, key, value, label, type):
        relation_dict = {
            "key": key,
            "value": value,
            "label": label,
            "type": type,
        }
        return relation_dict

    def is_admin(self, user):
        if "has-full-control" in user["scopes"]:
            return True
        if os.getenv("SUPER_ADMIN_ROLE") in user["roles"]:
            return True
        return False

    def tenat_user_relation_policy_check(self, content, user):
        if not self.is_admin(user):
            if multitenancy_enabled:
                if not (tenants := self._get_tenant()):
                    abort(400, message="Tenant not found")
                for item in content:
                    if item["type"] == "tenant":
                        if not any(item["key"] == tenant["key"] for tenant in tenants):
                            abort(
                                403,
                                message="Tenant in relations is not one of yours tenants.",
                            )
                    if item["type"] == "user":
                        abort(
                            403,
                            message="Non admin user can not modify users in the relations.",
                        )
            else:
                for item in content:
                    if item["type"] == "user":
                        abort(
                            403,
                            message="Non admin user can not modify users in the relations.",
                        )
