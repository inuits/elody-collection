import json
import mappers
import os

from app import policy_factory, rabbit, tenant_defining_types
from datetime import datetime, timezone, timedelta
from elody.csv import CSVSingleObject
from elody.util import (
    get_item_metadata_value,
    get_raw_id,
    mediafile_is_public,
    signal_entity_changed,
)
from flask import Response
from flask_restful import Resource, abort
from storage.storagemanager import StorageManager
from elody.validator import validate_json
from elody.schemas import (
    entity_schema,
    key_value_store_schema,
    mediafile_schema,
    saved_search_schema,
)


class BaseResource(Resource):
    known_collections = []
    schemas_by_type = {
        "entity": entity_schema,
        "key_value_store": key_value_store_schema,
        "mediafile": mediafile_schema,
        "saved_search": saved_search_schema,
    }

    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        self.collection_api_url = os.getenv("COLLECTION_API_URL")
        self.image_api_url_ext = os.getenv("IMAGE_API_URL_EXT")
        self.storage_api_url = os.getenv("STORAGE_API_URL")
        self.storage_api_url_ext = os.getenv("STORAGE_API_URL_EXT")

    def __group_user_relations_by_idp_role_status(
        self, user_relations, roles_per_tenant
    ):
        new, updated, deleted, untouched = [], [], [], []
        for relation in user_relations:
            if relation["type"] == "hasTenant":
                key = relation["key"]
                if key in roles_per_tenant.keys():
                    if roles_per_tenant[key] != relation["roles"]:
                        updated.append(
                            {
                                "key": key,
                                "roles": roles_per_tenant[key],
                                "type": "hasTenant",
                            }
                        )
                    else:
                        untouched.append(relation)
                    roles_per_tenant.pop(key)
                else:
                    deleted.append(relation)
            else:
                untouched.append(relation)
        for key, roles in roles_per_tenant.items():
            new.append({"key": key, "roles": roles, "type": "hasTenant"})
        return new, updated, deleted, untouched

    def __link_tenant_to_defining_entity(self, tenant_id, entity_id):
        defining_relation = {"key": entity_id, "type": "definedBy"}
        self.storage.add_relations_to_collection_item(
            "entities", tenant_id, [defining_relation]
        )

    def _abort_if_item_doesnt_exist(self, collection, id):
        if item := self.storage.get_item_from_collection_by_id(collection, id):
            return item
        abort(
            404, message=f"Item with id {id} doesn't exist in collection {collection}"
        )

    def _abort_if_not_valid_json(self, type, json):
        if validation_error := validate_json(json, self.schemas_by_type.get(type)):
            abort(
                400, message=f"{type} doesn't have a valid format. {validation_error}"
            )

    def _abort_if_not_valid_type(self, item, type):
        if type and "type" in item and item["type"] != type:
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

    def _check_if_collection_name_exists(self, collection):
        if collection in self.known_collections:
            return
        if collection not in self.storage.get_existing_collections():
            abort(400, message=f"Collection {collection} does not exist.")
        self.known_collections.append(collection)

    def _check_if_collection_and_item_exists(self, collection, id):
        self._check_if_collection_name_exists(collection)
        return self._abort_if_item_doesnt_exist(collection, id)

    def _create_linked_data(self, request, content_type):
        content = request.get_data(as_text=True)
        try:
            data = mappers.map_data_to_ldjson(content, content_type)
            rdf_data = json.loads(data)
        except Exception:
            abort(
                400,
                message="The request failed during mapping the data to ldjson. Check if the given RDF format is valid.",
            )
        return {"data": rdf_data}

    def _create_mediafile_for_entity(self, entity, filename, metadata=None):
        content = {
            "filename": filename,
            "date_created": datetime.now(timezone.utc),
            "version": 1,
            "thumbnail_file_location": f"/iiif/3/{filename}/full/,150/0/default.jpg",
            "original_file_location": f"/download/{filename}",
        }
        if metadata:
            content["metadata"] = metadata
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        self.storage.add_mediafile_to_collection_item(
            "entities",
            get_raw_id(entity),
            mediafile["_id"],
            mediafile_is_public(mediafile),
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

    def _create_tenant(self, entity):
        if tenant_defining_types and entity["type"] in tenant_defining_types:
            tenant_id = f'tenant:{entity.get("_id", entity.get("id"))}'
            if not (tenant_label := self._get_tenant_label(entity)):
                tenant_label = entity.get("_id", entity.get("id"))
            tenant = self.storage.save_item_to_collection(
                "entities",
                {
                    "_id": tenant_id,
                    "type": "tenant",
                    "identifiers": [tenant_id],
                    "metadata": [{"key": "label", "value": tenant_label}],
                },
            )
            self.__link_tenant_to_defining_entity(tenant["_id"], entity["_id"])
        elif entity["type"] not in ["role", "tenant", "user"]:
            if tenant_id := policy_factory.get_user_context().x_tenant.id:
                try:
                    self._link_entity_to_tenant(entity["_id"], tenant_id)
                except Exception as ex:
                    abort(400, message=str(ex))

    def _create_ticket(self, filename, mediafile_id=None):
        ticket = {
            "bucket": self._get_upload_bucket(),
            "exp": (
                datetime.now(tz=timezone.utc)
                + timedelta(seconds=int(os.getenv("TICKET_LIFESPAN", 3600)))
            ).timestamp(),
            "location": self._get_upload_location(filename),
            "type": "ticket",
            "user": policy_factory.get_user_context().email or "default_uploader",
        }
        if mediafile_id:
            ticket["mediafile_id"] = mediafile_id
        return self.storage.save_item_to_collection(
            "abstracts", ticket, only_return_id=True, create_sortable_metadata=False
        )

    def _create_user_from_idp(self, assign_roles_from_idp=True, roles_per_tenant=None):
        user_context = policy_factory.get_user_context()
        user = {
            "identifiers": list({user_context.id, user_context.email}),
            "metadata": [
                {"key": "idp_user_id", "value": user_context.id},
                {"key": "email", "value": user_context.email},
            ],
            "relations": [],
            "type": "user",
        }
        user = self.storage.save_item_to_collection("entities", user)
        if assign_roles_from_idp:
            self._sync_roles_from_idp(
                user,
                roles_per_tenant
                if roles_per_tenant
                else {"tenant:super": user_context.x_tenant.roles},
            )
        return user

    def _decorate_entity(self, entity):
        default_entity = {
            "type": "asset",
        }
        return default_entity | entity

    def _delete_tenant(self, entity):
        if tenant_defining_types and entity["type"] in tenant_defining_types:
            self.storage.delete_item_from_collection(
                "entities", f'tenant:{entity["_id"]}'
            )

    def _get_content_according_content_type(self, request, object_type="entity"):
        content_type = request.content_type
        match content_type:
            case "application/json":
                return request.get_json()
            case "text/csv":
                csv = request.get_data(as_text=True)
                parsed_csv = CSVSingleObject(csv, object_type)
                if object_type in ["metadata", "relations"]:
                    return getattr(parsed_csv, object_type)
                return parsed_csv.get_type(object_type)
            case _:
                return request.get_json()

    def _get_tenant_label(self, item):
        return get_item_metadata_value(item, "name")

    def _get_upload_bucket(self):
        return os.getenv("MINIO_BUCKET")

    def _get_upload_location(self, filename):
        if tenant_id := policy_factory.get_user_context().x_tenant.id:
            return f"{tenant_id}/{filename}"
        return filename

    def _inject_api_urls_into_entities(self, entities):
        for entity in entities:
            for mediafile_type in [
                "primary_mediafile_location",
                "primary_transcode_location",
            ]:
                if mediafile_type in entity and entity[mediafile_type] is not None:
                    mediafile_filename = entity[mediafile_type]
                    mediafile_filename = mediafile_filename.split("/download/")[-1]
                    ticket_id = self._create_ticket(mediafile_filename)
                    entity[
                        mediafile_type
                    ] = f"{self.storage_api_url_ext}/download-with-ticket/{mediafile_filename}?ticket_id={ticket_id}"
            if "primary_thumbnail_location" in entity:
                entity[
                    "primary_thumbnail_location"
                ] = f'{self.image_api_url_ext}{entity["primary_thumbnail_location"]}'
        return entities

    def _inject_api_urls_into_mediafiles(self, mediafiles):
        for mediafile in mediafiles:
            for mediafile_type in ["original_file_location", "transcode_file_location"]:
                if mediafile_type in mediafile:
                    mediafile_filename = mediafile[mediafile_type]
                    mediafile_filename = mediafile_filename.split("/download/")[-1]
                    ticket_id = self._create_ticket(mediafile_filename)
                    mediafile[
                        mediafile_type
                    ] = f"{self.storage_api_url_ext}/download-with-ticket/{mediafile_filename}?ticket_id={ticket_id}"
            if "thumbnail_file_location" in mediafile:
                mediafile[
                    "thumbnail_file_location"
                ] = f'{self.image_api_url_ext}{mediafile["thumbnail_file_location"]}'
        return mediafiles

    def _is_rdf_post_call(self, content_type):
        return content_type in [
            "application/ld+json",
            "application/n-triples",
            "application/rdf+xml",
            "text/turtle",
        ]

    def _link_entity_to_tenant(self, entity_id, tenant_id):
        tenant = self.storage.get_item_from_collection_by_id("entities", tenant_id)
        relation = {"key": tenant["_id"], "type": "isIn"}
        self.storage.add_relations_to_collection_item("entities", entity_id, [relation])

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

    def _sync_roles_from_idp(self, user, roles_per_tenant):
        (
            new,
            updated,
            deleted,
            untouched,
        ) = self.__group_user_relations_by_idp_role_status(
            user["relations"], roles_per_tenant
        )
        id = user["_id"]

        if len(new) > 0:
            self.storage.add_relations_to_collection_item("entities", id, new)
        if len(updated) > 0:
            self.storage.patch_collection_item_relations("entities", id, updated)
        if len(deleted) > 0:
            self.storage.delete_collection_item_relations("entities", id, deleted)
        user["relations"] = [*new, *updated, *untouched]
        return user

    def _update_tenant(self, entity, new_data):
        if not tenant_defining_types or entity["type"] not in tenant_defining_types:
            return
        if self._get_tenant_label(entity) == self._get_tenant_label(new_data):
            return
        metadata = [{"key": "label", "value": self._get_tenant_label(new_data)}]
        self.storage.patch_collection_item_metadata(
            "entities", f"tenant:{get_raw_id(entity)}", metadata
        )
        
    def _update_date_updated(self, collection, id, date_updated):
        content_date_updated = {"date_updated": date_updated}
        return self.storage.patch_item_from_collection(collection, id, content_date_updated)
