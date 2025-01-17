import json
import mappers
import csv
import io
import re

from configuration import get_object_configuration_mapper
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from elody.error_codes import ErrorCode, get_error_code, get_read, get_write
from elody.csv import CSVSingleObject
from elody.schemas import (
    entity_schema,
    key_value_store_schema,
    mediafile_schema,
    saved_search_schema,
)
from elody.util import (
    get_item_metadata_value,
    get_raw_id,
    mediafile_is_public,
    parse_string_to_bool,
    signal_entity_changed,
)
from elody.validator import validate_json
from flask import Response
from flask_restful import Resource, abort
from os import getenv
from policy_factory import get_user_context
from rabbit import get_rabbit
from serialization.serialize import serialize
from storage.storagemanager import StorageManager
from urllib.parse import quote


class BaseResource(Resource):
    known_collections = []
    schemas_by_type = {
        "entity": entity_schema,
        "key_value_store": key_value_store_schema,
        "mediafile": mediafile_schema,
        "saved_search": saved_search_schema,
    }
    map_name_to_relation = {}
    map_name_to_db_value = {}

    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        self.collection_api_url = getenv("COLLECTION_API_URL")
        self.image_api_url_ext = getenv("IMAGE_API_URL_EXT")
        self.storage_api_url = getenv("STORAGE_API_URL")
        self.storage_api_url_ext = getenv("STORAGE_API_URL_EXT")
        self.tenant_defining_types = getenv("TENANT_DEFINING_TYPES")
        self.tenant_defining_types = (
            self.tenant_defining_types.split(",") if self.tenant_defining_types else []
        )
        self.auto_create_tenants = getenv("AUTO_CREATE_TENANTS", False)

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
            404,
            message=f"{get_error_code(ErrorCode.ITEM_NOT_FOUND_IN_COLLECTION, get_read())} | id:{id} | collection:{collection} - Item with id {id} doesn't exist in collection {collection}",
        )

    def _abort_if_not_valid_json(self, type, json):
        if validation_error := validate_json(json, self.schemas_by_type.get(type)):
            abort(
                400,
                message=f"{get_error_code(ErrorCode.INVALID_FORMAT_FOR_TYPE, get_write())} | type:{type} - {type} doesn't have a valid format. {validation_error}",
            )

    def _abort_if_not_valid_type(self, item, type):
        if type and "type" in item and item["type"] != type:
            abort(
                400,
                message=f"{get_error_code(ErrorCode.INVALID_TYPE, get_write())} - Item has the wrong type",
            )

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

    def _check_if_collection_name_exists(self, collection, is_validating_content=False):
        try:
            if is_validating_content:
                item = get_user_context().bag.get("requested_item", None)
            else:
                item = get_user_context().bag.pop("requested_item", None)
                get_user_context().bag["item_being_processed"] = deepcopy(item)
        except Exception:
            pass
        if collection in self.known_collections:
            return
        if collection not in self.storage.get_existing_collections():
            abort(
                400,
                message=f"{get_error_code(ErrorCode.COLLECTION_NOT_FOUND, get_read())} | collection:{collection} - Collection {collection} does not exist.",
            )
        self.known_collections.append(collection)

    def _check_if_collection_and_item_exists(
        self, collection, id, item=None, is_validating_content=False
    ):
        try:
            if is_validating_content:
                item = item or get_user_context().bag.get("requested_item", None)
            else:
                item = item or get_user_context().bag.pop("requested_item", None)
                get_user_context().bag["item_being_processed"] = deepcopy(item)
        except Exception:
            pass
        if item:
            return item
        elif collection:
            self._check_if_collection_name_exists(collection, is_validating_content)
            return self._abort_if_item_doesnt_exist(collection, id)
        else:
            resolve_collections = get_user_context().bag["collection_resolver"]
            collections = resolve_collections(collection=collection, id=id)
            for collection in collections:
                if item := self.storage.get_item_from_collection_by_id(collection, id):
                    return item
            else:
                abort(
                    404,
                    message=f"{get_error_code(ErrorCode.ITEM_NOT_FOUND, get_read())} | id:{id} - Item with id {id} does not exist.",
                )

    def _count_children_from_mediafile(self, parent_mediafile, count=0):
        relations = self.storage.get_collection_item_relations(
            "mediafiles", parent_mediafile["_id"]
        )
        for relation in relations:
            if relation.get("type") == "hasChild":
                child_mediafile = self.storage.get_item_from_collection_by_id(
                    "mediafiles", relation["key"]
                )
                if child_mediafile:
                    count += 1
                    return self._count_children_from_mediafile(child_mediafile, count)
        return count

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

    def _create_mediafile_for_entity(
        self, entity, filename, metadata=None, relations=None, dry_run=False
    ):
        content = {
            "filename": filename,
            "date_created": datetime.now(timezone.utc),
            "version": 1,
            "thumbnail_file_location": f"/iiif/3/{filename}/full/,150/0/default.jpg",
            "original_file_location": f"/download/{filename}",
        }
        if metadata:
            content["metadata"] = metadata
        if dry_run:
            return content
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        self.storage.add_mediafile_to_collection_item(
            "entities",
            get_raw_id(entity),
            mediafile["_id"],
            mediafile_is_public(mediafile),
        )
        if relations:
            self.storage.add_relations_to_collection_item(
                "mediafiles", get_raw_id(mediafile), relations
            )
        signal_entity_changed(get_rabbit(), entity)
        return mediafile

    def _create_response_according_accept_header(
        self, response_data, accept_header=None, status_code=200, spec="elody"
    ):
        if spec != "elody":
            return response_data, status_code

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
        if not parse_string_to_bool(self.auto_create_tenants):
            return
        if self.tenant_defining_types and entity["type"] in self.tenant_defining_types:
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
            if tenant_id := get_user_context().x_tenant.id:
                try:
                    self._link_entity_to_tenant(entity["_id"], tenant_id)
                except Exception as ex:
                    abort(400, message=str(ex))

    def _create_ticket(self, filename, mediafile_id=None, exp=None):
        ticket = {
            "bucket": self._get_upload_bucket(),
            "location": self._get_upload_location(filename),
            "type": "ticket",
            "metadata": [
                {
                    "key": "ttl",
                    "value": (
                        datetime.now(tz=timezone.utc)
                        + timedelta(
                            seconds=int(getenv("TICKET_LIFESPAN", 3600))
                            + int(getenv("TICKET_CLEANUP", 86400))
                        )
                    ).timestamp(),
                }
            ],
        }
        try:
            user_context = get_user_context()
            ticket["user"] = user_context.email if user_context else "default_uploader"
        except Exception:
            ticket["user"] = "default_uploader"
        if exp:
            ticket["exp"] = exp
        else:
            ticket["exp"] = (
                datetime.now(tz=timezone.utc)
                + timedelta(seconds=int(getenv("TICKET_LIFESPAN", 3600)))
            ).timestamp()
        if mediafile_id:
            ticket["mediafile_id"] = mediafile_id
        return self.storage.save_item_to_collection(
            "abstracts", ticket, only_return_id=True, create_sortable_metadata=False
        )

    def _create_user_from_idp(self, assign_roles_from_idp=True, roles_per_tenant=None):
        metadata = [{"key": "idp_user_id", "value": get_user_context().id}]
        if get_user_context().email:
            metadata.append({"key": "email", "value": get_user_context().email})
        if get_user_context().preferred_username:
            metadata.append(
                {
                    "key": "preferred_username",
                    "value": get_user_context().preferred_username,
                }
            )
        user = {
            "identifiers": list(
                {
                    get_user_context().id,
                    get_user_context().email,
                    get_user_context().preferred_username,
                }
            ),
            "metadata": metadata,
            "relations": [],
            "type": "user",
        }
        user_collection = (
            get_object_configuration_mapper().get("user").crud()["collection"]
        )
        user = self.storage.save_item_to_collection(user_collection, user)
        if assign_roles_from_idp:
            self._sync_roles_from_idp(
                user,
                (
                    roles_per_tenant
                    if roles_per_tenant
                    else {"tenant:super": get_user_context().x_tenant.roles}
                ),
            )
        return user

    def _decorate_entity(self, entity):
        default_entity = {
            "type": "asset",
        }
        return default_entity | entity

    def _delete_tenant(self, entity):
        if self.tenant_defining_types and entity["type"] in self.tenant_defining_types:
            self.storage.delete_item_from_collection(
                "entities", f'tenant:{entity["_id"]}'
            )

    def _get_children_from_mediafile(self, parent_mediafile, linked_mediafiles=[]):
        relations = self.storage.get_collection_item_relations(
            "mediafiles", parent_mediafile["_id"]
        )
        for relation in relations:
            if relation.get("type") == "hasChild":
                child_mediafile = self.storage.get_item_from_collection_by_id(
                    "mediafiles", relation["key"]
                )
                if child_mediafile:
                    linked_mediafiles.append(child_mediafile)
                    return self._get_children_from_mediafile(
                        child_mediafile, linked_mediafiles
                    )
        return linked_mediafiles

    # this method will slowly transform into a simple unified method
    def _get_content_according_content_type(
        self,
        request,
        object_type="entity",
        content=None,
        item={},
        spec="elody",
        v2=False,
    ):
        if not content:
            content_type = request.content_type
            match content_type:
                case "application/json":
                    content = request.get_json()
                case "text/csv":
                    csv = request.get_data(as_text=True)
                    parsed_csv = CSVSingleObject(csv, object_type)
                    if object_type in ["metadata", "relations"]:
                        return getattr(parsed_csv, object_type)
                    return parsed_csv.get_type(object_type)
                case _:
                    content = request.get_json()
        if v2 and (item or content.get("type")):
            type = item.get("type", content.get("type"))
            schema_type = get_object_configuration_mapper().get(type).SCHEMA_TYPE
            return serialize(
                deepcopy(content),
                type=type,
                from_format=serialize.get_format(spec, request.args),
                to_format=(
                    item.get("storage_format", item)
                    .get("schema", {})
                    .get("type", "elody")
                    if item
                    else schema_type
                ),
            )
        else:
            return content

    def _get_date_from_object(self, object_dict, date_field):
        now = datetime.now(timezone.utc)
        if not date_field in object_dict:
            return now
        try:
            return datetime.strptime(object_dict.get(date_field), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            if isinstance(object_dict.get(date_field), datetime):
                return object_dict.get(date_field)
            return now

    def get_filters_from_query_parameters(self, request, **_):
        filters = []
        access_restricting_filters = get_user_context().access_restrictions.filters
        if access_restricting_filters:
            for filter in access_restricting_filters:
                filters.append(filter)
        if type := request.args.get("type"):
            filters.append({"type": "type", "value": type})
        return filters

    def get_original_items_from_csv(self, csv_data, collection="entities"):
        csv_file = io.StringIO(csv_data)
        reader = csv.reader(csv_file)
        header = next(reader)
        items = []

        for row in reader:
            identifier = None
            for index, value in enumerate(row):
                key = header[index]
                if key == "identifier":
                    identifier = value
            if identifier is not None:
                item = self.storage.get_item_from_collection_by_id(
                    collection, identifier
                )
                items.append(item)
        return items

    def get_original_items_from_json(self, updated_items, collection="entities"):
        items = []
        for item in updated_items:
            item = self.storage.get_item_from_collection_by_id(
                collection, get_raw_id(item)
            )
            items.append(item)
        return items

    def get_parent_mediafile(self, mediafile, parent_mediafile=None):
        relations = self.storage.get_collection_item_relations(
            "mediafiles", mediafile["_id"]
        )
        for relation in relations:
            if relation.get("type") == "belongsToParent":
                parent_mediafile = self.storage.get_item_from_collection_by_id(
                    "mediafiles", relation["key"]
                )
                if parent_mediafile:
                    return self.get_parent_mediafile(parent_mediafile, parent_mediafile)
        return parent_mediafile

    def _get_tenant_label(self, item):
        return get_item_metadata_value(item, "name")

    def get_downloadset_ttl(self, mediafile):
        ttl = None
        if mediafile.get("mimetype") == "application/zip":
            for relation in mediafile.get("relations", []):
                if "is_downloadset" in relation:
                    asset = self.storage.get_item_from_collection_by_id(
                        "entities", relation.get("key")
                    )
                    ttl = get_item_metadata_value(asset, "ttl")
        return ttl

    def _get_objects_from_ids_in_body_or_query(self, collection, request):
        if ids := request.args.get("ids"):
            ids = ids.split(",")
        if not ids:
            try:
                ids = request.get_json().get("identifiers")
            except InvalidObjectException as ex:
                return str(ex), 400
        if not ids:
            abort(
                422,
                message=f"{get_error_code(ErrorCode.INVALID_INPUT, get_write())} - No ids to delete given.",
            )
        objects = list()
        for id in ids:
            objects.append(self._check_if_collection_and_item_exists(collection, id))
        return objects

    def update_object_values_from_csv(self, csv_data, collection="entities"):
        csv_file = io.StringIO(csv_data)
        reader = csv.reader(csv_file)
        header = next(reader)

        items, updated_values = self.process_csv_rows(reader, header, collection)
        self.update_items(items, updated_values)
        return items

    def process_csv_rows(self, reader, header, collection):
        items = []
        updated_values = {}
        seen_identifiers = set()

        for row in reader:
            row_data = {header[index]: value for index, value in enumerate(row)}
            identifier = row_data.get("identifiers")

            if identifier:
                item = self.storage.get_item_from_collection_by_id(
                    collection, identifier
                )
                if not item:
                    abort(
                        400,
                        message=f"Item with {identifier} doesn't exist for {collection} in the uploaded csv.",
                    )
                item_identifiers = set(item.get("identifiers", []))

                if not item_identifiers.intersection(seen_identifiers):
                    items.append(item)
                    seen_identifiers.update(item_identifiers)
                updated_values[identifier] = row_data
        return items, updated_values

    def update_items(self, items, updated_values):
        for item in items:
            item_id = get_raw_id(item)
            if item_id in updated_values:
                updates = updated_values[item_id]
                item_metadata = item.get("metadata", [])
                item_relations = item.get("relations", [])
                self.update_metadata(item_metadata, updates)
                self.update_relations(item_relations, updates)
                item["metadata"] = item_metadata
                item["relations"] = item_relations

    def update_relations(self, item_relations, updates):
        for key, value in updates.items():
            if key == "identifier" or key == "identifiers":
                continue
            if key in self.map_name_to_relation:
                key = self.map_name_to_relation.get(key)
            if not re.match(r"^has\w+|^is\w+", key):
                continue
            relation_found = False
            for relation in item_relations:
                if relation.get("type") == key:
                    relation_item = self.get_relation_item_for_key_by_metadata(
                        key, value
                    )
                    relation["key"] = get_raw_id(relation_item)
                    relation_found = True
                    break

            if not relation_found:
                relation_item = self.get_relation_item_for_key_by_metadata(key, value)
                if relation_item:
                    new_relation = {"key": get_raw_id(relation_item), "type": key}
                    item_relations.append(new_relation)

    def get_relation_item_for_key_by_metadata(self, key, value):
        db_key = self.map_name_to_db_value.get(key, {}).get("db_key")
        relation_item = self.storage.get_item_from_collection_by_metadata(
            "entities",
            db_key,
            value,
            self.map_name_to_db_value.get(key, {}).get("type"),
        )

        return relation_item

    def update_metadata(self, item_metadata, updates):
        for key, value in updates.items():
            if key == "identifier" or key == "identifiers":
                continue
            if key in self.map_name_to_relation:
                continue

            metadata_found = False
            for metadata in item_metadata:
                if metadata.get("key") == key:
                    metadata["value"] = parse_string_to_bool(value)
                    metadata_found = True
                    break

            if not metadata_found:
                new_metadata = {"key": key, "value": parse_string_to_bool(value)}
                item_metadata.append(new_metadata)

    def _get_upload_bucket(self):
        return getenv("MINIO_BUCKET")

    def _get_upload_location(self, filename):
        if tenant_id := get_user_context().x_tenant.id:
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
                    entity[mediafile_type] = (
                        f"{self.storage_api_url_ext}/download-with-ticket/{quote(mediafile_filename)}?ticket_id={ticket_id}"
                    )
            if "primary_thumbnail_location" in entity:
                entity["primary_thumbnail_location"] = (
                    f'{self.image_api_url_ext}{entity["primary_thumbnail_location"]}'
                )
        return entities

    def _inject_api_urls_into_mediafiles(self, mediafiles, internal=False):
        for mediafile in mediafiles:
            for mediafile_type in ["original_file_location", "transcode_file_location"]:
                if mediafile_type in mediafile:
                    mediafile_filename = mediafile[mediafile_type]
                    mediafile_filename = mediafile_filename.split("/download/")[-1]
                    ttl = self.get_downloadset_ttl(mediafile)
                    ticket_id = self._create_ticket(mediafile_filename, exp=ttl)
                    base_url = (
                        self.storage_api_url if internal else self.storage_api_url_ext
                    )
                    mediafile[mediafile_type] = (
                        f"{base_url}/download-with-ticket/{quote(mediafile_filename)}?ticket_id={ticket_id}"
                    )
            if "thumbnail_file_location" in mediafile:
                mediafile["thumbnail_file_location"] = (
                    f'{self.image_api_url_ext}{mediafile["thumbnail_file_location"]}'
                )
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

    def _resolve_collections(self, **kwargs):
        if not kwargs.get("collection"):
            return ["entities", "mediafiles"]
        return [kwargs.get("collection")]

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
                        "transcountde_file_location"
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
        anonymous_user_id = getenv("ANONYMOUS_USER_ID", "anonymous_user")
        if get_raw_id(user) == anonymous_user_id:
            return user
        id = user["_id"]
        user_collection = (
            get_object_configuration_mapper().get("user").crud()["collection"]
        )

        if len(new) > 0:
            self.storage.add_relations_to_collection_item(user_collection, id, new)
        if len(updated) > 0:
            self.storage.patch_collection_item_relations(user_collection, id, updated)
        if len(deleted) > 0:
            self.storage.delete_collection_item_relations(user_collection, id, deleted)
        user["relations"] = [*new, *updated, *untouched]
        return user

    def _update_tenant(self, entity, new_data):
        if (
            not self.tenant_defining_types
            or entity["type"] not in self.tenant_defining_types
        ):
            return
        if self._get_tenant_label(entity) == self._get_tenant_label(new_data):
            return
        metadata = [{"key": "label", "value": self._get_tenant_label(new_data)}]
        self.storage.patch_collection_item_metadata(
            "entities", f"tenant:{get_raw_id(entity)}", metadata
        )

    def _update_date_updated_and_last_editor(self, collection, id):
        content_date_updated = {"date_updated": datetime.now(timezone.utc)}
        content_last_editor_updated = {
            "last_editor": get_user_context().email or "default_uploader"
        }
        return self.storage.patch_item_from_collection(
            collection, id, {**content_date_updated, **content_last_editor_updated}
        )
