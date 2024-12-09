import mappers

from elody.error_codes import ErrorCode, get_error_code, get_write
from elody.exceptions import InvalidObjectException, NonUniqueException
from elody.csv import CSVMultiObject
from elody.exceptions import ColumnNotFoundException
from elody.util import (
    get_raw_id,
    mediafile_is_public,
    signal_entity_changed,
    signal_entity_deleted,
    signal_mediafile_changed,
    signal_mediafile_deleted,
    signal_mediafiles_added_for_entity,
    signal_relations_deleted_for_entity,
)
from flask import after_this_request, request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies, get_user_context
from rabbit import get_rabbit
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
    GenericObjectMetadata,
    GenericObjectMetadataKey,
    GenericObjectRelations,
)
from urllib.parse import quote


class Entity(GenericObject):
    @apply_policies(RequestContext(request))
    def get(self, spec="elody", filters=None):
        accept_header = request.headers.get("Accept")
        exclude_non_editable_fields = request.args.get(
            "exclude_non_editable_fields", "false"
        ).lower() in ["true", "1"]
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        order_by = request.args.get("order_by", None)
        ascending = request.args.get("asc", 1, int)
        skip_relations = request.args.get("skip_relations", 0, int)
        filters = filters if filters else {}
        if item_type := request.args.get("type"):
            filters["type"] = item_type
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        access_restricting_filters = get_user_context().access_restrictions.filters
        if isinstance(access_restricting_filters, list):
            for filter in access_restricting_filters:
                filters.update(filter)
        entities = self.storage.get_entities(
            skip,
            limit,
            skip_relations,
            filters,
            order_by,
            ascending,
        )
        type_filter = f"type={item_type}&" if item_type else ""
        entities["limit"] = limit
        if skip + limit < entities["count"]:
            entities["next"] = (
                f"/entities?{type_filter}skip={skip + limit}&limit={limit}&skip_relations={skip_relations}"
            )
        if skip > 0:
            entities["previous"] = (
                f"/entities?{type_filter}skip={max(0, skip - limit)}&limit={limit}&skip_relations={skip_relations}"
            )
        entities["results"] = self._inject_api_urls_into_entities(entities["results"])
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(entities),
                accept_header,
                "entities",
                fields,
                spec,
                request.args,
                exclude_non_editable_fields=exclude_non_editable_fields,
            ),
            accept_header,
        )

    @apply_policies(RequestContext(request))
    def post(self, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
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
            try:
                content = self._get_content_according_content_type(request)
            except InvalidObjectException as ex:
                return str(ex), 400
        accept_header = request.headers.get("Accept")
        entity = self._decorate_entity(content)
        entity["date_created"] = self._get_date_from_object(entity, "date_created")
        entity["date_updated"] = self._get_date_from_object(entity, "date_updated")
        entity["created_by"] = get_user_context().email or "default_uploader"
        entity["version"] = 1
        if not linked_data_request:
            self._abort_if_not_valid_json("entity", entity)
        try:
            entity_relations = entity.get("relations", [])
            if entity_relations:
                entity.pop("relations")
                entity = self.storage.save_item_to_collection("entities", entity)
                self.storage.add_relations_to_collection_item(
                    "entities", get_raw_id(entity), entity_relations
                )
                entity = self.storage.get_item_from_collection_by_id(
                    "entities", get_raw_id(entity)
                )
            else:
                entity = self.storage.save_item_to_collection("entities", entity)
            if accept_header == "text/uri-list":
                response = ""
            else:
                response = entity
        except NonUniqueException as ex:
            return ex.args[0], 409
        if create_mediafile:
            for mediafile_filename in mediafile_filenames:
                mediafile = self._create_mediafile_for_entity(
                    entity,
                    mediafile_filename,
                )
                if accept_header == "text/uri-list":
                    ticket_id = self._create_ticket(mediafile_filename)
                    response += f"{self.storage_api_url}/upload-with-ticket/{quote(mediafile_filename)}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}\n"
        self._create_tenant(entity)
        signal_entity_changed(get_rabbit(), entity)
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )

    @apply_policies(RequestContext(request))
    def put(self, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        content = None
        if request.headers.get("content-type") == "text/csv":
            csv_data = request.get_data(as_text=True)
            content = self.update_object_values_from_csv(csv_data)
            entities = self.get_original_items_from_csv(csv_data)
        else:
            entities_from_body = self._get_content_according_content_type(
                request, "entities"
            )
            entities = self.get_original_items_from_json(entities_from_body)
        entity_dict = {get_raw_id(entity): entity for entity in entities}
        updated_entities = super().put("entities", content=content)
        for updated_entity in updated_entities:
            entity_id = get_raw_id(updated_entity)
            if entity_id in entity_dict:
                entity = entity_dict[entity_id]
                self._update_tenant(entity, updated_entity)
                signal_entity_changed(get_rabbit(), updated_entity)
        return updated_entities, 201


class EntityDetail(GenericObjectDetail):
    def get_entity_detail(self, id, spec="elody"):
        entity = super().get_object_detail("entities", id)
        entity = self._set_entity_mediafile_and_thumbnail(entity)
        if not request.args.get("skip_relations", 0, int):
            entity = self._add_relations_to_metadata(entity)
        return self._inject_api_urls_into_entities([entity])[0]

    @apply_policies(RequestContext(request))
    def get(self, id, spec="elody"):
        exclude_non_editable_fields = request.args.get(
            "exclude_non_editable_fields", "false"
        ).lower() in ["true", "1"]
        entity = self.get_entity_detail(id, spec)
        accept_header = request.headers.get("Accept")
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                self._inject_api_urls_into_entities([entity])[0],
                accept_header,
                "entity",
                fields,
                spec,
                request.args,
                exclude_non_editable_fields,
            ),
            accept_header,
        )

    @apply_policies(RequestContext(request))
    def put(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = None
        if request.headers.get("content-type") == "text/csv":
            csv_data = request.get_data(as_text=True)
            content = self.update_object_values_from_csv(csv_data)[0]
        updated_entity = super().put("entities", id, item=entity, content=content)[0]
        self._update_tenant(entity, updated_entity)
        signal_entity_changed(get_rabbit(), updated_entity)
        return updated_entity, 201

    @apply_policies(RequestContext(request))
    def patch(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = self._abort_if_item_doesnt_exist("entities", id)
        updated_entity = super().patch("entities", id, item=entity)[0]
        self._update_tenant(entity, updated_entity)
        signal_entity_changed(get_rabbit(), updated_entity)
        return updated_entity, 201

    @apply_policies(RequestContext(request))
    def delete(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        if request.args.get("delete_mediafiles", 0, int):
            mediafiles = self.storage.get_collection_item_mediafiles(
                "entities", get_raw_id(entity)
            )
            for mediafile in mediafiles:
                linked_entities = self.storage.get_mediafile_linked_entities(mediafile)
                self.storage.delete_item_from_collection(
                    "mediafiles", get_raw_id(mediafile)
                )
                if tenant_id := get_user_context().x_tenant.id:
                    mediafile["filename"] = f"{tenant_id}/{mediafile['filename']}"
                signal_mediafile_deleted(get_rabbit(), mediafile, linked_entities)
        response = super().delete("entities", id)
        self._delete_tenant(entity)
        signal_entity_deleted(get_rabbit(), entity)
        return response


class EntityMediafiles(GenericObjectDetail, GenericObject):
    @apply_policies(RequestContext(request))
    def get(self, id):
        exclude_non_editable_fields = request.args.get(
            "exclude_non_editable_fields", "false"
        ).lower() in ["true", "1"]
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        asc = request.args.get("asc", 1, int)
        order_by = request.args.get("order_by", "order", str)
        accept_header = request.headers.get("Accept")
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        entity = super().get_object_detail("entities", id)
        mediafiles = dict()
        mediafiles["count"] = self.storage.get_collection_item_mediafiles_count(
            entity["_id"]
        )
        mediafiles["results"] = self.storage.get_collection_item_mediafiles(
            "entities", get_raw_id(entity), skip, limit, asc, order_by
        )
        mediafiles["limit"] = limit
        mediafiles["skip"] = skip
        if skip + limit < mediafiles["count"]:
            mediafiles["next"] = (
                f"/entities/{id}/mediafiles?skip={skip + limit}&limit={limit}"
            )
        if skip > 0:
            mediafiles["previous"] = (
                f"/entities/{id}/mediafiles?skip={max(0, skip - limit)}&limit={limit}"
            )
        self._inject_api_urls_into_mediafiles(mediafiles["results"])

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                mediafiles,
                accept_header,
                "mediafiles",
                fields,
                "elody",
                request.args,
                exclude_non_editable_fields,
            ),
            accept_header,
        )
        return mediafiles

    @apply_policies(RequestContext(request))
    def post(self, id):
        entity = self._check_if_collection_and_item_exists("entities", id)
        content = self._get_content_according_content_type(request, "mediafile")
        mediafiles = content if isinstance(content, list) else [content]
        accept_header = request.headers.get("Accept")
        if accept_header == "text/uri-list":
            response = ""
        else:
            response = list()
        for mediafile in mediafiles:
            self._abort_if_not_valid_json("mediafile", mediafile)
            if any(x in mediafile for x in ["_id", "_key"]):
                mediafile = self._abort_if_item_doesnt_exist(
                    "mediafiles", get_raw_id(mediafile)
                )
            mediafile["date_created"] = self._get_date_from_object(
                mediafile, "date_created"
            )
            mediafile["date_updated"] = self._get_date_from_object(
                mediafile, "date_updated"
            )
            relation_properties = mediafile.pop("relation_properties", None)
            mediafile = self.storage.save_item_to_collection("mediafiles", mediafile)
            mediafile = self.storage.add_mediafile_to_collection_item(
                "entities",
                get_raw_id(entity),
                mediafile["_id"],
                mediafile_is_public(mediafile),
                relation_properties,
            )
            if accept_header == "text/uri-list":
                ticket_id = self._create_ticket(mediafile["filename"])
                response += f"{self.storage_api_url}/upload-with-ticket/{quote(mediafile['filename'])}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}\n"
            else:
                response.append(mediafile)
        signal_mediafiles_added_for_entity(get_rabbit(), entity, mediafiles)
        signal_entity_changed(get_rabbit(), entity)
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )

    @apply_policies(RequestContext(request))
    def put(self, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        content = None
        if request.headers.get("content-type") == "text/csv":
            csv_data = request.get_data(as_text=True)
            content = self.update_object_values_from_csv(
                csv_data, collection="mediafiles"
            )
            mediafiles = self.get_original_items_from_csv(
                csv_data, collection="mediafiles"
            )
        else:
            mediafiles_from_body = self._get_content_according_content_type(
                request, "mediafiles"
            )
            mediafiles = self.get_original_items_from_json(
                mediafiles_from_body, collection="mediafiles"
            )
        mediafile_dict = {get_raw_id(mediafile): mediafile for mediafile in mediafiles}
        updated_mediafiles = super(GenericObjectDetail, self).put(
            "mediafiles", content=content
        )
        for updated_mediafile in updated_mediafiles:
            mediafile_id = get_raw_id(updated_mediafile)
            if mediafile_id in mediafile_dict:
                old_mediafile = mediafile_dict[mediafile_id]
                signal_mediafile_changed(get_rabbit(), old_mediafile, updated_mediafile)
        return updated_mediafiles, 201


class EntityMediafilesCreate(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def post(self, id):
        entity = super().get_object_detail("entities", id)
        content = request.get_json()
        self._abort_if_not_valid_json("mediafile", content)
        content["original_file_location"] = f'/download/{content["filename"]}'
        content["thumbnail_file_location"] = (
            f'/iiif/3/{content["filename"]}/full/,150/0/default.jpg'
        )
        content["user"] = get_user_context().email or "default_uploader"
        content["date_created"] = self._get_date_from_object(content, "date_created")
        content["date_updated"] = self._get_date_from_object(content, "date_updated")
        content["version"] = 1
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        upload_location = f'{self.storage_api_url}/upload/{content["filename"]}?id={get_raw_id(mediafile)}'
        self.storage.add_mediafile_to_collection_item(
            "entities",
            get_raw_id(entity),
            mediafile["_id"],
            mediafile_is_public(mediafile),
        )
        signal_entity_changed(get_rabbit(), entity)

        @after_this_request
        def add_header(response):
            response.headers["Warning"] = "299 - Deprecated API"
            return response

        return upload_location, 201


# super is called using MRO (method resolution order)
class EntityMetadata(GenericObjectDetail, GenericObjectMetadata):
    @apply_policies(RequestContext(request))
    def get(self, id, spec="elody"):
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        return super(GenericObjectDetail, self).get("entities", id, fields=fields)

    @apply_policies(RequestContext(request))
    def post(self, id, spec="elody"):
        entity = super().get("entities", id)
        metadata = super(GenericObjectDetail, self).post("entities", id)
        signal_entity_changed(get_rabbit(), entity)
        return metadata, 201

    @apply_policies(RequestContext(request))
    def put(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        metadata = super(GenericObjectDetail, self).put("entities", id)[0]
        self._update_tenant(entity, {"metadata": metadata})
        signal_entity_changed(get_rabbit(), entity)
        return metadata, 201

    @apply_policies(RequestContext(request))
    def patch(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        metadata = super(GenericObjectDetail, self).patch("entities", id)[0]
        self._update_tenant(entity, {"metadata": metadata})
        signal_entity_changed(get_rabbit(), entity)
        return metadata, 201


class EntityMetadataKey(GenericObjectDetail, GenericObjectMetadataKey):
    @apply_policies(RequestContext(request))
    def get(self, id, key):
        super().get("entities", id)
        return super(GenericObjectDetail, self).get("entities", id, key)

    @apply_policies(RequestContext(request))
    def delete(self, id, key):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        response = super(GenericObjectDetail, self).delete("entities", id, key)
        signal_entity_changed(get_rabbit(), entity)
        return response


class EntityRelations(GenericObjectDetail, GenericObjectRelations):
    @apply_policies(RequestContext(request))
    def get(self, id, spec="elody"):
        return super(GenericObjectDetail, self).get("entities", id)

    @apply_policies(RequestContext(request))
    def post(self, id, spec="elody"):
        entity = super().get("entities", id)
        relations = super(GenericObjectDetail, self).post("entities", id)[0]
        mediafiles = []
        for relation in relations:
            if relation.get("type") == "hasMediafile":
                mediafile = self.storage.get_item_from_collection_by_id(
                    "mediafiles", id
                )
                mediafiles.append(mediafile)
        if mediafiles:
            signal_mediafiles_added_for_entity(get_rabbit(), entity, mediafiles)
        signal_entity_changed(get_rabbit(), entity)
        return relations, 201

    @apply_policies(RequestContext(request))
    def put(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        relations = super(GenericObjectDetail, self).put("entities", id)[0]
        signal_entity_changed(get_rabbit(), entity)
        return relations, 201

    @apply_policies(RequestContext(request))
    def patch(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        relations = super(GenericObjectDetail, self).patch("entities", id)[0]
        signal_entity_changed(get_rabbit(), entity)
        return relations, 201

    @apply_policies(RequestContext(request))
    def delete(self, id, spec="elody"):
        relations = self._get_content_according_content_type(request, "relations")
        entity = super().get("entities", id)
        response = super(GenericObjectDetail, self).delete("entities", id)
        signal_entity_changed(get_rabbit(), entity)
        signal_relations_deleted_for_entity(get_rabbit(), entity, relations)
        return response


class EntityRelationsAll(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        entity = super().get("entities", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(
            "entities", get_raw_id(entity), include_sub_relations=True
        )


class EntitySetPrimaryMediafile(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def put(self, id, mediafile_id):
        entity = super().get("entities", id)
        self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        self.storage.set_primary_field_collection_item(
            "entities", get_raw_id(entity), mediafile_id, "is_primary"
        )
        signal_entity_changed(get_rabbit(), entity)
        return "", 204


class EntitySetPrimaryThumbnail(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def put(self, id, mediafile_id):
        entity = super().get("entities", id)
        self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        self.storage.set_primary_field_collection_item(
            "entities", get_raw_id(entity), mediafile_id, "is_primary_thumbnail"
        )
        signal_entity_changed(get_rabbit(), entity)
        return "", 204


class EntityOrder(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        entity = super().get_object_detail("entities", id)
        raw_id = get_raw_id(entity)

        if entity.get("type") == "asset":
            data_type = "mediafiles"
            items = self.storage.get_collection_item_mediafiles("entities", raw_id)
            fields = ["identifier", "type", "filename"]
        else:
            data_type = "entities"
            relations = self.storage.get_collection_item_relations("entities", raw_id)
            items = [
                self.storage.get_item_from_collection_by_id(
                    "entities", relation.get("key")
                )
                for relation in relations
                if relation.get("type") == "hasAsset"
            ]
            fields = ["identifiers", "type"]

        data = {}
        data["results"] = items
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(data),
                "text/csv",
                data_type,
                fields,
                "elody",
                request.args,
            ),
            "text/csv",
        )

    @apply_policies(RequestContext(request))
    def post(self, id):
        entity = super().get("entities", id)
        if request.content_type != "text/csv":
            raise Exception(
                f"{get_error_code(ErrorCode.UNSUPPORTED_TYPE, get_write())} | type:{request.content_type} - Unsupported type {request.content_type}"
            )
        entity_type = self._determine_child_entity_type(entity)
        relation_type, relation_type_reverse = self._determine_relation_types(
            entity_type
        )
        csv_type = {"mediafiles": "mediafiles", "entities": "entities"}.get(entity_type)
        parsed_csv = self._get_parsed_csv(request.get_data(as_text=True), entity_type)
        items = self._get_items_from_csv(entity_type, parsed_csv)
        items_dict = {item["matching_id"]: item for item in items}
        items_order = {
            item["matching_id"]: csv_order + 1 for csv_order, item in enumerate(items)
        }

        self.update_sort_order_on_relations(
            id,
            entity.get("relations", []),
            relation_type,
            relation_type_reverse,
            csv_type,
            items_dict,
            items_order,
        )

        self.storage.update_item_from_collection("entities", id, entity)
        return super().get("entities", id)

    # Item with type asset will have mediafiles as children and other type e.g assetParts will have assets as children
    def _determine_child_entity_type(self, entity):
        if entity.get("type") == "asset":
            entity_type = "mediafiles"
        else:
            entity_type = "entities"
        return entity_type

    def _determine_relation_types(self, entity_type):
        relation_type = {"mediafiles": "hasMediafile", "entities": "hasAsset"}.get(
            entity_type
        )
        relation_type_reverse = {
            "mediafiles": "belongsTo",
            "entities": "isAssetFor",
        }.get(entity_type)
        return relation_type, relation_type_reverse

    def _get_items_from_csv(self, entity_type, parsed_csv):
        return (
            parsed_csv.get_entities()
            if entity_type == "entities"
            else parsed_csv.get_mediafiles()
        )

    def _get_parsed_csv(self, csv, entity_type):
        try:
            return CSVMultiObject(csv, {entity_type: "identifier"})
        except ColumnNotFoundException:
            abort(
                422,
                message=f"{get_error_code(ErrorCode.COLUMN_NOT_FOUND, get_write())} - One or more required columns headers aren't defined",
            )

    def update_relation_metadata(self, relation, key, value):
        existing_metadata = relation.get("metadata", [])
        existing_item = next(
            (item for item in existing_metadata if item["key"] == key), None
        )

        if existing_item:
            existing_item["value"] = int(value)
        else:
            existing_metadata.append({"key": key, "value": int(value)})

        relation["metadata"] = existing_metadata

    def update_relation_sort(self, relation, key, value):
        sort_field = relation.get("sort", {})
        order = sort_field.get(key, [])

        if not order:
            relation["sort"] = {key: [{"value": int(value)}]}
        else:
            order[0]["value"] = int(value)

    def update_sort_order_on_relations(
        self,
        id,
        entity_relations,
        relation_type,
        relation_type_reverse,
        csv_type,
        items_dict,
        items_order,
    ):
        for relation in entity_relations:
            relation_item_id = relation.get("key")
            if relation.get("type") == relation_type and relation_item_id in items_dict:
                item = items_dict[relation_item_id]
                # Use the order of the CSV
                order_value = items_order[relation_item_id]
                item = self.storage.get_item_from_collection_by_id(
                    csv_type, relation_item_id
                )
                for item_relation in item.get("relations"):
                    if (
                        item_relation.get("type") == relation_type_reverse
                        and item_relation.get("key") == id
                    ):
                        self.update_relation_metadata(
                            item_relation, "order", order_value
                        )
                        self.update_relation_sort(item_relation, "order", order_value)
                self.storage.update_item_from_collection(
                    csv_type, relation_item_id, item
                )
                self.update_relation_metadata(relation, "order", order_value)
                self.update_relation_sort(relation, "order", order_value)
