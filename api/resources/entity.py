import mappers

from app import policy_factory, rabbit
from datetime import datetime, timezone
from elody.exceptions import InvalidObjectException, NonUniqueException
from elody.util import (
    get_raw_id,
    mediafile_is_public,
    signal_entity_changed,
    signal_entity_deleted,
    signal_mediafiles_added_for_entity,
)
from flask import after_this_request, request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
    GenericObjectMetadata,
    GenericObjectMetadataKey,
    GenericObjectRelations,
)


class Entity(GenericObject):
    @policy_factory.apply_policies(RequestContext(request))
    def get(self, spec="elody", filters=None):
        accept_header = request.headers.get("Accept")
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
        access_restricting_filters = (
            policy_factory.get_user_context().access_restrictions.filters
        )
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
                policy_factory.get_user_context().access_restrictions.post_request_hook(
                    entities
                ),
                accept_header,
                "entities",
                fields,
                spec,
                request.args,
            ),
            accept_header,
        )

    @policy_factory.authenticate(RequestContext(request))
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
        now = datetime.now(timezone.utc)
        entity["date_created"] = now
        entity["date_updated"] = now
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
            return ex.args[0]["errmsg"], 409
        if create_mediafile:
            for mediafile_filename in mediafile_filenames:
                mediafile = self._create_mediafile_for_entity(
                    entity,
                    mediafile_filename,
                )
                if accept_header == "text/uri-list":
                    ticket_id = self._create_ticket(mediafile_filename)
                    response += f"{self.storage_api_url}/upload-with-ticket/{mediafile_filename}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}\n"
        self._create_tenant(entity)
        signal_entity_changed(rabbit, entity)
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class EntityDetail(GenericObjectDetail):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id, spec="elody"):
        entity = super().get("entities", id)
        entity = self._set_entity_mediafile_and_thumbnail(entity)
        if not request.args.get("skip_relations", 0, int):
            entity = self._add_relations_to_metadata(entity)
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
            ),
            accept_header,
        )

    @policy_factory.authenticate(RequestContext(request))
    def put(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = self._abort_if_item_doesnt_exist("entities", id)
        updated_entity = super().put("entities", id, item=entity)[0]
        self._update_tenant(entity, updated_entity)
        signal_entity_changed(rabbit, updated_entity)
        return updated_entity, 201

    @policy_factory.authenticate(RequestContext(request))
    def patch(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = self._abort_if_item_doesnt_exist("entities", id)
        updated_entity = super().patch("entities", id, item=entity)[0]
        self._update_tenant(entity, updated_entity)
        signal_entity_changed(rabbit, updated_entity)
        return updated_entity, 201

    @policy_factory.authenticate(RequestContext(request))
    def delete(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        if request.args.get("delete_mediafiles", 0, int):
            mediafiles = self.storage.get_collection_item_mediafiles(
                "entities", get_raw_id(entity)
            )
            for mediafile in mediafiles:
                self.storage.delete_item_from_collection(
                    "mediafiles", get_raw_id(mediafile)
                )
        response = super().delete("entities", id)
        self._delete_tenant(entity)
        signal_entity_deleted(rabbit, entity)
        return response


class EntityMediafiles(GenericObjectDetail):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        asc = request.args.get("asc", 1, int)
        entity = super().get("entities", id)
        mediafiles = dict()
        mediafiles["count"] = self.storage.get_collection_item_mediafiles_count(
            entity["_id"]
        )
        mediafiles["results"] = self.storage.get_collection_item_mediafiles(
            "entities", get_raw_id(entity), skip, limit, asc
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

        return mediafiles

    @policy_factory.authenticate(RequestContext(request))
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
            mediafile = self.storage.save_item_to_collection("mediafiles", mediafile)
            mediafile = self.storage.add_mediafile_to_collection_item(
                "entities",
                get_raw_id(entity),
                mediafile["_id"],
                mediafile_is_public(mediafile),
            )
            if accept_header == "text/uri-list":
                ticket_id = self._create_ticket(mediafile["filename"])
                response += f"{self.storage_api_url}/upload-with-ticket/{mediafile['filename']}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}\n"
            else:
                response.append(mediafile)
        signal_mediafiles_added_for_entity(rabbit, entity, mediafiles)
        signal_entity_changed(rabbit, entity)
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class EntityMediafilesCreate(GenericObjectDetail):
    @policy_factory.authenticate(RequestContext(request))
    def post(self, id):
        entity = super().get("entities", id)
        content = request.get_json()
        self._abort_if_not_valid_json("mediafile", content)
        content["original_file_location"] = f'/download/{content["filename"]}'
        content["thumbnail_file_location"] = (
            f'/iiif/3/{content["filename"]}/full/,150/0/default.jpg'
        )
        content["user"] = policy_factory.get_user_context().email or "default_uploader"
        now = datetime.now(timezone.utc)
        content["date_created"] = now
        content["date_updated"] = now
        content["version"] = 1
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        upload_location = f'{self.storage_api_url}/upload/{content["filename"]}?id={get_raw_id(mediafile)}'
        self.storage.add_mediafile_to_collection_item(
            "entities",
            get_raw_id(entity),
            mediafile["_id"],
            mediafile_is_public(mediafile),
        )
        signal_entity_changed(rabbit, entity)

        @after_this_request
        def add_header(response):
            response.headers["Warning"] = "299 - Deprecated API"
            return response

        return upload_location, 201


# super is called using MRO (method resolution order)
class EntityMetadata(GenericObjectDetail, GenericObjectMetadata):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id, spec="elody"):
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        return super(GenericObjectDetail, self).get("entities", id, fields=fields)

    @policy_factory.authenticate(RequestContext(request))
    def post(self, id, spec="elody"):
        entity = super().get("entities", id)
        metadata = super(GenericObjectDetail, self).post("entities", id)
        signal_entity_changed(rabbit, entity)
        return metadata, 201

    @policy_factory.authenticate(RequestContext(request))
    def put(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        metadata = super(GenericObjectDetail, self).put("entities", id)[0]
        self._update_tenant(entity, {"metadata": metadata})
        signal_entity_changed(rabbit, entity)
        return metadata, 201

    @policy_factory.authenticate(RequestContext(request))
    def patch(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        metadata = super(GenericObjectDetail, self).patch("entities", id)[0]
        self._update_tenant(entity, {"metadata": metadata})
        signal_entity_changed(rabbit, entity)
        return metadata, 201


class EntityMetadataKey(GenericObjectDetail, GenericObjectMetadataKey):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id, key):
        super().get("entities", id)
        return super(GenericObjectDetail, self).get("entities", id, key)

    @policy_factory.authenticate(RequestContext(request))
    def delete(self, id, key):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        response = super(GenericObjectDetail, self).delete("entities", id, key)
        signal_entity_changed(rabbit, entity)
        return response


class EntityRelations(GenericObjectDetail, GenericObjectRelations):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id, spec="elody"):
        return super(GenericObjectDetail, self).get("entities", id)

    @policy_factory.authenticate(RequestContext(request))
    def post(self, id, spec="elody"):
        entity = super().get("entities", id)
        relations = super(GenericObjectDetail, self).post("entities", id)
        signal_entity_changed(rabbit, entity)
        return relations, 201

    @policy_factory.authenticate(RequestContext(request))
    def put(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        relations = super(GenericObjectDetail, self).put("entities", id)[0]
        signal_entity_changed(rabbit, entity)
        return relations, 201

    @policy_factory.authenticate(RequestContext(request))
    def patch(self, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        entity = super().get("entities", id)
        relations = super(GenericObjectDetail, self).patch("entities", id)[0]
        signal_entity_changed(rabbit, entity)
        return relations, 201

    @policy_factory.authenticate(RequestContext(request))
    def delete(self, id, spec="elody"):
        entity = super().get("entities", id)
        response = super(GenericObjectDetail, self).delete("entities", id)
        signal_entity_changed(rabbit, entity)
        return response


class EntityRelationsAll(GenericObjectDetail):
    @policy_factory.authenticate(RequestContext(request))
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
    @policy_factory.authenticate(RequestContext(request))
    def put(self, id, mediafile_id):
        entity = super().get("entities", id)
        self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        self.storage.set_primary_field_collection_item(
            "entities", get_raw_id(entity), mediafile_id, "is_primary"
        )
        signal_entity_changed(rabbit, entity)
        return "", 204


class EntitySetPrimaryThumbnail(GenericObjectDetail):
    @policy_factory.authenticate(RequestContext(request))
    def put(self, id, mediafile_id):
        entity = super().get("entities", id)
        self._abort_if_item_doesnt_exist("mediafiles", mediafile_id)
        self.storage.set_primary_field_collection_item(
            "entities", get_raw_id(entity), mediafile_id, "is_primary_thumbnail"
        )
        signal_entity_changed(rabbit, entity)
        return "", 204
