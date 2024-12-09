import mappers

from elody.error_codes import ErrorCode, get_error_code, get_write
from elody.util import (
    get_raw_id,
    mediafile_is_public,
    signal_mediafile_changed,
    signal_mediafile_deleted,
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
)
from urllib.parse import quote


class Mediafile(GenericObject):
    @apply_policies(RequestContext(request))
    def get(self, spec="elody"):
        accept_header = request.headers.get("Accept")
        exclude_non_editable_fields = request.args.get(
            "exclude_non_editable_fields", "false"
        ).lower() in ["true", "1"]
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        filters = {}
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        access_restricting_filters = get_user_context().access_restrictions.filters
        if isinstance(access_restricting_filters, dict):
            filters = {**filters, **access_restricting_filters}
        if accept_header in [
            "application/ld+json",
            "application/n-triples",
            "application/rdf+xml",
            "text/csv",
            "text/turtle",
        ]:
            mediafiles = self.storage.get_items_from_collection(
                "mediafiles",
                skip=skip,
                limit=limit,
                fields=fields,
                filters=filters,
            )
        else:
            mediafiles = super().get(
                "mediafiles", skip=skip, limit=limit, filters=filters
            )[0]
        mediafiles["results"] = self._inject_api_urls_into_mediafiles(
            mediafiles["results"]
        )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(mediafiles),
                accept_header,
                "mediafiles",
                fields,
                "elody",
                request.args,
                exclude_non_editable_fields,
            ),
            accept_header,
        )

    @apply_policies(RequestContext(request))
    def post(self):
        if request.args.get("soft", 0, int):
            return "good", 200
        accept_header = request.headers.get("Accept")
        return super().post("mediafiles", type="mediafile", accept_header=accept_header)

    @apply_policies(RequestContext(request))
    def put(self):
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
                request, collection="mediafiles"
            )
            mediafiles = self.get_original_items_from_json(
                mediafiles_from_body, collection="mediafiles"
            )
        mediafile_dict = {get_raw_id(mediafile): mediafile for mediafile in mediafiles}
        updated_mediafiles = super().put("mediafiles", content=content)
        for updated_mediafile in updated_mediafiles:
            mediafile_id = get_raw_id(updated_mediafile)
            if mediafile_id in mediafile_dict:
                old_mediafile = mediafile_dict[mediafile_id]
                signal_mediafile_changed(get_rabbit(), old_mediafile, updated_mediafile)
        return updated_mediafiles, 201


class MediafileAssets(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        mediafile = super().get("mediafiles", id)
        entities = []
        for item in self.storage.get_mediafile_linked_entities(mediafile):
            entity = self.storage.get_item_from_collection_by_id(
                "entities", item["entity_id"].removeprefix("entities/")
            )
            entity = self._set_entity_mediafile_and_thumbnail(entity)
            entity = self._add_relations_to_metadata(entity)
            entities.append(entity)
        return self._inject_api_urls_into_entities(entities)


class MediafileCopyright(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        mediafile = super().get("mediafiles", id)
        if not mediafile_is_public(mediafile):
            return "none", 200
        for item in [x for x in mediafile["metadata"] if x["key"] == "rights"]:
            if "in copyright" in item["value"].lower():
                return "limited", 200
        return "full", 200


class MediafileDetail(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        mediafile = super().get_object_detail("mediafiles", id)
        exclude_non_editable_fields = request.args.get(
            "exclude_non_editable_fields", "false"
        ).lower() in ["true", "1"]
        accept_header = request.headers.get("Accept")
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        if request.args.get("raw", 0, int):
            return mediafile
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                self._inject_api_urls_into_mediafiles([mediafile])[0],
                accept_header,
                "entity",
                fields,
                "elody",
                request.args,
                exclude_non_editable_fields,
            ),
            accept_header,
        )

    @apply_policies(RequestContext(request))
    def put(self, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        content = None
        if request.headers.get("content-type") == "text/csv":
            csv_data = request.get_data(as_text=True)
            content = self.update_object_values_from_csv(
                csv_data, collection="mediafiles"
            )[0]
        updated_mediafile = super().put(
            "mediafiles",
            id,
            item=mediafile,
            content=content,
            type="mediafile",
        )[0]
        signal_mediafile_changed(get_rabbit(), mediafile, updated_mediafile)
        return updated_mediafile, 201

    @apply_policies(RequestContext(request))
    def patch(self, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        old_mediafile = super().get("mediafiles", id)
        mediafile = super().patch("mediafiles", id, item=old_mediafile)[0]
        signal_mediafile_changed(get_rabbit(), old_mediafile, mediafile)
        return mediafile, 201

    @apply_policies(RequestContext(request))
    def delete(self, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        mediafile = super().get("mediafiles", id)
        linked_entities = self.storage.get_mediafile_linked_entities(mediafile)
        mediafile_derivatives = self._get_children_from_mediafile(mediafile)
        for mediafile_derivative in mediafile_derivatives:
            super().delete(
                "mediafiles", get_raw_id(mediafile_derivative), mediafile_derivative
            )
        response = super().delete("mediafiles", id, item=mediafile)
        if tenant_id := get_user_context().x_tenant.id:
            mediafile["filename"] = f"{tenant_id}/{mediafile['filename']}"
        signal_mediafile_deleted(get_rabbit(), mediafile, linked_entities)
        return response


class MediafileMetadata(GenericObjectDetail, GenericObjectMetadata):
    @apply_policies(RequestContext(request))
    def patch(self, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        old_mediafile = super().get("mediafiles", id)
        metadata = super(GenericObjectDetail, self).patch(
            "mediafiles",
            id,
        )[0]
        new_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        signal_mediafile_changed(get_rabbit(), old_mediafile, new_mediafile)
        return metadata, 201


class MediafileDerivatives(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        parent_mediafile = super().get("mediafiles", id)
        mediafiles = dict()
        mediafiles["count"] = self._count_children_from_mediafile(parent_mediafile)
        mediafiles["results"] = self._get_children_from_mediafile(parent_mediafile)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return mediafiles, 200

    @apply_policies(RequestContext(request))
    def post(self, id):
        old_parent_mediafile = super().get("mediafiles", id)
        content = self._get_content_according_content_type(request, "mediafiles")
        mediafiles = content if isinstance(content, list) else [content]
        accept_header = request.headers.get("Accept")
        if accept_header == "text/uri-list":
            response = ""
        else:
            response = list()
            for mediafile in mediafiles:
                self._abort_if_not_valid_json("mediafile", mediafile)
                if any(x in mediafile for x in ["_id", "_key"]):
                    mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
                    return mediafile
                mediafile = self.storage.save_item_to_collection(
                    "mediafiles", mediafile
                )
                mediafile = self.storage.add_mediafile_to_parent(
                    id,
                    mediafile["_id"],
                )
                if accept_header == "text/uri-list":
                    ticket_id = self._create_ticket(mediafile["filename"])
                    response += f"{self.storage_api_url}/upload-with-ticket/{quote(mediafile['filename'])}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}\n"
                else:
                    response.append(mediafile)
                parent_mediafile = self.storage.get_item_from_collection_by_id(
                    "mediafiles", id
                )
                signal_mediafile_changed(
                    get_rabbit(), old_parent_mediafile, parent_mediafile
                )
            return self._create_response_according_accept_header(
                response, accept_header, 201
            )

    @apply_policies(RequestContext(request))
    def delete(self, id):
        mediafile = super().get("mediafiles", id)
        parent = self.get_parent_mediafile(mediafile)
        if not parent:
            abort(
                400,
                message=f"{get_error_code(ErrorCode.ALREADY_PARENT, get_write())} | id:{get_raw_id(mediafile)} - Mediafile with id {get_raw_id(mediafile)} is already a parent",
            )
        relations = self.storage.get_collection_item_relations("mediafiles", id)
        self.storage._delete_impacted_relations("mediafiles", id)
        self.storage.delete_collection_item_relations("mediafiles", id, relations)


class MediafileDownload(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        mediafile = super().get("mediafiles", id)
        filename = mediafile["filename"]
        ticket_id = self._create_ticket(filename, id)
        return f"{self.storage_api_url_ext}/download-with-ticket/{quote(filename)}?ticket_id={ticket_id}"


class MediafileParent(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        mediafile = super().get("mediafiles", id)
        parent_mediafile = self.get_parent_mediafile(mediafile)
        return parent_mediafile, 200
