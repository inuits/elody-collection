import mappers

from configuration import get_object_configuration_mapper
from configuration import get_storage_mapper
from datetime import datetime, timezone
from elody.exceptions import NonUniqueException
from elody.util import (
    get_raw_id,
)
from flask import after_this_request, request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies, get_user_context
from resources.base_filter_resource import BaseFilterResource
from resources.base_resource import BaseResource
from validation.validate import validate


class GenericObject(BaseResource):
    @apply_policies(RequestContext(request))
    def get(
        self,
        collection,
        skip=0,
        limit=20,
        fields=None,
        filters=None,
        sort=None,
        asc=True,
        spec="elody",
    ):
        config = get_object_configuration_mapper().get(collection)
        storage_type = config.crud()["storage_type"]
        if storage_type != "http":
            self._check_if_collection_name_exists(collection)
        accept_header = request.headers.get("Accept")
        if fields is None:
            fields = {}
        if filters is None:
            filters = {}
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        access_restricting_filters = get_user_context().access_restrictions.filters
        if isinstance(access_restricting_filters, list):
            for filter in access_restricting_filters:
                filters.update(filter)
        if storage_type == "http":
            http_storage = get_storage_mapper().get("http")
            collection_data = http_storage.get_items_from_collection(self, collection)
        else:
            collection_data = self.storage.get_items_from_collection(
                collection,
                skip=skip,
                limit=limit,
                fields=fields,
                filters=filters,
                sort=sort,
                asc=asc,
            )
        count = collection_data["count"]
        results = collection_data["results"]
        collection_data = {
            "total_count": count,
            "results": results,
            "limit": limit,
            "skip": skip,
        }
        if skip + limit < count:
            collection_data["next"] = f"/{collection}?skip={skip + limit}&limit={limit}"
        if skip >= limit:
            collection_data["previous"] = (
                f"/{collection}?skip={max(0, skip - limit)}&limit={limit}"
            )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(
                    collection_data
                ),
                accept_header,
                "entities",  # specific collection name not relevant for this method
                fields,
                spec,
                request.args,
            ),
            accept_header,
        )

    @apply_policies(RequestContext(request))
    def post(
        self,
        collection,
        content=None,
        type=None,
        date_created=None,
        version=1,
        user=None,
        accept_header=None,
        spec="elody",
    ):
        if request.args.get("soft", 0, int):
            return "good", 200
        if content is None:
            content = self._get_content_according_content_type(request, collection)
        if type in self.schemas_by_type:
            self._abort_if_not_valid_json(type, content)
        if user is not None:
            content["user"] = user
        if not date_created:
            date_created = datetime.now(timezone.utc)
        content["date_created"] = date_created
        content["date_updated"] = date_created
        content["version"] = version
        try:
            item_relations = content.get("relations", [])
            if item_relations:
                content.pop("relations")
                collection_item = self.storage.save_item_to_collection(
                    collection, content
                )
                self.storage.add_relations_to_collection_item(
                    collection, get_raw_id(collection_item), item_relations
                )
                collection_item = self.storage.get_item_from_collection_by_id(
                    collection, get_raw_id(collection_item)
                )
            else:
                collection_item = self.storage.save_item_to_collection(
                    collection, content
                )
        except NonUniqueException as ex:
            return ex.args[0]["errmsg"], 409
        if accept_header == "text/uri-list":
            ticket_id = self._create_ticket(collection_item["filename"])
            response = f"{self.storage_api_url}/upload-with-ticket/{collection_item['filename'].strip()}?id={get_raw_id(collection_item)}&ticket_id={ticket_id}"
        else:
            response = collection_item
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


# POC: currently only suitable when supporting multiples specs for a client
class GenericObjectV2(BaseFilterResource, BaseResource):
    @apply_policies(RequestContext(request))
    def get(self, collection, filters=None, spec="elody"):
        self._check_if_collection_name_exists(collection)
        accept_header = request.headers.get("Accept")
        if filters is None:
            filters = self.get_filters_from_query_parameters(request)
        items = self._execute_advanced_search_with_query_v2(filters, collection)
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(items),
                accept_header,
                "entities",
                [],
                spec,
                request.args,
            ),
            accept_header,
            spec=spec,
        )

    @apply_policies(RequestContext(request))
    @validate("post", request)
    def post(
        self,
        collection,
        content=None,
        spec="elody",
    ):
        if request.args.get("soft", 0, int):
            return "good", 200
        self._check_if_collection_name_exists(collection)
        accept_header = request.headers.get("Accept")
        content = self._get_content_according_content_type(
            request, collection, content, {}, spec, True
        )
        create = (
            get_object_configuration_mapper().get(content["type"]).crud()["creator"]
        )
        item = create(content, get_user_context=get_user_context)
        try:
            item = self.storage.save_item_to_collection_v2(collection, item)
        except NonUniqueException as ex:
            return ex.args[0]["errmsg"], 409
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                item,
                accept_header,
                "entity",
                [],
                spec,
                request.args,
            ),
            accept_header,
            spec=spec,
        )[0]


class GenericObjectDetail(BaseResource):
    def get_object_detail(self, collection, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        config = get_object_configuration_mapper().get(collection)
        storage_type = config.crud()["storage_type"]
        if storage_type != "http":
            item = self._check_if_collection_and_item_exists(collection, id)
        else:
            http_storage = get_storage_mapper().get("http")
            item = http_storage.get_item_from_collection_by_id(self, collection, id)
        return item

    @apply_policies(RequestContext(request))
    def get(self, collection, id, spec="elody"):
        item = self.get_object_detail(collection, id, spec)
        accept_header = request.headers.get("Accept")
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                item,
                accept_header,
                "entity",
                [],
                spec,
                request.args,
            ),
            accept_header,
        )[0]

    @apply_policies(RequestContext(request))
    def put(
        self,
        collection,
        id,
        item=None,
        type=None,
        content=None,
        date_updated=None,
        spec="elody",
    ):
        if request.args.get("soft", 0, int):
            return "good", 200
        self._check_if_collection_name_exists(collection)
        if item is None:
            collection_item = self._abort_if_item_doesnt_exist(collection, id)
        else:
            collection_item = item
        if content is None:
            content = self._get_content_according_content_type(request, collection)
        if type is not None:
            self._abort_if_not_valid_type(collection_item, type)
            if type in self.schemas_by_type:
                self._abort_if_not_valid_json(type, content)
        if not date_updated:
            date_updated = datetime.now(timezone.utc)
        content["date_updated"] = date_updated
        content["version"] = collection_item.get("version", 0) + 1
        content["last_editor"] = get_user_context().email or "default_uploader"
        try:
            collection_item = self.storage.update_item_from_collection(
                collection, get_raw_id(collection_item), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        return collection_item, 201

    @apply_policies(RequestContext(request))
    def patch(
        self,
        collection,
        id,
        item=None,
        type=None,
        content=None,
        version=True,
        date_updated=None,
        spec="elody",
    ):
        if request.args.get("soft", 0, int):
            return "good", 200
        self._check_if_collection_name_exists(collection)
        if item is None:
            collection_item = self._abort_if_item_doesnt_exist(collection, id)
        else:
            collection_item = item
        if content is None:
            content = self._get_content_according_content_type(request, collection)
        if type is not None:
            self._abort_if_not_valid_type(collection_item, type)
            if type in self.schemas_by_type:
                self._abort_if_not_valid_json(type, content)
        if not date_updated:
            date_updated = datetime.now(timezone.utc)
        content["date_updated"] = date_updated
        if version:
            content["version"] = collection_item.get("version", 0) + 1
        content["last_editor"] = get_user_context().email or "default_uploader"
        try:
            collection_item = self.storage.patch_item_from_collection(
                collection, get_raw_id(collection_item), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        return collection_item, 201

    @apply_policies(RequestContext(request))
    def delete(
        self,
        collection,
        id,
        item=None,
        spec="elody",
    ):
        if request.args.get("soft", 0, int):
            return "good", 200
        self._check_if_collection_name_exists(collection)
        if item is None:
            collection_item = self._abort_if_item_doesnt_exist(collection, id)
        else:
            collection_item = item
        self.storage.delete_item_from_collection(
            collection, get_raw_id(collection_item)
        )
        return "", 204


# POC: currently only suitable when supporting multiples specs for a client
class GenericObjectDetailV2(BaseResource):
    @apply_policies(RequestContext(request))
    def get(self, collection, id, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        item = self._check_if_collection_and_item_exists(collection, id)
        accept_header = request.headers.get("Accept")
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                item,
                accept_header,
                "entity",
                [],
                spec,
                request.args,
            ),
            accept_header,
            spec=spec,
        )[0]

    @apply_policies(RequestContext(request))
    @validate("put", request)
    def put(
        self,
        collection,
        id,
        content=None,
        spec="elody",
    ):
        if request.args.get("soft", 0, int):
            return "good", 200
        item = self._check_if_collection_and_item_exists(collection, id)
        content = self._get_content_according_content_type(
            request, collection, content, item, spec, True
        )
        try:
            item = self.storage.put_item_from_collection(
                collection, item, content, spec
            )
        except NonUniqueException as error:
            return str(error), 409
        accept_header = request.headers.get("Accept")
        return (
            self._create_response_according_accept_header(
                mappers.map_data_according_to_accept_header(
                    item,
                    accept_header,
                    "entity",
                    [],
                    spec,
                    request.args,
                ),
                accept_header,
                spec=spec,
            )[0],
            200,
        )

    @apply_policies(RequestContext(request))
    @validate("patch", request)
    def patch(
        self,
        collection,
        id,
        content=None,
        spec="elody",
    ):
        if request.args.get("soft", 0, int):
            return "good", 200
        item = self._check_if_collection_and_item_exists(collection, id)
        content = self._get_content_according_content_type(
            request, collection, content, item, spec, True
        )
        try:
            item = self.storage.patch_item_from_collection_v2(
                collection, item, content, spec
            )
        except NonUniqueException as error:
            return str(error), 409
        accept_header = request.headers.get("Accept")
        return (
            self._create_response_according_accept_header(
                mappers.map_data_according_to_accept_header(
                    item,
                    accept_header,
                    "entity",
                    [],
                    spec,
                    request.args,
                ),
                accept_header,
                spec=spec,
            )[0],
            200,
        )

    @apply_policies(RequestContext(request))
    def delete(
        self,
        collection,
        id,
        item=None,
        spec="elody",
    ):
        if request.args.get("soft", 0, int):
            return "good", 200
        item = self._check_if_collection_and_item_exists(collection, id)
        self.storage.delete_item(item)
        return "", 204


class GenericObjectMetadata(BaseResource):
    @apply_policies(RequestContext(request))
    def get(self, collection, id, fields=None, spec="elody"):
        item = self._check_if_collection_and_item_exists(collection, id)
        if fields is None:
            fields = {}
        metadata = item["metadata"]
        accept_header = request.headers.get("Accept")
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                metadata, accept_header, "metadata", fields, spec
            ),
            accept_header,
        )

    @apply_policies(RequestContext(request))
    def post(self, collection, id, content=None, spec="elody"):
        self._abort_if_item_doesnt_exist(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "metadata")
        self._update_date_updated_and_last_editor(collection, id)
        metadata = self.storage.add_sub_item_to_collection_item(
            collection, id, "metadata", content
        )
        return metadata, 201

    @apply_policies(RequestContext(request))
    def put(self, collection, id, content=None, spec="elody"):
        self._check_if_collection_and_item_exists(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "metadata")
        self._update_date_updated_and_last_editor(collection, id)
        metadata = self.storage.update_collection_item_sub_item(
            collection, id, "metadata", content
        )
        return metadata, 201

    @apply_policies(RequestContext(request))
    def patch(self, collection, id, content=None, spec="elody"):
        self._check_if_collection_and_item_exists(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "metadata")
        self._update_date_updated_and_last_editor(collection, id)
        metadata = self.storage.patch_collection_item_metadata(collection, id, content)
        if not metadata:
            abort(400, message=f"Item with id {id} has no metadata")
        return metadata, 201


class GenericObjectMetadataKey(BaseResource):
    @apply_policies(RequestContext(request))
    def get(self, collection, id, key):
        self._check_if_collection_and_item_exists(collection, id)
        return self.storage.get_collection_item_sub_item_key(
            collection, id, "metadata", key
        )

    @apply_policies(RequestContext(request))
    def delete(self, collection, id, key):
        self._check_if_collection_and_item_exists(collection, id)
        self.storage.delete_collection_item_sub_item_key(
            collection, id, "metadata", key
        )
        return "", 204


class GenericObjectRelations(BaseResource):
    @apply_policies(RequestContext(request))
    def get(self, collection, id, spec="elody"):
        self._check_if_collection_and_item_exists(collection, id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(collection, id), 200

    @apply_policies(RequestContext(request))
    def post(self, collection, id, content=None, spec="elody"):
        entity = self._check_if_collection_and_item_exists(collection, id) or {}
        if content is None:
            content = self._get_content_according_content_type(request, "relations")
        self._update_date_updated_and_last_editor(collection, id)
        relations = self.storage.add_relations_to_collection_item(
            collection, entity["_id"], content
        )
        return relations, 201

    @apply_policies(RequestContext(request))
    def put(self, collection, id, content=None, spec="elody"):
        entity = self._check_if_collection_and_item_exists(collection, id) or {}
        if content is None:
            content = self._get_content_according_content_type(request, "relations")
        self._update_date_updated_and_last_editor(collection, id)
        relations = self.storage.update_collection_item_relations(
            collection, entity["_id"], content
        )
        return relations, 201

    @apply_policies(RequestContext(request))
    def patch(self, collection, id, content=None, spec="elody"):
        entity = self._check_if_collection_and_item_exists(collection, id) or {}
        if content is None:
            content = self._get_content_according_content_type(request, "relations")
        self._update_date_updated_and_last_editor(collection, id)
        relations = self.storage.patch_collection_item_relations(
            collection, entity["_id"], content
        )
        return relations, 201

    @apply_policies(RequestContext(request))
    def delete(self, collection, id, content=None, spec="elody"):
        entity = self._check_if_collection_and_item_exists(collection, id) or {}
        if content is None:
            content = self._get_content_according_content_type(request, "relations")
        self.storage.delete_collection_item_relations(
            collection, entity["_id"], content
        )
        return "", 204
