import mappers

from app import policy_factory
from datetime import datetime, timezone
from elody.exceptions import NonUniqueException
from elody.util import (
    get_raw_id,
)
from flask import after_this_request, request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource


class GenericObject(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(
        self,
        collection,
        skip=0,
        limit=20,
        fields=None,
        filters=None,
        sort=None,
        asc=True,
    ):
        self._check_if_collection_name_exists(collection)
        if fields is None:
            fields = {}
        if filters is None:
            filters = {}
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        collection_data = self.storage.get_items_from_collection(
            collection,
            skip=skip,
            limit=limit,
            fields=fields,
            filters=filters,
            sort=sort,
            asc=asc,
        )
        collection_data["pagination"] = {
            "total_count": collection_data["count"],
            "limit": limit,
            "skip": skip,
        }
        if skip + limit < collection_data["count"]:
            collection_data["pagination"][
                "next"
            ] = f"/{collection}?skip={skip + limit}&limit={limit}"
        if skip >= limit:
            collection_data["pagination"][
                "previous"
            ] = f"/{collection}?skip={max(0, skip - limit)}&limit={limit}"
        return collection_data

    @policy_factory.authenticate(RequestContext(request))
    def post(
        self,
        collection,
        content=None,
        type=None,
        date_created=datetime.now(timezone.utc),
        version=1,
        user=None,
        accept_header=None,
    ):
        self._check_if_collection_name_exists(collection)
        if content is None:
            content = self._get_content_according_content_type(request, collection)
        if type in self.schemas_by_type:
            self._abort_if_not_valid_json(type, content)
        if user is not None:
            content["user"] = user
        content["date_created"] = date_created
        content["version"] = version
        try:
            collection_item = self.storage.save_item_to_collection(collection, content)
        except NonUniqueException as ex:
            return str(ex)
        if accept_header == "text/uri-list":
            ticket_id = self._create_ticket(collection_item["filename"])
            response = f"{self.storage_api_url}/upload-with-ticket/{collection_item['filename'].strip()}?id={get_raw_id(collection_item)}&ticket_id={ticket_id}"
        else:
            response = collection_item
        return self._create_response_according_accept_header(
            response, accept_header, 201
        )


class GenericObjectDetail(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, collection, id):
        return self._check_if_collection_and_item_exists(collection, id)

    @policy_factory.authenticate(RequestContext(request))
    def put(
        self,
        collection,
        id,
        item=None,
        type=None,
        content=None,
        date_updated=datetime.now(timezone.utc),
    ):
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
        content["date_updated"] = date_updated
        content["version"] = collection_item.get("version", 0) + 1
        content["last_editor"] = (
            policy_factory.get_user_context().email or "default_uploader"
        )
        try:
            collection_item = self.storage.update_item_from_collection(
                collection, get_raw_id(collection_item), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        return collection_item, 201

    @policy_factory.authenticate(RequestContext(request))
    def patch(
        self,
        collection,
        id,
        item=None,
        type=None,
        content=None,
        version=True,
        date_updated=datetime.now(timezone.utc),
    ):
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
        content["date_updated"] = date_updated
        if version:
            content["version"] = collection_item.get("version", 0) + 1
        content["last_editor"] = (
            policy_factory.get_user_context().email or "default_uploader"
        )
        try:
            collection_item = self.storage.patch_item_from_collection(
                collection, get_raw_id(collection_item), content
            )
        except NonUniqueException as ex:
            return str(ex), 409
        return collection_item, 201

    @policy_factory.authenticate(RequestContext(request))
    def delete(
        self,
        collection,
        id,
        item=None,
    ):
        self._check_if_collection_name_exists(collection)
        if item is None:
            collection_item = self._abort_if_item_doesnt_exist(collection, id)
        else:
            collection_item = item
        self.storage.delete_item_from_collection(
            collection, get_raw_id(collection_item)
        )
        return "", 204


class GenericObjectMetadata(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, collection, id, fields=None):
        self._check_if_collection_and_item_exists(collection, id)
        if fields is None:
            fields = {}
        metadata = self.storage.get_collection_item_sub_item(collection, id, "metadata")
        accept_header = request.headers.get("Accept")
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                metadata, accept_header, "metadata", fields
            ),
            accept_header,
        )

    @policy_factory.authenticate(RequestContext(request))
    def post(
        self, collection, id, content=None, date_created=datetime.now(timezone.utc)
    ):
        self._check_if_collection_and_item_exists(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "metadata")
        content["date_created"] = date_created
        metadata = self.storage.add_sub_item_to_collection_item(
            collection, id, "metadata", content
        )
        return metadata, 201

    @policy_factory.authenticate(RequestContext(request))
    def put(
        self, collection, id, content=None, date_updated=datetime.now(timezone.utc)
    ):
        self._check_if_collection_and_item_exists(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "metadata")
        content["date_updated"] = date_updated
        metadata = self.storage.update_collection_item_sub_item(
            collection, id, "metadata", content
        )
        return metadata, 201

    @policy_factory.authenticate(RequestContext(request))
    def patch(
        self, collection, id, content=None, date_updated=datetime.now(timezone.utc)
    ):
        self._check_if_collection_and_item_exists(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "metadata")
        content["date_updated"] = date_updated
        metadata = self.storage.patch_collection_item_metadata(collection, id, content)
        if not metadata:
            abort(400, message=f"Item with id {id} has no metadata")
        return metadata, 201


class GenericObjectMetadataKey(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, collection, id, key):
        self._check_if_collection_and_item_exists(collection, id)
        return self.storage.get_collection_item_sub_item_key(
            collection, id, "metadata", key
        )

    @policy_factory.authenticate(RequestContext(request))
    def delete(self, collection, id, key):
        self._check_if_collection_and_item_exists(collection, id)
        self.storage.delete_collection_item_sub_item_key(
            collection, id, "metadata", key
        )
        return "", 204


class GenericObjectRelations(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, collection, id):
        self._check_if_collection_and_item_exists(collection, id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(collection, id)

    @policy_factory.authenticate(RequestContext(request))
    def post(
        self, collection, id, content=None, date_created=datetime.now(timezone.utc)
    ):
        self._check_if_collection_and_item_exists(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "relations")
        content["date_created"] = date_created
        relations = self.storage.add_relations_to_collection_item(
            collection, id, content
        )
        return relations, 201

    @policy_factory.authenticate(RequestContext(request))
    def put(
        self, collection, id, content=None, date_updated=datetime.now(timezone.utc)
    ):
        self._check_if_collection_and_item_exists(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "relations")
        content["date_updated"] = date_updated
        relations = self.storage.update_collection_item_relations(
            collection, id, content
        )
        return relations, 201

    @policy_factory.authenticate(RequestContext(request))
    def patch(
        self, collection, id, content=None, date_updated=datetime.now(timezone.utc)
    ):
        self._check_if_collection_and_item_exists(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "relations")
        content["date_updated"] = date_updated
        relations = self.storage.patch_collection_item_relations(
            collection, id, content
        )
        return relations, 201

    @policy_factory.authenticate(RequestContext(request))
    def delete(self, collection, id, content=None):
        self._check_if_collection_and_item_exists(collection, id)
        if content is None:
            content = self._get_content_according_content_type(request, "relations")
        self.storage.delete_collection_item_relations(collection, id, content)
        return "", 204
