from configuration import get_object_configuration_mapper
from elody.util import get_raw_id
from filters_v2.filter_manager import FilterManager
from flask import g, request
from inuits_policy_based_auth import RequestContext
from policy_factory import authenticate
from resources.base_resource import BaseResource
from resources.elody._blueprint import api
from resources.elody.document_relations import ElodyDocumentRelations
from resources.elody.mediafiles.mediafile import ElodyMediafile
from resources.elody.mediafiles.mediafile_relations import ElodyMediafileRelations
from serialization.case_converter import camel_to_snake
from serialization.serialize import serialize
from werkzeug.exceptions import BadRequest, NotFound


class ElodyMediafileDuplicates(BaseResource):
    """Merge a duplicate mediafile into an existing one.

    Called by the storage-api when an uploaded file turns out to be a duplicate
    (same md5sum) of an already existing mediafile. Everything that references
    the duplicate is re-linked to the existing mediafile, after which the
    duplicate is deleted.

    How the link between an entity and its mediafile is stored is declared by
    the client, the same way it is for ElodyDocumentMediafiles:
    - store_relations_on_mediafile=True: the relations live on the mediafile, the
      relations of the duplicate are added to the existing mediafile.
    - store_relations_on_mediafile=False: the relations live on the entity. The
      entities of the given parent_document_types that reference the duplicate
      through parent_filter_key are re-pointed to the existing mediafile.
    """

    @authenticate(RequestContext(request))
    def post(
        self,
        *,
        id,
        spec,
        store_relations_on_mediafile=True,
        mediafile_relation_type=None,
        parent_document_types=None,
        parent_filter_key=None,
    ):
        duplicate_id = (request.get_json() or {}).get("duplicate_id")
        if not duplicate_id:
            raise BadRequest("Provide the id of the duplicate as 'duplicate_id'")
        if not store_relations_on_mediafile and not (
            mediafile_relation_type and parent_document_types and parent_filter_key
        ):
            raise ValueError(
                "mediafile_relation_type, parent_document_types and parent_filter_key"
                " are required when relations are not stored on the mediafile"
            )

        existing = self._check_if_collection_and_item_exists(None, id)
        try:
            duplicate = self._check_if_collection_and_item_exists(None, duplicate_id)
        except NotFound:
            # already merged, makes retries safe
            return self.__serialize(existing, spec), 200
        if get_raw_id(existing) == get_raw_id(duplicate):
            raise BadRequest("A mediafile cannot be a duplicate of itself")

        existing_ids = self.__get_ids(existing)
        duplicate_ids = [
            identifier
            for identifier in self.__get_ids(duplicate)
            if identifier not in existing_ids
        ]

        # Link first, delete last: when something fails halfway, we end up with
        # an extra link instead of a lost one.
        if store_relations_on_mediafile:
            self.__merge_relations_on_mediafile(existing, duplicate, spec)
        else:
            self.__relink_parents(
                existing=existing,
                duplicate=duplicate,
                existing_ids=existing_ids,
                duplicate_ids=duplicate_ids,
                spec=spec,
                relation_type=camel_to_snake(mediafile_relation_type),
                parent_document_types=parent_document_types,
                parent_filter_key=parent_filter_key,
            )

        # serialize before deleting, deleting changes the current request
        existing = self._check_if_collection_and_item_exists(None, get_raw_id(existing))
        serialized_existing = self.__serialize(existing, spec)

        response = ElodyMediafile().delete(id=get_raw_id(duplicate), spec=spec)
        if isinstance(response, tuple) and response[1] >= 400:
            return response

        return serialized_existing, 200

    def __get_ids(self, document):
        ids = [
            document.get(key) for key in ("_id", "_key", "id") if document.get(key)
        ] + list(document.get("identifiers", []))
        return list(dict.fromkeys(ids))

    def __get_replacement_key(self, matched_key, existing, duplicate):
        # entities can reference either the id or the _id, keep using the same kind
        if existing.get("id") and matched_key == duplicate.get("id"):
            return existing["id"]
        return get_raw_id(existing)

    def __merge_relations_on_mediafile(self, existing, duplicate, spec):
        serialized_existing = self.__serialize(existing, spec)
        serialized_duplicate = self.__serialize(duplicate, spec)
        known_relations = {
            (relation["key"], camel_to_snake(relation["type"]))
            for relation in serialized_existing.get("relations", [])
        }
        missing_relations = [
            relation
            for relation in serialized_duplicate.get("relations", [])
            if (relation["key"], camel_to_snake(relation["type"]))
            not in known_relations
        ]
        if missing_relations:
            g.content = missing_relations
            ElodyMediafileRelations().post(id=get_raw_id(existing), spec=spec)

    def __relink_parents(
        self,
        *,
        existing,
        duplicate,
        existing_ids,
        duplicate_ids,
        spec,
        relation_type,
        parent_document_types,
        parent_filter_key,
    ):
        if not duplicate_ids:
            return

        parents = {}
        for document_type in parent_document_types:
            collection = (
                get_object_configuration_mapper().get(document_type).crud()["collection"]
            )
            results = (
                FilterManager()
                .get_filter_engine()
                .filter(
                    [
                        {"type": "type", "value": document_type},
                        {
                            "type": "selection",
                            "key": parent_filter_key,
                            "value": duplicate_ids,
                            "match_exact": True,
                        },
                    ],
                    0,
                    999999,
                    collection,
                )
                .get("results", [])
            )
            for result in results:
                parents[(collection, get_raw_id(result))] = None

        for collection, parent_id in parents:
            parent = self.storage.get_item_from_collection_by_id(collection, parent_id)
            if not parent:
                continue
            serialized_parent = self.__serialize(parent, spec)
            relations, changed = self.__swap_relations(
                serialized_parent.get("relations", []),
                existing,
                duplicate,
                existing_ids,
                duplicate_ids,
                relation_type,
            )
            if changed:
                g.content = relations
                # Clients resolve the collection from the public id (AICAP does
                # not recognise an _id), so prefer it over the raw id.
                ElodyDocumentRelations().put(
                    id=parent.get("id") or parent_id,
                    spec=spec,
                    document=serialized_parent,
                )

    def __serialize(self, document, spec):
        return serialize(document, type=document.get("type"), to_format=spec)

    def __swap_relations(
        self, relations, existing, duplicate, existing_ids, duplicate_ids, relation_type
    ):
        has_existing = any(
            relation["key"] in existing_ids
            and camel_to_snake(relation["type"]) == relation_type
            for relation in relations
        )
        swapped_relations, changed = [], False
        for relation in relations:
            if (
                relation["key"] not in duplicate_ids
                or camel_to_snake(relation["type"]) != relation_type
            ):
                swapped_relations.append(relation)
                continue

            changed = True
            if has_existing:
                continue
            has_existing = True
            swapped_relations.append(
                {
                    **relation,
                    "key": self.__get_replacement_key(
                        relation["key"], existing, duplicate
                    ),
                }
            )
        return swapped_relations, changed


def resource_rules():
    return [
        {
            "route": "/mediafiles/<string:id>/duplicates",
            "resource": ElodyMediafileDuplicates,
            "api": api,
        }
    ]
