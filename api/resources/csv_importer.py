from app import policy_factory, rabbit
from elody.exceptions import NonUniqueException
from elody.util import mediafile_is_public, signal_entity_changed
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource
from validator import mediafile_schema


class AddEntities(BaseResource):
    """
    Endpoint: /csv-importer/add-etntities\n
    Imports entities with metadata from CSV file.\n
    First column in CSV has to be "entity_type".\n
    Second and next columns are for metadata.\n
    For metadata it is possible to create a dictionary(key/value)
    using dot.syntax, or create a list(array) by list-index [n].
    Dict and list could be combine.\n
    https://github.com/fabiocaccamo/python-benedict for more info.\n
    Use ; or , as a separator.\n
    CSV exapmple:\n
        entity_type;is_printable;dict.one;dict.two;list[0];list[1];comb.dict.list[0];comb.dict.list[1]\n
        asset;yes;One;Two;1;2;Comb1;Comb2\n
        asset;0;1;2;1.123;2.345;1000;2000\n
    """

    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        entities = []
        responses = []
        initial_data_type = "entity_type"

        items = self._parse_items_from_csv(request, initial_data_type)
        for item in items:
            metadata = []
            for key, value in item["bdict"].items():
                metadata.append({"key": key, "value": value})
            entities.append({"type": item["entity_type"], "metadata": metadata})

        for entity in entities:
            try:
                entity_saved = self.storage.save_item_to_collection("entities", entity)
                response = {
                    "id": entity_saved["_id"],
                    "type": entity_saved["type"],
                    "status": "Created",
                }
                responses.append(response)
                signal_entity_changed(rabbit, entity_saved)
            except NonUniqueException as ex:
                responses.append(
                    {
                        "type": entity["type"],
                        "status": "Not created",
                        "exception": str(ex),
                    }
                )

        return responses


class PatchEntitiesMetadata(BaseResource):
    """
    Endpoint: /csv-importer/patch-etntities-metadata\n
    Add metadata to entity via a CSV file.\n
    First column in CSV has to be "id".\n
    Second and next columns are for metadata.\n
    For metadata it is possible to create a dictionary(key/value)
    using dot.syntax, or create a list(array) by list-index [n].
    Dict and list could be combine.\n
    https://github.com/fabiocaccamo/python-benedict for more info.\n
    Use ; or , as a separator.\n
    CSV exapmple:\n
        id;is_printable;dict.one;dict.two;list[0];list[1];comb.dict.list[0];comb.dict.list[1]\n
        XYZ;yes;One;Two;1;2;Comb1;Comb2\n
        ZYX;0;1;2;1.123;2.345;1000;2000\n
    """

    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        entities = []
        responses = []
        initial_data_type = "id"

        items = self._parse_items_from_csv(request, initial_data_type)
        for item in items:
            metadata = []
            for key, value in item["bdict"].items():
                metadata.append({"key": key, "value": value})
            entity = self._abort_if_item_doesnt_exist("entities", item["id"])
            self._abort_if_no_access(entity)
            entities.append(
                {"_id": entity["_id"], "metadata": metadata, "data": entity}
            )

        for entity in entities:
            entity_metadata = self.storage.patch_collection_item_metadata(
                "entities", entity["_id"], entity["metadata"]
            )
            if not entity_metadata:
                entity_metadata = self.storage.add_sub_item_to_collection_item(
                    "entities", entity["_id"], "metadata", entity["metadata"]
                )
            responses.append({"id": entity["_id"], "status": "Metadata added"})
            signal_entity_changed(rabbit, entity["data"])

        return responses


class PutEntitiesMetadata(BaseResource):
    """
    Endpoint: /csv-importer/put-etntities-metadata\n
    Overwrite metadata to entity via a CSV file.\n
    First column in CSV has to be "id".\n
    Second and next columns are for metadata.\n
    For metadata it is possible to create a dictionary(key/value)
    using dot.syntax, or create a list(array) by list-index [n].
    Dict and list could be combine.\n
    https://github.com/fabiocaccamo/python-benedict for more info.\n
    Use ; or , as a separator.\n
    CSV exapmple:\n
        id;is_printable;dict.one;dict.two;list[0];list[1];comb.dict.list[0];comb.dict.list[1]\n
        XYZ;yes;One;Two;1;2;Comb1;Comb2\n
        ZYX;0;1;2;1.123;2.345;1000;2000\n
    """

    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        entities = []
        responses = []
        initial_data_type = "id"

        items = self._parse_items_from_csv(request, initial_data_type)
        for item in items:
            metadata = []
            for key, value in item["bdict"].items():
                metadata.append({"key": key, "value": value})
            entity = self._abort_if_item_doesnt_exist("entities", item["id"])
            self._abort_if_no_access(entity)
            entities.append(
                {"_id": entity["_id"], "metadata": metadata, "data": entity}
            )

        for entity in entities:
            self.storage.update_collection_item_sub_item(
                "entities", entity["_id"], "metadata", entity["metadata"]
            )
            responses.append({"id": entity["_id"], "status": "Metadata updated"})
            signal_entity_changed(rabbit, entity["data"])

        return responses


class AddEntitiesWithMediafiles(BaseResource):
    """
    Endpoint: /csv-importer/add-etntities-with-mediafiles\n
    Imports entities with metadata and their mediafiles with metadata from CSV file.\n
    First column in CSV has to be "entity_type".\n
    Next columns are for metadata of an entity till column mediafile[0].filename.\n
    Metadata of the first mediafile as: mediafile[0].metadata[0].first, mediafile[0].metadata[1].second\n
    Next mediafile as: mediafile[1].filename and its metadata as: mediafile[1].metadata[0].first,mediafile[1].metadata[1].second\n
    For metadata it is possible to create a dictionary(key/value)
    using dot.syntax, or create a list(array) by list-index [n].
    Dict and list could be combine.\n
    https://github.com/fabiocaccamo/python-benedict for more info.\n
    Use ; or , as a separator.\n
    CSV exapmple:\n
        entity_type,title,shortname,mediafile[0].filename,mediafile[0].metadata[0].title,mediafile[0].metadata[1].number,mediafile[1].filename,mediafile[1].metadata[0].title,mediafile[1].metadata[1].number\n
        asset,Asset1,A1,MediaFile1,mfTitle1,123,MediaFile2,mfTitle2,456\n
        asset,Asset2,A2,MediaFile3,mfTitle3,321,MediaFile4,mfTitle4,654\n
    """

    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        entities = []
        responses = []
        initial_data_type = "entity_type"

        items = self._parse_items_from_csv(request, initial_data_type)
        for item in items:
            metadata = []
            for key, value in item["bdict"].items():
                if key == "mediafile":
                    for z in value:
                        if not z.get("filename"):
                            abort(400, message="Missing mediafile name.")
                        if z.get("metadata"):
                            mf_metadata = []
                            for x in z["metadata"]:
                                if x:
                                    for a, b in x.items():
                                        mf_metadata.append({"key": a, "value": b})
                            z["metadata"] = mf_metadata
                    mediafiles = value
                else:
                    metadata.append({"key": key, "value": value})
            entity = {
                "content": {"type": item["entity_type"]},
                "metadata": metadata,
                "mediafiles": mediafiles,
            }
            entities.append(entity)

        for entity in entities:
            try:
                entity_saved = self.storage.save_item_to_collection(
                    "entities", entity["content"]
                )
                self.storage.add_sub_item_to_collection_item(
                    "entities", entity_saved["_id"], "metadata", entity["metadata"]
                )
                response = {
                    "id": entity_saved["_id"],
                    "type": entity_saved["type"],
                    "status": "Created",
                }
                responses.append(response)
                signal_entity_changed(rabbit, entity_saved)
                for mediafile in entity["mediafiles"]:
                    self._abort_if_not_valid_json(
                        "Mediafile", mediafile, mediafile_schema
                    )
                    mediafile_saved = self.storage.save_item_to_collection(
                        "mediafiles", mediafile
                    )
                    response = {
                        "id": mediafile_saved["_id"],
                        "type": "Mediafile",
                        "status": "Created",
                    }
                    responses.append(response)
                    self.storage.add_mediafile_to_collection_item(
                        "entities",
                        entity_saved["_id"],
                        mediafile_saved["_id"],
                        mediafile_is_public(mediafile_saved),
                    )
            except NonUniqueException as ex:
                responses.append(
                    {
                        "type": entity["content"]["type"],
                        "status": "Not created",
                        "exception": str(ex),
                    }
                )

        return responses
