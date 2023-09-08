import csv
import json
import re

from benedict import benedict
import mappers
import os

from app import policy_factory, rabbit
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

    def _abort_if_no_access(self, item, collection="entities"):
        if not self._has_access_to_item(item, collection):
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
            "date_created": datetime.now(timezone.utc),
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

    def _has_access_to_item(self, item, collection="entities"):
        return True

    def _inject_api_urls_into_entities(self, entities):
        for entity in entities:
            for mediafile_type in [
                "primary_mediafile_location",
                "primary_thumbnail_location",
                "primary_transcode_location",
            ]:
                if mediafile_type in entity:
                    mediafile_filename = entity[mediafile_type]
                    mediafile_filename = mediafile_filename.replace(
                        "/download/", "/download-with-ticket/"
                    )
                    ticket_id = self._create_ticket(mediafile_filename)
                    entity[
                        mediafile_type
                    ] = f"{self.storage_api_url_ext}{mediafile_filename}?ticket_id={ticket_id}"
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
                    mediafile_filename = mediafile_filename.replace(
                        "/download/", "/download-with-ticket/"
                    )
                    ticket_id = self._create_ticket(mediafile_filename)
                    mediafile[
                        mediafile_type
                    ] = f"{self.storage_api_url_ext}{mediafile_filename}?ticket_id={ticket_id}"
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

    def _create_ticket(self, filename: str) -> str:
        content = {
            "location": filename,
            "exp": (datetime.now(tz=timezone.utc) + timedelta(minutes=1)).timestamp(),
            "user": policy_factory.get_user_context().email or "default_uploader",
            "type": "ticket",
        }
        ticket_id = self.storage.save_item_to_collection(
            "abstracts", content, only_return_id=True, create_sortable_metadata=False
        )
        return ticket_id

    def _parse_items_from_csv(self, request, initial_data_type):
        items = []
        if not (request_data := request.get_data(as_text=True)):
            abort(400, message="Missing data")
        if not request_data.startswith(initial_data_type):
            abort(400, message=f"Missing {initial_data_type}.")

        try:
            separator = csv.Sniffer().sniff(request_data).delimiter
        except csv.Error:
            abort(
                400,
                message="Problem with a number of columns for entities in CSV.",
            )
        #  if there is no separator in csv (e.g. contains only 1 col) - sniffer returns nonsens:
        if re.search("[a-zA-Z0-9_\"']", separator):
            separator = ","

        request_dict = csv.DictReader(request_data.splitlines(), delimiter=separator)
        for item in [row for row in request_dict]:
            bdict = benedict()
            for key, value in item.items():
                if key == initial_data_type and not value:
                    abort(400, message=f"{initial_data_type} is not filled")
                if key != initial_data_type and value:
                    try:
                        value = int(value)
                    except ValueError:
                        try:
                            value = float(value)
                        except ValueError:
                            pass
                    bdict[key] = value
            try:
                items.append(
                    {f"{initial_data_type}": item[initial_data_type], "bdict": bdict}
                )
            except KeyError:
                abort(
                    400,
                    message="Problem with a number of columns or with a separator - check your CSV file.",
                )

        return items
