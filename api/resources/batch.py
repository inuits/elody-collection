from app import policy_factory, rabbit
from elody.csv import CSVMultiObject
from elody.util import (
    get_raw_id,
    mediafile_is_public,
)
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource


class Batch(BaseResource):
    def __add_matching_mediafiles_to_entity(
        self, entity_matching_id, entity, mediafiles
    ):
        output = list()
        for mediafile in mediafiles:
            if mediafile.get("matching_id") == entity_matching_id:
                mediafile = mediafile.copy()
                mediafile.pop("matching_id")
                mediafile = self.storage.save_item_to_collection(
                    "mediafiles", mediafile
                )
                mediafile = self.storage.add_mediafile_to_collection_item(
                    "entities",
                    get_raw_id(entity),
                    mediafile["_id"],
                    mediafile_is_public(mediafile),
                )
                output.append(mediafile)
        return output

    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        content_type = request.content_type
        if content_type == "text/csv":
            output = dict()
            accept_header = request.headers.get("Accept")
            if accept_header == "text/uri-list":
                output = ""
            csv = request.get_data(as_text=True)
            parsed_csv = CSVMultiObject(
                csv,
                {"entities": "same_entity", "mediafiles": "filename"},
                {"mediafiles": ["filename", "publication_status"]},
            )
            for entity in parsed_csv.objects.get("entities"):
                if accept_header != "text/uri-list":
                    output.setdefault("entities", list())
                clean_entity = entity.copy()
                entity_matching_id = clean_entity.pop("matching_id")
                entity = self.storage.save_item_to_collection("entities", clean_entity)
                if entity_matching_id:
                    mediafiles = self.__add_matching_mediafiles_to_entity(
                        entity_matching_id, entity, parsed_csv.objects.get("mediafiles")
                    )
                    if mediafiles:
                        if accept_header != "text/uri-list":
                            output.setdefault("mediafiles", list())
                            output.get("mediafiles", list()).extend(mediafiles)
                        else:
                            for mediafile in mediafiles:
                                ticket_id = self._create_ticket(
                                    mediafile.get("filename")
                                )
                                output += f"{self.storage_api_url}/upload-with-ticket/{mediafile.get('filename')}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}\n"
                if accept_header != "text/uri-list":
                    output.get("entities", list()).append(
                        self.storage.get_item_from_collection_by_id(
                            "entities", entity.get("_id")
                        )
                    )
            return self._create_response_according_accept_header(
                output, accept_header, 201
            )
        abort(415, message=f"Only content type text/csv is allowed, not {content_type}")
