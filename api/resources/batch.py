from app import policy_factory
from datetime import datetime, timezone
from elody.csv import CSVMultiObject
from elody.exceptions import ColumnNotFoundException
from elody.util import get_raw_id
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource


class Batch(BaseResource):
    def __add_matching_mediafiles_to_entity(
        self, entity_matching_id, entity, mediafiles, dry_run=False
    ):
        output = list()
        for mediafile in mediafiles:
            if mediafile.get("matching_id") == entity_matching_id:
                mediafile = mediafile.copy()
                mediafile.pop("matching_id", None)
                mediafile = self._create_mediafile_for_entity(
                    entity,
                    mediafile.get("filename"),
                    mediafile.get("metadata"),
                    dry_run,
                )
                output.append(mediafile)
        return output

    def _get_parsed_csv(self, csv):
        try:
            return CSVMultiObject(
                csv,
                {"entities": "same_entity", "mediafiles": "filename"},
                {
                    "mediafiles": [
                        "copyright_color",
                        "filename",
                        "publication_status",
                    ]
                },
                {"mediafiles": {"copyright_color": "red"}},
            )
        except ColumnNotFoundException:
            abort(422, message="One or more required columns headers aren't defined")

    def _get_entities_and_mediafiles_from_csv(self, parsed_csv, dry_run=False):
        entities_and_mediafiles = dict()
        entities_and_mediafiles.setdefault("entities", list())
        for entity in parsed_csv.objects.get("entities"):
            entity_matching_id = entity.pop("matching_id", None)
            if not dry_run:
                relations = entity.pop("relations", list())
                date_created = datetime.now(timezone.utc)
                entity["date_created"] = date_created
                entity["date_updated"] = date_created
                entity["version"] = 1
                entity = self.storage.save_item_to_collection("entities", entity)
                self.storage.add_relations_to_collection_item(
                    "entities", get_raw_id(entity), relations
                )
            if entity_matching_id:
                mediafiles = self.__add_matching_mediafiles_to_entity(
                    entity_matching_id,
                    entity,
                    parsed_csv.objects.get("mediafiles"),
                    dry_run,
                )
                if mediafiles:
                    entities_and_mediafiles.setdefault("mediafiles", list())
                    entities_and_mediafiles.get("mediafiles", list()).extend(mediafiles)
            if not dry_run:
                entities_and_mediafiles.get("entities", list()).append(
                    self.storage.get_item_from_collection_by_id(
                        "entities", get_raw_id(entity)
                    )
                )
            else:
                entities_and_mediafiles.get("entities", list()).append(entity)
        return entities_and_mediafiles

    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        content_type = request.content_type
        dry_run = request.args.get("dry_run", 0, int)
        if content_type == "text/csv":
            output = dict()
            accept_header = request.headers.get("Accept")
            parsed_csv = self._get_parsed_csv(request.get_data(as_text=True))
            entities_and_mediafiles = self._get_entities_and_mediafiles_from_csv(
                parsed_csv, dry_run
            )
            if accept_header != "text/uri-list" or dry_run:
                output = entities_and_mediafiles
                output["errors"] = parsed_csv.get_errors()
                return output, 201
            else:
                output = ""
                for mediafile in entities_and_mediafiles.get("mediafiles", list()):
                    ticket_id = self._create_ticket(mediafile.get("filename"))
                    output += f"{self.storage_api_url}/upload-with-ticket/{mediafile.get('filename')}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}\n"
            return self._create_response_according_accept_header(
                output, accept_header, 201
            )
        abort(415, message=f"Only content type text/csv is allowed, not {content_type}")
