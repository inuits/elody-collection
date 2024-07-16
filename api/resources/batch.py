from datetime import datetime, timezone
from elody.csv import CSVMultiObject
from elody.exceptions import ColumnNotFoundException
from elody.util import get_raw_id
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from policy_factory import authenticate
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
                    mediafile.get("relations"),
                    dry_run,
                )
                output.append(mediafile)
        return output

    def _get_parsed_csv(self, csv):
        try:
            csv_multi_object = CSVMultiObject(
                csv,
                {"entities": "same_entity", "mediafiles": "filename"},
                {
                    "mediafiles": [
                        "filename",
                        "publication_status",
                        "mediafile_copyright_color",
                        "license",
                    ]
                },
                {"mediafiles": {"copyright_color": "red"}},
                {
                    "asset_copyright_color": {
                        "target": "entities",
                        "map_to": "copyright_color",
                    },
                    "mediafile_copyright_color": {
                        "target": "mediafiles",
                        "map_to": "copyright_color",
                    },
                },
            )
                
            items_for_parsing = {
                "mediafiles": {
                    "key": "license",
                    "map_to_key": "hasLicense",
                }
            }
            self._parse_metadata_key_to_relation(csv_multi_object, items_for_parsing)
            return csv_multi_object
        except ColumnNotFoundException:
            abort(422, message="One or more required columns headers aren't defined")
        

    def _parse_metadata_key_to_relation(self, csv_multi_object, items_for_parsing):
        for key, parse_item in items_for_parsing.items():
            method_name = f"get_{key}"
            if hasattr(csv_multi_object, method_name):
                items = getattr(csv_multi_object, method_name)()
                for item in items:
                    list_ = item.get("metadata", [])
                    for list_item in list_:
                        if list_item["key"] == parse_item["key"]:
                            related_item = (
                                self.storage.get_item_from_collection_by_metadata(
                                    "entities", "key", list_item["value"]
                                )
                            )
                            if not related_item:
                                csv_multi_object.errors.update(
                                   {"related_item": [f"Item for key {parse_item['key']} with value {list_item['value']} doesn't exist.\n"]}
                                )
                                break
                            list_.remove(list_item)
                            map_list = item.get("relations", [])
                            map_list.append(
                                {
                                    "key": get_raw_id(related_item),
                                    "type": parse_item["map_to_key"],
                                }
                            )
                            item.update({"relations": map_list})
                setattr(csv_multi_object, f"set_{key}", items)

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

    @authenticate(RequestContext(request))
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
