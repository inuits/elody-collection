from datetime import datetime, timezone
from elody.error_codes import ErrorCode, get_error_code, get_write
from elody.csv import CSVMultiObject
from elody.exceptions import ColumnNotFoundException
from elody.util import get_raw_id
from elody.job import start_job, finish_job, fail_job
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies, get_user_context
from rabbit import get_rabbit
from resources.base_resource import BaseResource
from urllib.parse import quote


class Batch(BaseResource):
    def __init__(self):
        super().__init__()
        self.main_job_id_with_dry_run = ""
        self.main_job_id_without_dry_run = ""
        self.get_rabbit = lambda: get_rabbit()

    def _add_matching_mediafiles_to_entity(
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
            return CSVMultiObject(
                csv,
                {"entities": "same_entity", "mediafiles": "filename"},
                {
                    "mediafiles": [
                        "filename",
                        "publication_status",
                        "mediafile_copyright_color",
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
        except ColumnNotFoundException as missing_columns:
            message = f"{get_error_code(ErrorCode.COLUMN_NOT_FOUND, get_write())} | missing_columns:{missing_columns} - Not all identifying columns are present in CSV: {missing_columns}"
            fail_job(self.main_job_id_with_dry_run, message, get_rabbit=self.get_rabbit)
            fail_job(
                self.main_job_id_without_dry_run, message, get_rabbit=self.get_rabbit
            )
            abort(
                422,
                message=f"{get_error_code(ErrorCode.COLUMN_NOT_FOUND, get_write())} - {message}",
            )

    def _parse_metadata_key_to_relation(
        self, csv_multi_object, items_for_parsing, key_to_remove_in_metadata=[]
    ):
        for key, parse_item in items_for_parsing.items():
            method_name = f"get_{key}"
            if hasattr(csv_multi_object, method_name):
                items = getattr(csv_multi_object, method_name)()
                if isinstance(parse_item, list):
                    for sub_item in parse_item:
                        for item in items:
                            metadata_copy = item.get("metadata", [])[:]
                            for metadata_item in metadata_copy:
                                if metadata_item["key"] == sub_item["csv_key"]:
                                    self._parse_key_to_item(
                                        csv_multi_object, sub_item, item, metadata_item
                                    )
                else:
                    for item in items:
                        metadata_copy = item.get("metadata", [])[:]
                        for metadata_item in metadata_copy:
                            if metadata_item["key"] == parse_item["csv_key"]:
                                self._parse_key_to_item(
                                    csv_multi_object, parse_item, item, metadata_item
                                )
            if key_to_remove_in_metadata:
                for key in key_to_remove_in_metadata:
                    items = self.remove_metadata_by_key(items, key)
                setattr(csv_multi_object, f"set_{key}", items)

    def remove_metadata_by_key(self, data, key_to_remove):
        for item in data:
            item["metadata"] = [
                meta for meta in item["metadata"] if meta["key"] != key_to_remove
            ]
        return data

    def _parse_key_to_item(self, csv_multi_object, parse_item, item, metadata_item):
        related_item = self.storage.get_item_from_collection_by_metadata(
            "entities",
            parse_item["db_key"],
            metadata_item["value"],
            parse_item["type"],
        )
        if not related_item:
            self._add_error_to_csv_multi_object(
                csv_multi_object, parse_item, metadata_item
            )
            return

        item["metadata"].remove(metadata_item)
        self._add_relation_to_relation_list(parse_item, item, related_item)

    def _add_relation_to_relation_list(self, parse_item, item, related_item):
        relation_list = item.get("relations", [])
        relation_list.append(
            {
                "key": get_raw_id(related_item),
                "type": parse_item["map_to_key"],
            }
        )
        item.update({"relations": relation_list})

    def _add_error_to_csv_multi_object(self, csv_multi_object, parse_item, list_item):
        message = f"Item for key {parse_item['csv_key']} with value {list_item['value']} doesn't exist.\n"
        fail_job(self.main_job_id_with_dry_run, message, get_rabbit=self.get_rabbit)
        fail_job(self.main_job_id_without_dry_run, message, get_rabbit=self.get_rabbit)
        csv_multi_object.errors.update({"related_item": [message]})

    def _get_entities_and_mediafiles_from_csv(self, parsed_csv, dry_run=False, extra_mediafile_type=None):
        entities_and_mediafiles = dict()
        entities_and_mediafiles.setdefault("entities", list())
        for entity in parsed_csv.objects.get("entities"):
            entity_matching_id = entity.pop("matching_id", None)
            if not dry_run:
                relations = entity.pop("relations", list())
                date_created = datetime.now(timezone.utc)
                entity["date_created"] = date_created
                entity["date_updated"] = date_created
                entity["created_by"] = get_user_context().email or "default_uploader"
                entity["version"] = 1
                entity = self.storage.save_item_to_collection("entities", entity)
                self.storage.add_relations_to_collection_item(
                    "entities", get_raw_id(entity), relations
                )
            if entity_matching_id:
                mediafiles = self._add_matching_mediafiles_to_entity(
                    entity_matching_id,
                    entity,
                    parsed_csv.objects.get("mediafiles", []),
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

    @apply_policies(RequestContext(request))
    def post(self):
        self.main_job_id_with_dry_run = start_job(
            "Start Import for CSV with a dry run",
            "Start Import",
            get_rabbit=self.get_rabbit,
            user_email=get_user_context().email,
        )
        content_type = request.content_type
        dry_run = request.args.get("dry_run", 0, int)
        extra_mediafile_type = request.args.get("extra_mediafile_type")
        if not dry_run:
            self.main_job_id_without_dry_run = start_job(
                "Start Import for CSV without a dry_run",
                "Start Import",
                get_rabbit=self.get_rabbit,
                user_email=get_user_context().email,
            )
        if content_type == "text/csv":
            output = dict()
            accept_header = request.headers.get("Accept")
            parsed_csv = self._get_parsed_csv(request.get_data(as_text=True))
            entities_and_mediafiles = self._get_entities_and_mediafiles_from_csv(
                parsed_csv, dry_run, extra_mediafile_type
            )
            if accept_header != "text/uri-list" or dry_run:
                output = entities_and_mediafiles
                output["errors"] = parsed_csv.get_errors()
                finish_job(self.main_job_id_with_dry_run, get_rabbit=self.get_rabbit)
                finish_job(self.main_job_id_without_dry_run, get_rabbit=self.get_rabbit)
                return output, 201
            else:
                output = ""
                for mediafile in entities_and_mediafiles.get("mediafiles", list()):
                    ticket_id = self._create_ticket(mediafile.get("filename"))
                    output += f"{self.storage_api_url}/upload-with-ticket/{quote(mediafile.get('filename'))}?id={get_raw_id(mediafile)}&ticket_id={ticket_id}\n"
            finish_job(self.main_job_id_with_dry_run, get_rabbit=self.get_rabbit)
            finish_job(self.main_job_id_without_dry_run, get_rabbit=self.get_rabbit)
            return self._create_response_according_accept_header(
                output, accept_header, 201
            )
        message = f"Only content type text/csv is allowed, not {content_type}"
        fail_job(self.main_job_id_with_dry_run, message, get_rabbit=self.get_rabbit)
        fail_job(self.main_job_id_without_dry_run, message, get_rabbit=self.get_rabbit)
        abort(
            415,
            message=f"{get_error_code(ErrorCode.ONLY_TYPE_CSV_ALLOWED, get_write())} - {message}",
        )
