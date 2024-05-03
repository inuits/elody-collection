from app import logger, policy_factory
from elody.util import read_json_as_dict
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource


class Config(BaseResource):
    filter_options_map = {
        "publication_status_media_file": "mediafiles_publication_status_values",
        "publication_status": "entities_publication_status_values",
        "rights": "mediafiles_rights_values",
        "source": "mediafiles_source_values",
        "type": "entities_type_values",
    }
    hard_coded_values = {
        "publication_status": [
            "publiek",
            "niet-publiek",
            "kandidaat",
            "werkbeeld",
            "beschermd",
            "expliciet",
        ],
    }
    keys_for_config = {
        "mediafiles": ["publication_status", "rights", "source"],
        "entities": ["publication_status", "type"],
    }

    def __add_asset_fields_for_csv(self):
        asset_fields = [
            "id",
            "title",
            "collection",
            "externalIdentifier",
            "collectiontype",
            "type",
            "format",
            "creator",
            "date",
            "publisher",
            "source",
            "description",
            "dc1_language",
            "dc1_relation",
            "brocade_archief",
            "closed_deposit",
            "copyright_object",
            "copyright_paid",
            "copyright_asset",
            "copyright_color",
            "copyright_permission_needed",
            "isshownat",
        ]
        return asset_fields

    def __add_mediafile_fields_for_csv(self):
        mediafile_fields = [
            "filename",
            "title",
            "copyright_color",
            "original_filename",
            "mimetype",
            "date_updated",
            "img_height",
            "img_width",
        ]
        return mediafile_fields

    def __add_options_to_filters(self, options, filters, filter_options_map):
        for collection, collection_filters in filters.items():
            for filter in collection_filters:
                key = filter["key"]
                if key in filter_options_map and filter_options_map[key] in options:
                    self.__add_options_to_specific_filter(
                        filter, options[filter_options_map[key]]
                    )

    def __add_options_to_specific_filter(self, filter, options):
        filter["options"] = list()
        for option in options:
            filter["options"].append({"label": option, "value": option})

    def __get_allowed_filters(self):
        allowed_filters = dict()
        filters = read_json_as_dict("filters_new.json", logger)
        permissions = policy_factory.get_user_context().x_tenant.scopes
        for collection, collection_filters in filters.items():
            allowed_filters[collection] = list()
            for filter in collection_filters:
                filter_permission = f"filter-on-{filter['key'].replace('_', '-')}"
                if filter_permission in permissions:
                    allowed_filters[collection].append(filter)
        return allowed_filters

    @policy_factory.authenticate(RequestContext(request))
    def get(self):
        config = dict()
        for collection, keys in self.keys_for_config.items():
            for key in keys:
                if key in self.hard_coded_values:
                    config[f"{collection}_{key}_values"] = self.hard_coded_values[key]
                else:
                    config[f"{collection}_{key}_values"] = (
                        self.storage.get_metadata_values_for_collection_item_by_key(
                            collection, key
                        )
                    )
        config["filters"] = self.__get_allowed_filters()
        config["mediafile_fields"] = self.__add_mediafile_fields_for_csv()
        config["asset_fields"] = self.__add_asset_fields_for_csv()
        self.__add_options_to_filters(
            config, config["filters"], self.filter_options_map
        )
        return config
