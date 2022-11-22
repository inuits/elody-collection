import app
import util

from resources.base_resource import BaseResource


class Config(BaseResource):
    filter_options_map = {
        "publication_status_media_file": "mediafiles_publication_status_values",
        "publication_status": "entities_publication_status_values",
        "rights": "mediafiles_rights_values",
        "source": "mediafiles_source_values",
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
        "entities": ["publication_status"],
    }

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
        filters = util.read_json_as_dict("filters.json")
        permissions = app.require_oauth.get_token_permissions(
            app.validator.role_permission_mapping
        )
        for collection, collection_filters in filters.items():
            allowed_filters[collection] = list()
            for filter in collection_filters:
                if (
                    f"filter-on-{filter['key'].replace('_', '-')}" in permissions
                    or not app.require_oauth.require_token
                ):
                    allowed_filters[collection].append(filter)
        return allowed_filters

    @app.require_oauth("read-config")
    def get(self):
        config = dict()
        for collection, keys in self.keys_for_config.items():
            for key in keys:
                if key in self.hard_coded_values:
                    config[f"{collection}_{key}_values"] = self.hard_coded_values[key]
                else:
                    config[
                        f"{collection}_{key}_values"
                    ] = self.storage.get_metadata_values_for_collection_item_by_key(
                        collection, key
                    )
        config["filters"] = self.__get_allowed_filters()
        self.__add_options_to_filters(
            config, config["filters"], self.filter_options_map
        )
        return config
