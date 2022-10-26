import app

from resources.base_resource import BaseResource


class Config(BaseResource):
    @app.require_oauth("read-config")
    def get(self):
        config = dict()
        hard_coded_values = {
            "publication_status": [
                "publiek",
                "niet_publiek",
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
        for collection, keys in keys_for_config.items():
            for key in keys:
                if key in hard_coded_values:
                    config[f"{collection}_{key}_values"] = hard_coded_values[key]
                else:
                    config[
                        f"{collection}_{key}_values"
                    ] = self.storage.get_metadata_values_for_collection_item_by_key(
                        collection, key
                    )
        return config
