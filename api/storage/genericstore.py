class GenericStorageManager:
    def add_mediafile_to_collection_item(
        self, collection, id, mediafile_id, mediafile_public
    ):
        pass

    def add_relations_to_collection_item(self, collection, id, relations, parent=True):
        pass

    def add_sub_item_to_collection_item(self, collection, id, sub_item, content):
        pass

    def check_health(self):
        pass

    def delete_collection_item_relations(self, collection, id, content, parent=True):
        pass

    def delete_collection_item_sub_item_key(self, collection, id, sub_item, key):
        pass

    def delete_item_from_collection(self, collection, id):
        pass

    def drop_all_collections(self):
        pass

    def get_collection_item_mediafiles(self, collection, id):
        pass

    def get_collection_item_relations(
        self, collection, id, include_sub_relations=False, exclude=None
    ):
        pass

    def get_collection_item_sub_item(self, collection, id, sub_item) -> list:
        pass

    def get_collection_item_sub_item_key(self, collection, id, sub_item, key):
        pass

    def get_entities(self, skip=0, limit=20, skip_relations=0, filters=None):
        pass

    def get_item_from_collection_by_id(self, collection, id):
        pass

    def get_items_from_collection(
        self,
        collection,
        skip=0,
        limit=20,
        fields=None,
        filters=None,
        sort=None,
        asc=True,
    ):
        pass

    def get_mediafile_linked_entities(self, mediafile):
        pass

    def get_metadata_values_for_collection_item_by_key(self, collection, key):
        pass

    def handle_mediafile_deleted(self, parents):
        pass

    def handle_mediafile_status_change(self, mediafile):
        pass

    def patch_collection_item_metadata(self, collection, id, content):
        metadata = self.get_collection_item_sub_item(collection, id, "metadata")
        for item in content:
            if existing := next((x for x in metadata if x["key"] == item["key"]), None):
                metadata.remove(existing)
            metadata.append(item)
        return self.patch_item_from_collection(collection, id, {"metadata": metadata})[
            "metadata"
        ]

    def patch_collection_item_relations(self, collection, id, content, parent=True):
        pass

    def patch_item_from_collection(self, collection, id, content) -> dict:
        pass

    def reindex_mediafile_parents(self, mediafile=None, parents=None):
        pass

    def save_item_to_collection(self, collection, content):
        pass

    def set_primary_field_collection_item(
        self, collection, entity_id, mediafile_id, field
    ):
        pass

    def update_collection_item_relations(self, collection, id, content, parent=True):
        pass

    def update_collection_item_sub_item(self, collection, id, sub_item, content):
        pass

    def update_item_from_collection(self, collection, id, content):
        pass

    def update_parent_relation_values(self, collection, parent_id):
        pass
