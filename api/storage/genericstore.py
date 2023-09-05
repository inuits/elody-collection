class GenericStorageManager:
    def add_mediafile_to_collection_item(
        self, collection, id, mediafile_id, mediafile_public
    ):
        pass

    def add_relations_to_collection_item(
        self, collection, id, relations, parent=True, dst_collection=None
    ):
        pass

    def add_sub_item_to_collection_item(self, collection, id, sub_item, content):
        pass

    def check_health(self):
        return True

    def delete_collection_item_relations(self, collection, id, content, parent=True):
        pass

    def delete_collection_item_sub_item_key(self, collection, id, sub_item, key):
        sub_items = self.get_collection_item_sub_item(collection, id, sub_item)
        if not sub_items:
            return
        patch_data = {sub_item: []}
        for obj in sub_items:
            if obj["key"] != key:
                patch_data[sub_item].append(obj)
        self.patch_item_from_collection(collection, id, patch_data)

    def delete_item_from_collection(self, collection, id):
        pass

    def drop_all_collections(self):
        pass

    def get_collection_item_mediafiles(self, collection, id, skip=0, limit=0):
        pass

    def get_collection_item_relations(
        self, collection, id, include_sub_relations=False, exclude=None
    ):
        pass

    def get_collection_item_sub_item(self, collection, id, sub_item):
        if item := self.get_item_from_collection_by_id(collection, id):
            return item.get(sub_item)
        return None

    def get_collection_item_sub_item_key(self, collection, id, sub_item, key):
        if sub_items := self.get_collection_item_sub_item(collection, id, sub_item):
            return list(filter(lambda x: x["key"] == key, sub_items))
        return None

    def get_entities(
        self,
        skip=0,
        limit=20,
        skip_relations=0,
        filters=None,
        order_by=None,
        ascending=True,
    ):
        pass

    def get_history_for_item(self, collection, id, timestamp=None, all_entries=None):
        pass

    def get_item_from_collection_by_id(self, collection, id) -> dict:
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
        if not metadata:
            return None
        for item in content:
            if existing := next((x for x in metadata if x["key"] == item["key"]), None):
                metadata.remove(existing)
            metadata.append(item)
        return self.patch_item_from_collection(collection, id, {"metadata": metadata})[
            "metadata"
        ]

    def patch_collection_item_relations(self, collection, id, content, parent=True):
        pass

    def patch_item_from_collection(
        self, collection, id, content, create_sortable_metadata=True
    ) -> dict:
        pass

    def reindex_mediafile_parents(self, mediafile=None, parents=None):
        pass

    def save_item_to_collection(
        self,
        collection,
        content,
        only_return_id=False,
        create_sortable_metadata=True,
    ):
        pass

    def set_primary_field_collection_item(self, collection, id, mediafile_id, field):
        pass

    def update_collection_item_relations(self, collection, id, content, parent=True):
        pass

    def update_collection_item_sub_item(self, collection, id, sub_item, content):
        patch_data = {sub_item: content}
        item = self.patch_item_from_collection(collection, id, patch_data)
        return item[sub_item]

    def update_item_from_collection(
        self, collection, id, content, create_sortable_metadata=True
    ):
        pass

    def update_parent_relation_values(self, collection, parent_id):
        pass
