import app
import os

from apps.coghent.storage.storagemanager import CoghentStorageManager
from datetime import datetime
from flask_restful import abort
from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource


class CoghentBaseResource(BaseResource):
    def __init__(self):
        super().__init__()
        self.storage = CoghentStorageManager().get_db_engine()
        self.mapping = {
            os.getenv("ARCHIEFGENT_ID"): "all-archiefgent",
            os.getenv("DMG_ID"): "all-dmg",
            os.getenv("HVA_ID"): "all-hva",
            os.getenv("INDUSTRIEMUSEUM_ID"): "all-industriemuseum",
            os.getenv("SIXTH_COLLECTION_ID"): "all-sixth",
            os.getenv("STAM_ID"): "all-stam",
        }

    def _abort_if_no_access(self, item, token, collection="entities", do_abort=True):
        if super()._abort_if_no_access(item, token, do_abort=False):
            return
        permission = self.mapping.get(self._get_museum_id(item, collection))
        if not permission or not app.require_oauth.check_permission(permission):
            abort(403, message="Access denied")

    def _create_box_visit(self, content):
        if "story_id" not in content:
            abort(405, message="Invalid input")
        story = self._abort_if_item_doesnt_exist("entities", content["story_id"])
        story = self._add_relations_to_metadata(story)
        num_frames = sum(
            map(lambda x: "type" in x and x["type"] == "frames", story["metadata"])
        )
        code = self.storage.generate_box_visit_code()
        box_visit = {
            "type": "box_visit",
            "identifiers": [code],
            "code": code,
            "start_time": datetime.now().isoformat(),
            "metadata": [],
            "frames_seen_last_visit": 0,
            "touch_table_time": None,
        }
        box_visit = self.storage.save_item_to_collection("box_visits", box_visit)
        relation = {
            "type": "stories",
            "label": "story",
            "key": story["_id"],
            "active": True,
            "total_frames": num_frames,
            "order": 0,
            "last_frame": "",
        }
        self.storage.add_relations_to_collection_item(
            "box_visits", self._get_raw_id(box_visit), [relation], False
        )
        relation = {
            "type": "story_box_visits",
            "label": "box_visit",
            "key": box_visit["_id"],
        }
        self.storage.add_relations_to_collection_item(
            "entities", self._get_raw_id(story), [relation], False
        )
        return self._add_relations_to_metadata(box_visit, "box_visits", sort_by="order")

    def _get_item_permissions(self, item_id, collection):
        item = self._abort_if_item_doesnt_exist(collection, item_id)
        full = ["can-get", "can-put", "can-patch", "can-delete"]
        if app.require_oauth.is_super_admin():
            return full, 200
        if self._is_owner_of_item(item, current_token):
            return full, 200
        if app.require_oauth.is_super_admin():
            return full, 200
        permission = self.mapping.get(self._get_museum_id(item, collection))
        if permission and app.require_oauth.check_permission(permission):
            return full, 200
        return [full[0]], 200

    def _get_museum_id(self, item, collection):
        if collection == "mediafiles":
            linked_entities = self.storage.get_mediafile_linked_entities(item)
            if not linked_entities:
                return ""
            # FIXME: won't work if mediafile is linked to different museums
            entity_id = linked_entities[0]["entity_id"].removeprefix("entities/")
        else:
            entity_id = self._get_raw_id(item)
        app.logger.info(f"Item to get museum from: {item}")
        relations = self.storage.get_collection_item_relations(
            "entities", entity_id, True
        )
        app.logger.info(f"Item relations: {relations}")
        for relation in relations:
            if relation["type"] == "isIn":
                app.logger.info(f'Returning: {relation["key"]}')
                return relation["key"]
        app.logger.info("isIn relation not found")
        return ""
