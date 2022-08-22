from apps.coghent.storage.storagemanager import CoghentStorageManager
from datetime import datetime
from flask import abort
from resources.base_resource import BaseResource


class CoghentBaseResource(BaseResource):
    def __init__(self):
        super().__init__()
        self.storage = CoghentStorageManager().get_db_engine()

    def _create_box_visit(self, content):
        if "story_id" not in content:
            abort(405, message="Invalid input")
        story = self.abort_if_item_doesnt_exist("entities", content["story_id"])
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
