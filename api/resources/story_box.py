import app

from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource


class StoryBox(BaseResource):
    @app.require_oauth("get-story-box")
    def get(self):
        filters = {"type": "frame", "user": current_token["email"]}
        return self.storage.get_items_from_collection_by_fields("entities", filters)


class StoryBoxLink(BaseResource):
    @app.require_oauth("link-story-box")
    def post(self, code):
        box_visit = self.abort_if_item_doesnt_exist("box_visits", code)
        relations = self.storage.get_collection_item_relations(
            "box_visits", self._get_raw_id(box_visit)
        )
        if story_box := next((x for x in relations if x["type"] == "story_box"), None):
            content = {"user": current_token["email"]}
            story_box = self.storage.patch_item_from_collection(
                "entities", self._get_raw_id(story_box), content
            )
        else:
            content = {
                "type": "frame",
                "metadata": [{"key": "type", "value": "frame", "language": "en"}],
                "user": current_token["email"],
            }
            story_box = self.storage.save_item_to_collection("entities", content)
            content = [{"key": story_box["_id"], "type": "story_box"}]
            self.storage.patch_collection_item_relations(
                "box_visits", self._get_raw_id(box_visit), content, False
            )
        return story_box, 201
