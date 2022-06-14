import app

from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource


class StoryBox(BaseResource):
    @app.require_oauth("get-story-box")
    def get(self):
        filters = {"type": "frame", "user": current_token["Email"]}
        return self.storage.get_items_from_collection_by_fields("entities", filters)


class StoryBoxLink(BaseResource):
    @app.require_oauth("link-story-box")
    def post(self, code):
        box_visit = self.abort_if_item_doesnt_exist("entities", code)
        content = {
            "type": "frame",
            "metadata": {"key": "type", "value": "frame", "language": "en"},
            "user": current_token["Email"],
        }
        frame = self.storage.save_item_to_collection("entities", content)
        relations = self.storage.get_collection_item_relations(
            "box_visits", self._get_raw_id(box_visit)
        )
        in_basket = [x["key"] for x in relations if x["type"] == "inBasket"]
        new_relations = [
            {"key": item, "label": "asset", "type": "components"} for item in in_basket
        ]
        new_relations = self.storage.add_relations_to_collection_item(
            "entities", self._get_raw_id(frame), new_relations
        )
        frame["metadata"] |= new_relations
        return frame, 201
