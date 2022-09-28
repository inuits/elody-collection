import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint, request
from flask_restful import abort, Api
from inuits_jwt_auth.authorization import current_token

api_bp = Blueprint("story_box", __name__)
api = Api(api_bp)


class StoryBox(CoghentBaseResource):
    @app.require_oauth("get-story-box")
    def get(self):
        self._abort_if_not_logged_in(current_token)
        fields = {"type": "frame", "user": current_token["email"]}
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        return self.storage.get_items_from_collection("entities", skip, limit, fields)


class StoryBoxLink(CoghentBaseResource):
    @app.require_oauth("link-story-box")
    def post(self, code):
        self._abort_if_not_logged_in(current_token)
        box_visit = self._abort_if_item_doesnt_exist("box_visits", code)
        relations = self.storage.get_collection_item_relations(
            "box_visits", self._get_raw_id(box_visit)
        )
        if story_box := next((x for x in relations if x["type"] == "story_box"), None):
            if "linked" in story_box and story_box["linked"]:
                abort(400, message="Code has already been linked")
            story_box["linked"] = True
            self.storage.patch_collection_item_relations(
                "box_visits", self._get_raw_id(box_visit), [story_box], False
            )
            content = {"user": current_token["email"]}
            story_box = self.storage.patch_item_from_collection(
                "entities", story_box["key"].removeprefix("entities/"), content
            )
        else:
            content = {
                "type": "frame",
                "metadata": [{"key": "type", "value": "frame", "language": "en"}],
                "user": current_token["email"],
            }
            story_box = self.storage.save_item_to_collection("entities", content)
            content = [{"key": story_box["_id"], "type": "story_box", "linked": True}]
            self.storage.patch_collection_item_relations(
                "box_visits", self._get_raw_id(box_visit), content, False
            )
        return story_box, 201


class StoryBoxPublish(CoghentBaseResource):
    @app.require_oauth("publish-story-box")
    def post(self, id):
        self._abort_if_not_logged_in(current_token)
        story_box = self._abort_if_item_doesnt_exist("entities", id)
        story_box_relations = self.storage.get_collection_item_relations(
            "entities", self._get_raw_id(story_box)
        )
        if story := next(
            (x for x in story_box_relations if x["type"] == "stories"), None
        ):
            story_relations = self.storage.get_collection_item_relations(
                "entities", story["key"].removeprefix("entities/")
            )
            if box_visit := next(
                (x for x in story_relations if x["type"] == "story_box_visits"), None
            ):
                box_visit = self.storage.get_item_from_collection_by_id(
                    "box_visits", box_visit["key"].removeprefix("box_visits/")
                )
                return self._add_relations_to_metadata(
                    box_visit, "box_visits", sort_by="order"
                )
        story = {
            "type": "story",
            "metadata": [
                {"key": "type", "value": "story", "language": "en"},
                {"key": "title", "value": "Storybox"},
                {"key": "description", "value": "Storybox"},
            ],
        }
        story = self.storage.save_item_to_collection("entities", story)
        content = [
            {"key": story_box["_id"], "label": "Frame", "type": "frames", "order": 1}
        ]
        self.storage.patch_collection_item_relations(
            "entities", self._get_raw_id(story), content
        )
        return self._create_box_visit({"story_id": self._get_raw_id(story)}), 201


api.add_resource(StoryBox, "/story_box")
api.add_resource(StoryBoxLink, "/story_box/link/<string:code>")
api.add_resource(StoryBoxPublish, "/story_box/publish/<string:id>")
