import requests
import srt
import util

from app import policy_factory
from apps.coghent.resources.base_resource import CoghentBaseResource
from datetime import timedelta
from flask import Blueprint, request
from flask_restful import abort, Api
from inuits_policy_based_auth import RequestContext
from srt import Subtitle

api_bp = Blueprint("story_box", __name__)
api = Api(api_bp)


class StoryBox(CoghentBaseResource):
    @policy_factory.apply_policies(RequestContext(request, ["get-story-box"]))
    def get(self):
        token = policy_factory.get_user_context().auth_objects.get("token")
        self._abort_if_not_logged_in(token)
        fields = {"type": "frame", "user": token["email"]}
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        return self.storage.get_items_from_collection("entities", skip, limit, fields)


class StoryBoxLink(CoghentBaseResource):
    @policy_factory.apply_policies(RequestContext(request, ["link-story-box"]))
    def post(self, code):
        token = policy_factory.get_user_context().auth_objects.get("token")
        self._abort_if_not_logged_in(token)
        box_visit = self._abort_if_item_doesnt_exist("box_visits", code)
        relations = self.storage.get_collection_item_relations(
            "box_visits", util.get_raw_id(box_visit)
        )
        if story_box := next((x for x in relations if x["type"] == "story_box"), None):
            if "linked" in story_box and story_box["linked"]:
                abort(400, message="Code has already been linked")
            story_box["linked"] = True
            self.storage.patch_collection_item_relations(
                "box_visits", util.get_raw_id(box_visit), [story_box], False
            )
            content = {"user": token["email"]}
            story_box = self.storage.patch_item_from_collection(
                "entities", story_box["key"].removeprefix("entities/"), content
            )
        else:
            content = {
                "type": "frame",
                "metadata": [{"key": "type", "value": "frame", "language": "en"}],
                "user": token["email"],
            }
            story_box = self.storage.save_item_to_collection("entities", content)
            content = [{"key": story_box["_id"], "type": "story_box", "linked": True}]
            self.storage.patch_collection_item_relations(
                "box_visits", util.get_raw_id(box_visit), content, False
            )
        return story_box, 201


class StoryBoxPublish(CoghentBaseResource):
    @policy_factory.apply_policies(RequestContext(request, ["publish-story-box"]))
    def post(self, id):
        self._abort_if_not_logged_in(
            policy_factory.get_user_context().auth_objects.get("token")
        )
        story_box = self._abort_if_item_doesnt_exist("entities", id)
        story_box_relations = self.storage.get_collection_item_relations(
            "entities", util.get_raw_id(story_box)
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
            "entities", util.get_raw_id(story), content
        )
        return self._create_box_visit({"story_id": util.get_raw_id(story)}), 201


class StoryBoxSubtitles(CoghentBaseResource):
    @policy_factory.apply_policies(RequestContext(request, ["subtitles-story-box"]))
    def post(self, id):
        token = policy_factory.get_user_context().auth_objects.get("token")
        self._abort_if_not_logged_in(token)
        story_box = self._abort_if_item_doesnt_exist("entities", id)
        relations = [
            x
            for x in self.storage.get_collection_item_relations(
                "entities", util.get_raw_id(story_box)
            )
            if x["type"] == "components"
        ]
        # FIXME: delete file from storage-api (directly, not using event)
        if subtitle := next(
            (x for x in relations if x.get("label", "") == "subtitle"), None
        ):
            self.storage.delete_item_from_collection(
                "mediafiles", subtitle["key"].removeprefix("mediafiles/")
            )
        subtitles = []
        relations = [
            x
            for x in relations
            if all(y in x for y in ["value", "timestamp_start", "timestamp_end"])
        ]
        for relation in relations:
            subtitles.append(
                Subtitle(
                    index=None,
                    start=timedelta(seconds=relation["timestamp_start"]),
                    end=timedelta(seconds=relation["timestamp_end"]),
                    content=relation["value"],
                )
            )
        mediafile = self.storage.save_item_to_collection(
            "mediafiles",
            {
                "filename": "storybox_srt.srt",
                "metadata": [],
                "user": token["email"],
            },
        )
        if not mediafile:
            abort(400, message="Failed to create mediafile")
        srt_string = f"{{\\{util.get_raw_id(story_box)}}}\n{srt.compose(subtitles)}"
        # FIXME: add auth headers
        req = requests.post(
            f"{self.storage_api_url}/upload?id={util.get_raw_id(mediafile)}",
            files={"file": ("storybox_srt.srt", bytes(srt_string, "utf-8"))},
        )
        if req.status_code == 409:
            abort(409, message=f"Duplicate srt found: {req.text.strip()}")
        elif req.status_code != 201:
            self.storage.delete_item_from_collection(
                "mediafiles", util.get_raw_id(mediafile)
            )
            abort(400, message=req.text.strip())
        new_relation = {
            "key": mediafile["_id"],
            "type": "components",
            "label": "subtitle",
            "order": 1,
        }
        self.storage.add_relations_to_collection_item(
            "entities", util.get_raw_id(story_box), [new_relation]
        )
        return "", 201


api.add_resource(StoryBox, "/story_box")
api.add_resource(StoryBoxLink, "/story_box/link/<string:code>")
api.add_resource(StoryBoxPublish, "/story_box/publish/<string:id>")
api.add_resource(StoryBoxSubtitles, "/story_box/subtitles/<string:id>")
