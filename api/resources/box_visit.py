import app

from datetime import datetime
from flask import request, after_this_request
from flask_restful import abort
from resources.base_resource import BaseResource
from validator import box_visit_schema


class BoxVisit(BaseResource):
    @app.require_oauth("create-box-visit")
    def post(self):
        content = self.get_request_body()
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

    @app.require_oauth("read-box-visit")
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        item_type = request.args.get("type", None)
        type_filter = f"type={item_type}&" if item_type else ""
        ids = request.args.get("ids", None)
        if ids:
            ids = ids.split(",")
        box_visits = self.storage.get_box_visits(skip, limit, item_type, ids)
        count = box_visits["count"]
        box_visits["limit"] = limit
        if skip + limit < count:
            box_visits[
                "next"
            ] = f"/box_visits?{type_filter}skip={skip + limit}&limit={limit}"
        if skip > 0:
            box_visits[
                "previous"
            ] = f"/box_visits?{type_filter}skip={max(0, skip - limit)}&limit={limit}"
        return box_visits


class BoxVisitDetail(BaseResource):
    @app.require_oauth("read-box-visit")
    def get(self, id):
        box_visit = self.abort_if_item_doesnt_exist("box_visits", id)
        box_visit = self._add_relations_to_metadata(
            box_visit, "box_visits", sort_by="order"
        )
        return box_visit

    @app.require_oauth("update-box-visit")
    def put(self, id):
        self.abort_if_item_doesnt_exist("box_visits", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("BoxVisit", content, box_visit_schema)
        box_visit = self.storage.update_item_from_collection("box_visits", id, content)
        return box_visit, 201

    @app.require_oauth("patch-box-visit")
    def patch(self, id):
        self.abort_if_item_doesnt_exist("box_visits", id)
        content = self.get_request_body()
        box_visit = self.storage.patch_item_from_collection("box_visits", id, content)
        return box_visit, 201

    @app.require_oauth("delete-box-visit")
    def delete(self, id):
        box_visit = self.abort_if_item_doesnt_exist("box_visits", id)
        self.storage.delete_item_from_collection("box_visits", id)
        return "", 204


class BoxVisitRelationsAll(BaseResource):
    @app.require_oauth("get-box-visit-relations")
    def get(self, id):
        self.abort_if_item_doesnt_exist("box_visits", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations(
            "box_visits", id, include_sub_relations=True
        )


class BoxVisitRelations(BaseResource):
    @app.require_oauth("get-box-visit-relations")
    def get(self, id):
        self.abort_if_item_doesnt_exist("box_visits", id)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        return self.storage.get_collection_item_relations("box_visits", id)

    @app.require_oauth("add-box-visit-relations")
    def post(self, id):
        box_visit = self.abort_if_item_doesnt_exist("box_visits", id)
        content = self.get_request_body()
        relations = self.storage.add_relations_to_collection_item(
            "box_visits", id, content, False
        )
        return relations, 201

    @app.require_oauth("update-box-visit-relations")
    def put(self, id):
        box_visit = self.abort_if_item_doesnt_exist("box_visits", id)
        content = self.get_request_body()
        relations = self.storage.update_collection_item_relations(
            "box_visits", id, content, False
        )
        return relations, 201

    @app.require_oauth("patch-box-visit-relations")
    def patch(self, id):
        box_visit = self.abort_if_item_doesnt_exist("box_visits", id)
        content = self.get_request_body()
        relations = self.storage.patch_collection_item_relations(
            "box_visits", id, content, False
        )
        return relations, 201

    @app.require_oauth("delete-box-visit-relations")
    def delete(self, id):
        box_visit = self.abort_if_item_doesnt_exist("box_visits", id)
        content = self.get_request_body()
        self.storage.delete_collection_item_relations("box_visits", id, content, False)
        return "", 204
