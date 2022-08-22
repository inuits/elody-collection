import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint, request, after_this_request
from flask_restful import Api
from validator import box_visit_schema

api_bp = Blueprint("box_visit", __name__)
api = Api(api_bp)


class BoxVisit(CoghentBaseResource):
    @app.require_oauth("create-box-visit")
    def post(self):
        content = self.get_request_body()
        return self._create_box_visit(content)

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


class BoxVisitDetail(CoghentBaseResource):
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


class BoxVisitRelations(CoghentBaseResource):
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


class BoxVisitRelationsAll(CoghentBaseResource):
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


api.add_resource(BoxVisit, "/box_visits")
api.add_resource(BoxVisitDetail, "/box_visits/<string:id>")
api.add_resource(BoxVisitRelations, "/box_visits/<string:id>/relations")
api.add_resource(BoxVisitRelationsAll, "/box_visits/<string:id>/relations/all")
