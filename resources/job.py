from flask import request

import app

from resources.base_resource import BaseResource
from validator import JobValidator

validator = JobValidator()


class Job(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json(validator, "job", content)
        Job = self.storage.save_item_to_collection("jobs", content)
        return Job, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        ids = request.args.get("ids")
        if ids:
            ids = ids.split(",")
            return self.storage.get_items_from_collection_by_ids("jobs", ids)
        jobs = self.storage.get_items_from_collection("jobs", skip, limit)
        count = jobs["count"]
        jobs["limit"] = limit
        if skip + limit < count:
            jobs["next"] = "/{}?skip={}&limit={}".format(
                "jobs", skip + limit, limit
            )
        if skip > 0:
            jobs["previous"] = "/{}?skip={}&limit={}".format(
                "jobs", max(0, skip - limit), limit
            )
        return jobs


class JobDetail(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        Job = self.abort_if_item_doesnt_exist("Jobs", id)
        return Job

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist("Jobs", id)
        content = self.get_request_body()
        Job = self.storage.patch_item_from_collection("Jobs", id, content)
        return Job, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("Jobs", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json(validator, "Job", content)
        Job = self.storage.update_item_from_collection("Jobs", id, content)
        return Job, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id):
        self.abort_if_item_doesnt_exist("Jobs", id)
        self.storage.delete_item_from_collection("Jobs", id)
        return "", 204
