from flask import request
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies
from resources.base_resource import BaseResource
from rabbit import get_rabbit
from elody.job import start_job, finish_job, fail_job


class StartJob(BaseResource):
    @apply_policies(RequestContext(request))
    def post(self):
        content = request.get_json()
        job = start_job(
            content.get("name"),
            content.get("job_type"),
            get_rabbit=get_rabbit,
            user_email=content.get("user_email"),
            parent_id=content.get("parent_id"),
        )

        return job, 200


class FinishJob(BaseResource):
    @apply_policies(RequestContext(request))
    def post(self, id):
        finish_job(id, get_rabbit=get_rabbit)
        return None, 200


class FailJob(BaseResource):
    @apply_policies(RequestContext(request))
    def post(self, id):
        content = request.get_json()
        fail_job(
            id,
            content.get("exception_message"),
            get_rabbit=get_rabbit,
        )

        return None, 200
