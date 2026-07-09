from elody.job import (
    add_document_to_job,
    fail_job,
    finish_job,
    finish_job_with_warning,
    init_job,
    start_job,
)
from flask import request
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies
from rabbit import get_rabbit
from resources.base_resource import BaseResource


class InitJob(BaseResource):
    @apply_policies(RequestContext(request))
    def post(self):
        content = request.get_json()
        job = init_job(
            content.get("name"),
            content.get("job_type"),
            get_rabbit=get_rabbit,
            user_email=content.get("user_email"),
            parent_id=content.get("parent_id"),
            id_of_document_job_was_initiated_for=content.get(
                "id_of_document_job_was_initiated_for"
            ),
            track_async_children=content.get("track_async_children", False),
        )

        return {"job_id": job}, 200


class AddDocumentToJob(BaseResource):
    @apply_policies(RequestContext(request))
    def post(self, id):
        content = request.get_json()
        add_document_to_job(
            id,
            content.get("id_of_document_job_was_initiated_for"),
            get_rabbit=get_rabbit,
        )
        return None, 200


class StartJob(BaseResource):
    @apply_policies(RequestContext(request))
    def post(self, id):
        start_job(id, get_rabbit=get_rabbit)
        return None, 200


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


class FinishJobWithWarning(BaseResource):
    @apply_policies(RequestContext(request))
    def post(self, id):
        content = request.get_json()
        finish_job_with_warning(
            id,
            info_message=content.get("info_message"),
            get_rabbit=get_rabbit,
        )

        return None, 200
