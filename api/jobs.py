import json
import string
import uuid
from datetime import datetime
from enum import Enum

from cloudevents.http import CloudEvent, to_json
from flask import g
from rabbitmq_pika_flask import RabbitMQ

from storage.storagemanager import StorageManager


class Status(Enum):
    QUEUED = "queued"
    IN_PROGRESS = "in-progress"
    FINISHED = "finished"
    FAILED = "failed"


class JobExtension:
    def __init__(self, rabbit: RabbitMQ):
        self.storage = StorageManager().get_db_engine()
        self.rabbit = rabbit

    def create_new_job(
        self,
        job_info: string,
        job_type: string,
        identifier: uuid.uuid1,
        asset_id=None,
        mediafile_id=None,
        parent_job_id=None,
    ):
        new_job = {
            "job_type": job_type,
            "job_info": job_info,
            "status": Status.QUEUED.value,
            "start_time": str(datetime.utcnow()),
            "user": g.oidc_token_info["email"]
            if hasattr(g, "oidc_token_info")
            else "default_uploader",
            "asset_id": "" if asset_id is None else asset_id,
            "mediafile_id": "" if mediafile_id is None else mediafile_id,
            "parent_job_id": "" if parent_job_id is None else parent_job_id,
            "completed_jobs": 0,
            "amount_of_jobs": 1,
            "identifiers": [identifier],
        }
        self.send_cloud_event(new_job, "dams.job_created")
        return new_job

    def progress_job(
        self,
        job,
        asset_id=None,
        mediafile_id=None,
        parent_job_id=None,
        amount_of_jobs=None,
        count_up_completed_jobs=False,
    ):

        if asset_id is not None:
            job["asset_id"] = asset_id
        if mediafile_id is not None:
            job["mediafile_id"] = mediafile_id
        if parent_job_id is not None:
            job["parent_job_id"] = parent_job_id
        if amount_of_jobs is not None:
            job["amount_of_jobs"] = amount_of_jobs
        if count_up_completed_jobs:
            job["completed_jobs"] = job["completed_jobs"] + 1
        job["status"] = Status.IN_PROGRESS.value
        self.send_cloud_event(job, "dams.job_changed")
        return job

    def finish_job(self, job, parent_job=None):
        job["status"] = Status.FINISHED.value
        job["completed_jobs"] = job["amount_of_jobs"]
        job["end_time"] = str(datetime.utcnow())
        if job["parent_job_id"] not in ["", None] and parent_job is not None:
            self.progress_job(parent_job, count_up_completed_jobs=True)
        self.send_cloud_event(job, "dams.job_changed")
        return job

    def fail_job(self, job, error_message=""):
        job["status"] = Status.FAILED.value
        job["end_time"] = str(datetime.utcnow())
        job["error_message"] = error_message
        self.send_cloud_event(job, "dams.job_changed")
        return job

    def send_cloud_event(self, job, event_type):
        attributes = {"type": event_type, "source": "dams"}
        event = CloudEvent(attributes, job)
        message = json.loads(to_json(event))
        self.rabbit.send(message, routing_key=event_type)

    def save_to_db(self, job):
        self.storage.save_item_to_collection("jobs", job)
        return True
