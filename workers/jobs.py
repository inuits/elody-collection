import hashlib
import json
import os
import uuid

import requests
from flask_restful import abort

import app
from storage.mongostore import MongoStorageManager

storage_api = os.getenv("STORAGE_API_URL", "http://localhost:8001")
mount_point = os.getenv("MOUNT_POINT", "")
location = os.getenv("UPLOAD_FOLDER", "/mnt/media-import")


class DBFetch(MongoStorageManager):
    def is_duplicate(self, signature):
        fetch_data = self.db["jobs"].find_one({"signature": signature})
        return fetch_data is None


class CreateJobs:
    def __init__(self, job_type, job, user):
        self.job_type = job_type
        self.job = job
        self.user = user

    def create_single_job(self):
        """ creates  jobs """
        data_fetch = DBFetch()

        job_data = {
            "job_info": self.job.get("info"),
            "job_type": self.job_type,
            "user": self.user,
            "status": "Queued",
        }
        message_id = (str(uuid.uuid4()),)
        if self.job_type == "single":
            # generate file signatire
            signature = generate_file_signature(self.job.get("asset"))
            m_message = {
                "data": {
                    "job_folder": os.path.join(location, self.job.get("asset").filename)
                },
                "job_id": self.job_type,
                "asset": self.job.get("asset").filename,
            }
            job_data["signature"] = signature

            if data_fetch.is_duplicate(signature):
                save_job = data_fetch.save_item_to_collection("jobs", job_data)
                m_message["job_id"] = save_job["_id"]
            else:
                abort(409, message=f'File {self.job.get("asset").filename} exists')

            m_message["message_id"] = message_id
            app.ramq.send(
                m_message,
                exchange_name=os.getenv("EXCHANGE_NAME"),
                routing_key=os.getenv("ROUTING_KEY"),
            )
            return m_message, 201
        else:
            return {'message': "This end-point is in development"}


@app.ramq.queue(
    exchange_name=os.getenv("EXCHANGE_NAME"),
    routing_key=os.getenv("ROUTING_KEY"),
)
def job_status(body):
    data = json.loads(body)
    data_fetcher = DBFetch()
    # fetch job from collection
    job = data_fetcher.get_jobs_from_collection("jobs", data["job_id"])
    job["status"] = "In-Progress"
    # Update Job Status
    data_fetcher.patch_item_from_collection("jobs", job["job_id"], job)
    # process multiple jobs
    if data["job_type"] == "multiple":
        for data in data['data']:
            save = process_data(mount_point + data["job_folder"])
            job["status"] = "Finished" if save.status_code is 201 else "Failed"
    else:
        # process single jobs
        save_file = process_data(mount_point + data["asset"])
        job["status"] = "Finished" if save_file.status_code == 201 else "Failed"
    # Update Job Status
    data_fetcher.patch_item_from_collection("jobs", job["job_id"], job)
    return True


def process_data(path):
    file_name = os.path.basename(path)
    return requests.post(f"{storage_api}/upload/{os.path.basename(file_name)}")


def generate_file_signature(file):
    obj = hashlib.md5()
    size = 128 * obj.block_size
    parts = file.read(size)
    while parts:
        obj.update(parts)
        parts = file.read(size)
    return obj.hexdigest()
