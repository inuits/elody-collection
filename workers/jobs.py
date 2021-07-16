import json
import os

import requests

import app
from storage.storagemanager import MongoStorageManager as storage_manager

storage_api = os.getenv("STORAGE_API_URL", "http://localhost:8001")
mount_point = os.getenv("MOUNT_POINT", "")


class CreateJobs:
    def __init__(self, job_type, job):
        self.job_type = job_type
        self.job = job
        self.storage = storage_manager
        self.db = self.storage().db
        self.location = os.getenv("UPLOAD_FOLDER", "/mnt/media-import")

    def create_job(self):
        """ creates  jobs """
        message = {}
        job_data = {}
        data = list()
        if self.job_type == "multiple":
            # For multiple file upload prepare the data and append to a list
            for item in self.job:
                upload_folder = {
                    "upload_folder": os.path.join(self.location, item["file_name"])
                }
                data.append(upload_folder)

            message["asset"] = data
        else:
            # For single upload
            message["asset"] = {
                "upload_folder": os.path.join(self.location, self.job["file_name"])
            }
        message["job_type"] = self.job_type

        save_job = self.storage().save_item_to_collection(
            "jobs", self.job
        )  # Save the job to collection
        message["job_id"] = save_job["_id"]
        app.ramq.send(
            message,
            exchange_name="dams",
            routing_key="dams.import_start",
        )

        return message, 201


@app.ramq.queue(
    exchange_name="dams",
    routing_key="dams.import_start",
)
def job_status(body):
    data = json.loads(body)

    # fetch job from collection
    job = storage_manager().get_jobs_from_collection("jobs", "job_id", data["job_id"])

    # process multiple jobs
    if data["job_type"] == "multiple":
        for data in data:
            save_file = process_data(mount_point + data["asset"])
            job["status"] = "finished" if save_file.status_code == 201 else "failed"

    else:
        # process single jobs
        save_file = process_data(mount_point + data["asset"])
        job["status"] = "finished" if save_file.status_code == 201 else "failed"
    storage_manager().patch_item_from_collection("jobs", job["job_id"], job)
    return True


def process_data(path):
    file_name = os.path.basename(path)
    return requests.post(f"{storage_api}/upload/{os.path.basename(file_name)}")
