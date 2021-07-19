import app

import hashlib
import json
import os
import requests

from resources.base_resource import BaseResource

mount_point = os.getenv("MOUNT_POINT", "")
storage_api = os.getenv("STORAGE_API_URL", "http://localhost:8001")


@app.ramq.queue(
    exchange_name=os.getenv("EXCHANGE_NAME", "dams"),
    routing_key=os.getenv("ROUTING_KEY", "dams.job_status"),
)
def job_status(body):
    data = json.loads(body)
    data_fetcher = BaseResource().storage
    # fetch job from collection
    job = data_fetcher.get_jobs_from_collection("jobs", data["job_id"])
    job["status"] = "In-Progress"
    # Update Job Status
    data_fetcher.patch_item_from_collection("jobs", job["job_id"], job)
    # process multiple jobs
    if data["job_type"] == "multiple":
        for data in data["data"]:
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
