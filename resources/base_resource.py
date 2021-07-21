import hashlib
import os
import uuid

from flask import request, g
from flask_restful import Resource, abort, reqparse
from storage.storagemanager import StorageManager
from werkzeug.exceptions import BadRequest


class BaseResource(Resource):
    token_required = os.getenv("REQUIRE_TOKEN", "True").lower() in ["true", "1"]

    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        self.storage_api_url = os.getenv("STORAGE_API_URL", "http://localhost:8001")
        self.req = reqparse.RequestParser()
        self.set_initial_conf()

    def set_initial_conf(self):
        # may be subject to changes
        if not self.storage.get_item_from_collection_by_id("config", "0"):
            location = os.getenv("UPLOAD_LOCATION", "/mnt/media-import")
            content = {
                "identifiers": ["0"],
                "upload_sources": [location],
                "upload_location": location,
            }
            self.storage.save_item_to_collection("config", content)

    def get_request_body(self):
        invalid_input = False
        try:
            request_body = request.get_json()
            invalid_input = request_body is None
        except BadRequest:
            invalid_input = True
        if invalid_input:
            abort(
                405,
                message="Invalid input",
            )
        return request_body

    def abort_if_item_doesnt_exist(self, collection, id):
        item = self.storage.get_item_from_collection_by_id(collection, id)
        if item is None:
            abort(
                404,
                message="Item {} doesn't exist in collection {}".format(id, collection),
            )
        return item

    def get_job_by_signature(self, signature):
        """ This method is necessary for reuse in some parts of job creation """
        return self.storage.get_jobs_from_collection("jobs", signature)

    def prepare_job_data(self, job_type):
        parse_data = self.req.parse_args(self)
        job_data = {
            "job_info": parse_data.get("info"),
            "job_type": job_type,
            "user": g.oidc_token_info["email"],
            "status": "Queued",
        }
        return job_data

    def create_single_job(self, file):
        """ creates  single job """
        data_fetch = self.get_job_by_signature(self.generate_file_signature(file))
        message_id = str(uuid.uuid4())
        file.sa
        m_message = {
            "data": {"job_folder": os.path.join(self.upload_source, file.filename)},
            "asset": file.filename,
        }
        if data_fetch is None:
            save_job = self.storage.save_item_to_collection(
                "jobs", self.prepare_job_data(job_type="single")
            )
            m_message["job_id"] = save_job["_id"]
        else:
            abort(409, message=f"File {file.filename} exists")

        m_message["message_id"] = message_id

        return m_message, 201

    def create_multiple_jobs(self):
        job = self.prepare_job_data(job_type="multiple")
        parse_data = self.req.parse_args(self)
        message = {}
        file_errors = list()
        asset = list()
        for item in parse_data.get("asset"):
            signature = self.generate_file_signature(item)
            data = self.get_job_by_signature(signature)
            if data is None:
                job_folder = os.path.join(self.location, item.filename)
                asset.append({"job_folder": job_folder})
            else:
                file_errors.append({"file_name": item.filename, "state": "file exists"})
        message["asset"] = asset
        job["asset"] = asset
        if len(file_errors) > 0:
            message["file_states"] = file_errors
            return message, 409
        else:
            save = self.storage.save_item_to_collection("jobs", job)
            message["message_id"] = save["_id"]
            return message, 201

    @staticmethod
    def generate_file_signature(file):
        path = os.path.join(os.getenv("UPLOAD_SOURCE", "/mnt/media-import"), file)
        obj = hashlib.md5()
        size = 128 * obj.block_size
        if file:
            parts = file.read(size)
            while parts:
                obj.update(parts)
                parts = file.read(size)
        return obj.hexdigest()
