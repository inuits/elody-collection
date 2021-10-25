import app
import json
import os
import uuid

from cloudevents.http import CloudEvent, to_json
from flask import jsonify
from resources.base_resource import BaseResource


class ImporterStart(BaseResource):
    @app.require_oauth()
    def post(self):
        request_body = self.get_request_body()
        message_id = str(uuid.uuid4())
        upload_folder = os.path.join(
            self.upload_source, str(request_body["upload_folder"]).removeprefix("/")
        )
        attributes = {
            "id": message_id,
            "message_id": message_id,
            "type": "dams.import_start",
            "source": "dams"
        }
        data = {
                "upload_source": self.upload_source,
                "upload_folder": upload_folder,
                "collection_api_url": self.collection_api_url,
                "storage_api_url": self.storage_api_url,
        }
        event = CloudEvent(attributes, data)
        message = json.loads(to_json(event))
        app.ramq.send(message, routing_key="dams.import_start", exchange_name="dams")
        return message, 201


class ImporterDirectories(BaseResource):
    @app.require_oauth()
    def get(self):
        directories = [
            str(x[0]).removeprefix(self.upload_source)
            for x in os.walk(self.upload_source)
        ]
        return jsonify(directories)
