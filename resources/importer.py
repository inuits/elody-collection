from resources.base_resource import BaseResource
import json
from workers.importer import Importer
from storage.storagemanager import StorageManager

import app


storage = StorageManager().get_db_engine()
importer = Importer(storage)


@app.ramq.queue(exchange_name="dams", routing_key="dams.import_start")
def csv_import(body):
    body_dict = json.loads(body)
    upload_folder = body_dict["data"]["upload_folder"]
    importer.import_from_csv(upload_folder)
    return True


class ImporterStart(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        message = {"message_id": 1, "data": {"upload_folder": "/app/import"}}
        app.ramq.send(message, routing_key="dams.import_start", exchange_name="dams")
        return message
