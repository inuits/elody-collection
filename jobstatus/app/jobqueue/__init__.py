import json
import os

import requests

from app.config import ramq

storage_api = os.getenv("STORAGE_API_URL", "http://localhost:8001")
mount_point = os.getenv("MOUNT_POINT", "")


@ramq.queue(
    exchange_name=os.getenv("RABMQ_SEND_EXCHANGE_NAME"),
    routing_key=os.getenv("RABMQ_ROUTING_KEY"),
)
def flask_rabmq_test(body):
    data = json.loads(body)
    if data.job_type == "multiple":
        file_path = mount_point + data.asset
        file_name = os.path.basename(file_path)
        save_file = requests.post(f"{storage_api}/upload/{os.path.basename(file_path)}")
    return True
