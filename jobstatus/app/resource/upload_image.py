import os
import uuid

import werkzeug
from flask_jwt_extended import jwt_required, current_user
from flask_restful import Resource, reqparse

from app.config import ramq
from app.model import Job


# Switch Authorization tokens if you use openIDconnect uncomment line 25 and comment out line 24
@jwt_required()
# @oidc.accept_token(require_token=True, scopes_required=['openid'])


class FileUpload(Resource):
    def post(self, job):
        media_id = str(uuid.uuid4())
        rename = lambda a: [e.filename == f'{e}_{media_id.split("-")[0]}' for e in a]

        job_info = job["description"]
        uploaded_files = job["file"]
        job_type = "multiple" if isinstance(uploaded_files, list) else "single"
        files = (
            rename(uploaded_files)
            if isinstance(uploaded_files, list)
            else uploaded_files
        )
        media_location = os.getenv("UPLOAD_FOLDER", f"/mnt/media-import/{media_id}")
        uploaded_files.save()
        job_state = Job(
            job_info=job_info,
            asset=media_location,
            user=current_user.name,
            mefiafile_id=media_id,
            status="queued",
        )

        job_state.save()

        message = {
            "media_file": media_location,
            "user": current_user,
            "job_id": job_state._id,
            "job_info": job["description"],
            "job_type": job_type,
            "job_status": job_state.status,
        }

        ramq.send(message, exchange_name="dams", routing_key="dams.import_start")

        return message
