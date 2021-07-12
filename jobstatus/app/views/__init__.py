import os
from app.config import app
from app.model import Job

# Temporal HTTP endpoints - browser version

@app.route("/job/<id>", methods=["GET", "POST"])
def job_status_api(id):
    return Job.query.filter(Job.asset == id)


@app.route("/job/<asset>", methods=["POST", "GET"])
def get_job_by_asset(asset):
    return Job.query.filter(Job.asset == asset)
