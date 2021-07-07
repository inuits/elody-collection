from flask_jwt_extended import current_user

from model.jobs import Job, JobStatus
from model.user import User


def process_enum_instace(jobs):
    """
    this method will be simplified as of now it is feasible for small amount of satasets - less than 1000 or so ,
    optimizing this method will increase its performance @param jobs: @return:
    """
    for job in jobs:
        for state in JobStatus:
            if job.status == state:
                job.status = state.name

    return jobs


class QueryResolverClass:
    """
    QueryResolver
    process all request from queries
    """

    def __init__(self, name):
        self.name = name

    def welcome_data(self):
        """

        @return:
        """
        jobs = Job.query.all()

        return {
            "message": f"Hello {self.name}, welcome to our jobstatus GraphQL server",
            "status": True,
            "jobs": process_enum_instace(jobs),
        }

    @classmethod
    def get_job_by_id(cls, job_id):
        """Gets Jobs by an id sent the request. Upon successful validation of job_id  it returns an
        instance of Jobs model"""
        # SQLAlchemy error handling on db access will be implemented here.
        # search jobs from DB based on the id.
        job = Job.query.filter_by(job_id=job_id).one_or_none()
        print(job.status)
        for state in JobStatus:
            if job.status == state:
                job.status = state.name
        # Return job and other metadata if job is found else returns an error message.
        return (
            {"message": "fetched Job data successfully", "status": True, "job": job}
            if job is not None
            else {"message": "data fetching failed", "status": False}
        )

    @classmethod
    def get_jobs_by_asset(cls, asset):
        """Gets Jobs by an id provide in the request. Upon successful validation of assets  it returns an
        instance of Jobs model"""
        jobs = Job.query.filter_by(asset=asset).all()

        response = {
            "message": f"fetched {len(jobs)} jobs by asset",
            "jobs": process_enum_instace(jobs) if jobs is not None else None,
        }
        return response

    @classmethod
    def get_jobs_by_user(cls, option=None):
        """ Gets all Jobs n in the database for the current logged in user"""
        try:
            current_user_jobs = Job.query.filter_by(owner=current_user.user_id)

            response = {
                "message": f"fetched all jobs for {current_user.name}",
                "status": True,
                "jobs": process_enum_instace(current_user_jobs)
                if current_user_jobs is not None
                else None,
            }
        except BaseException as err:
            response = {
                "message": f"Could not fetch jobs for {current_user.name}",
                "error": [err],
            }

        return response
