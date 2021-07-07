import uuid

from flask_jwt_extended import current_user

from configuration.config import entity
from model.jobs import Job
from model.user import User


class MutationResolver:
    def __init__(self):
        pass

    @classmethod
    def save_job(cls, job):
        """Saves A Job into Jobs model.

        :param job this is an object of Job model
        """
        try:

            init_job = Job(
                start_date=job["start_date"],
                end_date=job["end_date"],
                # get the user from the token decoded inside JWTManager - setup is done in configuration.config file
                owner=current_user.user_id,
                asset=job["asset"],
                job_id=uuid.uuid4(),
                # this is an optional field as such it might be empty
                mediafile_id=job["mediafile_id"]
                if job["mediafile_id"] is not None
                else None,
                job_type=job["job_type"],
                status=job["status"],
                job_info=job["job_info"],
            )
            entity.session.add(init_job)
            entity.session.commit()
            response = {
                "message": f'saved {job["job_info"]}, uuid: {init_job.job_id}',
                "status": True,
            }
        except BaseException as error:
            response = {
                "message": f'Saving {job["job_info"]} failed',
                "error": [error],
                "status": False,
            }

        return response

    @classmethod
    def save_user(cls, user):
        """

        @param user:
        @return: Result object - { message: String, status:Boolean, error: [String]}
        """
        try:
            # initiate and save User
            entity.session.add(
                User(name=user["name"], u_id=uuid.uuid4(), email=user["email"])
            )
            entity.session.commit()
            response = {"message": f'{user["name"]}, saved successfully'}
        except BaseException as error:
            response = {"message": f"User not saved", "error": [error]}
        return response
        # entity.session.commit()
