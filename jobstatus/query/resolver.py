"""" All methods found in this file intercepts Query Request from outside the app which then gets processed inside
QueryResolverClass whose sole purpose is to generate response based on the data requested

"""
from ariadne import convert_kwargs_to_snake_case
from flask_jwt_extended import jwt_required, create_access_token

from model.user import User
from query.root_query_class import QueryResolverClass


@convert_kwargs_to_snake_case
def get_job_by_id(obj, info, job_id):
    """

    @param obj:
    @param info:
    @param job_id: - uuid string whiose main job is to query the database
    @return:
    """
    #
    return QueryResolverClass.get_job_by_id(job_id)


def get_jobs_by_asset(onj, info, asset):
    """  gets a job in the database given its assist """

    return QueryResolverClass.get_job_by_asset(asset)


def get_all_jobs(obj, info):
    """ Gets a list of all jobs available in the system,"""
    return QueryResolverClass.get_all_jobs()


def test_server_access(obj, info):
    """ Test endpoint for sample success response"""
    return {"message": "Welcome To Job-Status System", "status": True}


def welcome(obj, info, name):
    """ Provides GraphQL application entry point testing if the server is up and running."""
    return QueryResolverClass(name).welcome_data()


@jwt_required()
def get_jobs_by_user(obj, info):
    """
    Fetch all jobs for a specifi user
    @param obj:
    @param info:
    @return:
    """
    return QueryResolverClass.get_jobs_by_user()


def get_jobs_for_user(obj, info, user_id):
    """
    Get all jobs given the user_id of the person
    @param obj:
    @param info:
    @param user_id: identifier for the jobs to be fetched
    @return:
    """
    return QueryResolverClass.get_jobs_for_user(user_id=user_id)


def login_user(obj, info, email):
    """

    @param obj:
    @param info:
    @param email:
    @return:
    """
    try:

        user = User.query.filter_by(email=email).one_or_none()
        response = {
            "token": create_access_token(identity=user.u_id),
            "message": "Access Granted",
            "status": True,
        }
    except BaseException as ex:
        response = {"message": "access denied", "status": False, "error": [ex]}

    return response
