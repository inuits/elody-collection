from ariadne import convert_kwargs_to_snake_case

from configuration.config import entity
from mutation.root_mutation import MutationResolver
from flask_jwt_extended import jwt_required

@convert_kwargs_to_snake_case
@jwt_required()
def save_job(obj, info, job):
    """ saves a job post :
        :param job:
        :param obj
        :param info are internally connected to internal ariadne module which process requests bases on these two values.
    """
    return MutationResolver.save_job(job)


@convert_kwargs_to_snake_case
def save_user(obj, info, user):
    """
    Saves a user
    :param user: This is an input request which caried with it User Object
    :param obj : is internally connected to internal ariadne module which process requests bases on these two value.
    :param info: is internally connected to internal ariadne module which process requests bases on these two values.
    """
    return MutationResolver.save_user(user)


@convert_kwargs_to_snake_case
def initialize_tables(obj, info):
    """"
    Initialize all tables using SQLAlchemy ORM
     """
    entity.create_all()
    entity.session.commit()
    return {"message": "tables initialized", 'status': True}
