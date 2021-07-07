from query.resolver import (
    get_all_jobs,
    get_jobs_by_asset,
    get_job_by_id,
    welcome,
    get_jobs_for_user,
    get_jobs_by_user,
    login_user,
)
from ariadne import ObjectType

# Set the Endpoint resolver for Queries


custom_query = ObjectType("Query")

custom_query.set_field("get_job_by_id", get_job_by_id)  # get job by id

custom_query.set_field("get_job_by_asset", get_jobs_by_asset)  # get job by asset

custom_query.set_field("get_jobs_for_user", get_all_jobs)  # query all data

custom_query.set_field("welcome_query", welcome)  # for testing

custom_query.set_field("get_jobs_for_user", get_jobs_for_user)
custom_query.set_field("get_jobs_by_user", get_jobs_by_user)

custom_query.set_field("login_user", login_user)
