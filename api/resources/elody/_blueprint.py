from api_base import CustomApi
from flask import Blueprint, request

blueprint = Blueprint("elody", __name__)
api = CustomApi(blueprint)


@blueprint.before_request
def set_spec():
    request.view_args = request.view_args or {}
    request.view_args["spec"] = "elody"
