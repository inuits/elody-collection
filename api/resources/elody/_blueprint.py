from flask import Blueprint, request
from flask_restful import Api


blueprint = Blueprint("elody", __name__)
api = Api(blueprint)


@blueprint.before_request
def set_spec():
    request.view_args = request.view_args or {}
    request.view_args["spec"] = "elody"
