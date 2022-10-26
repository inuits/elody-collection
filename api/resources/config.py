import app

from flask import request, after_this_request
from flask_restful import abort
from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource


class Config(BaseResource):
    @app.require_oauth("read-config")
    def get(self):
        return []
