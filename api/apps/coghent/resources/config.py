import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import Api
from resources.config import Config

api_bp = Blueprint("config", __name__)
api = Api(api_bp)


class CoghentConfig(CoghentBaseResource, Config):
    @app.require_oauth("read-config")
    def get(self):
        return super().get()


api.add_resource(CoghentConfig, "/config")
