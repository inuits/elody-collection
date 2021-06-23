from flask_restful import Resource
from flask import send_from_directory
from resources.base_resource import BaseResource

import app


class Spec(BaseResource):
    def get(self, spec):
        return send_from_directory("", spec)
