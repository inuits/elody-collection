from flask_restful import Resource
from flask import request

class BaseResource(Resource):
    def get_response_body(self):
        return request.get_json(force=True)
