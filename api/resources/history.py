from flask import request
from resources.base_resource import BaseResource


class History(BaseResource):
    def get(self, collection, id):
        timestamp = request.args.get("timestamp")
        pass
