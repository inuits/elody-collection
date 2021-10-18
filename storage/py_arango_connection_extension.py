import json
import sys

from abc import ABC
from pyArango.connection import Connection
from pyArango.theExceptions import CreationError


class PyArangoConnection(Connection, ABC):
    def create_helper(self, name, db_name, db_collection, args, expected_status):
        if args is None:
            args = {}
        args["name"] = name
        payload = json.dumps(args, default=str)
        url = "{}/_db/{}/_api/{}".format(self.getEndpointURL(), db_name, db_collection)
        r = self.session.post(url, data=payload)
        data = r.json()
        if r.status_code == expected_status and not data["error"]:
            return True
        else:
            raise CreationError(data["errorMessage"], r.content)

    def createCollection(self, name, db_name, args=None):
        return self.create_helper(name, db_name, "collection", args, 200)

    def createEdge(self, name, db_name, args=None):
        if args is None:
            args = {}
        args["type"] = 3
        return self.create_helper(name, db_name, "collection", args, 200)

    def createGraph(self, name, db_name, args=None):
        return self.create_helper(name, db_name, "gharial", args, 202)

    def updateDocumentEdge(self, db_name, edge_name, data):
        data.pop("_rev", None)
        url = "{}/_db/{}/_api/document/{}?returnNew=true".format(self.getEndpointURL(), db_name, edge_name)
        datalist = list()
        datalist.append(data)
        datalist =json.dumps(datalist, default=str)
        r = self.session.put(url, data=datalist)
        data = r.json()
        if r.status_code == 202 and "error" not in data:
            return True
        else:
            raise CreationError(data["errorMessage"], r.content)
