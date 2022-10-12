import json

from abc import ABC
from pyArango.connection import Connection
from pyArango.theExceptions import CreationError


class PyArangoConnection(Connection, ABC):
    def __create_helper(
        self, name, db_name, db_collection, args, expected_status, type=None
    ):
        if not args:
            args = {}
        if name:
            args["name"] = name
        if type:
            args["type"] = type
        payload = json.dumps(args, default=str)
        url = f"{self.getEndpointURL()}/_db/{db_name}/_api/{db_collection}"
        r = self.session.post(url, data=payload)
        data = r.json()
        if r.status_code != expected_status or data["error"]:
            raise CreationError(data["errorMessage"], r.content)

    def createCollection(self, name, db_name, args=None):
        self.__create_helper(name, db_name, "collection", args, 200)

    def createEdge(self, name, db_name, args=None):
        self.__create_helper(name, db_name, "collection", args, 200, "3")

    def createGraph(self, name, db_name, args=None):
        self.__create_helper(name, db_name, "gharial", args, 202)

    def define_edge_in_graph(self, graph, db_name, definition):
        self.__create_helper(None, db_name, f"gharial/{graph}/edge", definition, 202)

    def get_cluster_health(self, db_name):
        return self.session.get(
            f"{self.getEndpointURL()}/_db/{db_name}/_admin/cluster/health"
        )
