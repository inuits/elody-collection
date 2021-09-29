import json
import sys
from abc import ABC

from pyArango.connection import Connection
from pyArango.database import Database
from pyArango.theExceptions import CreationError


class PyArangoConnection(Connection, ABC):
    def createCollection(self, name, db_name, collectionArgs=None):
        "use collectionArgs for arguments other than name. for a full list of arguments please have a look at arangoDB's doc"
        if collectionArgs is None:
            collectionArgs = {}
        collectionArgs["name"] = name
        payload = json.dumps(collectionArgs, default=str)
        url = self.arangoURL[0] + "/_db/" + db_name + "/_api/collection"
        r = self.session.post(url, data=payload)
        data = r.json()
        print(data, file=sys.stderr)
        if r.status_code == 200 and not data["error"]:
            return True
        else:
            raise CreationError(data["errorMessage"], r.content)

    def createEdge(self, name, db_name, collectionArgs=None):
        "use collectionArgs for arguments other than name. for a full list of arguments please have a look at arangoDB's doc"
        if collectionArgs is None:
            collectionArgs = {}
        collectionArgs["name"] = name
        collectionArgs["type"] = 3
        payload = json.dumps(collectionArgs, default=str)
        url = self.arangoURL[0] + "/_db/" + db_name + "/_api/collection"
        r = self.session.post(url, data=payload)
        data = r.json()
        print(data, file=sys.stderr)
        if r.status_code == 200 and not data["error"]:
            return True
        else:
            raise CreationError(data["errorMessage"], r.content)

    def createGraph(self, name, db_name, graphArgs=None):
        "use collectionArgs for arguments other than name. for a full list of arguments please have a look at arangoDB's doc"
        if graphArgs is None:
            graphArgs = {}
        graphArgs["name"] = name
        payload = json.dumps(graphArgs, default=str)
        url = self.arangoURL[0] + "/_db/" + db_name + "/_api/gharial"
        r = self.session.post(url, data=payload)
        data = r.json()
        print(data, file=sys.stderr)
        if r.status_code == 202 and not data["error"]:
            return True
        else:
            raise CreationError(data["errorMessage"], r.content)
