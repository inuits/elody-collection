from resources.elody._blueprint import api
from resources.elody.document import ElodyDocument


class ElodyMediafile(ElodyDocument):
    pass


def resource_rules():
    return [
        {"route": "/mediafiles/<string:id>", "resource": ElodyMediafile, "api": api}
    ]
