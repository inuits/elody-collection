from resources.elody._blueprint import api
from resources.elody.document_metadata import ElodyDocumentMetadata


class ElodyMediafileMetadata(ElodyDocumentMetadata):
    pass


def resource_rules():
    return [
        {
            "route": "/mediafiles/<string:id>/metadata",
            "resource": ElodyMediafileMetadata,
            "api": api,
        }
    ]
