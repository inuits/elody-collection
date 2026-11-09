from resources.elody._blueprint import api
from resources.elody.document_relations import ElodyDocumentRelations


class ElodyMediafileRelations(ElodyDocumentRelations):
    pass


def resource_rules():
    return [
        {
            "route": "/mediafiles/<string:id>/relations",
            "resource": ElodyMediafileRelations,
            "api": api,
        }
    ]
