"""Linked-data sources declared as data: one JSON file, no client code.

SPARQL_SOURCES names a JSON file keyed by the Elody type a source's resources
get. Each entry configures the SPARQL engine (storage/sparqlstore.py) and says
which predicate fills which metadata key:

    {"cellType": {
        "endpoint": "https://ubergraph.apps.renci.org/sparql",
        "selectQuery": "...",          # the members (or "targetClass" + "typePredicate")
        "searchQuery": "...",          # the members matching $searchTerm, optional
        "identifierPrefix": "...",     # or "identifierEncoding": "iri"
        "language": "en", "userAgent": "...",
        "fields": {"label": "http://www.w3.org/2000/01/rdf-schema#label", "iri": "@iri"}}}

For every source this module gives an object configuration on the engine, a
serializer between the source's resources and Elody entities, and the routes
of a collection (/<type>, /<type>/filter, /<type>/<id>). The sources are
read-only: Elody links to them, it never writes back. The UI declaration
generator (uiDeclarationModule) writes the file from the shapes.
"""

import json
from os import getenv

from elody.object_configurations.elody_configuration import ElodyConfiguration
from logging_elody.log import log

# JSON key → engine setting
SETTINGS = {
    "endpoint": "endpoint",
    "graph": "graph",
    "selectQuery": "select_query",
    "searchQuery": "search_query",
    "targetClass": "target_class",
    "typePredicate": "type_predicate",
    "identifierPrefix": "identifier_prefix",
    "identifierEncoding": "identifier_encoding",
    "identifierPredicate": "identifier_predicate",
    "sortPredicate": "sort_predicate",
    "requiredPredicate": "required_predicate",
    "language": "language",
    "userAgent": "user_agent",
}


def load() -> dict:
    """The sources in the file SPARQL_SOURCES names; none without it."""
    path = getenv("SPARQL_SOURCES")
    if not path:
        return {}
    try:
        with open(path) as file:
            sources = json.load(file)
    except (OSError, ValueError) as error:
        log.error(f"SPARQL_SOURCES {path} could not be read: {error}")
        return {}
    return sources if isinstance(sources, dict) else {}


class SparqlSourceSerializer:
    """A source's resource ↔ an Elody entity, by the source's fields."""

    def __init__(self, type_key, fields):
        self.type_key = type_key
        self.fields = fields

    def from_sparql_to_elody(self, subject, **kwargs):
        identifier = subject.get("identifier")
        iri = subject.get("iri", "")
        properties = subject.get("properties", {})
        if not identifier:
            return {}
        metadata = []
        for key, predicate in self.fields.items():
            if predicate == "@id":
                value = identifier
            elif predicate == "@iri":
                value = iri
            else:
                values = properties.get(predicate) or []
                value = values[0] if values else None
            if value is None and key == "label":
                # a resource the source does not label is still offered: by its local name
                value = iri.rstrip("/#").rsplit("/", 1)[-1].rsplit("#", 1)[-1] or identifier
            if value is not None:
                metadata.append({"key": key, "value": value})
        return {
            "_id": identifier,
            "identifiers": [identifier, iri] if iri else [identifier],
            "type": self.type_key,
            "metadata": metadata,
            "relations": [],
        }

    def from_elody_filter_to_sparql_filter(self, filters, **kwargs):
        """What the engine understands of an Elody filter: the text typed, and requested ids."""
        restrictions = {}
        for filter in filters or []:
            kind = filter.get("type")
            value = filter.get("value")
            if kind == "text" and isinstance(value, str) and value.strip():
                restrictions["search"] = value.strip()
            elif kind == "selection" and "identifiers" in str(filter.get("key", [])):
                ids = [] if value is None else value if isinstance(value, list) else [value]
                # an explicit selection of identifiers: an empty one matches nothing
                restrictions["ids"] = ids
        return restrictions

    def from_elody_to_sparql(self, entity, **kwargs):
        # read-only: nothing is written back to the source
        return entity


def configurations(sources: dict) -> dict:
    """An object configuration class per source, keyed by its type."""
    classes = {}
    for type_key, source in sources.items():
        fields = dict(source.get("fields") or {"label": "http://www.w3.org/2000/01/rdf-schema#label"})
        sparql = {setting: source[key] for key, setting in SETTINGS.items() if source.get(key)}
        sparql["properties"] = sorted({p for p in fields.values() if not p.startswith("@")})

        def crud(self, type_key=type_key, sparql=sparql):
            return {
                **ElodyConfiguration.crud(self),
                "storage_type": "sparql",
                "collection": type_key,
                "type": type_key,
                "sparql": dict(sparql),
            }

        def document_info(self):
            return {
                "object_lists": {"metadata": "key", "relations": "type"},
                "tenant_id_resolver": lambda _: "",
            }

        def serialization(self, from_format, to_format, type_key=type_key, fields=fields):
            serializer = SparqlSourceSerializer(type_key, fields)
            return getattr(serializer, f"from_{from_format}_to_{to_format}")

        classes[type_key] = type(
            f"SparqlSource_{type_key}",
            (ElodyConfiguration,),
            {
                "SCHEMA_TYPE": "sparql",
                "SCHEMA_VERSION": 1,
                "crud": crud,
                "document_info": document_info,
                "serialization": serialization,
            },
        )
    return classes


def with_sources(mapper: dict, sources: dict) -> dict:
    """The client's object configuration mapper plus the sources; a client's own type wins."""
    return {**configurations(sources), **mapper}


def blueprint(sources: dict):
    """The routes of every source: a listing, a filter and one resource."""
    from flask import Blueprint, request
    from flask_restful import Api
    from inuits_policy_based_auth import RequestContext
    from policy_factory import apply_policies
    from resources.filter import FilterGenericObjectsV2
    from resources.generic_object import GenericObject, GenericObjectDetail

    bp = Blueprint("sparql_sources", __name__)
    api = Api(bp)

    class Uploads:
        def _get_upload_bucket(self):
            return getenv("MINIO_BUCKET")

        def _get_upload_location(self, filename):
            return filename

    for type_key in sources:

        class Listing(Uploads, GenericObject):
            collection = type_key

            @apply_policies(RequestContext(request))
            def get(self):
                return super().get(collection=self.collection)

        class Filter(Uploads, FilterGenericObjectsV2):
            collection = type_key

            @apply_policies(RequestContext(request))
            def post(self):
                return super().post(collection=self.collection)

        class Detail(Uploads, GenericObjectDetail):
            collection = type_key

            @apply_policies(RequestContext(request))
            def get(self, id):
                item = self.get_object_detail(collection=self.collection, id=id)
                if not item:
                    return {"message": f"{self.collection} {id} does not exist"}, 404
                return item

        api.add_resource(Listing, f"/{type_key}", endpoint=f"sparql_source_{type_key}")
        api.add_resource(Filter, f"/{type_key}/filter", endpoint=f"sparql_source_{type_key}_filter")
        api.add_resource(Detail, f"/{type_key}/<string:id>", endpoint=f"sparql_source_{type_key}_detail")
    return bp
