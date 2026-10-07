"""Linked-data sources declared as data, not as client code.

A client that reads values from a SPARQL endpoint -- a vocabulary, an ontology's
class tree, the candidates of a SHACL UI search query -- describes each source
in one JSON file (SPARQL_SOURCES), keyed by the Elody type its resources get:

    {"cellType": {
        "endpoint": "https://ubergraph.apps.renci.org/sparql",
        "selectQuery": "SELECT ?value WHERE { ... }",
        "searchQuery": "SELECT ?value WHERE { ... $searchTerm ... }",
        "identifierPrefix": "http://purl.obolibrary.org/obo/",
        "language": "en",
        "fields": {"label": "http://www.w3.org/2000/01/rdf-schema#label", "iri": "@iri"}}}

collection-api turns each into an object configuration on the SPARQL engine, a
serializer and the routes of a collection, so no client writes Python for it.
The UI declaration generator writes this file from the shapes.
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_api_path = Path(__file__).resolve().parents[2]
if str(_api_path) not in sys.path:
    sys.path.insert(0, str(_api_path))

LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
OBO = "http://purl.obolibrary.org/obo/"
SOURCES = {
    "cellType": {
        "endpoint": "https://ubergraph.example/sparql",
        "selectQuery": "SELECT ?value WHERE { ?value ?p ?o }",
        "searchQuery": "SELECT ?value WHERE { ?value ?p $searchTerm }",
        "identifierPrefix": OBO,
        "language": "en",
        "userAgent": "elody (elody.eu)",
        "fields": {"label": LABEL, "iri": "@iri"},
    }
}


@pytest.fixture
def configuration():
    import sparql_sources

    return sparql_sources.configurations(SOURCES)["cellType"]()


class TestTheConfiguration:
    def test_reads_through_the_sparql_engine_into_its_own_collection(self, configuration):
        crud = configuration.crud()
        assert crud["storage_type"] == "sparql"
        assert crud["collection"] == "cellType"

    def test_hands_the_engine_its_settings(self, configuration):
        sparql = configuration.crud()["sparql"]
        assert sparql["endpoint"] == "https://ubergraph.example/sparql"
        assert sparql["select_query"] == SOURCES["cellType"]["selectQuery"]
        assert sparql["search_query"] == SOURCES["cellType"]["searchQuery"]
        assert sparql["identifier_prefix"] == OBO
        assert sparql["language"] == "en"
        assert sparql["user_agent"] == "elody (elody.eu)"
        # only the predicates the fields read are fetched
        assert sparql["properties"] == [LABEL]


class TestTheSerializer:
    def test_a_resource_becomes_an_entity_named_by_its_identifier_and_its_iri(self, configuration):
        to_elody = configuration.serialization("sparql", "elody")
        entity = to_elody(
            {"iri": f"{OBO}CL_0000540", "identifier": "CL_0000540", "properties": {LABEL: ["neuron"]}}
        )
        assert entity["_id"] == "CL_0000540"
        assert entity["identifiers"] == ["CL_0000540", f"{OBO}CL_0000540"]
        assert entity["type"] == "cellType"
        assert {"key": "label", "value": "neuron"} in entity["metadata"]
        assert {"key": "iri", "value": f"{OBO}CL_0000540"} in entity["metadata"]
        assert entity["relations"] == []

    def test_a_resource_without_a_label_is_labelled_by_its_local_name(self, configuration):
        entity = configuration.serialization("sparql", "elody")(
            {"iri": f"{OBO}CL_0000540", "identifier": "CL_0000540", "properties": {}}
        )
        assert {"key": "label", "value": "CL_0000540"} in entity["metadata"]

    def test_the_text_typed_becomes_the_search_and_a_selection_of_identifiers_the_ids(self, configuration):
        to_sparql = configuration.serialization("elody_filter", "sparql_filter")
        filters = to_sparql(
            [
                {"type": "type", "value": "cellType"},
                {"type": "text", "key": ["elody:1|metadata.label.value"], "value": "neur"},
                {"type": "selection", "key": ["elody:1|identifiers"], "value": ["CL_0000540"]},
            ]
        )
        assert filters == {"search": "neur", "ids": ["CL_0000540"]}

    def test_an_empty_text_is_no_search(self, configuration):
        to_sparql = configuration.serialization("elody_filter", "sparql_filter")
        assert to_sparql([{"type": "text", "key": "x", "value": ""}]) == {}


class TestLoading:
    def test_no_file_means_no_sources(self):
        import sparql_sources

        with patch.dict("os.environ", {}, clear=True):
            assert sparql_sources.load() == {}

    def test_the_file_named_by_the_environment_is_read(self, tmp_path):
        import sparql_sources

        path = tmp_path / "sources.json"
        path.write_text(json.dumps(SOURCES))
        with patch.dict("os.environ", {"SPARQL_SOURCES": str(path)}):
            assert sparql_sources.load() == SOURCES

    def test_the_sources_join_the_client_s_mapper_without_replacing_its_types(self):
        import sparql_sources

        own = object()
        merged = sparql_sources.with_sources({"cellType": own, "entity": own}, SOURCES)
        assert merged["cellType"] is own
        assert merged["entity"] is own
        merged = sparql_sources.with_sources({"entity": own}, SOURCES)
        assert merged["cellType"]().crud()["storage_type"] == "sparql"


class TestRoutes:
    def test_every_source_is_a_collection_with_a_listing_a_filter_and_a_detail(self, monkeypatch):
        monkeypatch.setenv("DB_ENGINE", "memory")
        import configuration

        configuration.init_mappers()
        import sparql_sources
        from flask import Flask

        app = Flask(__name__)
        app.register_blueprint(sparql_sources.blueprint(SOURCES))
        rules = {rule.rule for rule in app.url_map.iter_rules()}
        assert {"/cellType", "/cellType/filter", "/cellType/<string:id>"} <= rules
