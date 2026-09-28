import json

import pytest
from rdflib import Graph, Literal, URIRef

from mappers import (
    build_linked_data_node,
    get_linked_data_base_uri,
    map_entity_to_rdf_data,
)

BASE = "https://podiumnet-dev.elody.eu"
VOCAB = "https://elody.eu/"

PRODUCTION = {
    "_id": "f4ce76c0-5d3a-4df1-bfc1-bb6d33759c2b",
    "id": "PR-6V8VLIHP0",
    "identifiers": ["PR-6V8VLIHP0", "f4ce76c0-5d3a-4df1-bfc1-bb6d33759c2b"],
    "type": "production",
    "document_version": 5,
    "schema": {"type": "elody", "version": 1},
    "audit": {"created": {"by": "developers@inuits.eu"}},
    "metadata": [
        {"key": "title", "value": "Café Chantant"},
        {"key": "duration", "value": 0},
        {"key": "genre", "value": "drama"},
        {"key": "genre", "value": "comedy"},
    ],
    "relations": [
        {"key": "ORG-JQGB344X", "type": "refBookingAgency"},
        {"key": "ORG-SYOO1RU3", "type": "refCompanies"},
    ],
}


@pytest.fixture(autouse=True)
def base_uri(monkeypatch):
    monkeypatch.delenv("ELODY_LD_BASE_URI", raising=False)
    monkeypatch.setenv("DAMS_FRONTEND_URL", BASE)
    monkeypatch.setenv("ELODY_LD_CONTEXT", VOCAB)


def test_base_uri_falls_back_to_the_frontend_url():
    assert get_linked_data_base_uri() == BASE


def test_explicit_base_uri_overrides_the_frontend_url(monkeypatch):
    monkeypatch.setenv("ELODY_LD_BASE_URI", "https://data.podiumnet.be/")

    assert get_linked_data_base_uri() == "https://data.podiumnet.be"


def graph_for(objects):
    graph = Graph()
    graph.parse(data=map_entity_to_rdf_data(objects, "turtle"), format="turtle")
    return graph


def test_entity_is_a_dereferenceable_uri_not_a_blank_node():
    graph = graph_for([PRODUCTION])
    subject = URIRef(f"{BASE}/PR-6V8VLIHP0")

    assert (subject, None, None) in graph
    assert not any(not isinstance(s, URIRef) for s in graph.subjects())


def test_entity_type_is_an_rdf_class_not_a_literal():
    from rdflib.namespace import RDF

    graph = graph_for([PRODUCTION])
    subject = URIRef(f"{BASE}/PR-6V8VLIHP0")

    assert (subject, RDF.type, URIRef(f"{VOCAB}production")) in graph
    assert (subject, URIRef(f"{VOCAB}type"), Literal("production")) not in graph


def test_metadata_becomes_direct_predicates():
    graph = graph_for([PRODUCTION])
    subject = URIRef(f"{BASE}/PR-6V8VLIHP0")

    assert (subject, URIRef(f"{VOCAB}title"), Literal("Café Chantant")) in graph
    assert set(graph.objects(subject, URIRef(f"{VOCAB}genre"))) == {
        Literal("drama"),
        Literal("comedy"),
    }


def test_relations_point_at_the_related_entity_uri():
    graph = graph_for([PRODUCTION])
    subject = URIRef(f"{BASE}/PR-6V8VLIHP0")

    assert (
        subject,
        URIRef(f"{VOCAB}refBookingAgency"),
        URIRef(f"{BASE}/ORG-JQGB344X"),
    ) in graph


def test_two_entities_link_to_each_other():
    agency = {"id": "ORG-JQGB344X", "type": "organization", "metadata": [], "relations": []}
    graph = graph_for([PRODUCTION, agency])
    subject = URIRef(f"{BASE}/PR-6V8VLIHP0")
    target = URIRef(f"{BASE}/ORG-JQGB344X")

    assert (subject, URIRef(f"{VOCAB}refBookingAgency"), target) in graph
    assert (target, None, None) in graph


def test_audit_and_schema_are_not_published():
    graph = graph_for([PRODUCTION])
    predicates = {str(p) for p in graph.predicates()}

    assert f"{VOCAB}audit" not in predicates
    assert f"{VOCAB}schema" not in predicates


def test_iri_unsafe_metadata_keys_are_skipped_not_crashing():
    entity = {
        "id": "PR-1",
        "type": "production",
        "metadata": [
            {"key": "a key with spaces", "value": "x"},
            {"key": "valid_key", "value": "y"},
        ],
        "relations": [],
    }
    graph = graph_for([entity])
    subject = URIRef(f"{BASE}/PR-1")

    assert (subject, URIRef(f"{VOCAB}valid_key"), Literal("y")) in graph
    assert not any(" " in str(p) for p in graph.predicates())


def test_node_uses_id_over_internal_id():
    node = build_linked_data_node(PRODUCTION)

    assert node["@id"] == f"{BASE}/PR-6V8VLIHP0"


def test_entity_without_relations_or_metadata_still_serializes():
    graph = graph_for([{"id": "PR-2", "type": "production"}])

    assert (URIRef(f"{BASE}/PR-2"), None, None) in graph


def test_json_ld_output_carries_the_id():
    payload = json.loads(map_entity_to_rdf_data([PRODUCTION], "json-ld"))
    ids = {node.get("@id") for node in payload}

    assert f"{BASE}/PR-6V8VLIHP0" in ids
