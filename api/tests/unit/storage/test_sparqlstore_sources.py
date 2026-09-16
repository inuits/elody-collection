"""Reading a public linked-data source through the SPARQL engine.

The engine was written against a store we control: every resource carries an
identifying literal, lives in a named graph and is typed with rdf:type. A
public source such as Wikidata offers none of that -- resources are named by
their IRI alone, there is no named graph, classes hang off a vocabulary-
specific predicate, every literal comes in every language, and a resource
carries far more properties than a listing wants. These tests pin the
configuration that makes such a source readable without a client subclass:

    "graph":               optional -- absent means the default graph
    "type_predicate":      the predicate that types a resource (default rdf:type)
    "identifier_prefix":   the resource IRI minus this prefix is its identifier
    "properties":          only these predicates are fetched
    "language":            only literals in this language (or untagged) arrive
    "required_predicate":  a resource without it is not listed or counted
    "user_agent":          sent with every request (public endpoints ask for one)
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_api_path = Path(__file__).resolve().parents[3]
if str(_api_path) not in sys.path:
    sys.path.insert(0, str(_api_path))

WD = "http://www.wikidata.org/entity/"
P31 = "http://www.wikidata.org/prop/direct/P31"
STYLE = f"{WD}Q1998962"
LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
DESCRIPTION = "http://schema.org/description"

SOURCE_CONFIG = {
    "endpoint": "https://query.wikidata.org/sparql",
    "type_predicate": P31,
    "target_class": STYLE,
    "identifier_prefix": WD,
    "sort_predicate": LABEL,
    "properties": [LABEL, DESCRIPTION],
    "language": "nl",
    "required_predicate": LABEL,
    "user_agent": "mat2elody/0.1 (elody.eu)",
}

TWO_STYLES = f"""
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix schema: <http://schema.org/> .

<{WD}Q348229> rdfs:label "Belgisch bier"@nl ; schema:description "bier uit België"@nl .
<{WD}Q1265882> rdfs:label "Dunkel"@nl .
"""

PAGE_RESULT = json.dumps(
    {
        "results": {
            "bindings": [
                {"s": {"type": "uri", "value": f"{WD}Q1265882"}, "id": {"value": "Q1265882"}},
                {"s": {"type": "uri", "value": f"{WD}Q348229"}, "id": {"value": "Q348229"}},
            ]
        }
    }
)
COUNT_RESULT = '{"results": {"bindings": [{"count": {"value": "2"}}]}}'


def _response(text, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    return response


def _router(construct=TWO_STYLES, count=COUNT_RESULT, page=PAGE_RESULT):
    def respond(url, data=None, headers=None, **kwargs):
        query = (data or {}).get("query", "").lstrip()
        if query.startswith("SELECT (COUNT"):
            return _response(count)
        if query.startswith("SELECT"):
            return _response(page)
        return _response(construct)

    return respond


@pytest.fixture
def store():
    from storage.sparqlstore import SparqlStorageManager

    manager = SparqlStorageManager()
    with patch.object(manager, "_sparql_config", return_value=dict(SOURCE_CONFIG)):
        yield manager


@pytest.fixture
def passthrough_serialize():
    with patch(
        "storage.sparqlstore.serialize",
        side_effect=lambda subject, **kwargs: {
            "_id": subject["identifier"],
            "iri": subject["iri"],
            "properties": subject["properties"],
        },
    ) as serialize:
        yield serialize


def _calls(post):
    return [
        (call.kwargs["data"]["query"], call.kwargs.get("headers") or {})
        for call in post.call_args_list
    ]


def _queries(store, **kwargs):
    with patch("storage.sparqlstore.requests.post", side_effect=_router()) as post, patch(
        "storage.sparqlstore.serialize", side_effect=lambda s, **k: s
    ):
        store.get_items_from_collection("bierstijl", **kwargs)
    return _calls(post)


def _of_kind(store, prefix, **kwargs):
    return next(q for q, _ in _queries(store, **kwargs) if q.lstrip().startswith(prefix))


class TestASourceWithoutOurConventionsIsUsable:
    def test_a_graph_is_not_required(self, store):
        assert store._is_usable(dict(SOURCE_CONFIG), "bierstijl")

    def test_an_identifier_prefix_stands_in_for_an_identifying_property(self, store):
        config = dict(SOURCE_CONFIG)
        config.pop("identifier_prefix")
        assert not store._is_usable(config, "bierstijl")
        config["identifier_predicate"] = "http://example.org/id"
        assert store._is_usable(config, "bierstijl")

    def test_our_own_store_is_configured_as_before(self, store):
        """The alerts configuration -- graph, class, identifying literal -- is untouched."""
        assert store._is_usable(
            {
                "endpoint": "http://triplestore:3030/alerts/sparql",
                "graph": "http://example.org/graphs/errors",
                "target_class": "http://open-services.net/ns/core#Error",
                "identifier_predicate": "http://mu.semte.ch/vocabularies/core/uuid",
            },
            "alerts",
        )


class TestTheQueriesItBuilds:
    def test_no_graph_means_no_graph_clause(self, store):
        for query, _ in _queries(store):
            assert "GRAPH" not in query

    def test_resources_are_typed_through_the_configured_predicate(self, store):
        page = _of_kind(store, "SELECT ?s")
        assert f"<{P31}> <{STYLE}>" in page
        assert " a <" not in page

    def test_the_identifier_is_the_iri_minus_the_prefix(self, store):
        page = _of_kind(store, "SELECT ?s")
        assert f'STRAFTER(STR(?s), "{WD}")' in page
        assert f'STRSTARTS(STR(?s), "{WD}")' in page

    def test_the_properties_are_fetched_by_iri_for_exactly_the_page(self, store):
        construct = _of_kind(store, "CONSTRUCT")
        assert "VALUES ?s {" in construct
        assert f"<{WD}Q1265882>" in construct
        assert f"<{WD}Q348229>" in construct
        assert "VALUES ?id" not in construct

    def test_only_the_configured_properties_are_fetched(self, store):
        construct = _of_kind(store, "CONSTRUCT")
        assert f"VALUES ?p {{ <{LABEL}> <{DESCRIPTION}> }}" in construct

    def test_only_literals_in_the_configured_language_arrive(self, store):
        construct = _of_kind(store, "CONSTRUCT")
        assert 'LANG(?o) = "nl"' in construct
        assert 'LANG(?o) = ""' in construct  # untagged literals still pass

    def test_the_sort_property_is_read_in_the_configured_language(self, store):
        page = _of_kind(store, "SELECT ?s")
        assert 'LANG(?sort) = "nl"' in page

    def test_a_resource_without_the_required_property_is_neither_listed_nor_counted(self, store):
        page = _of_kind(store, "SELECT ?s")
        count = _of_kind(store, "SELECT (COUNT")
        for query in (page, count):
            assert f"<{LABEL}> ?required" in query
            assert 'LANG(?required) = "nl"' in query

    def test_the_user_agent_is_sent(self, store):
        for _, headers in _queries(store):
            assert headers.get("User-Agent") == "mat2elody/0.1 (elody.eu)"

    def test_requested_identifiers_become_iris(self, store):
        page = _of_kind(store, "SELECT ?s", filters={"ids": ["Q348229"]})
        assert f"VALUES ?s {{ <{WD}Q348229> }}" in page
        assert "?requested" not in page


class TestWhatItHandsOver:
    def test_the_identifier_is_derived_from_the_subject(self, store, passthrough_serialize):
        with patch("storage.sparqlstore.requests.post", side_effect=_router()):
            page = store.get_items_from_collection("bierstijl")
        assert [item["_id"] for item in page["results"]] == ["Q1265882", "Q348229"]
        assert page["count"] == 2

    def test_a_subject_outside_the_prefix_is_skipped(self, store, passthrough_serialize):
        stray = TWO_STYLES + '<http://example.org/other> <http://www.w3.org/2000/01/rdf-schema#label> "x"@nl .\n'
        with patch("storage.sparqlstore.requests.post", side_effect=_router(construct=stray)):
            page = store.get_items_from_collection("bierstijl")
        assert [item["_id"] for item in page["results"]] == ["Q1265882", "Q348229"]

    def test_one_resource_is_fetched_by_its_identifier(self, store, passthrough_serialize):
        one = f'<{WD}Q348229> <{LABEL}> "Belgisch bier"@nl .'
        with patch("storage.sparqlstore.requests.post", side_effect=_router(construct=one)) as post:
            item = store.get_item_from_collection_by_id("bierstijl", "Q348229")
        (query, _), = _calls(post)
        assert f"<{WD}Q348229>" in query
        assert f"<{P31}> <{STYLE}>" in query
        assert f"VALUES ?p {{ <{LABEL}> <{DESCRIPTION}> }}" in query
        assert item["_id"] == "Q348229"

    def test_an_identifier_that_could_break_out_of_the_query_never_reaches_the_endpoint(
        self, store, passthrough_serialize
    ):
        with patch("storage.sparqlstore.requests.post", side_effect=_router()) as post:
            item = store.get_item_from_collection_by_id("bierstijl", "Q1> } <x")
        assert post.call_count == 0
        assert item == {}
