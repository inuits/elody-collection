"""A source whose members are chosen by a query, and searched by one.

SHACL 1.2 UI lets a shapes graph say which values a field offers with SPARQL:
`sh:in [ sh:select ... ]` names the candidates, `shui:searchQuery` finds them
for what the user typed ($searchTerm, in $uiLanguage), and `shui:SubClassEditor`
offers a root class and its subclasses. None of these is "every instance of one
class", so a source can be configured by its queries instead of a class:

    "select_query":        SELECT ?value ...  -- the members, for a listing
    "search_query":        SELECT ?value ...  -- the members matching $searchTerm
    "identifier_encoding": "iri" -- the identifier is the IRI itself, encoded,
                           for sources whose IRIs share no prefix

The queries are sent as they are, with $searchTerm and $uiLanguage filled in as
string literals; paging and counting happen on the IRIs they return.
"""

import base64
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_api_path = Path(__file__).resolve().parents[3]
if str(_api_path) not in sys.path:
    sys.path.insert(0, str(_api_path))

OBO = "http://purl.obolibrary.org/obo/"
LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
SELECT = (
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
    "SELECT ?value WHERE { ?value rdfs:subClassOf <http://purl.obolibrary.org/obo/CL_0000000> }"
)
SEARCH = (
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
    "SELECT ?value WHERE { ?value rdfs:subClassOf <http://purl.obolibrary.org/obo/CL_0000000> ; rdfs:label ?l .\n"
    "  FILTER(CONTAINS(LCASE(STR(?l)), LCASE($searchTerm)) && (LANG(?l) = \"\" || LANG(?l) = $uiLanguage)) }"
)
CONFIG = {
    "endpoint": "https://ubergraph.example/sparql",
    "select_query": SELECT,
    "search_query": SEARCH,
    "identifier_prefix": OBO,
    "properties": [LABEL],
    "language": "en",
}


def _values(*iris, literal=None):
    bindings = [{"value": {"type": "uri", "value": iri}} for iri in iris]
    if literal:
        bindings.append({"value": {"type": "literal", "value": literal}})
    return json.dumps({"head": {"vars": ["value"]}, "results": {"bindings": bindings}})


THREE = _values(f"{OBO}CL_0000540", f"{OBO}CL_0000028", f"{OBO}CL_0000029")
LABELS = f"""
<{OBO}CL_0000540> <{LABEL}> "neuron" .
<{OBO}CL_0000028> <{LABEL}> "CNS neuron" .
<{OBO}CL_0000029> <{LABEL}> "neural crest derived neuron" .
<http://example.org/x> <{LABEL}> "x" .
"""


def _response(text, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    return response


def _router(values=THREE, construct=LABELS):
    def respond(url, data=None, headers=None, **kwargs):
        query = (data or {}).get("query", "")
        if "CONSTRUCT" in query:
            return _response(construct)
        return _response(values)

    return respond


@pytest.fixture
def store():
    from storage.sparqlstore import SparqlStorageManager

    manager = SparqlStorageManager()
    with patch.object(manager, "_sparql_config", return_value=dict(CONFIG)):
        yield manager


@pytest.fixture
def serialized():
    with patch(
        "storage.sparqlstore.serialize",
        side_effect=lambda subject, **kwargs: {
            "_id": subject["identifier"],
            "iri": subject["iri"],
            "properties": subject["properties"],
        },
    ):
        yield


def _list(store, values=THREE, construct=LABELS, **kwargs):
    with patch("storage.sparqlstore.requests.post", side_effect=_router(values, construct)) as post:
        page = store.get_items_from_collection("cellType", **kwargs)
    return page, [call.kwargs["data"]["query"] for call in post.call_args_list]


class TestAQueryDrivenSourceIsUsable:
    def test_queries_stand_in_for_a_class(self, store):
        assert store._is_usable(dict(CONFIG), "cellType")

    def test_without_a_class_or_a_select_query_it_is_not(self, store):
        config = dict(CONFIG)
        config.pop("select_query")
        assert not store._is_usable(config, "cellType")


class TestListing:
    def test_the_select_query_is_sent_as_it_is_capped_at_max_results(self, store, serialized):
        _, queries = _list(store)
        assert queries[0] == SELECT + "\nLIMIT 500"

    def test_the_cap_is_configurable_and_a_query_s_own_limit_is_kept(self, store, serialized):
        config = dict(CONFIG, max_results=50)
        with patch.object(store, "_sparql_config", return_value=config):
            _, queries = _list(store)
        assert queries[0].endswith("\nLIMIT 50")
        config = dict(CONFIG, select_query=SELECT + " LIMIT 10")
        with patch.object(store, "_sparql_config", return_value=config):
            _, queries = _list(store)
        assert queries[0] == SELECT + " LIMIT 10"

    def test_the_page_is_cut_from_the_values_it_returns_and_counted(self, store, serialized):
        page, queries = _list(store, skip=1, limit=1)
        assert [item["_id"] for item in page["results"]] == ["CL_0000028"]
        assert page["count"] == 3
        construct = queries[-1]
        assert f"VALUES ?s {{ <{OBO}CL_0000028> }}" in construct
        assert f"VALUES ?p {{ <{LABEL}> }}" in construct

    def test_a_negative_skip_starts_at_the_first_value(self, store, serialized):
        page, _ = _list(store, skip=-5, limit=2)
        assert [item["_id"] for item in page["results"]] == ["CL_0000540", "CL_0000028"]

    def test_the_order_is_the_query_s(self, store, serialized):
        page, _ = _list(store)
        assert [item["_id"] for item in page["results"]] == ["CL_0000540", "CL_0000028", "CL_0000029"]

    def test_a_value_outside_the_prefix_or_not_an_iri_is_skipped(self, store, serialized):
        values = _values(f"{OBO}CL_0000540", "http://example.org/x", literal="neuron")
        page, _ = _list(store, values=values)
        assert [item["_id"] for item in page["results"]] == ["CL_0000540"]
        assert page["count"] == 1

    def test_requested_identifiers_skip_the_select_query(self, store, serialized):
        page, queries = _list(store, filters={"ids": ["CL_0000029", "CL_0000540"]})
        assert len(queries) == 1 and "CONSTRUCT" in queries[0]
        assert [item["_id"] for item in page["results"]] == ["CL_0000029", "CL_0000540"]


class TestSearching:
    def test_the_search_query_runs_with_the_term_and_the_language_filled_in(self, store, serialized):
        _, queries = _list(store, filters={"search": "neur"})
        assert 'LCASE("neur")' in queries[0]
        assert 'LANG(?l) = "en"' in queries[0]
        assert "$searchTerm" not in queries[0] and "$uiLanguage" not in queries[0]

    def test_the_term_cannot_break_out_of_its_literal(self, store, serialized):
        _, queries = _list(store, filters={"search": 'a" } DROP ALL #\n\\'})
        assert 'LCASE("a\\" } DROP ALL #\\n\\\\")' in queries[0]
        assert "\n\\" not in queries[0].split("LCASE(", 2)[2]

    def test_without_a_search_query_the_select_query_lists_everything(self, store, serialized):
        config = dict(CONFIG)
        config.pop("search_query")
        with patch.object(store, "_sparql_config", return_value=config):
            _, queries = _list(store, filters={"search": "neur"})
        assert queries[0].startswith(SELECT)

    def test_an_empty_term_lists_everything(self, store, serialized):
        _, queries = _list(store, filters={"search": "  "})
        assert queries[0].startswith(SELECT)


class TestOneResource:
    def test_it_is_fetched_by_its_iri_without_any_class_pattern(self, store, serialized):
        one = f'<{OBO}CL_0000540> <{LABEL}> "neuron" .'
        with patch("storage.sparqlstore.requests.post", side_effect=_router(construct=one)) as post:
            item = store.get_item_from_collection_by_id("cellType", "CL_0000540")
        (call,) = post.call_args_list
        query = call.kwargs["data"]["query"]
        assert f"VALUES ?s {{ <{OBO}CL_0000540> }}" in query
        assert "None" not in query
        assert item["_id"] == "CL_0000540"


class TestIrisWithoutACommonPrefix:
    @staticmethod
    def _id(iri):
        return base64.urlsafe_b64encode(iri.encode()).decode().rstrip("=")

    @pytest.fixture
    def encoded(self, store):
        config = dict(CONFIG)
        config.pop("identifier_prefix")
        config["identifier_encoding"] = "iri"
        with patch.object(store, "_sparql_config", return_value=config):
            yield store

    def test_the_identifier_is_the_encoded_iri(self, encoded, serialized):
        values = _values(f"{OBO}CL_0000540", "http://example.org/x")
        page, _ = _list(encoded, values=values)
        assert [item["_id"] for item in page["results"]] == [
            self._id(f"{OBO}CL_0000540"),
            self._id("http://example.org/x"),
        ]
        assert page["results"][1]["iri"] == "http://example.org/x"

    def test_an_encoded_identifier_is_fetched_by_its_iri(self, encoded, serialized):
        one = f'<http://example.org/x> <{LABEL}> "x" .'
        with patch("storage.sparqlstore.requests.post", side_effect=_router(construct=one)) as post:
            item = encoded.get_item_from_collection_by_id("cellType", self._id("http://example.org/x"))
        assert "VALUES ?s { <http://example.org/x> }" in post.call_args.kwargs["data"]["query"]
        assert item["iri"] == "http://example.org/x"

    def test_an_identifier_that_decodes_to_an_unsafe_iri_is_refused(self, encoded, serialized):
        with patch("storage.sparqlstore.requests.post", side_effect=_router()) as post:
            item = encoded.get_item_from_collection_by_id("cellType", self._id("http://x> } DROP ALL {"))
        assert post.call_count == 0
        assert item == {}
