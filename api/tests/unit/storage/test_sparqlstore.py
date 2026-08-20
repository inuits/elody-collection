"""The SPARQL read-through storage engine.

The engine is deliberately client-agnostic: it learns the endpoint, named graph
and vocabulary from the collection's own object configuration, and hands each
subject it finds to that configuration's serializer. So these tests assert the
queries it builds and the shape it hands over -- never a particular vocabulary.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_api_path = Path(__file__).resolve().parents[3]
if str(_api_path) not in sys.path:
    sys.path.insert(0, str(_api_path))

GRAPH = "http://example.org/graphs/errors"
TARGET = "http://open-services.net/ns/core#Error"
ID_PRED = "http://mu.semte.ch/vocabularies/core/uuid"
SORT_PRED = "http://purl.org/dc/terms/created"

SPARQL_CONFIG = {
    "endpoint": "http://triplestore:3030/alerts/sparql",
    "graph": GRAPH,
    "target_class": TARGET,
    "identifier_predicate": ID_PRED,
    "sort_predicate": SORT_PRED,
}

# Two subjects with a differing number of properties, so a page boundary that
# counted solutions instead of subjects would visibly cut one in half.
TWO_ALERTS = f"""
@prefix oslc: <http://open-services.net/ns/core#> .
@prefix mu:   <http://mu.semte.ch/vocabularies/core/> .
@prefix dct:  <http://purl.org/dc/terms/> .

<http://example.org/alerts/a> a oslc:Error ;
    mu:uuid "a" ;
    oslc:message "first" ;
    dct:created "2026-01-01T00:00:00Z" ;
    dct:creator <http://example.org/agent> ;
    oslc:largePreview "detail" .

<http://example.org/alerts/b> a oslc:Error ;
    mu:uuid "b" ;
    oslc:message "second" ;
    dct:created "2026-01-02T00:00:00Z" .
"""

ONE_ALERT = """
@prefix oslc: <http://open-services.net/ns/core#> .
@prefix mu:   <http://mu.semte.ch/vocabularies/core/> .

<http://example.org/alerts/a> a oslc:Error ; mu:uuid "a" ; oslc:message "first" .
"""

COUNT_RESULT = '{"results": {"bindings": [{"count": {"value": "2"}}]}}'

# The page query returns subjects in the order the endpoint chose. Deliberately
# not alphabetical, so a manager that re-sorted by IRI would be caught.
PAGE_RESULT = """{"results": {"bindings": [
    {"s": {"value": "http://example.org/alerts/b"}},
    {"s": {"value": "http://example.org/alerts/a"}}
]}}"""


def _response(text, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    return response


def _router(construct=TWO_ALERTS, count=COUNT_RESULT, page=PAGE_RESULT):
    """Answer each of the three query kinds the engine issues."""

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
    # The engine reads its settings from the collection's configuration; stand
    # in for one rather than registering a real object configuration.
    with patch.object(manager, "_sparql_config", return_value=dict(SPARQL_CONFIG)):
        yield manager


@pytest.fixture
def passthrough_serialize():
    """Identify each subject by its IRI so tests can assert on grouping."""
    with patch(
        "storage.sparqlstore.serialize",
        side_effect=lambda subject, **kwargs: {
            "_id": subject["properties"][ID_PRED][0],
            "iri": subject["iri"],
            "properties": subject["properties"],
        },
    ) as serialize:
        yield serialize


def _queries(post):
    return [call.kwargs["data"]["query"] for call in post.call_args_list]




class TestListing:
    def test_it_returns_the_envelope_the_resources_expect(
        self, store, passthrough_serialize
    ):
        with patch("storage.sparqlstore.requests.post", side_effect=_router()):
            result = store.get_items_from_collection("alerts", skip=0, limit=20)
        assert set(result) >= {"results", "count", "limit", "skip"}
        assert result["count"] == 2
        assert result["limit"] == 20
        assert result["skip"] == 0
        assert len(result["results"]) == 2

    def test_each_subject_arrives_whole(self, store, passthrough_serialize):
        with patch("storage.sparqlstore.requests.post", side_effect=_router()):
            result = store.get_items_from_collection("alerts")
        by_id = {item["_id"]: item for item in result["results"]}
        assert set(by_id) == {"a", "b"}
        # The five properties of alert a stay together on one subject.
        assert len(by_id["a"]["properties"]) == 6  # 5 + rdf:type
        assert by_id["a"]["properties"]["http://open-services.net/ns/core#message"] == [
            "first"
        ]

    def test_the_order_the_endpoint_chose_is_preserved(
        self, store, passthrough_serialize
    ):
        # A CONSTRUCT returns a graph, which has no order at all. The sequence
        # therefore has to come from the query that picked the page -- here
        # b before a, which is neither alphabetical nor insertion order.
        with patch("storage.sparqlstore.requests.post", side_effect=_router()):
            result = store.get_items_from_collection("alerts")
        assert [item["_id"] for item in result["results"]] == ["b", "a"]

    def test_a_subject_missing_the_sort_property_is_not_dropped(
        self, store, passthrough_serialize
    ):
        no_sort = """
        @prefix oslc: <http://open-services.net/ns/core#> .
        @prefix mu:   <http://mu.semte.ch/vocabularies/core/> .
        <http://example.org/alerts/c> a oslc:Error ; mu:uuid "c" .
        """
        with patch(
            "storage.sparqlstore.requests.post", side_effect=_router(construct=no_sort)
        ):
            result = store.get_items_from_collection("alerts")
        assert [item["_id"] for item in result["results"]] == ["c"]

    def test_a_subject_without_an_identifier_is_skipped(self, store):
        anonymous = """
        @prefix oslc: <http://open-services.net/ns/core#> .
        <http://example.org/alerts/x> a oslc:Error .
        """
        with patch(
            "storage.sparqlstore.requests.post",
            side_effect=_router(construct=anonymous),
        ):
            result = store.get_items_from_collection("alerts")
        assert result["results"] == []


class TestTheQueriesItBuilds:
    def _all(self, store, **kwargs):
        with patch(
            "storage.sparqlstore.requests.post", side_effect=_router()
        ) as post, patch("storage.sparqlstore.serialize", side_effect=lambda s, **k: s):
            store.get_items_from_collection("alerts", **kwargs)
        return _queries(post)

    def _of_kind(self, store, prefix, **kwargs):
        return next(q for q in self._all(store, **kwargs) if q.lstrip().startswith(prefix))

    def test_it_scopes_to_the_configured_graph_and_class(self, store):
        page = self._of_kind(store, "SELECT ?s")
        assert f"<{GRAPH}>" in page
        assert f"<{TARGET}>" in page

    def test_paging_bounds_subjects_not_triples(self, store):
        # A LIMIT on the CONSTRUCT would bound triples and cut the last alert
        # in half, so the page is chosen by selecting subjects...
        page = self._of_kind(store, "SELECT ?s", skip=40, limit=10)
        assert "LIMIT 10" in page
        assert "OFFSET 40" in page
        assert "?s ?p ?o" not in page

    def test_the_properties_are_fetched_for_exactly_the_selected_subjects(self, store):
        # ...and the CONSTRUCT is then restricted to those subjects, carrying
        # no bounds of its own, so every one of them arrives whole.
        construct = self._of_kind(store, "CONSTRUCT")
        assert "VALUES ?s" in construct
        assert "<http://example.org/alerts/a>" in construct
        assert "<http://example.org/alerts/b>" in construct
        assert "LIMIT" not in construct
        assert "OFFSET" not in construct

    def test_it_sorts_on_the_configured_property_without_requiring_it(self, store):
        page = self._of_kind(store, "SELECT ?s")
        assert f"<{SORT_PRED}>" in page
        assert "OPTIONAL" in page
        assert "ORDER BY" in page

    def test_the_sort_direction_follows_the_asc_argument(self, store):
        assert "DESC(?sort)" in self._of_kind(store, "SELECT ?s", asc=False)
        assert "ASC(?sort)" in self._of_kind(store, "SELECT ?s", asc=True)

    def test_the_count_is_over_distinct_subjects(self, store):
        assert "COUNT(DISTINCT ?s)" in self._of_kind(store, "SELECT (COUNT")


class TestByIdentifier:
    def test_it_looks_up_on_the_configured_identifier_property(
        self, store, passthrough_serialize
    ):
        with patch(
            "storage.sparqlstore.requests.post", side_effect=_router(construct=ONE_ALERT)
        ) as post:
            item = store.get_item_from_collection_by_id("alerts", "a")
        assert item["_id"] == "a"
        query = _queries(post)[0]
        assert f"<{ID_PRED}>" in query
        assert '"a"' in query

    def test_an_unknown_identifier_returns_nothing(self, store, passthrough_serialize):
        with patch("storage.sparqlstore.requests.post", side_effect=_router(construct="")):
            assert store.get_item_from_collection_by_id("alerts", "nope") == {}

    @pytest.mark.parametrize(
        "identifier",
        [
            '" } INSERT DATA { <http://evil> <http://evil> "x',
            "a\\",
            "a}b",
            "a<b>",
            "a\nb",
        ],
    )
    def test_an_identifier_that_could_break_out_of_the_query_never_reaches_the_endpoint(
        self, store, identifier
    ):
        # The id comes straight off a URL path, so it is interpolated into a
        # query string only after passing a conservative character check.
        with patch("storage.sparqlstore.requests.post") as post:
            assert store.get_item_from_collection_by_id("alerts", identifier) == {}
        post.assert_not_called()

    def test_ordinary_identifiers_are_still_accepted(self, store, passthrough_serialize):
        with patch(
            "storage.sparqlstore.requests.post", side_effect=_router(construct=ONE_ALERT)
        ) as post:
            store.get_item_from_collection_by_id(
                "alerts", "2f8c1d94-5a3b-4e7f-9c21-6b0d8e4a1f37"
            )
        post.assert_called()


class TestItDegradesInsteadOfFailing:
    """A dead triplestore must not take the API down with it."""

    def test_a_connection_error_yields_an_empty_list(self, store):
        import requests

        with patch(
            "storage.sparqlstore.requests.post",
            side_effect=requests.exceptions.ConnectionError("refused"),
        ):
            result = store.get_items_from_collection("alerts")
        assert result == {"results": [], "count": 0, "limit": 20, "skip": 0}

    def test_a_connection_error_yields_no_item(self, store):
        import requests

        with patch(
            "storage.sparqlstore.requests.post",
            side_effect=requests.exceptions.ConnectionError("refused"),
        ):
            assert store.get_item_from_collection_by_id("alerts", "a") == {}

    def test_a_server_error_yields_an_empty_list(self, store):
        with patch(
            "storage.sparqlstore.requests.post", return_value=_response("boom", 500)
        ):
            assert store.get_items_from_collection("alerts")["results"] == []

    def test_an_unparseable_response_yields_an_empty_list(self, store):
        with patch(
            "storage.sparqlstore.requests.post",
            side_effect=_router(construct="this is not turtle {{{"),
        ):
            assert store.get_items_from_collection("alerts")["results"] == []

    def test_a_collection_without_sparql_configuration_never_calls_out(self, store):
        with patch.object(store, "_sparql_config", return_value={}), patch(
            "storage.sparqlstore.requests.post"
        ) as post:
            assert store.get_items_from_collection("things")["results"] == []
            assert store.get_item_from_collection_by_id("things", "a") == {}
        post.assert_not_called()

    def test_an_endpoint_is_required(self, store):
        config = dict(SPARQL_CONFIG, endpoint="")
        with patch.object(store, "_sparql_config", return_value=config), patch(
            "storage.sparqlstore.requests.post"
        ) as post:
            assert store.get_items_from_collection("alerts")["results"] == []
        post.assert_not_called()


# --------------------------------------------------------------------------
# Graph-scoped collections
#
# A resource that is a whole named graph rather than one subject: a pipeline
# definition is several subjects and mostly blank nodes, so the engine hands
# the graph over instead of a property bag -- and because a graph is what the
# Graph Store Protocol addresses, these can be written.
# --------------------------------------------------------------------------

PIPELINE_GRAPH_BASE = "http://example.org/graphs/pipeline-definitions"
PIPELINE_CLASS = "https://w3id.org/toolchain#PipelineDefinition"
DCT_IDENTIFIER = "http://purl.org/dc/terms/identifier"

GRAPH_CONFIG = {
    "endpoint": "http://triplestore:3030/store/sparql",
    "gsp_endpoint": "http://triplestore:3030/store/data",
    "graph": PIPELINE_GRAPH_BASE,
    "graph_per_item": True,
    "user": "elody",
    "password": "s3cret",
    "target_class": PIPELINE_CLASS,
    "identifier_predicate": DCT_IDENTIFIER,
}

# One document: two subjects and a blank node, which is why a property bag per
# subject could not describe it.
ONE_DEFINITION = """
@prefix tcs:   <https://w3id.org/toolchain#> .
@prefix dct:   <http://purl.org/dc/terms/> .
@prefix pplan: <http://purl.org/net/p-plan#> .

<http://example.org/pipelines/one> a tcs:PipelineDefinition ; dct:identifier "one" .
<http://example.org/pipelines/one/step/a> a tcs:InstancePipelineComponent ;
    pplan:isStepOfPlan <http://example.org/pipelines/one> ;
    pplan:hasInputVar [ a tcs:PipelineConfig ] .
"""

TWO_GRAPHS = """{"results": {"bindings": [
    {"g": {"value": "%s/one"}, "s": {"value": "http://example.org/pipelines/one"}},
    {"g": {"value": "%s/two"}, "s": {"value": "http://example.org/pipelines/two"}}
]}}""" % (PIPELINE_GRAPH_BASE, PIPELINE_GRAPH_BASE)


@pytest.fixture
def graph_store():
    from storage.sparqlstore import SparqlStorageManager

    manager = SparqlStorageManager()
    with patch.object(manager, "_sparql_config", return_value=dict(GRAPH_CONFIG)):
        yield manager


@pytest.fixture
def graph_serialize():
    """Hand back what the engine passed, so the payload can be asserted on."""
    with patch(
        "storage.sparqlstore.serialize",
        side_effect=lambda payload, **kwargs: {
            "_id": "one",
            "type": "pipeline",
            "iri": payload["iri"],
            "graph_name": payload.get("graph_name"),
            "triples": len(payload["graph"]),
        },
    ) as serialize:
        yield serialize


def _graph_router(construct=ONE_DEFINITION, graphs=TWO_GRAPHS):
    def respond(url, data=None, headers=None, **kwargs):
        query = (data or {}).get("query", "").lstrip()
        if query.startswith("SELECT"):
            return _response(graphs)
        return _response(construct)

    return respond


class TestGraphScopedReads:
    def test_a_document_is_read_from_the_graph_its_id_names(
        self, graph_store, graph_serialize
    ):
        """No search: the id is what names the graph, so it is addressed."""
        with patch(
            "storage.sparqlstore.requests.post", side_effect=_graph_router()
        ) as post:
            graph_store.get_item_from_collection_by_id("pipelines", "one")

        assert f"GRAPH <{PIPELINE_GRAPH_BASE}/one>" in _queries(post)[0]

    def test_the_whole_graph_is_handed_to_the_serializer(
        self, graph_store, graph_serialize
    ):
        """A property bag per subject could not describe a definition.

        The blank node carrying the step's config is not addressable on its
        own, so anything less than the graph would drop it.
        """
        with patch("storage.sparqlstore.requests.post", side_effect=_graph_router()):
            document = graph_store.get_item_from_collection_by_id("pipelines", "one")

        assert document["triples"] == 6
        assert document["iri"] == "http://example.org/pipelines/one"
        assert document["graph_name"] == f"{PIPELINE_GRAPH_BASE}/one"

    def test_an_empty_graph_is_not_a_document(self, graph_store, graph_serialize):
        with patch(
            "storage.sparqlstore.requests.post",
            side_effect=_graph_router(construct=""),
        ):
            assert graph_store.get_item_from_collection_by_id("pipelines", "x") == {}

    def test_a_graph_without_the_target_class_is_not_a_document(
        self, graph_store, graph_serialize
    ):
        """Something else's graph under the same base is not this collection."""
        with patch(
            "storage.sparqlstore.requests.post",
            side_effect=_graph_router(
                construct='<http://example.org/x> <http://example.org/p> "v" .'
            ),
        ):
            assert graph_store.get_item_from_collection_by_id("pipelines", "x") == {}

    def test_an_unsafe_id_never_reaches_the_endpoint(
        self, graph_store, graph_serialize
    ):
        with patch("storage.sparqlstore.requests.post") as post:
            assert graph_store.get_item_from_collection_by_id(
                "pipelines", "one> <urn:evil"
            ) == {}
        post.assert_not_called()

    def test_listing_finds_one_graph_per_document(self, graph_store, graph_serialize):
        with patch(
            "storage.sparqlstore.requests.post", side_effect=_graph_router()
        ) as post:
            result = graph_store.get_items_from_collection("pipelines")

        assert result["count"] == 2
        assert len(result["results"]) == 2
        # one SELECT for the graphs, then one CONSTRUCT per graph: merging them
        # into a single query would fuse the blank nodes of two pipelines
        assert len(_queries(post)) == 3

    def test_listing_stays_inside_the_configured_base(
        self, graph_store, graph_serialize
    ):
        """The alert graph shares the dataset; it must not leak in."""
        with patch(
            "storage.sparqlstore.requests.post", side_effect=_graph_router()
        ) as post:
            graph_store.get_items_from_collection("pipelines")

        assert f'STRSTARTS(STR(?g), "{PIPELINE_GRAPH_BASE}/")' in _queries(post)[0]

    def test_a_page_is_a_page_of_documents(self, graph_store, graph_serialize):
        with patch("storage.sparqlstore.requests.post", side_effect=_graph_router()):
            result = graph_store.get_items_from_collection(
                "pipelines", skip=1, limit=1
            )

        assert len(result["results"]) == 1
        # the count is every document, not the page
        assert result["count"] == 2


class TestGraphScopedWrites:
    def _serializer(self, turtle=ONE_DEFINITION):
        return patch("storage.sparqlstore.serialize", side_effect=lambda *a, **k: turtle)

    def test_saving_replaces_the_document_graph(self, graph_store):
        with self._serializer(), patch(
            "storage.sparqlstore.requests.put", return_value=_response("", 204)
        ) as put, patch.object(
            graph_store, "get_item_from_collection_by_id", return_value={"_id": "one"}
        ):
            graph_store.save_item_to_collection(
                "pipelines", {"_id": "one", "type": "pipeline"}
            )

        assert put.call_args.args[0] == GRAPH_CONFIG["gsp_endpoint"]
        assert put.call_args.kwargs["params"] == {
            "graph": f"{PIPELINE_GRAPH_BASE}/one"
        }
        assert put.call_args.kwargs["auth"] == ("elody", "s3cret")

    def test_a_document_without_an_id_is_given_one(self, graph_store):
        with self._serializer(), patch(
            "storage.sparqlstore.requests.put", return_value=_response("", 204)
        ) as put, patch.object(
            graph_store, "get_item_from_collection_by_id", return_value={}
        ):
            saved = graph_store.save_item_to_collection("pipelines", {"type": "x"})

        assert saved["_id"]
        assert saved["_id"] in put.call_args.kwargs["params"]["graph"]

    def test_an_empty_document_withdraws_the_graph(self, graph_store):
        """The serializer's way of saying "this does not belong in the store".

        An incompatible pipeline chain is the case it exists for: the exports
        answer 409 rather than let it out, so the store must not keep the last
        version that happened to validate either.
        """
        with self._serializer(turtle=""), patch(
            "storage.sparqlstore.requests.delete", return_value=_response("", 204)
        ) as delete, patch(
            "storage.sparqlstore.requests.put", return_value=_response("", 204)
        ) as put, patch.object(
            graph_store, "get_item_from_collection_by_id", return_value={}
        ):
            graph_store.update_item_from_collection(
                "pipelines", "one", {"_id": "one", "type": "pipeline"}
            )

        put.assert_not_called()
        assert delete.call_args.kwargs["params"] == {
            "graph": f"{PIPELINE_GRAPH_BASE}/one"
        }

    def test_patching_reads_modifies_and_writes_the_whole_document(self, graph_store):
        """There is no partial write: the unit the store replaces is a graph."""
        stored = {"_id": "one", "type": "pipeline", "metadata": [], "relations": []}
        written = {}

        def capture(document, **kwargs):
            written.update(document)
            return ONE_DEFINITION

        with patch("storage.sparqlstore.serialize", side_effect=capture), patch(
            "storage.sparqlstore.requests.put", return_value=_response("", 204)
        ), patch.object(
            graph_store, "get_item_from_collection_by_id", return_value=stored
        ):
            graph_store.patch_item_from_collection(
                "pipelines", "one", {"metadata": [{"key": "name", "value": "n"}]}
            )

        assert written["metadata"] == [{"key": "name", "value": "n"}]
        assert written["type"] == "pipeline"

    def test_patching_something_the_store_does_not_have_writes_nothing(
        self, graph_store
    ):
        with patch("storage.sparqlstore.requests.put") as put, patch.object(
            graph_store, "get_item_from_collection_by_id", return_value={}
        ):
            assert graph_store.patch_item_from_collection("pipelines", "x", {}) == {}
        put.assert_not_called()

    def test_deleting_drops_the_graph(self, graph_store):
        with patch(
            "storage.sparqlstore.requests.delete", return_value=_response("", 204)
        ) as delete:
            graph_store.delete_item_from_collection("pipelines", "one")

        assert delete.call_args.kwargs["params"] == {
            "graph": f"{PIPELINE_GRAPH_BASE}/one"
        }

    def test_a_graph_that_was_never_written_is_not_an_error(self, graph_store):
        with patch(
            "storage.sparqlstore.requests.delete", return_value=_response("", 404)
        ):
            graph_store.delete_item_from_collection("pipelines", "one")

    def test_a_refused_write_is_raised_rather_than_swallowed(self, graph_store):
        """A save that did not happen must not answer as though it had."""
        from storage.sparqlstore import ExternalStorageError

        with self._serializer(), patch(
            "storage.sparqlstore.requests.put", return_value=_response("nope", 403)
        ):
            with pytest.raises(ExternalStorageError):
                graph_store.update_item_from_collection("pipelines", "one", {})

    def test_an_unreachable_store_is_raised(self, graph_store):
        import requests as requests_module
        from storage.sparqlstore import ExternalStorageError

        with self._serializer(), patch(
            "storage.sparqlstore.requests.put",
            side_effect=requests_module.exceptions.ConnectionError("down"),
        ):
            with pytest.raises(ExternalStorageError):
                graph_store.update_item_from_collection("pipelines", "one", {})

    def test_a_property_scoped_collection_stays_read_only(self, store):
        """Alerts are written by the pipeline, not by Elody.

        The refusal is explicit rather than inherited as a silent `pass`: the
        engine used to have no write half at all, and a caller must not be able
        to think a write landed.
        """
        with pytest.raises(NotImplementedError):
            store.update_item_from_collection("alerts", "a", {})



class TestSubDocumentEdits:
    """Metadata and relation edits, which the framework builds on patch."""

    def _store_with(self, graph_store, document):
        written = []

        def capture(doc, **kwargs):
            written.append(doc)
            return ONE_DEFINITION

        return (
            patch("storage.sparqlstore.serialize", side_effect=capture),
            patch(
                "storage.sparqlstore.requests.put", return_value=_response("", 204)
            ),
            patch.object(
                graph_store, "get_item_from_collection_by_id", return_value=document
            ),
            written,
        )

    def test_patching_a_relation_rewires_it_instead_of_adding_a_copy(
        self, graph_store
    ):
        document = {
            "_id": "one",
            "type": "pipeline",
            "relations": [
                {"key": "a", "type": "hasProcessor", "metadata": [{"key": "x"}]},
                {"key": "b", "type": "hasProcessor", "metadata": []},
            ],
        }
        serialize, put, get, written = self._store_with(graph_store, document)
        with serialize, put, get:
            graph_store.patch_collection_item_relations(
                "pipelines",
                "one",
                [{"key": "a", "type": "hasProcessor", "metadata": [{"key": "y"}]}],
            )

        relations = written[-1]["relations"]
        assert [r["key"] for r in relations] == ["b", "a"]
        assert relations[-1]["metadata"] == [{"key": "y"}]

    def test_putting_relations_replaces_all_of_them(self, graph_store):
        document = {
            "_id": "one",
            "type": "pipeline",
            "relations": [{"key": "a", "type": "hasProcessor"}],
        }
        serialize, put, get, written = self._store_with(graph_store, document)
        with serialize, put, get:
            graph_store.update_collection_item_relations(
                "pipelines", "one", [{"key": "b", "type": "hasProcessor"}]
            )

        assert [r["key"] for r in written[-1]["relations"]] == ["b"]

    def test_deleting_a_relation_matches_key_and_type(self, graph_store):
        document = {
            "_id": "one",
            "type": "pipeline",
            "relations": [
                {"key": "a", "type": "hasProcessor"},
                {"key": "a", "type": "hasSomethingElse"},
            ],
        }
        serialize, put, get, written = self._store_with(graph_store, document)
        with serialize, put, get:
            graph_store.delete_collection_item_relations(
                "pipelines", "one", [{"key": "a", "type": "hasProcessor"}]
            )

        assert [r["type"] for r in written[-1]["relations"]] == ["hasSomethingElse"]

    def test_relations_keep_the_order_the_store_gave_them(self, graph_store):
        """Unlike the database engine, nothing is re-sorted by an `order` key.

        A pipeline's order is the one its channels imply, reconstructed by the
        serializer; sorting here would overrule it with a field the store does
        not carry.
        """
        document = {
            "_id": "one",
            "relations": [
                {"key": "b", "type": "hasProcessor", "metadata": [
                    {"key": "order", "value": 2}
                ]},
                {"key": "a", "type": "hasProcessor", "metadata": [
                    {"key": "order", "value": 1}
                ]},
            ],
        }
        with patch.object(
            graph_store, "get_item_from_collection_by_id", return_value=document
        ):
            relations = graph_store.get_collection_item_relations("pipelines", "one")

        assert [r["key"] for r in relations] == ["b", "a"]

    def test_adding_metadata_appends_without_duplicating(self, graph_store):
        document = {
            "_id": "one",
            "type": "pipeline",
            "metadata": [{"key": "name", "value": "n"}],
        }
        serialize, put, get, written = self._store_with(graph_store, document)
        with serialize, put, get:
            graph_store.add_sub_item_to_collection_item(
                "pipelines",
                "one",
                "metadata",
                [{"key": "name", "value": "n"}, {"key": "description", "value": "d"}],
            )

        assert written[-1]["metadata"] == [
            {"key": "name", "value": "n"},
            {"key": "description", "value": "d"},
        ]


    def test_adding_relations_appends_them(self, graph_store):
        """The create path saves the document first and adds its relations
        after, so a missing implementation here loses every connection the new
        pipeline was drawn with."""
        document = {"_id": "one", "type": "pipeline", "relations": []}
        serialize, put, get, written = self._store_with(graph_store, document)
        with serialize, put, get:
            graph_store.add_relations_to_collection_item(
                "pipelines", "one", [{"key": "a", "type": "hasProcessor"}]
            )

        assert written[-1]["relations"] == [{"key": "a", "type": "hasProcessor"}]

    def test_a_relation_without_a_type_is_not_a_relation(self, graph_store):
        document = {"_id": "one", "type": "pipeline", "relations": []}
        serialize, put, get, written = self._store_with(graph_store, document)
        with serialize, put, get:
            graph_store.add_relations_to_collection_item(
                "pipelines", "one", [{"key": "a"}]
            )

        assert written[-1]["relations"] == []


class TestTheV2WriteFamily:
    """The paths the Elody document resources use.

    What they add over v1 is the configuration's crud hooks: the document is
    patched, stamped and checked by its own configuration rather than by the
    engine. That is storage-agnostic, so it has to happen here too -- otherwise
    a form save against the store skips the validation and the audit stamps
    that the same save against the database gets.
    """

    @pytest.fixture
    def configuration(self):
        """A configuration whose hooks and patcher record that they ran."""
        calls = []

        class Configuration:
            SCHEMA_TYPE = "sparql"

            def crud(self):
                return {
                    "collection": "pipelines",
                    "document_content_patcher": self._patch,
                    "content_changes_checker": self._changed,
                    "pre_crud_hook": self._pre,
                    "post_crud_hook": self._post,
                }

            def document_info(self):
                return {"object_lists": {"metadata": "key", "relations": "type"}}

            @staticmethod
            def _patch(*, document, content, crud, timestamp, overwrite=False, **_):
                calls.append(("patch", overwrite))
                return {**document, **content} if not overwrite else dict(content)

            @staticmethod
            def _changed(*, document, unpatched_document, **_):
                return document != unpatched_document

            @staticmethod
            def _pre(*, crud, document=None, **_):
                calls.append(("pre", crud))
                return {**(document or {}), "stamped": True}

            @staticmethod
            def _post(*, crud, **_):
                calls.append(("post", crud))

        Configuration.calls = calls
        return Configuration

    @pytest.fixture
    def hooked(self, graph_store, configuration):
        """The engine, with that configuration and a store that accepts writes."""
        written = []
        mapper = MagicMock()
        mapper.get.return_value = configuration()
        with patch(
            "storage.sparqlstore.get_object_configuration_mapper", return_value=mapper
        ), patch(
            "storage.sparqlstore.serialize",
            side_effect=lambda document, **kwargs: (
                written.append(document) or ONE_DEFINITION
            ),
        ), patch(
            "storage.sparqlstore.requests.put", return_value=_response("", 204)
        ), patch.object(
            graph_store, "get_item_from_collection_by_id", return_value={"_id": "one"}
        ):
            yield graph_store, configuration.calls, written

    def test_creating_runs_the_hooks_and_writes(self, hooked):
        store, calls, written = hooked

        store.save_item_to_collection_v2("pipelines", {"type": "pipeline"})

        assert ("pre", "create") in calls
        assert ("post", "create") in calls
        assert written[-1]["stamped"] is True

    def test_creating_mints_an_id_to_name_the_graph_with(self, hooked):
        store, _, written = hooked

        store.save_item_to_collection_v2("pipelines", {"type": "pipeline"})

        assert written[-1]["_id"]

    def test_the_post_hook_can_be_suppressed(self, hooked):
        store, calls, _ = hooked

        store.save_item_to_collection_v2(
            "pipelines", {"type": "pipeline"}, run_post_crud_hook=False
        )

        assert ("post", "create") not in calls

    def test_history_is_not_written(self, hooked):
        """There is no second collection to keep it in, and versioning a
        definition is not in scope."""
        store, calls, written = hooked

        store.save_item_to_collection_v2(
            "pipelines", {"type": "pipeline"}, is_history=True
        )

        assert written == []
        assert calls == []

    def test_a_put_overwrites_and_a_patch_merges(self, hooked):
        """The distinction is the configuration's, not the engine's."""
        store, calls, _ = hooked
        item = {"_id": "one", "type": "pipeline", "metadata": []}

        store.put_item_from_collection("pipelines", item, {"metadata": [1]}, "elody")
        store.patch_item_from_collection_v2(
            "pipelines", item, {"metadata": [2]}, "elody"
        )

        assert ("patch", True) in calls
        assert ("patch", False) in calls

    def test_an_update_runs_the_hooks(self, hooked):
        store, calls, written = hooked

        store.patch_item_from_collection_v2(
            "pipelines",
            {"_id": "one", "type": "pipeline", "metadata": []},
            {"metadata": [{"key": "name", "value": "n"}]},
            "elody",
        )

        assert ("pre", "update") in calls
        assert ("post", "update") in calls
        assert written[-1]["metadata"] == [{"key": "name", "value": "n"}]

    def test_an_update_that_changes_nothing_is_not_written(self, hooked):
        """The same guard the database engine applies: a form save that altered
        nothing should not produce a new document in the store."""
        store, _, written = hooked
        item = {"_id": "one", "type": "pipeline", "metadata": []}

        result = store.patch_item_from_collection_v2("pipelines", item, {}, "elody")

        assert written == []
        assert result == item

    def test_deleting_runs_the_hooks_and_drops_the_graph(
        self, graph_store, configuration
    ):
        mapper = MagicMock()
        mapper.get.return_value = configuration()
        with patch(
            "storage.sparqlstore.get_object_configuration_mapper", return_value=mapper
        ), patch(
            "storage.sparqlstore.requests.delete", return_value=_response("", 204)
        ) as delete:
            graph_store.delete_item({"_id": "one", "type": "pipeline"})

        assert ("pre", "delete") in configuration.calls
        assert ("post", "delete") in configuration.calls
        assert delete.call_args.kwargs["params"] == {
            "graph": f"{PIPELINE_GRAPH_BASE}/one"
        }

