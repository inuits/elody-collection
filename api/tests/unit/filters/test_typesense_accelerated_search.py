"""
Unit tests for Typesense accelerated search in BaseFilterResource.

Tests pagination, multi-type filters, fallback behavior, and count consistency.
"""

import pytest
from unittest.mock import MagicMock, patch


def make_mongo_doc(doc_id, doc_type="work_word"):
    return {"_id": doc_id, "type": doc_type, "identifiers": []}


def make_ts_result(ids, total_count):
    return {"ids": ids, "count": total_count}


@pytest.fixture
def flask_app():
    from flask import Flask

    app = Flask(__name__)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def mock_filter_engine():
    engine = MagicMock()
    engine.filter.return_value = {
        "results": [],
        "count": 0,
        "skip": 0,
        "limit": 20,
    }
    return engine


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage._prepare_mongo_document.side_effect = lambda doc, _: doc
    return storage


def make_mock_config(collection_name):
    config = MagicMock()
    config.crud.return_value = {"collection": collection_name}
    return config


@pytest.fixture
def mock_config_mapper():
    """Mock object configuration mapper with type->collection mapping."""
    type_to_collection = {
        "work_word": "bibliographic_entities_actual",
        "work_music": "bibliographic_entities_actual",
        "work_computer_file": "bibliographic_entities_actual",
        "work_serial": "bibliographic_entities_actual",
        "manifestation_word": "bibliographic_entities_actual",
        "person": "entities_actual",
        "corporation": "entities_actual",
        "subject": "entities_actual",
        "genre": "entities_actual",
    }
    mapper = MagicMock()
    mapper.get.side_effect = lambda t: make_mock_config(
        type_to_collection.get(t, "entities_actual")
    )
    return mapper


@pytest.fixture
def resource(mock_filter_engine, mock_storage, mock_config_mapper):
    with patch("resources.base_filter_resource.FilterManagerV2") as mock_fm, patch(
        "resources.base_filter_resource.StorageManager"
    ) as mock_sm, patch(
        "resources.base_filter_resource.get_object_configuration_mapper"
    ) as mock_mapper:
        mock_fm.return_value.get_filter_engine.return_value = mock_filter_engine

        mock_sm_instance = MagicMock()
        mock_sm_instance.get_db_engine.return_value = mock_storage
        mock_sm.return_value = mock_sm_instance

        mock_mapper.return_value = mock_config_mapper

        from resources.base_filter_resource import BaseFilterResource

        res = BaseFilterResource.__new__(BaseFilterResource)
        res.filter_engine_v2 = mock_filter_engine

        yield res


class TestTypesenseFilterClassification:
    """Test that filters are correctly classified into text, type, and remaining."""

    def test_multiple_type_filters_collected(self, flask_app, resource):
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {"type": "type", "value": "work_word", "match_exact": True},
                {"type": "type", "value": "work_music", "match_exact": True},
                {"type": "type", "value": "person", "match_exact": True},
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "mozart",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result(["id1", "id2"], 2)

                with patch("resources.base_filter_resource.StorageManager") as mock_sm:
                    mock_storage = MagicMock()
                    mock_storage.db.__getitem__.return_value.find.return_value = [
                        make_mongo_doc("id1"),
                        make_mongo_doc("id2"),
                    ]
                    mock_storage._prepare_mongo_document.side_effect = (
                        lambda doc, _: doc
                    )
                    mock_sm.return_value.get_db_engine.return_value = mock_storage

                    resource._execute_typesense_accelerated_search(
                        query,
                        "entities",
                        {
                            "enabled": True,
                            "collection": "entities",
                            "search_fields": ["properties.name.value"],
                        },
                    )

                # Verify Typesense was called with all 3 types
                call_kwargs = mock_ts.call_args
                filter_by = call_kwargs.kwargs.get("filter_by") or call_kwargs[1].get(
                    "filter_by"
                )
                assert "work_word" in filter_by
                assert "work_music" in filter_by
                assert "person" in filter_by

    def test_selection_type_filter_also_collected(self, flask_app, resource):
        """Selection filter with key='type' extracts types for Typesense filter_by.
        Since it's not added to remaining_filters, the normal search path is used."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "selection",
                    "key": "type",
                    "value": ["work_word", "work_music"],
                    "match_exact": True,
                },
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "bach",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result(["id1"], 1)

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                call_kwargs = mock_ts.call_args
                filter_by = call_kwargs.kwargs.get("filter_by") or call_kwargs[1].get(
                    "filter_by"
                )
                assert "work_word" in filter_by
                assert "work_music" in filter_by

    def test_selection_with_match_exact_false_treated_as_text(
        self, flask_app, resource
    ):
        """A selection filter with match_exact=false and a string value is a text search."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "selection",
                    "key": "type",
                    "value": ["work_word"],
                    "match_exact": True,
                },
                {
                    "type": "selection",
                    "key": ["vlacc:1|properties.ref_authors.key"],
                    "value": "brusselmans",
                    "match_exact": False,
                },
            ]

            text, types, exact, remaining = resource._classify_filters_for_typesense(
                query
            )
            assert len(text) == 1
            assert text[0]["value"] == "brusselmans"
            assert "work_word" in types
            assert len(remaining) == 0

    def test_selection_with_match_exact_true_is_exact_match(self, flask_app, resource):
        """A selection filter with match_exact=true is an exact-match filter."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "selection",
                    "key": ["vlacc:1|properties.ref_authors.key"],
                    "value": "brusselmans",
                    "match_exact": True,
                },
            ]

            text, types, exact, remaining = resource._classify_filters_for_typesense(
                query
            )
            assert len(text) == 0
            assert len(exact) == 1
            assert len(remaining) == 0

    def test_selection_with_list_value_is_exact_match(self, flask_app, resource):
        """A selection filter with a list value (multi-select) is an exact match."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "selection",
                    "key": "properties.ref_genre.value",
                    "value": ["fiction", "thriller"],
                    "match_exact": True,
                },
            ]

            text, types, exact, remaining = resource._classify_filters_for_typesense(
                query
            )
            assert len(text) == 0
            assert len(exact) == 1
            assert len(remaining) == 0

    def test_lookup_filter_resolved_via_typesense(self, flask_app, resource):
        """A filter with a lookup should be resolved via Typesense search on the foreign collection."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "selection",
                    "key": "type",
                    "value": ["work_word"],
                    "match_exact": True,
                },
                {
                    "type": "selection",
                    "key": ["vlacc:1|properties.ref_authors.key"],
                    "value": "brusselmans",
                    "match_exact": False,
                    "lookup": {
                        "from": "entities_actual",
                        "local_field": "properties.ref_authors.value",
                        "foreign_field": "_id",
                        "as": "lookup.virtual_relations.ref_authors",
                    },
                },
            ]

            text, types, exact, remaining = resource._classify_filters_for_typesense(
                query
            )
            # The lookup filter value is a string with match_exact=False,
            # so it is classified as text and resolved into an ID-based filter
            # via Typesense lookup later in the execute path.
            assert "work_word" in types

    def test_lookup_filter_with_match_exact_is_exact_match(self, flask_app, resource):
        """A lookup filter with match_exact=True is classified as exact-match,
        keeping its lookup so the execute path can defer it to MongoDB when the
        key isn't a Typesense search field."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "selection",
                    "key": ["vlacc:1|identifiers"],
                    "value": ["PERS-12345"],
                    "match_exact": True,
                    "lookup": {
                        "from": "entities_actual",
                        "local_field": "properties.ref_authors.value",
                        "foreign_field": "_id",
                        "as": "lookup.virtual_relations.ref_authors",
                    },
                },
            ]

            text, types, exact, remaining = resource._classify_filters_for_typesense(
                query
            )
            assert len(exact) == 1
            assert exact[0].get("lookup") is not None

    def test_single_type_filter_uses_exact_match(self, flask_app, resource):
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {"type": "type", "value": "work_word", "match_exact": True},
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result([], 0)

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                call_kwargs = mock_ts.call_args
                filter_by = call_kwargs.kwargs.get("filter_by") or call_kwargs[1].get(
                    "filter_by"
                )
                assert filter_by == "type:=work_word"

    def test_wildcard_text_filter_sent_to_typesense(
        self, flask_app, resource, mock_filter_engine
    ):
        """Since commit 38fbea30 wildcard text filters are routed through
        Typesense (q="*" + group_by) rather than excluded, so the '*' term is
        part of the search query alongside any concrete term."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "*",
                    "match_exact": False,
                },
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.title.value"],
                    "value": "mars",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result([], 0)

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": [
                            "properties.name.value",
                            "properties.title.value",
                        ],
                    },
                )

                # Both terms are sent to Typesense as the search query.
                call_args = mock_ts.call_args
                search_query = call_args[0][1]
                assert search_query == "* mars"


class TestMultiKeyExactMatchFilter:
    """A multi-key OR exact-match selection filter must query ALL keys.

    Regression: previously only key[0] survived, so entity types whose data
    lives in a non-first key (e.g. nomen/time/place use properties.title.value)
    returned no results, while person/corporation (properties.name.value)
    worked by accident.
    """

    SUBJECT_KEYS = [
        "vlacc:1|properties.name.value",
        "vlacc:1|properties.title.value",
        "vlacc:1|properties.non_preferred_name.value",
        "vlacc:1|properties.non_preferred_title.value",
    ]
    SEARCH_FIELDS = [
        "properties.name.value",
        "properties.title.value",
        "properties.non_preferred_name.value",
        "properties.non_preferred_title.value",
    ]

    def test_all_keys_present_in_filter_by_or_group(self, flask_app, resource):
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "selection",
                    "key": self.SUBJECT_KEYS,
                    "value": ["Bladmuziek"],
                    "match_exact": True,
                    "operator": "or",
                },
                {
                    "type": "selection",
                    "key": "type",
                    "value": ["nomen", "person"],
                    "match_exact": True,
                },
            ]
            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result(["id1"], 1)
                with patch("resources.base_filter_resource.StorageManager") as mock_sm:
                    mock_storage = MagicMock()
                    mock_storage.db.__getitem__.return_value.find.return_value = [
                        make_mongo_doc("id1")
                    ]
                    mock_storage._prepare_mongo_document.side_effect = (
                        lambda doc, _: doc
                    )
                    mock_sm.return_value.get_db_engine.return_value = mock_storage

                    resource._execute_typesense_accelerated_search(
                        query,
                        "entities",
                        {
                            "enabled": True,
                            "collection": "entities",
                            "search_fields": self.SEARCH_FIELDS,
                        },
                    )

            filter_by = mock_ts.call_args.kwargs.get("filter_by") or mock_ts.call_args[
                1
            ].get("filter_by")
            assert "properties_name_value:=" in filter_by
            assert "properties_title_value:=" in filter_by
            assert "properties_non_preferred_name_value:=" in filter_by
            assert "properties_non_preferred_title_value:=" in filter_by
            # The keys must be OR-ed together, not AND-ed.
            assert "||" in filter_by

    def test_non_indexed_key_falls_back_to_mongo(self, flask_app, resource):
        """If any key is not indexed in Typesense, push the whole filter to the
        v2 MongoDB engine rather than silently dropping an OR branch."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "selection",
                    "key": [
                        "vlacc:1|properties.name.value",
                        "vlacc:1|properties.not_indexed.value",
                    ],
                    "value": ["Bach"],
                    "match_exact": True,
                    "operator": "or",
                },
            ]
            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )
                # No exact-match Typesense query — resolved via MongoDB v2 engine.
                mock_ts.assert_not_called()
            assert resource.filter_engine_v2.filter.called


class TestMultiKeyTextFilter:
    """A multi-key (OR) text filter must search ALL keys in Typesense.

    Regression: previously only key[0] survived in both the indexed-field gate
    and the query_by builder, so an entity picker searching e.g. code + title
    only ever queried the code field — searching by title returned nothing
    (vlacc language picker on the expression create form).
    """

    LANGUAGE_KEYS = [
        "vlacc:1|properties.code.value",
        "vlacc:1|properties.title.value",
    ]
    SEARCH_FIELDS = ["properties.code.value", "properties.title.value"]

    def test_all_keys_present_in_query_by(self, flask_app, resource):
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {"type": "type", "value": "language", "match_exact": True},
                {
                    "type": "text",
                    "key": self.LANGUAGE_KEYS,
                    "value": "engels",
                    "match_exact": False,
                    "operator": "or",
                },
            ]
            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result(["id1"], 1)
                with patch("resources.base_filter_resource.StorageManager") as mock_sm:
                    mock_storage = MagicMock()
                    mock_storage.db.__getitem__.return_value.find.return_value = [
                        make_mongo_doc("id1")
                    ]
                    mock_storage._prepare_mongo_document.side_effect = (
                        lambda doc, _: doc
                    )
                    mock_sm.return_value.get_db_engine.return_value = mock_storage

                    resource._execute_typesense_accelerated_search(
                        query,
                        "entities",
                        {
                            "enabled": True,
                            "collection": "entities",
                            "search_fields": self.SEARCH_FIELDS,
                        },
                    )

            # query_by is the 3rd positional arg to typesense_search.
            query_by = mock_ts.call_args[0][2]
            assert "properties_code_value" in query_by
            assert "properties_title_value" in query_by

    def test_non_indexed_key_falls_back_to_mongo(self, flask_app, resource):
        """If any key of a multi-key text filter is not indexed, defer the whole
        filter to the MongoDB engine rather than silently dropping an OR branch."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": [
                        "vlacc:1|properties.code.value",
                        "vlacc:1|properties.not_indexed.value",
                    ],
                    "value": "engels",
                    "match_exact": False,
                },
            ]
            with patch.object(
                resource, "_execute_advanced_search_with_query_v2"
            ) as mock_mongo:
                mock_mongo.return_value = {
                    "results": [],
                    "count": 0,
                    "skip": 0,
                    "limit": 20,
                }
                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.code.value"],
                    },
                )
                mock_mongo.assert_called_once_with(query, "entities")


class TestTypesensePagination:
    """Test pagination behavior with Typesense."""

    def test_page1_returns_correct_count(self, flask_app, resource):
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {"type": "type", "value": "work_word", "match_exact": True},
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "mars",
                    "match_exact": False,
                },
            ]
            ids = [f"id{i}" for i in range(20)]

            with patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts, patch(
                "resources.base_filter_resource.StorageManager"
            ) as mock_sm:
                mock_ts.return_value = make_ts_result(ids, 50)

                mock_storage = MagicMock()
                mock_storage.db.__getitem__.return_value.find.return_value = [
                    make_mongo_doc(id) for id in ids
                ]
                mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
                mock_sm.return_value.get_db_engine.return_value = mock_storage

                result = resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

            assert result["count"] == 50
            assert len(result["results"]) == 20

    def test_typesense_receives_correct_offset_and_limit(self, flask_app, resource):
        """Typesense should receive offset=skip and per_page=limit."""
        with flask_app.test_request_context(
            "/entities/filter?limit=10&skip=30",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result([], 50)

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                call_kwargs = mock_ts.call_args
                assert call_kwargs.kwargs.get("per_page") == 10
                assert call_kwargs.kwargs.get("offset") == 30

    def test_empty_page_beyond_results_keeps_count(self, flask_app, resource):
        """When skip is beyond total results, count should still be total."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=60",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {"type": "type", "value": "work_word", "match_exact": True},
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "mars",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                # Typesense returns 0 hits but found=30 (total matches)
                mock_ts.return_value = make_ts_result([], 30)

                result = resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

            assert result["count"] == 30
            assert result["results"] == []
            assert result["skip"] == 60
            assert result["limit"] == 20

    def test_count_consistent_across_pages(self, flask_app, resource):
        """Count should be the same on page 1, 2, and beyond."""
        total = 45

        for skip, num_ids in [(0, 20), (20, 20), (40, 5), (60, 0)]:
            with flask_app.test_request_context(
                f"/entities/filter?limit=20&skip={skip}",
                method="POST",
                content_type="application/json",
            ):
                query = [
                    {
                        "type": "text",
                        "key": ["vlacc:1|properties.name.value"],
                        "value": "test",
                        "match_exact": False,
                    },
                ]
                ids = [f"id{skip + i}" for i in range(num_ids)]

                with patch(
                    "resources.base_filter_resource.typesense_search"
                ) as mock_ts, patch(
                    "resources.base_filter_resource.StorageManager"
                ) as mock_sm:
                    mock_ts.return_value = make_ts_result(ids, total)

                    mock_storage = MagicMock()
                    mock_storage.db.__getitem__.return_value.find.return_value = [
                        make_mongo_doc(id) for id in ids
                    ]
                    mock_storage._prepare_mongo_document.side_effect = (
                        lambda doc, _: doc
                    )
                    mock_sm.return_value.get_db_engine.return_value = mock_storage

                    result = resource._execute_typesense_accelerated_search(
                        query,
                        "entities",
                        {
                            "enabled": True,
                            "collection": "entities",
                            "search_fields": ["properties.name.value"],
                        },
                    )

                assert (
                    result["count"] == total
                ), f"Count should be {total} at skip={skip}, got {result['count']}"

    def test_next_link_present_when_more_results(self, flask_app, resource):
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]
            ids = [f"id{i}" for i in range(20)]

            with patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts, patch(
                "resources.base_filter_resource.StorageManager"
            ) as mock_sm:
                mock_ts.return_value = make_ts_result(ids, 50)

                mock_storage = MagicMock()
                mock_storage.db.__getitem__.return_value.find.return_value = [
                    make_mongo_doc(id) for id in ids
                ]
                mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
                mock_sm.return_value.get_db_engine.return_value = mock_storage

                result = resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

            assert "next" in result
            assert "skip=20" in result["next"]

    def test_no_next_link_on_last_page(self, flask_app, resource):
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=40",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]
            ids = [f"id{i}" for i in range(5)]

            with patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts, patch(
                "resources.base_filter_resource.StorageManager"
            ) as mock_sm:
                mock_ts.return_value = make_ts_result(ids, 45)

                mock_storage = MagicMock()
                mock_storage.db.__getitem__.return_value.find.return_value = [
                    make_mongo_doc(id) for id in ids
                ]
                mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
                mock_sm.return_value.get_db_engine.return_value = mock_storage

                result = resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

            assert "next" not in result
            assert "previous" in result


class TestTypesenseFallback:
    """Test fallback to MongoDB when Typesense is unavailable."""

    def test_fallback_when_typesense_returns_none(self, flask_app, resource):
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = None  # Typesense unavailable

                with patch.object(
                    resource, "_execute_advanced_search_with_query_v2"
                ) as mock_mongo:
                    mock_mongo.return_value = {
                        "results": [],
                        "count": 0,
                        "skip": 0,
                        "limit": 20,
                    }

                    resource._execute_typesense_accelerated_search(
                        query,
                        "entities",
                        {
                            "enabled": True,
                            "collection": "entities",
                            "search_fields": ["properties.name.value"],
                        },
                    )

                    mock_mongo.assert_called_once_with(query, "entities")

    def test_falls_back_when_not_enabled(self, flask_app, resource):
        """When typesense_config has enabled=False, fall back to MongoDB."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]

            with patch.object(
                resource, "_execute_advanced_search_with_query_v2"
            ) as mock_mongo:
                mock_mongo.return_value = {
                    "results": [],
                    "count": 0,
                    "skip": 0,
                    "limit": 20,
                }

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": False,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                mock_mongo.assert_called_once_with(query, "entities")

    def test_falls_back_when_no_text_filters(self, flask_app, resource):
        """When query has no text filters, fall back to MongoDB."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {"type": "type", "value": "work_word", "match_exact": True},
                {
                    "type": "selection",
                    "key": "status",
                    "value": ["active"],
                    "match_exact": True,
                },
            ]

            with patch.object(
                resource, "_execute_advanced_search_with_query_v2"
            ) as mock_mongo:
                mock_mongo.return_value = {
                    "results": [],
                    "count": 0,
                    "skip": 0,
                    "limit": 20,
                }

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                mock_mongo.assert_called_once_with(query, "entities")


class TestTypesenseResultOrdering:
    """Test that MongoDB results preserve Typesense relevance order."""

    def test_results_ordered_by_typesense_relevance(self, flask_app, resource):
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]
            # Typesense returns IDs in relevance order
            ts_ids = ["best_match", "good_match", "ok_match"]

            with patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts, patch(
                "resources.base_filter_resource.StorageManager"
            ) as mock_sm:
                mock_ts.return_value = make_ts_result(ts_ids, 3)

                # MongoDB returns in different order
                mock_storage = MagicMock()
                mock_storage.db.__getitem__.return_value.find.return_value = [
                    make_mongo_doc("ok_match"),
                    make_mongo_doc("best_match"),
                    make_mongo_doc("good_match"),
                ]
                mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
                mock_sm.return_value.get_db_engine.return_value = mock_storage

                result = resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

            result_ids = [r["_id"] for r in result["results"]]
            assert result_ids == [
                "best_match",
                "good_match",
                "ok_match",
            ], "Results should be in Typesense relevance order"


class TestTypesenseRemainingFilters:
    """Test behavior when query has filters beyond text and type."""

    def test_remaining_filters_use_search_all_ids(self, flask_app, resource):
        """When remaining filters exist, all IDs should be fetched from Typesense."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
                {
                    "type": "selection",
                    "key": "status",
                    "value": ["active"],
                    "match_exact": True,
                },
            ]

            with patch(
                "resources.base_filter_resource.typesense_search_all_ids"
            ) as mock_ts_all, patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts:
                mock_ts_all.return_value = make_ts_result(["id1", "id2", "id3"], 3)

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                # search_all_ids should be called, not search
                mock_ts_all.assert_called_once()
                mock_ts.assert_not_called()

    def test_remaining_filters_passed_to_mongodb(
        self, flask_app, resource, mock_filter_engine
    ):
        """Remaining filters + Typesense IDs should be passed to filter engine."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
                {
                    "type": "selection",
                    "key": "status",
                    "value": ["active"],
                    "match_exact": True,
                },
            ]

            with patch(
                "resources.base_filter_resource.typesense_search_all_ids"
            ) as mock_ts_all:
                mock_ts_all.return_value = make_ts_result(["id1", "id2", "id3"], 3)
                mock_filter_engine.filter.return_value = {
                    "results": [make_mongo_doc("id1")],
                    "count": 1,
                    "skip": 0,
                    "limit": 20,
                }

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                # filter_engine should receive remaining filters + ID filter
                call_args = mock_filter_engine.filter.call_args[0]
                filters = call_args[0]
                id_filter = next(
                    (
                        f
                        for f in filters
                        if f.get("key") == "_id" and f.get("type") == "selection"
                    ),
                    None,
                )
                assert id_filter is not None
                assert id_filter["value"] == ["id1", "id2", "id3"]


class TestTypesenseCrossCollectionFiltering:
    """Test that types from other MongoDB collections are excluded from Typesense query."""

    def test_cross_collection_types_all_sent_to_typesense(self, flask_app, resource):
        """All types are sent to Typesense and all relevant MongoDB collections are queried."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            # Query has work_word (bibliographic_entities_actual) and person (entities_actual)
            query = [
                {"type": "type", "value": "work_word", "match_exact": True},
                {"type": "type", "value": "person", "match_exact": True},
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "mars",
                    "match_exact": False,
                },
            ]

            with patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts, patch(
                "resources.base_filter_resource.StorageManager"
            ) as mock_sm:
                mock_ts.return_value = make_ts_result(["id1", "id2"], 2)

                mock_storage = MagicMock()
                mock_storage.db.__getitem__.return_value.find.return_value = [
                    make_mongo_doc("id1", "work_word"),
                    make_mongo_doc("id2", "person"),
                ]
                mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
                mock_sm.return_value.get_db_engine.return_value = mock_storage

                resource._execute_typesense_accelerated_search(
                    query,
                    "bibliographic_entities_actual",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                # Typesense should receive both work_word and person
                call_kwargs = mock_ts.call_args
                filter_by = call_kwargs.kwargs.get("filter_by") or call_kwargs[1].get(
                    "filter_by"
                )
                assert "work_word" in filter_by
                assert "person" in filter_by

                # MongoDB should be queried for both collections
                db_calls = mock_storage.db.__getitem__.call_args_list
                queried_collections = {call[0][0] for call in db_calls}
                assert "bibliographic_entities_actual" in queried_collections
                assert "entities_actual" in queried_collections

    def test_all_types_same_collection_kept(self, flask_app, resource):
        """When all types map to the same collection, all are included."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {"type": "type", "value": "work_word", "match_exact": True},
                {"type": "type", "value": "work_music", "match_exact": True},
                {"type": "type", "value": "work_serial", "match_exact": True},
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result([], 0)

                resource._execute_typesense_accelerated_search(
                    query,
                    "bibliographic_entities_actual",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                call_kwargs = mock_ts.call_args
                filter_by = call_kwargs.kwargs.get("filter_by") or call_kwargs[1].get(
                    "filter_by"
                )
                assert "work_word" in filter_by
                assert "work_music" in filter_by
                assert "work_serial" in filter_by

    def test_cross_collection_count_includes_all_types(self, flask_app, resource):
        """Count should reflect all matching types across collections."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {"type": "type", "value": "work_word", "match_exact": True},
                {"type": "type", "value": "person", "match_exact": True},
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "mars",
                    "match_exact": False,
                },
            ]

            with patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts, patch(
                "resources.base_filter_resource.StorageManager"
            ) as mock_sm:
                # Typesense returns 20 (both work_word and person matches)
                ids = [f"id{i}" for i in range(20)]
                mock_ts.return_value = make_ts_result(ids, 25)

                mock_storage = MagicMock()
                # Each collection returns its subset; use side_effect
                # to return docs only on first call, empty on second
                all_docs = [make_mongo_doc(f"id{i}") for i in range(20)]
                mock_storage.db.__getitem__.return_value.find.side_effect = [
                    all_docs,
                    [],
                ]
                mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
                mock_sm.return_value.get_db_engine.return_value = mock_storage

                result = resource._execute_typesense_accelerated_search(
                    query,
                    "bibliographic_entities_actual",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

            # Count includes all types (work_word + person)
            assert result["count"] == 25
            assert len(result["results"]) == 20

    def test_page2_with_cross_collection_types(self, flask_app, resource):
        """Page 2 should return results even when cross-collection types are in the query."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=20",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {"type": "type", "value": "work_word", "match_exact": True},
                {"type": "type", "value": "person", "match_exact": True},
                {"type": "type", "value": "corporation", "match_exact": True},
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]

            with patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts, patch(
                "resources.base_filter_resource.StorageManager"
            ) as mock_sm:
                # 5 results on page 2 (offset=20), 45 total matches
                page2_ids = [f"id{20+i}" for i in range(5)]
                mock_ts.return_value = make_ts_result(page2_ids, 45)

                mock_storage = MagicMock()
                # Return docs only from first collection, empty from second
                all_docs = [make_mongo_doc(id) for id in page2_ids]
                mock_storage.db.__getitem__.return_value.find.side_effect = [
                    all_docs,
                    [],
                ]
                mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
                mock_sm.return_value.get_db_engine.return_value = mock_storage

                result = resource._execute_typesense_accelerated_search(
                    query,
                    "bibliographic_entities_actual",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

            assert result["count"] == 45
            assert len(result["results"]) == 5
            assert result["skip"] == 20


class TestSmartFilterClassification:
    """Test that text filters on non-indexed fields are moved to remaining_filters."""

    def test_non_indexed_field_moves_to_remaining(
        self, flask_app, resource, mock_filter_engine
    ):
        """Text filter on a field not in search_fields should become a remaining_filter."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "mozart",
                    "match_exact": False,
                },
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.isbn.value"],
                    "value": "978-123",
                    "match_exact": False,
                },
            ]

            with patch(
                "resources.base_filter_resource.typesense_search_all_ids"
            ) as mock_ts_all:
                mock_ts_all.return_value = make_ts_result(["id1", "id2"], 2)
                mock_filter_engine.filter.return_value = {
                    "results": [make_mongo_doc("id1")],
                    "count": 1,
                    "skip": 0,
                    "limit": 20,
                }

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                # Typesense should only get "mozart" (name is indexed)
                call_args = mock_ts_all.call_args
                search_query = call_args[0][1]
                assert search_query == "mozart"
                assert "978-123" not in search_query

                # isbn filter should be passed to MongoDB as remaining
                mongo_call = mock_filter_engine.filter.call_args[0]
                mongo_filters = mongo_call[0]
                isbn_filter = next(
                    (
                        f
                        for f in mongo_filters
                        if f.get("key") == ["vlacc:1|properties.isbn.value"]
                    ),
                    None,
                )
                assert isbn_filter is not None

    def test_all_non_indexed_falls_back_to_mongodb(self, flask_app, resource):
        """When ALL text filters are on non-indexed fields, fall back entirely to MongoDB."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.isbn.value"],
                    "value": "978-123",
                    "match_exact": False,
                },
            ]

            with patch.object(
                resource, "_execute_advanced_search_with_query_v2"
            ) as mock_mongo:
                mock_mongo.return_value = {
                    "results": [],
                    "count": 0,
                    "skip": 0,
                    "limit": 20,
                }

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                # Should fall back to MongoDB with the full original query
                mock_mongo.assert_called_once_with(query, "entities")

    def test_indexed_field_stays_in_typesense(self, flask_app, resource):
        """Text filter on a field in search_fields should be sent to Typesense."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "test",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result([], 0)

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                # Typesense search should be called (not search_all_ids)
                mock_ts.assert_called_once()
                search_query = mock_ts.call_args[0][1]
                assert search_query == "test"

    def test_string_key_format_checked_against_search_fields(self, flask_app, resource):
        """Text filter with a plain string key should be checked against search_fields."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": "properties.name.value",
                    "value": "test",
                    "match_exact": False,
                },
            ]

            with patch("resources.base_filter_resource.typesense_search") as mock_ts:
                mock_ts.return_value = make_ts_result([], 0)

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                mock_ts.assert_called_once()

    def test_or_operator_non_indexed_logs_warning(self, flask_app, resource):
        """Text filter with operator 'or' on a non-indexed field should log a warning."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.isbn.value"],
                    "value": "978-123",
                    "match_exact": False,
                    "operator": "or",
                },
            ]

            with patch.object(
                resource, "_execute_advanced_search_with_query_v2"
            ) as mock_mongo, patch("resources.base_filter_resource.log") as mock_log:
                mock_mongo.return_value = {
                    "results": [],
                    "count": 0,
                    "skip": 0,
                    "limit": 20,
                }

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                mock_log.warning.assert_called_once()
                warning_msg = mock_log.warning.call_args[0][0]
                assert "properties.isbn.value" in warning_msg
                assert "or" in warning_msg.lower()

    def test_mixed_indexed_and_non_indexed_uses_search_all_ids(
        self, flask_app, resource, mock_filter_engine
    ):
        """When some text filters are indexed and others aren't, use search_all_ids
        because non-indexed filters become remaining_filters."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "mozart",
                    "match_exact": False,
                },
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.publisher.value"],
                    "value": "oxford",
                    "match_exact": False,
                },
            ]

            with patch(
                "resources.base_filter_resource.typesense_search_all_ids"
            ) as mock_ts_all, patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts:
                mock_ts_all.return_value = make_ts_result(["id1"], 1)
                mock_filter_engine.filter.return_value = {
                    "results": [make_mongo_doc("id1")],
                    "count": 1,
                    "skip": 0,
                    "limit": 20,
                }

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": ["properties.name.value"],
                    },
                )

                # search_all_ids used because publisher is a remaining_filter
                mock_ts_all.assert_called_once()
                mock_ts.assert_not_called()

    def test_all_fields_indexed_no_remaining(self, flask_app, resource):
        """When all text filter fields are in search_fields, no remaining_filters
        are created from text filters — regular typesense_search is used."""
        with flask_app.test_request_context(
            "/entities/filter?limit=20&skip=0",
            method="POST",
            content_type="application/json",
        ):
            query = [
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.name.value"],
                    "value": "mozart",
                    "match_exact": False,
                },
                {
                    "type": "text",
                    "key": ["vlacc:1|properties.isbn.value"],
                    "value": "978-123",
                    "match_exact": False,
                },
            ]

            with patch(
                "resources.base_filter_resource.typesense_search"
            ) as mock_ts, patch(
                "resources.base_filter_resource.StorageManager"
            ) as mock_sm:
                mock_ts.return_value = make_ts_result(["id1"], 1)

                mock_storage = MagicMock()
                mock_storage.db.__getitem__.return_value.find.return_value = [
                    make_mongo_doc("id1")
                ]
                mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
                mock_sm.return_value.get_db_engine.return_value = mock_storage

                resource._execute_typesense_accelerated_search(
                    query,
                    "entities",
                    {
                        "enabled": True,
                        "collection": "entities",
                        "search_fields": [
                            "properties.name.value",
                            "properties.isbn.value",
                        ],
                    },
                )

                # Regular search used (not search_all_ids)
                mock_ts.assert_called_once()
                search_query = mock_ts.call_args[0][1]
                assert "mozart" in search_query
                assert "978-123" in search_query


class TestClassifyFiltersForTypesense:
    """Unit tests for _classify_filters_for_typesense helper."""

    def test_text_filter_classified(self, resource):
        query = [{"type": "text", "value": "mars", "match_exact": False}]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert len(text) == 1
        assert text[0]["value"] == "mars"
        assert types == []
        assert exact == []
        assert remaining == []

    def test_exact_text_filter_goes_to_remaining(self, resource):
        query = [{"type": "text", "value": "mars", "match_exact": True}]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert text == []
        assert types == []
        assert exact == []
        assert len(remaining) == 1

    def test_wildcard_text_classified_as_text(self, resource):
        # Since commit 38fbea30 the `value != "*"` exclusion was dropped:
        # wildcard text filters are routed through Typesense (q="*" + group_by).
        query = [{"type": "text", "value": "*", "match_exact": False}]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert len(text) == 1
        assert types == []
        assert exact == []
        assert remaining == []

    def test_empty_value_text_goes_to_remaining(self, resource):
        query = [{"type": "text", "value": "", "match_exact": False}]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert text == []
        assert types == []
        assert exact == []
        assert len(remaining) == 1

    def test_none_value_text_goes_to_remaining(self, resource):
        query = [{"type": "text", "value": None, "match_exact": False}]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert text == []
        assert types == []
        assert exact == []
        assert len(remaining) == 1

    def test_type_filter_single_value(self, resource):
        query = [{"type": "type", "value": "work_word"}]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert text == []
        assert types == ["work_word"]
        assert remaining == []

    def test_type_filter_list_value(self, resource):
        query = [{"type": "type", "value": ["a", "b"]}]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert text == []
        assert types == ["a", "b"]
        assert remaining == []

    def test_selection_type_filter_extracted(self, resource):
        query = [{"type": "selection", "key": "type", "value": ["a", "b"]}]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert text == []
        assert types == ["a", "b"]
        assert remaining == []

    def test_exact_match_selection_classified_as_exact(self, resource):
        # Since commit 47db92da a match_exact selection with a concrete value
        # is an exact-match filter (resolved natively by Typesense).
        query = [
            {
                "type": "selection",
                "key": "properties.name.value",
                "value": ["Bach"],
                "match_exact": True,
            }
        ]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert text == []
        assert types == []
        assert len(exact) == 1
        assert remaining == []

    def test_non_type_selection_goes_to_remaining(self, resource):
        query = [{"type": "selection", "key": "status", "value": ["active"]}]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert text == []
        assert types == []
        assert exact == []
        assert len(remaining) == 1

    def test_mixed_filters(self, resource):
        query = [
            {"type": "text", "value": "mars", "match_exact": False},
            {"type": "type", "value": "work_word"},
            {"type": "selection", "key": "type", "value": ["person"]},
            {"type": "selection", "key": "status", "value": ["active"]},
        ]
        text, types, exact, remaining = resource._classify_filters_for_typesense(query)
        assert len(text) == 1
        assert types == ["work_word", "person"]
        assert len(remaining) == 1
        assert remaining[0]["key"] == "status"


class TestResolveMongoCollections:
    """Unit tests for _resolve_mongo_collections helper."""

    def test_empty_types_returns_default(self, resource):
        with patch("resources.base_filter_resource.get_object_configuration_mapper"):
            result = resource._resolve_mongo_collections([], "entities")
        assert result == {"entities"}

    def test_single_type_maps_correctly(self, resource):
        with patch(
            "resources.base_filter_resource.get_object_configuration_mapper"
        ) as mock_mapper:
            mock_config = MagicMock()
            mock_config.crud.return_value = {
                "collection": "bibliographic_entities_actual"
            }
            mock_mapper.return_value.get.return_value = mock_config

            result = resource._resolve_mongo_collections(["work_word"], "entities")
        assert result == {"bibliographic_entities_actual"}

    def test_multiple_types_multiple_collections(self, resource):
        with patch(
            "resources.base_filter_resource.get_object_configuration_mapper"
        ) as mock_mapper:
            type_map = {
                "work_word": "bibliographic_entities_actual",
                "person": "entities_actual",
            }

            def mock_get(t):
                config = MagicMock()
                config.crud.return_value = {"collection": type_map[t]}
                return config

            mock_mapper.return_value.get.side_effect = mock_get

            result = resource._resolve_mongo_collections(
                ["work_word", "person"], "entities"
            )
        assert result == {"bibliographic_entities_actual", "entities_actual"}

    def test_unknown_type_falls_back_to_default(self, resource):
        with patch(
            "resources.base_filter_resource.get_object_configuration_mapper"
        ) as mock_mapper:
            mock_mapper.return_value.get.side_effect = Exception("Unknown type")

            result = resource._resolve_mongo_collections(["unknown_type"], "entities")
        assert result == {"entities"}


class TestFetchDocumentsFromMongo:
    """Unit tests for _fetch_documents_from_mongo helper."""

    def test_preserves_typesense_relevance_order(self, resource):
        with patch("resources.base_filter_resource.StorageManager") as mock_sm:
            mock_storage = MagicMock()
            mock_storage.db.__getitem__.return_value.find.return_value = [
                make_mongo_doc("a"),
                make_mongo_doc("b"),
                make_mongo_doc("c"),
            ]
            mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
            mock_sm.return_value.get_db_engine.return_value = mock_storage

            results = resource._fetch_documents_from_mongo(
                ["c", "a", "b"], {"entities"}
            )

        assert [r["_id"] for r in results] == ["c", "a", "b"]

    def test_fetches_from_multiple_collections(self, resource):
        with patch("resources.base_filter_resource.StorageManager") as mock_sm:
            mock_storage = MagicMock()
            mock_col = MagicMock()
            mock_col.find.return_value = []
            mock_storage.db.__getitem__.return_value = mock_col
            mock_sm.return_value.get_db_engine.return_value = mock_storage

            resource._fetch_documents_from_mongo(["id1"], {"col1", "col2"})

        db_calls = mock_storage.db.__getitem__.call_args_list
        queried = {call[0][0] for call in db_calls}
        assert "col1" in queried
        assert "col2" in queried

    def test_unknown_docs_sorted_to_end(self, resource):
        with patch("resources.base_filter_resource.StorageManager") as mock_sm:
            mock_storage = MagicMock()
            mock_storage.db.__getitem__.return_value.find.return_value = [
                make_mongo_doc("a"),
                make_mongo_doc("b"),
                make_mongo_doc("unknown"),
            ]
            mock_storage._prepare_mongo_document.side_effect = lambda doc, _: doc
            mock_sm.return_value.get_db_engine.return_value = mock_storage

            results = resource._fetch_documents_from_mongo(["a", "b"], {"entities"})

        result_ids = [r["_id"] for r in results]
        assert result_ids == ["a", "b", "unknown"]


class TestAddPaginationLinks:
    """Unit tests for _add_pagination_links helper."""

    def test_next_link_when_more_results(self, resource):
        items = {"results": [], "count": 50}
        result = resource._add_pagination_links(items, 0, 20, "other_col")
        assert "next" in result
        assert "skip=20" in result["next"]

    def test_no_next_on_last_page(self, resource):
        items = {"results": [], "count": 45}
        result = resource._add_pagination_links(items, 40, 20, "other_col")
        assert "next" not in result

    def test_previous_link_when_skip_positive(self, resource):
        items = {"results": [], "count": 50}
        result = resource._add_pagination_links(items, 20, 20, "other_col")
        assert "previous" in result
        assert "skip=0" in result["previous"]

    def test_no_previous_on_first_page(self, resource):
        items = {"results": [], "count": 50}
        result = resource._add_pagination_links(items, 0, 20, "other_col")
        assert "previous" not in result

    def test_api_urls_injected_for_entity_collections(self, resource):
        items = {"results": [make_mongo_doc("id1")], "count": 1}
        with patch.object(
            resource, "_inject_api_urls_into_entities", return_value=[]
        ) as mock_inject:
            resource._add_pagination_links(items, 0, 20, "entities")
            mock_inject.assert_called_once()


class TestTypesenseAcceleratedSearchWithFacets:
    """Test that facets from Typesense are passed through in the response."""

    def test_facets_included_in_direct_typesense_response(
        self, flask_app, resource, mock_storage
    ):
        """When Typesense returns facets (no remaining_filters), they appear in the response."""
        query = [{"type": "text", "key": "properties.name.value", "value": "alice"}]
        ts_config = {
            "enabled": True,
            "collection": "entities",
            "search_fields": ["properties.name.value"],
            "facet_fields": ["type"],
        }
        ts_facets = [{"type": [{"_id": "work_word", "count": 5}]}]
        ts_result = {"ids": ["a"], "count": 1, "facets": ts_facets}

        with flask_app.test_request_context("/?skip=0&limit=20"):
            with patch.object(
                resource,
                "_classify_filters_for_typesense",
                return_value=([query[0]], [], [], []),
            ), patch.object(
                resource,
                "_build_typesense_query",
                return_value=("entities", "properties_name_value", "alice", None, None),
            ), patch.object(
                resource,
                "_execute_typesense_search",
                return_value=ts_result,
            ), patch.object(
                resource,
                "_fetch_documents_from_mongo",
                return_value=[make_mongo_doc("a")],
            ), patch.object(
                resource,
                "_resolve_mongo_collections",
                return_value=["entities_actual"],
            ), patch.object(
                resource,
                "_add_cors_headers",
            ), patch.object(
                resource,
                "_inject_api_urls_into_entities",
                side_effect=lambda x: x,
            ):
                result = resource._execute_typesense_accelerated_search(
                    query, "entities", ts_config
                )

        assert result.get("facets") == ts_facets

    def test_facet_fields_converted_to_facet_by_string(
        self, flask_app, resource, mock_storage
    ):
        """facet_fields from config are converted to Typesense facet_by format."""
        query = [{"type": "text", "key": "properties.name.value", "value": "test"}]
        ts_config = {
            "enabled": True,
            "collection": "entities",
            "search_fields": ["properties.name.value"],
            "facet_fields": ["type", "properties.ref_genre.value"],
        }

        with flask_app.test_request_context("/?skip=0&limit=20"):
            with patch.object(
                resource,
                "_classify_filters_for_typesense",
                return_value=([query[0]], [], [], []),
            ), patch.object(
                resource,
                "_build_typesense_query",
                return_value=("entities", "properties_name_value", "test", None, None),
            ), patch.object(
                resource,
                "_execute_typesense_search",
                return_value={"ids": [], "count": 0, "facets": []},
            ) as mock_ts_search, patch.object(
                resource,
                "_add_cors_headers",
            ):
                resource._execute_typesense_accelerated_search(
                    query, "entities", ts_config
                )

        call_args = mock_ts_search.call_args
        assert call_args is not None
        # facet_by should be the 8th positional arg or keyword arg
        if len(call_args[0]) > 7:
            assert call_args[0][7] == "type,properties_ref_genre_value"
        else:
            assert call_args[1].get("facet_by") == "type,properties_ref_genre_value"

    def test_no_facets_when_config_has_no_facet_fields(
        self, flask_app, resource, mock_storage
    ):
        """When facet_fields is absent from config, no facets are requested."""
        query = [{"type": "text", "key": "properties.name.value", "value": "test"}]
        ts_config = {
            "enabled": True,
            "collection": "entities",
            "search_fields": ["properties.name.value"],
        }
        ts_result = {"ids": ["a"], "count": 1}

        with flask_app.test_request_context("/?skip=0&limit=20"):
            with patch.object(
                resource,
                "_classify_filters_for_typesense",
                return_value=([query[0]], [], [], []),
            ), patch.object(
                resource,
                "_build_typesense_query",
                return_value=("entities", "properties_name_value", "test", None, None),
            ), patch.object(
                resource,
                "_execute_typesense_search",
                return_value=ts_result,
            ), patch.object(
                resource,
                "_fetch_documents_from_mongo",
                return_value=[make_mongo_doc("a")],
            ), patch.object(
                resource,
                "_resolve_mongo_collections",
                return_value=["entities_actual"],
            ), patch.object(
                resource,
                "_add_cors_headers",
            ), patch.object(
                resource,
                "_inject_api_urls_into_entities",
                side_effect=lambda x: x,
            ):
                result = resource._execute_typesense_accelerated_search(
                    query, "entities", ts_config
                )

        assert "facets" not in result or result.get("facets") is None

    def test_lookup_resolved_via_typesense_then_used_as_id_filter(
        self, flask_app, resource, mock_storage, mock_filter_engine, mock_config_mapper
    ):
        """A lookup filter should resolve IDs via Typesense, then pass resolved
        ID-based filter to the standard filter pipeline on the correct collection."""
        query = [
            {
                "type": "selection",
                "key": "type",
                "value": ["work_word"],
                "match_exact": True,
            },
            {
                "type": "selection",
                "key": ["vlacc:1|properties.ref_authors.key"],
                "value": "brusselmans",
                "match_exact": False,
                "lookup": {
                    "from": "entities_actual",
                    "local_field": "properties.ref_authors.value",
                    "foreign_field": "_id",
                    "as": "lookup.virtual_relations.ref_authors",
                },
            },
        ]
        ts_config = {
            "enabled": True,
            "collection": "entities",
            "search_fields": ["properties.name.value"],
        }

        with flask_app.test_request_context("/?skip=0&limit=20"):
            mock_mongo_storage = MagicMock()
            mock_mongo_storage.db.__getitem__.return_value.find.return_value = [
                {"_id": "uuid-1", "identifiers": ["uuid-1", "PERS-123"]},
                {"_id": "uuid-2", "identifiers": ["uuid-2", "PERS-456"]},
            ]

            with patch(
                "resources.base_filter_resource.typesense_search_all_ids"
            ) as mock_ts_all, patch(
                "resources.base_filter_resource.StorageManager"
            ) as mock_sm_lookup, patch.object(
                resource,
                "_execute_advanced_search_with_query_v2",
                return_value={"results": [make_mongo_doc("work-1")], "count": 1},
            ) as mock_v2, patch.object(
                resource,
                "_add_cors_headers",
            ):
                mock_sm_lookup.return_value.get_db_engine.return_value = (
                    mock_mongo_storage
                )
                mock_ts_all.return_value = {"ids": ["uuid-1", "uuid-2"], "count": 2}

                resource._execute_typesense_accelerated_search(
                    query, "entities", ts_config
                )

                # Verify Typesense was called to resolve the lookup
                assert mock_ts_all.called
                assert "brusselmans" in mock_ts_all.call_args[0][1]

                # Verify the standard filter pipeline was called with resolved filters
                assert mock_v2.called
                resolved_query = mock_v2.call_args[0][0]
                target_collection = mock_v2.call_args[0][1]

                # Should target the correct MongoDB collection, not "entities"
                assert target_collection == "bibliographic_entities_actual"

                # Should contain the resolved ID filter
                id_filter = next(
                    (
                        f
                        for f in resolved_query
                        if f.get("key") == "properties.ref_authors.value"
                    ),
                    None,
                )
                assert id_filter is not None
                assert set(id_filter["value"]) == {
                    "uuid-1",
                    "PERS-123",
                    "uuid-2",
                    "PERS-456",
                }
                assert id_filter["match_exact"] is True

                # Should still contain the type filter
                type_filter = next(
                    (f for f in resolved_query if f.get("key") == "type"),
                    None,
                )
                assert type_filter is not None

    def test_fallback_to_mongodb_preserves_facets(
        self, flask_app, resource, mock_filter_engine
    ):
        """When Typesense is unavailable, MongoDB fallback still returns facets."""
        query = [{"type": "text", "key": "properties.name.value", "value": "test"}]
        ts_config = {
            "enabled": True,
            "collection": "entities",
            "search_fields": ["properties.name.value"],
            "facet_fields": ["type"],
        }
        mongo_facets = [{"type": [{"_id": "person", "count": 10}]}]
        mock_filter_engine.filter.return_value = {
            "results": [],
            "count": 0,
            "skip": 0,
            "limit": 20,
            "facets": mongo_facets,
        }

        with flask_app.test_request_context("/?skip=0&limit=20"):
            with patch.object(
                resource,
                "_classify_filters_for_typesense",
                return_value=([query[0]], [], [], []),
            ), patch.object(
                resource,
                "_build_typesense_query",
                return_value=("entities", "properties_name_value", "test", None, None),
            ), patch.object(
                resource,
                "_execute_typesense_search",
                return_value=None,
            ), patch.object(
                resource,
                "_add_cors_headers",
            ), patch.object(
                resource,
                "_inject_api_urls_into_entities",
                side_effect=lambda x: x,
            ):
                result = resource._execute_typesense_accelerated_search(
                    query, "entities", ts_config
                )

        assert result.get("facets") == mongo_facets


class TestSourceRelationLookupResolution:
    """_resolve_source_relation_lookups rewrites a marked relation lookup into an
    indexed selection on the source entity's own relation values (no $lookup)."""

    def _make_resource(self):
        from resources.base_filter_resource import BaseFilterResource

        return BaseFilterResource.__new__(BaseFilterResource)

    def _forward_lookup_filter(self, value="W-CUR"):
        return {
            "type": "selection",
            "key": ["vlacc:1|lookup.virtual_relations.ref_related_works.identifiers"],
            "operator": "or",
            "value": value,
            "match_exact": True,
            "lookup": {
                "from": "bibliographic_entities_actual",
                "local_field": "id",
                "foreign_field": "properties.ref_related_works.value",
                "as": "lookup.virtual_relations.ref_related_works",
                "resolve_to_source_ids": True,
            },
        }

    def test_marked_lookup_rewritten_to_indexed_id_selection(self):
        mock_storage = MagicMock()
        mock_storage.db.__getitem__.return_value.find.return_value = [
            {"properties": {"ref_related_works": {"value": ["W-A", "W-B", "W-A"]}}}
        ]
        with patch("resources.base_filter_resource.StorageManager") as mock_sm:
            mock_sm.return_value.get_db_engine.return_value = mock_storage
            resource = self._make_resource()
            query = [
                {
                    "type": "selection",
                    "key": "type",
                    "value": ["work_word"],
                    "match_exact": True,
                },
                {
                    "type": "selection",
                    "key": ["vlacc:1|properties.ref_related_works.value"],
                    "operator": "or",
                    "value": "W-CUR",
                    "match_exact": True,
                },
                self._forward_lookup_filter(),
            ]

            new_query, did_resolve = resource._resolve_source_relation_lookups(query)

        assert did_resolve is True
        forward = next(f for f in new_query if f.get("key") == ["vlacc:1|id"])
        assert forward["type"] == "selection"
        assert forward["key"] == ["vlacc:1|id"]  # schema prefix preserved
        assert forward["value"] == ["W-A", "W-B"]  # deduped, order preserved
        assert forward["match_exact"] is True
        assert forward["operator"] == "or"
        assert "lookup" not in forward
        # the redundant type filter is dropped on resolve; the reverse sibling stays
        assert all(f.get("key") != "type" for f in new_query)
        assert any(
            f.get("key") == ["vlacc:1|properties.ref_related_works.value"]
            for f in new_query
        )

    def test_type_filter_dropped_only_when_resolved(self):
        resource = self._make_resource()
        # no marked lookup -> nothing resolved -> type filter preserved
        query = [
            {"type": "selection", "key": "type", "value": ["work_word"], "match_exact": True},
            {"type": "selection", "key": ["vlacc:1|properties.ref_related_works.value"], "value": "W-CUR"},
        ]
        new_query, did_resolve = resource._resolve_source_relation_lookups(query)
        assert did_resolve is False
        assert any(f.get("key") == "type" for f in new_query)

    def test_source_entity_looked_up_by_id_or_identifiers(self):
        mock_storage = MagicMock()
        find = mock_storage.db.__getitem__.return_value.find
        find.return_value = []
        with patch("resources.base_filter_resource.StorageManager") as mock_sm:
            mock_sm.return_value.get_db_engine.return_value = mock_storage
            resource = self._make_resource()
            resource._resolve_source_relation_lookups([self._forward_lookup_filter("W-CUR")])

        query_arg = find.call_args[0][0]
        assert query_arg == {
            "$or": [
                {"_id": {"$in": ["W-CUR"]}},
                {"identifiers": {"$in": ["W-CUR"]}},
            ]
        }
        # source collection comes from the lookup config, not the queried collection
        assert mock_storage.db.__getitem__.call_args[0][0] == "bibliographic_entities_actual"

    def test_no_source_doc_yields_empty_selection(self):
        mock_storage = MagicMock()
        mock_storage.db.__getitem__.return_value.find.return_value = []
        with patch("resources.base_filter_resource.StorageManager") as mock_sm:
            mock_sm.return_value.get_db_engine.return_value = mock_storage
            resource = self._make_resource()
            new_query, did_resolve = resource._resolve_source_relation_lookups(
                [self._forward_lookup_filter()]
            )

        assert did_resolve is True
        assert new_query[0]["key"] == ["vlacc:1|id"]
        assert new_query[0]["value"] == []

    def test_unmarked_lookup_left_untouched(self):
        resource = self._make_resource()
        query = [
            {
                "type": "selection",
                "key": ["x"],
                "value": "v",
                "lookup": {
                    "from": "c",
                    "local_field": "id",
                    "foreign_field": "f",
                    "as": "a",
                },
            }
        ]
        new_query, did_resolve = resource._resolve_source_relation_lookups(query)

        assert did_resolve is False
        assert new_query == query

    def test_query_without_lookup_is_passthrough(self):
        resource = self._make_resource()
        query = [{"type": "selection", "key": "type", "value": ["work_word"]}]
        new_query, did_resolve = resource._resolve_source_relation_lookups(query)

        assert did_resolve is False
        assert new_query == query

    def test_extract_nested_values_descends_into_lists(self):
        resource = self._make_resource()
        doc = {"properties": {"ref_related_works": {"value": ["W-A", "W-B"]}}}
        assert resource._extract_nested_values(
            doc, "properties.ref_related_works.value"
        ) == ["W-A", "W-B"]
        # missing path → empty, no error
        assert resource._extract_nested_values({}, "a.b.c") == []
