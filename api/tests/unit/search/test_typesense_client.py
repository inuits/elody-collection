"""Unit tests for typesense_client functions."""

from unittest.mock import MagicMock, patch

import search.typesense_client as tc
from search.typesense_client import (
    build_filter_by,
    build_type_filter,
    delete_document,
    get_nested_value,
    prepare_document_for_typesense,
    search,
    search_all_ids,
    upsert_document,
)


def _make_hit(doc_id):
    return {"document": {"_id": doc_id}}


def _make_search_result(ids, found):
    return {"hits": [_make_hit(i) for i in ids], "found": found}


class TestBuildTypeFilter:
    def test_empty_returns_none(self):
        assert build_type_filter([]) is None

    def test_none_returns_none(self):
        assert build_type_filter(None) is None

    def test_single_value_exact_match(self):
        assert build_type_filter(["work_word"]) == "type:=work_word"

    def test_multiple_values_array_syntax(self):
        assert build_type_filter(["work_word", "person"]) == "type:[work_word,person]"


class TestBuildFilterBy:
    def test_empty_returns_none(self):
        assert build_filter_by([], []) is None

    def test_type_only(self):
        assert build_filter_by(["person"], []) == "type:=person"

    def test_single_string_key_single_value(self):
        assert (
            build_filter_by([], [("properties_name_value", "Bach")])
            == "properties_name_value:=Bach"
        )

    def test_single_string_key_list_value(self):
        assert (
            build_filter_by([], [("properties_name_value", ["a", "b"])])
            == "properties_name_value:=[a,b]"
        )

    def test_value_with_space_is_backtick_quoted(self):
        assert (
            build_filter_by([], [("properties_name_value", "Bogaerts, Theo")])
            == "properties_name_value:=`Bogaerts, Theo`"
        )

    def test_multi_key_single_value_builds_or_group(self):
        result = build_filter_by(
            [], [(["properties_name_value", "properties_title_value"], "Bach")]
        )
        assert result == "(properties_name_value:=Bach || properties_title_value:=Bach)"

    def test_multi_key_list_value_builds_or_group(self):
        result = build_filter_by(
            [], [(["properties_name_value", "properties_title_value"], ["a", "b"])]
        )
        assert (
            result
            == "(properties_name_value:=[a,b] || properties_title_value:=[a,b])"
        )

    def test_multi_key_quotes_values_with_spaces(self):
        result = build_filter_by(
            [],
            [(["properties_name_value", "properties_title_value"], "Bogaerts, Theo")],
        )
        assert (
            result
            == "(properties_name_value:=`Bogaerts, Theo` || "
            "properties_title_value:=`Bogaerts, Theo`)"
        )

    def test_multi_key_combined_with_type_filter(self):
        result = build_filter_by(
            ["nomen", "person"],
            [(["properties_name_value", "properties_title_value"], "Bladmuziek")],
        )
        assert (
            result
            == "type:[nomen,person] && "
            "(properties_name_value:=Bladmuziek || properties_title_value:=Bladmuziek)"
        )

    def test_single_element_list_key_has_no_parens(self):
        result = build_filter_by([], [(["properties_name_value"], "Bach")])
        assert result == "properties_name_value:=Bach"

    def test_multi_key_empty_list_value_skipped(self):
        assert (
            build_filter_by(
                [], [(["properties_name_value", "properties_title_value"], [])]
            )
            is None
        )


class TestGetNestedValue:
    def test_single_key(self):
        assert get_nested_value({"name": "alice"}, "name") == "alice"

    def test_nested_key(self):
        obj = {"properties": {"name": {"value": "alice"}}}
        assert get_nested_value(obj, "properties.name.value") == "alice"

    def test_missing_key_returns_none(self):
        assert get_nested_value({"a": 1}, "b") is None

    def test_missing_nested_key_returns_none(self):
        assert get_nested_value({"a": {"b": 1}}, "a.c") is None

    def test_non_dict_intermediate_returns_none(self):
        assert get_nested_value({"a": "string"}, "a.b") is None

    def test_list_valued_property(self):
        obj = {"properties": {"title": [{"value": "My Production"}]}}
        assert get_nested_value(obj, "properties.title.value") == "My Production"

    def test_empty_list_returns_none(self):
        obj = {"properties": {"title": []}}
        assert get_nested_value(obj, "properties.title.value") is None

    def test_multi_valued_list_collects_all_elements(self):
        obj = {
            "properties": {
                "isbn_group": {
                    "value": [
                        {"isbn": "9789049202798"},
                        {"isbn": "9789049205416"},
                    ]
                }
            }
        }
        assert get_nested_value(obj, "properties.isbn_group.value.isbn") == [
            "9789049202798",
            "9789049205416",
        ]

    def test_multi_valued_list_skips_elements_missing_the_key(self):
        obj = {"tags": [{"name": "a"}, {"other": "x"}, {"name": "b"}]}
        assert get_nested_value(obj, "tags.name") == ["a", "b"]

    def test_multi_valued_list_of_scalars_at_leaf(self):
        obj = {"properties": {"ean": {"value": ["111", "222"]}}}
        assert get_nested_value(obj, "properties.ean.value") == ["111", "222"]

    def test_nested_lists_are_flattened(self):
        obj = {"groups": [{"codes": ["a", "b"]}, {"codes": ["c"]}]}
        assert get_nested_value(obj, "groups.codes") == ["a", "b", "c"]


class TestPrepareDocumentForTypesense:
    def test_basic_document(self):
        entity = {"_id": "ent-1", "type": "work_word"}
        result = prepare_document_for_typesense(entity, [])
        assert result == {"id": "ent-1", "_id": "ent-1", "type": "work_word"}

    def test_includes_search_fields(self):
        entity = {
            "_id": "ent-1",
            "type": "work_word",
            "properties": {"name": {"value": "alice"}},
        }
        result = prepare_document_for_typesense(entity, ["properties.name.value"])
        assert result["properties_name_value"] == "alice"

    def test_missing_field_excluded(self):
        entity = {"_id": "ent-1", "type": "work_word"}
        result = prepare_document_for_typesense(entity, ["properties.name.value"])
        assert "properties_name_value" not in result

    def test_non_string_value_converted(self):
        entity = {"_id": "ent-1", "type": "work_word", "count": 42}
        result = prepare_document_for_typesense(entity, ["count"])
        assert result["count"] == "42"

    def test_string_value_kept_as_is(self):
        entity = {"_id": "ent-1", "type": "work_word", "title": "hello"}
        result = prepare_document_for_typesense(entity, ["title"])
        assert result["title"] == "hello"

    def test_empty_type_defaults_to_empty_string(self):
        entity = {"_id": "ent-1"}
        result = prepare_document_for_typesense(entity, [])
        assert result["type"] == ""


class TestSearch:
    def test_returns_ids_and_count(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            ["a", "b"], 2
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            result = search("entities", "mars", "name")

        assert result == {"ids": ["a", "b"], "count": 2}

    def test_uses_offset_when_provided(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            [], 0
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            search("entities", "mars", "name", offset=20, per_page=10)

        params = (
            mock_client.collections.__getitem__.return_value.documents.search.call_args[
                0
            ][0]
        )
        assert params["offset"] == 20
        assert "page" not in params
        assert params["per_page"] == 10

    def test_uses_page_when_no_offset(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            [], 0
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            search("entities", "mars", "name", page=3)

        params = (
            mock_client.collections.__getitem__.return_value.documents.search.call_args[
                0
            ][0]
        )
        assert params["page"] == 3
        assert "offset" not in params

    def test_includes_filter_by(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            [], 0
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            search("entities", "mars", "name", filter_by="type:=work_word")

        params = (
            mock_client.collections.__getitem__.return_value.documents.search.call_args[
                0
            ][0]
        )
        assert params["filter_by"] == "type:=work_word"

    def test_no_filter_by_when_none(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            [], 0
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            search("entities", "mars", "name")

        params = (
            mock_client.collections.__getitem__.return_value.documents.search.call_args[
                0
            ][0]
        )
        assert "filter_by" not in params

    def test_returns_none_when_no_client(self):
        with patch.object(tc, "get_typesense_client", return_value=None):
            assert search("entities", "mars", "name") is None

    def test_returns_none_on_exception(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.side_effect = Exception(
            "connection error"
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            assert search("entities", "mars", "name") is None


class TestSearchAllIds:
    def test_single_page(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            ["a", "b", "c"], 3
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            result = search_all_ids("entities", "mars", "name")

        assert result == {"ids": ["a", "b", "c"], "count": 3}

    def test_paginates_multiple_pages(self):
        mock_client = MagicMock()
        page1_ids = [f"id{i}" for i in range(250)]
        page2_ids = [f"id{250+i}" for i in range(50)]

        mock_client.collections.__getitem__.return_value.documents.search.side_effect = [
            _make_search_result(page1_ids, 300),
            _make_search_result(page2_ids, 300),
        ]

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            result = search_all_ids("entities", "mars", "name")

        assert result["count"] == 300
        assert len(result["ids"]) == 300

    def test_stops_when_all_fetched(self):
        mock_client = MagicMock()
        mock_search = mock_client.collections.__getitem__.return_value.documents.search

        mock_search.side_effect = [
            _make_search_result([f"id{i}" for i in range(250)], 250),
        ]

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            result = search_all_ids("entities", "mars", "name")

        assert mock_search.call_count == 1
        assert result["count"] == 250

    def test_includes_filter_by(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            [], 0
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            search_all_ids("entities", "mars", "name", filter_by="type:=work_word")

        params = (
            mock_client.collections.__getitem__.return_value.documents.search.call_args[
                0
            ][0]
        )
        assert params["filter_by"] == "type:=work_word"

    def test_returns_none_when_no_client(self):
        with patch.object(tc, "get_typesense_client", return_value=None):
            assert search_all_ids("entities", "mars", "name") is None

    def test_returns_none_on_exception(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.side_effect = Exception(
            "timeout"
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            assert search_all_ids("entities", "mars", "name") is None


class TestUpsertDocument:
    def test_upserts_document(self):
        mock_client = MagicMock()
        doc = {"id": "ent-1", "_id": "ent-1", "type": "work_word"}

        with patch.object(
            tc, "get_typesense_client", return_value=mock_client
        ), patch.object(tc, "ensure_collection") as mock_ensure:
            result = upsert_document("entities", doc)

        assert result is True
        mock_ensure.assert_called_once_with("entities")
        mock_client.collections.__getitem__.return_value.documents.upsert.assert_called_once_with(
            doc
        )

    def test_skips_when_no_client(self):
        with patch.object(tc, "get_typesense_client", return_value=None):
            assert upsert_document("entities", {"id": "ent-1"}) is False

    def test_retries_transient_failure_then_succeeds(self):
        mock_client = MagicMock()
        upsert = mock_client.collections.__getitem__.return_value.documents.upsert
        upsert.side_effect = [Exception("timeout"), None]

        with patch.object(
            tc, "get_typesense_client", return_value=mock_client
        ), patch.object(tc, "ensure_collection"), patch.object(tc, "sleep"):
            result = upsert_document("entities", {"id": "ent-1"})

        assert result is True
        assert upsert.call_count == 2

    def test_returns_false_and_logs_error_after_exhausting_retries(self):
        mock_client = MagicMock()
        upsert = mock_client.collections.__getitem__.return_value.documents.upsert
        upsert.side_effect = Exception("write error")

        with patch.object(
            tc, "get_typesense_client", return_value=mock_client
        ), patch.object(tc, "ensure_collection"), patch.object(
            tc, "sleep"
        ), patch.object(tc, "log") as mock_log:
            result = upsert_document("entities", {"id": "ent-1"}, max_attempts=3)

        assert result is False
        assert upsert.call_count == 3
        mock_log.error.assert_called_once()


class TestDeleteDocument:
    def test_deletes_document(self):
        mock_client = MagicMock()

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            assert delete_document("entities", "ent-1") is True

        mock_client.collections.__getitem__.return_value.documents.__getitem__.return_value.delete.assert_called_once()

    def test_skips_when_no_client(self):
        with patch.object(tc, "get_typesense_client", return_value=None):
            assert delete_document("entities", "ent-1") is False

    def test_missing_document_counts_as_success(self):
        class ObjectNotFound(Exception):
            pass

        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.__getitem__.return_value.delete.side_effect = ObjectNotFound(
            "404"
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            assert delete_document("entities", "ent-1") is True

    def test_returns_false_and_logs_error_after_exhausting_retries(self):
        mock_client = MagicMock()
        delete = mock_client.collections.__getitem__.return_value.documents.__getitem__.return_value.delete
        delete.side_effect = Exception("write error")

        with patch.object(
            tc, "get_typesense_client", return_value=mock_client
        ), patch.object(tc, "sleep"), patch.object(tc, "log") as mock_log:
            assert delete_document("entities", "ent-1", max_attempts=3) is False

        assert delete.call_count == 3
        mock_log.error.assert_called_once()


class TestEnsureCollection:
    def test_creates_collection_when_not_exists(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.retrieve.side_effect = (
            Exception("404")
        )

        # Clear the cached set for this test
        tc._ensured_collections.discard("new_col")

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            tc.ensure_collection("new_col")

        mock_client.collections.create.assert_called_once_with(
            {"name": "new_col", "fields": [{"name": ".*", "type": "auto"}]}
        )

    def test_creates_collection_with_facet_fields(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.retrieve.side_effect = (
            Exception("404")
        )

        tc._ensured_collections.discard("facet_col")

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            tc.ensure_collection(
                "facet_col", facet_fields=["type", "properties.ref_genre.value"]
            )

        expected_fields = [
            {"name": ".*", "type": "auto"},
            {"name": "type", "type": "auto", "facet": True},
            {"name": "properties_ref_genre_value", "type": "auto", "facet": True},
        ]
        mock_client.collections.create.assert_called_once_with(
            {"name": "facet_col", "fields": expected_fields}
        )

        tc._ensured_collections.discard("facet_col")

        tc._ensured_collections.discard("new_col")

    def test_skips_when_already_exists(self):
        mock_client = MagicMock()

        tc._ensured_collections.discard("existing_col")

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            tc.ensure_collection("existing_col")

        mock_client.collections.__getitem__.return_value.retrieve.assert_called_once()
        mock_client.collections.create.assert_not_called()

        tc._ensured_collections.discard("existing_col")

    def test_caches_after_ensure(self):
        mock_client = MagicMock()

        tc._ensured_collections.discard("cached_col")

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            tc.ensure_collection("cached_col")
            tc.ensure_collection("cached_col")

        # retrieve should only be called once (second call is cached)
        assert mock_client.collections.__getitem__.return_value.retrieve.call_count == 1

        tc._ensured_collections.discard("cached_col")

    def test_skips_when_no_client(self):
        tc._ensured_collections.discard("no_client_col")

        with patch.object(tc, "get_typesense_client", return_value=None):
            tc.ensure_collection("no_client_col")

        assert "no_client_col" not in tc._ensured_collections


def _make_search_result_with_facets(ids, found, facet_counts):
    return {
        "hits": [_make_hit(i) for i in ids],
        "found": found,
        "facet_counts": facet_counts,
    }


class TestSearchWithFacets:
    def test_facet_by_passed_to_search_params(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            ["a"], 1
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            search("entities", "mars", "name", facet_by="type,properties_name_value")

        params = (
            mock_client.collections.__getitem__.return_value.documents.search.call_args[
                0
            ][0]
        )
        assert params["facet_by"] == "type,properties_name_value"

    def test_no_facet_by_when_not_provided(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            ["a"], 1
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            search("entities", "mars", "name")

        params = (
            mock_client.collections.__getitem__.return_value.documents.search.call_args[
                0
            ][0]
        )
        assert "facet_by" not in params

    def test_facets_transformed_to_mongo_format(self):
        mock_client = MagicMock()
        typesense_facets = [
            {
                "field_name": "type",
                "counts": [
                    {"value": "work_word", "count": 42},
                    {"value": "person", "count": 15},
                ],
            },
            {
                "field_name": "properties_name_value",
                "counts": [
                    {"value": "alice", "count": 3},
                ],
            },
        ]
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result_with_facets(
            ["a", "b"], 2, typesense_facets
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            result = search(
                "entities", "mars", "name", facet_by="type,properties_name_value"
            )

        assert result["facets"] == [
            {
                "type": [
                    {"_id": "work_word", "count": 42},
                    {"_id": "person", "count": 15},
                ]
            },
            {"properties_name_value": [{"_id": "alice", "count": 3}]},
        ]

    def test_facets_none_when_no_facet_by(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result(
            ["a"], 1
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            result = search("entities", "mars", "name")

        assert "facets" not in result

    def test_facets_empty_when_no_facet_counts(self):
        mock_client = MagicMock()
        mock_client.collections.__getitem__.return_value.documents.search.return_value = _make_search_result_with_facets(
            ["a"], 1, []
        )

        with patch.object(tc, "get_typesense_client", return_value=mock_client):
            result = search("entities", "mars", "name", facet_by="type")

        assert result["facets"] == []


class TestPrepareDocumentWithFacetFields:
    def test_includes_facet_fields(self):
        entity = {
            "_id": "ent-1",
            "type": "work_word",
            "properties": {"ref_genre": {"value": "fiction"}},
        }
        result = prepare_document_for_typesense(
            entity,
            ["properties.name.value"],
            facet_fields=["properties.ref_genre.value"],
        )
        assert result["properties_ref_genre_value"] == "fiction"

    def test_facet_fields_default_empty(self):
        entity = {"_id": "ent-1", "type": "work_word"}
        result = prepare_document_for_typesense(entity, [])
        assert result == {"id": "ent-1", "_id": "ent-1", "type": "work_word"}

    def test_overlapping_search_and_facet_fields(self):
        entity = {
            "_id": "ent-1",
            "type": "work_word",
            "properties": {"name": {"value": "alice"}},
        }
        result = prepare_document_for_typesense(
            entity, ["properties.name.value"], facet_fields=["properties.name.value"]
        )
        assert result["properties_name_value"] == "alice"

    def test_missing_facet_field_excluded(self):
        entity = {"_id": "ent-1", "type": "work_word"}
        result = prepare_document_for_typesense(
            entity, [], facet_fields=["properties.ref_genre.value"]
        )
        assert "properties_ref_genre_value" not in result
