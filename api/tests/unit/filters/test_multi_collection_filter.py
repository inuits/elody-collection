from unittest.mock import MagicMock, patch

import pytest


def make_results(prefix, count):
    return [{"_id": f"{prefix}_{i}"} for i in range(count)]


def make_search_result(prefix, total_count, returned_count):
    return {
        "results": make_results(prefix, returned_count),
        "count": total_count,
    }


@pytest.fixture
def filter_instance():
    with patch("resources.filter.get_object_configuration_mapper") as mock_mapper, patch(
        "resources.base_filter_resource.FilterManagerV2"
    ), patch(
        "resources.base_resource.StorageManager"
    ):
        from resources.filter import FilterGenericObjectsV2

        instance = FilterGenericObjectsV2()
        instance._execute_advanced_search_with_query_v2 = MagicMock()

        def make_config(collection_name):
            config = MagicMock()
            config.crud.return_value = {"collection": collection_name}
            return config

        def mapper_get(type_value):
            type_to_collection = {
                "type_a": "collection_a",
                "type_b": "collection_b",
                "type_c": "collection_a",
            }
            return make_config(type_to_collection.get(type_value, "entities"))

        mock_mapper.return_value.get = mapper_get

        yield instance


@pytest.fixture
def base_query():
    return [
        {"type": "type", "value": ["type_a", "type_b"]},
        {"type": "text", "key": "title", "value": "test"},
    ]


class TestMultiCollectionFilter:
    def test_basic_pagination_first_page(self, filter_instance, base_query, flask_app):
        with flask_app.test_request_context("/filter?skip=0&limit=10"):
            filter_instance._execute_advanced_search_with_query_v2.side_effect = [
                make_search_result("a", 25, 10),
                make_search_result("b", 50, 10),
            ]

            result = filter_instance._execute_multi_collection_filter(
                base_query, ["type_a", "type_b"]
            )

            assert result["count"] == 75
            assert len(result["results"]) == 10
            assert result["results"][0]["_id"] == "a_0"

    def test_skip_past_first_collection(self, filter_instance, base_query, flask_app):
        with flask_app.test_request_context("/filter?skip=30&limit=10"):
            filter_instance._execute_advanced_search_with_query_v2.side_effect = [
                make_search_result("a", 25, 25),
                make_search_result("b", 50, 40),
            ]

            result = filter_instance._execute_multi_collection_filter(
                base_query, ["type_a", "type_b"]
            )

            assert result["count"] == 75
            assert len(result["results"]) == 10
            assert result["results"][0]["_id"] == "b_5"

    def test_sub_queries_use_skip_0_and_limit_skip_plus_limit(
        self, filter_instance, base_query, flask_app
    ):
        with flask_app.test_request_context("/filter?skip=20&limit=10"):
            filter_instance._execute_advanced_search_with_query_v2.side_effect = [
                make_search_result("a", 25, 25),
                make_search_result("b", 50, 30),
            ]

            filter_instance._execute_multi_collection_filter(
                base_query, ["type_a", "type_b"]
            )

            calls = filter_instance._execute_advanced_search_with_query_v2.call_args_list
            assert len(calls) == 2
            for call in calls:
                assert call.kwargs.get("skip", call[1].get("skip")) == 0
                assert call.kwargs.get("limit", call[1].get("limit")) == 30

    def test_no_next_previous_in_result(self, filter_instance, base_query, flask_app):
        with flask_app.test_request_context("/filter?skip=10&limit=10"):
            result_with_nav = make_search_result("a", 25, 20)
            result_with_nav["next"] = "/filter?skip=20"
            result_with_nav["previous"] = "/filter?skip=0"

            filter_instance._execute_advanced_search_with_query_v2.side_effect = [
                result_with_nav,
                make_search_result("b", 10, 10),
            ]

            result = filter_instance._execute_multi_collection_filter(
                base_query, ["type_a", "type_b"]
            )

            assert "next" not in result
            assert "previous" not in result

    def test_same_collection_types_merged(self, filter_instance, flask_app):
        query = [
            {"type": "type", "value": ["type_a", "type_c"]},
            {"type": "text", "key": "title", "value": "test"},
        ]

        with flask_app.test_request_context("/filter?skip=0&limit=10"):
            filter_instance._execute_advanced_search_with_query_v2.side_effect = [
                make_search_result("a", 30, 10),
            ]

            result = filter_instance._execute_multi_collection_filter(
                query, ["type_a", "type_c"]
            )

            assert filter_instance._execute_advanced_search_with_query_v2.call_count == 1
            call_args = filter_instance._execute_advanced_search_with_query_v2.call_args
            query_arg = call_args[0][0]
            type_filter = next(f for f in query_arg if f.get("type") == "type")
            assert type_filter["value"] == ["type_a", "type_c"]

    def test_extra_filters_preserved_in_sub_queries(self, filter_instance, flask_app):
        query = [
            {"type": "type", "value": ["type_a", "type_b"]},
            {"type": "text", "key": "title", "value": "herman", "match_all_words": True},
            {"type": "selection", "key": "status", "value": ["published"]},
        ]

        with flask_app.test_request_context("/filter?skip=0&limit=10"):
            filter_instance._execute_advanced_search_with_query_v2.side_effect = [
                make_search_result("a", 5, 5),
                make_search_result("b", 3, 3),
            ]

            filter_instance._execute_multi_collection_filter(
                query, ["type_a", "type_b"]
            )

            calls = filter_instance._execute_advanced_search_with_query_v2.call_args_list
            for call in calls:
                sub_query = call[0][0]
                text_filter = next(f for f in sub_query if f.get("type") == "text")
                assert text_filter["key"] == "title"
                assert text_filter["value"] == "herman"
                assert text_filter["match_all_words"] is True
                selection_filter = next(
                    f for f in sub_query if f.get("type") == "selection"
                )
                assert selection_filter["key"] == "status"
                assert selection_filter["value"] == ["published"]

    def test_type_filter_split_per_collection(self, filter_instance, flask_app):
        query = [
            {"type": "type", "value": ["type_a", "type_b", "type_c"]},
            {"type": "text", "key": "title", "value": "test"},
        ]

        with flask_app.test_request_context("/filter?skip=0&limit=10"):
            filter_instance._execute_advanced_search_with_query_v2.side_effect = [
                make_search_result("a", 5, 5),
                make_search_result("b", 3, 3),
            ]

            filter_instance._execute_multi_collection_filter(
                query, ["type_a", "type_b", "type_c"]
            )

            calls = filter_instance._execute_advanced_search_with_query_v2.call_args_list
            assert len(calls) == 2

            # collection_a gets type_a and type_c
            query_a = calls[0][0][0]
            type_filter_a = next(f for f in query_a if f.get("type") == "type")
            assert set(type_filter_a["value"]) == {"type_a", "type_c"}
            assert calls[0][0][1] == "collection_a"

            # collection_b gets type_b
            query_b = calls[1][0][0]
            type_filter_b = next(f for f in query_b if f.get("type") == "type")
            assert type_filter_b["value"] == ["type_b"]
            assert calls[1][0][1] == "collection_b"

    def test_original_query_not_mutated(self, filter_instance, flask_app):
        query = [
            {"type": "type", "value": ["type_a", "type_b"]},
            {"type": "text", "key": "title", "value": "test"},
        ]
        original_type_value = query[0]["value"].copy()
        original_text_value = query[1]["value"]

        with flask_app.test_request_context("/filter?skip=0&limit=10"):
            filter_instance._execute_advanced_search_with_query_v2.side_effect = [
                make_search_result("a", 5, 5),
                make_search_result("b", 3, 3),
            ]

            filter_instance._execute_multi_collection_filter(
                query, ["type_a", "type_b"]
            )

            assert query[0]["value"] == original_type_value
            assert query[1]["value"] == original_text_value

    def test_empty_results(self, filter_instance, base_query, flask_app):
        with flask_app.test_request_context("/filter?skip=0&limit=10"):
            filter_instance._execute_advanced_search_with_query_v2.side_effect = [
                {"results": [], "count": 0},
                {"results": [], "count": 0},
            ]

            result = filter_instance._execute_multi_collection_filter(
                base_query, ["type_a", "type_b"]
            )

            assert result["count"] == 0
            assert result["results"] == []
