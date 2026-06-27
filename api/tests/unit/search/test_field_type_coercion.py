"""Unit tests for Typesense field-type-aware value coercion.

Regression coverage for the bug where zizo entities (whose ``properties.code.value``
is an array) failed to index into the shared ``entities`` collection, because the
field was locked to a scalar ``string`` by works/manifestations. Coercing each
value to the field's existing cardinality keeps every document indexable.
"""

from unittest.mock import MagicMock, patch

import search.typesense_client as tc
from search.typesense_client import (
    _coerce_value_to_field_type,
    get_collection_field_types,
    prepare_document_for_typesense,
)


class TestCoerceValueToFieldType:
    def test_scalar_field_single_element_list_is_unwrapped(self):
        assert _coerce_value_to_field_type(["AHAAAA"], "string") == "AHAAAA"

    def test_scalar_field_multi_element_list_is_joined(self):
        assert (
            _coerce_value_to_field_type(["AHAAAA", "AHAADE"], "string")
            == "AHAAAA AHAADE"
        )

    def test_scalar_field_scalar_value_is_unchanged(self):
        assert _coerce_value_to_field_type("AHAAAA", "string") == "AHAAAA"

    def test_scalar_field_non_string_value_is_stringified(self):
        assert _coerce_value_to_field_type(42, "int32") == "42"

    def test_scalar_field_single_element_non_string_list_is_stringified(self):
        assert _coerce_value_to_field_type([42], "string") == "42"

    def test_array_field_scalar_is_wrapped(self):
        assert _coerce_value_to_field_type("PERS-X", "string[]") == ["PERS-X"]

    def test_array_field_list_is_preserved(self):
        assert _coerce_value_to_field_type(["PERS-X", "PERS-Y"], "string[]") == [
            "PERS-X",
            "PERS-Y",
        ]

    def test_array_field_stringifies_elements(self):
        assert _coerce_value_to_field_type([1, 2], "string[]") == ["1", "2"]


class TestPrepareDocumentWithFieldTypes:
    def _entity(self, **props):
        return {
            "_id": "ZIZO-OCDXH55",
            "type": "zizo",
            "properties": {k: {"value": v} for k, v in props.items()},
        }

    def test_zizo_single_code_array_collapsed_to_scalar_field(self):
        entity = self._entity(code=["AHAAAA"], title="Aarde")
        doc = prepare_document_for_typesense(
            entity,
            ["properties.code.value", "properties.title.value"],
            field_types={
                "properties_code_value": "string",
                "properties_title_value": "string",
            },
        )
        assert doc["properties_code_value"] == "AHAAAA"
        assert doc["properties_title_value"] == "Aarde"

    def test_zizo_multi_code_array_joined_for_scalar_field(self):
        entity = self._entity(code=["AHAAAA", "AHAADE"])
        doc = prepare_document_for_typesense(
            entity,
            ["properties.code.value"],
            field_types={"properties_code_value": "string"},
        )
        assert doc["properties_code_value"] == "AHAAAA AHAADE"

    def test_scalar_value_wrapped_for_array_field(self):
        entity = self._entity(ref_authors="PERS-X")
        doc = prepare_document_for_typesense(
            entity,
            ["properties.ref_authors.value"],
            field_types={"properties_ref_authors_value": "string[]"},
        )
        assert doc["properties_ref_authors_value"] == ["PERS-X"]

    def test_without_field_types_behaviour_is_unchanged(self):
        entity = self._entity(code=["AHAAAA"], title="Aarde")
        doc = prepare_document_for_typesense(
            entity, ["properties.code.value", "properties.title.value"]
        )
        # No coercion: list stays a list, scalar stays a scalar (legacy behaviour).
        assert doc["properties_code_value"] == ["AHAAAA"]
        assert doc["properties_title_value"] == "Aarde"

    def test_unknown_field_type_falls_back_to_legacy_behaviour(self):
        entity = self._entity(code=["AHAAAA"])
        doc = prepare_document_for_typesense(
            entity,
            ["properties.code.value"],
            field_types={"some_other_field": "string"},
        )
        assert doc["properties_code_value"] == ["AHAAAA"]


class TestGetCollectionFieldTypes:
    def setup_method(self):
        tc._field_types_cache.clear()

    def teardown_method(self):
        tc._field_types_cache.clear()

    def _client_with_schema(self, fields):
        client = MagicMock()
        client.collections.__getitem__.return_value.retrieve.return_value = {
            "fields": fields
        }
        return client

    def test_omits_wildcard_and_auto_fields(self):
        client = self._client_with_schema(
            [
                {"name": ".*", "type": "auto"},
                {"name": "properties_code_value", "type": "string"},
                {"name": "properties_ref_authors_value", "type": "string[]"},
                {"name": "some_auto_field", "type": "auto"},
            ]
        )
        with patch.object(tc, "get_typesense_client", return_value=client):
            field_types = get_collection_field_types("entities")
        assert field_types == {
            "properties_code_value": "string",
            "properties_ref_authors_value": "string[]",
        }

    def test_returns_empty_when_no_client(self):
        with patch.object(tc, "get_typesense_client", return_value=None):
            assert get_collection_field_types("entities") == {}

    def test_returns_empty_when_collection_missing(self):
        client = MagicMock()
        client.collections.__getitem__.return_value.retrieve.side_effect = Exception(
            "not found"
        )
        with patch.object(tc, "get_typesense_client", return_value=client):
            assert get_collection_field_types("entities") == {}

    def test_result_is_cached(self):
        client = self._client_with_schema(
            [{"name": "properties_code_value", "type": "string"}]
        )
        with patch.object(tc, "get_typesense_client", return_value=client) as gc:
            get_collection_field_types("entities")
            get_collection_field_types("entities")
        # Schema retrieved only once thanks to the cache.
        assert gc.call_count == 1
