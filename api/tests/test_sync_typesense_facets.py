"""Unit tests for the pure schema-change logic of scripts/sync_typesense_facets.py.

The Typesense calls themselves are validated against a live collection; here we
only cover build_change, which decides the in-place schema update that flips a
field to a facet without losing its existing attributes.
"""

from scripts.sync_typesense_facets import (
    build_change,
    build_collection_change,
    field_is_up_to_date,
    merge_array_fields,
    merge_field_flags,
    merge_search_only_fields,
    wait_until_applied,
)


class TestBuildChange:
    def test_new_field_is_declared_optional_string_facet(self):
        change = build_change("properties_x_value", None)
        assert change == {
            "fields": [
                {
                    "name": "properties_x_value",
                    "type": "string",
                    "facet": True,
                    "optional": True,
                }
            ]
        }

    def test_existing_field_is_dropped_then_re_added_as_facet(self):
        current = {
            "name": "properties_material_type_value",
            "type": "string",
            "facet": False,
            "optional": True,
            "infix": True,
            "sort": False,
        }
        change = build_change("properties_material_type_value", current)

        # first drops the old declaration, then re-adds it
        assert change["fields"][0] == {
            "name": "properties_material_type_value",
            "drop": True,
        }
        readded = change["fields"][1]
        assert readded["name"] == "properties_material_type_value"
        assert readded["facet"] is True

    def test_existing_attributes_are_preserved(self):
        # infix search (and type) must survive the facet flip, not be reset.
        current = {
            "name": "f",
            "type": "string",
            "facet": False,
            "infix": True,
            "optional": True,
        }
        readded = build_change("f", current)["fields"][1]
        assert readded["type"] == "string"
        assert readded["infix"] is True
        assert readded["optional"] is True

    def test_unknown_attributes_are_not_carried_over(self):
        # transient/server-only keys must not be echoed back into the schema change.
        current = {"name": "f", "type": "string", "facet": False, "num_documents": 5}
        readded = build_change("f", current)["fields"][1]
        assert "num_documents" not in readded


class TestBuildChangeForSortFields:
    def test_new_field_is_declared_optional_sortable_string(self):
        change = build_change("audit_updated_at", None, {"sort": True})
        assert change == {
            "fields": [
                {
                    "name": "audit_updated_at",
                    "type": "string",
                    "sort": True,
                    "optional": True,
                }
            ]
        }

    def test_existing_field_is_re_added_as_sortable_keeping_its_facet(self):
        current = {
            "name": "properties_title_value",
            "type": "string",
            "facet": True,
            "sort": False,
            "optional": True,
        }
        change = build_change("properties_title_value", current, {"sort": True})

        assert change["fields"][0] == {"name": "properties_title_value", "drop": True}
        readded = change["fields"][1]
        assert readded["sort"] is True
        assert readded["facet"] is True
        assert readded["type"] == "string"

    def test_array_field_cannot_be_made_sortable(self):
        current = {"name": "f", "type": "string[]", "sort": False, "optional": True}
        assert build_change("f", current, {"sort": True}) is None


class TestFieldIsUpToDate:
    def test_missing_flag_is_not_up_to_date(self):
        assert not field_is_up_to_date({"facet": True, "sort": False}, {"sort": True})

    def test_all_flags_present_is_up_to_date(self):
        assert field_is_up_to_date(
            {"facet": True, "sort": True}, {"facet": True, "sort": True}
        )

    def test_absent_field_is_not_up_to_date(self):
        assert not field_is_up_to_date(None, {"sort": True})


class TestMergeFieldFlags:
    def test_facet_and_sort_fields_are_merged_per_collection(self):
        configs = [
            {
                "enabled": True,
                "collection": "entities",
                "facet_fields": ["type"],
                "sort_fields": ["type", "audit.updated.at"],
            },
            {"enabled": False, "collection": "entities", "sort_fields": ["x"]},
            {
                "enabled": True,
                "collection": "entities",
                "sort_fields": ["properties.title.value"],
            },
        ]
        assert merge_field_flags(configs) == {
            "entities": {
                "type": {"facet": True, "sort": True},
                "audit_updated_at": {"sort": True},
                "properties_title_value": {"sort": True},
            }
        }

    def test_search_fields_become_facets_for_distinct_by_grouping(self):
        configs = [
            {
                "enabled": True,
                "collection": "entities",
                "facet_fields": ["type"],
                "search_fields": ["properties.name.value", "type"],
                "sort_fields": ["properties.name.value"],
            }
        ]
        assert merge_field_flags(configs) == {
            "entities": {
                "type": {"facet": True},
                "properties_name_value": {"facet": True, "sort": True},
            }
        }


class TestMergeArrayFields:
    def test_array_fields_are_merged_per_collection(self):
        configs = [
            {
                "enabled": True,
                "collection": "entities",
                "array_fields": ["properties.isbn_group.value.isbn"],
            },
            {"enabled": False, "collection": "entities", "array_fields": ["x"]},
            {"enabled": True, "array_fields": ["properties.ean_group.value.ean"]},
        ]
        assert merge_array_fields(configs) == {
            "entities": {
                "properties_isbn_group_value_isbn",
                "properties_ean_group_value_ean",
            }
        }


class TestBuildChangeArrayField:
    def test_new_array_field_is_declared_as_string_array_facet(self):
        change = build_change("properties_isbn_value", None, array=True)
        assert change == {
            "fields": [
                {
                    "name": "properties_isbn_value",
                    "type": "string[]",
                    "facet": True,
                    "optional": True,
                }
            ]
        }

    def test_new_array_field_cannot_be_made_sortable(self):
        assert (
            build_change("properties_isbn_value", None, {"sort": True}, array=True)
            is None
        )

    def test_collection_change_declares_absent_array_fields_as_arrays(self):
        change, _ = build_collection_change(
            {},
            {"properties_isbn_value": {"facet": True}},
            array_fields={"properties_isbn_value"},
        )
        assert change["fields"] == [
            {
                "name": "properties_isbn_value",
                "type": "string[]",
                "facet": True,
                "optional": True,
            }
        ]


class TestSearchOnlyFields:
    """A search field that no document carries yet has an unknown cardinality:
    declaring it as a string would reject later documents that hold a list. It
    is left to Typesense's auto detection and gets its facet on a later run."""

    def test_search_fields_without_facet_or_sort_config(self):
        configs = [
            {
                "enabled": True,
                "collection": "entities",
                "facet_fields": ["type"],
                "search_fields": ["type", "author_names", "properties.name.value"],
                "sort_fields": ["properties.name.value"],
            }
        ]
        assert merge_search_only_fields(configs) == {"entities": {"author_names"}}

    def test_absent_search_only_field_is_skipped(self):
        change, report = build_collection_change(
            {
                "properties_title_value": {
                    "name": "properties_title_value",
                    "type": "string",
                }
            },
            {
                "author_names": {"facet": True},
                "properties_title_value": {"facet": True},
            },
            search_only_fields={"author_names", "properties_title_value"},
        )

        assert change["fields"][-1]["name"] == "properties_title_value"
        assert report["absent"] == ["author_names"]
        assert report["change"] == ["properties_title_value"]


class TestBuildCollectionChange:
    """All fields of a collection go into one alter: Typesense walks every
    document once per alter, so one alter per field multiplies the run time."""

    def test_changes_are_combined_into_one_alter(self):
        schema_fields = {
            "audit_updated_at": {"name": "audit_updated_at", "type": "string"},
            "properties_title_value": {
                "name": "properties_title_value",
                "type": "string",
            },
        }
        change, report = build_collection_change(
            schema_fields,
            {
                "audit_updated_at": {"sort": True},
                "properties_title_value": {"sort": True},
                "properties_last_activity_at_value": {"sort": True},
            },
        )

        assert change["fields"] == [
            {"name": "audit_updated_at", "drop": True},
            {"name": "properties_title_value", "drop": True},
            {"name": "audit_updated_at", "type": "string", "sort": True},
            {
                "name": "properties_last_activity_at_value",
                "type": "string",
                "sort": True,
                "optional": True,
            },
            {"name": "properties_title_value", "type": "string", "sort": True},
        ]
        assert report == {
            "change": [
                "audit_updated_at",
                "properties_last_activity_at_value",
                "properties_title_value",
            ],
            "ok": [],
            "skip": [],
            "absent": [],
        }

    def test_up_to_date_and_array_fields_are_left_out(self):
        schema_fields = {
            "type": {"name": "type", "type": "string", "facet": True},
            "properties_isbn_value": {
                "name": "properties_isbn_value",
                "type": "string[]",
            },
        }
        change, report = build_collection_change(
            schema_fields,
            {"type": {"facet": True}, "properties_isbn_value": {"sort": True}},
        )

        assert change is None
        assert report == {
            "change": [],
            "ok": ["type"],
            "skip": ["properties_isbn_value"],
            "absent": [],
        }


class TestWaitUntilApplied:
    def _client(self, *schemas):
        from unittest.mock import MagicMock

        client = MagicMock()
        client.collections.__getitem__.return_value.retrieve.side_effect = [
            {"fields": fields} for fields in schemas
        ]
        return client

    def test_polls_until_every_flag_is_present(self):
        client = self._client(
            [{"name": "f", "type": "string", "sort": False}],
            [{"name": "f", "type": "string", "sort": True}],
        )

        pending = wait_until_applied(
            client, "entities", {"f": {"sort": True}}, interval=0, attempts=5
        )

        assert pending == []

    def test_reports_fields_still_pending_after_the_last_attempt(self):
        client = self._client(
            [{"name": "f", "type": "string"}], [{"name": "f", "type": "string"}]
        )

        pending = wait_until_applied(
            client, "entities", {"f": {"sort": True}}, interval=0, attempts=2
        )

        assert pending == ["f"]
