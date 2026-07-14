"""Unit tests for the pure schema-change logic of scripts/sync_typesense_facets.py.

The Typesense calls themselves are validated against a live collection; here we
only cover build_change, which decides the in-place schema update that flips a
field to a facet without losing its existing attributes.
"""

from scripts.sync_typesense_facets import build_change


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
