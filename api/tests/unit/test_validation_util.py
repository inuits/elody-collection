"""Schemas come from the plugins, the base builds and compiles each variant once.

The same schema object is handed out on every request, so nothing may mutate
it: a change made while handling one request would leak into all later ones.
"""

from unittest.mock import patch

import pytest
from jsonschema import ValidationError
from plugins import CollectionApiPlugin
from werkzeug.exceptions import BadRequest

built = []


class Productions(CollectionApiPlugin):
    name = "productions"

    def validated_document_types(self):
        return ["production"]

    def build_schema(self, document_type, include_required):
        if document_type != "production":
            return None
        built.append((document_type, include_required))
        return {
            "type": "object",
            "properties": {"title": {"type": "string"}},
            "required": ["title"] if include_required else [],
            "additionalProperties": False,
        }


class Tags(CollectionApiPlugin):
    name = "tags"

    def validated_document_types(self):
        return ["tag"]

    def build_schema(self, document_type, include_required):
        if document_type not in ("tag", "production"):
            return None
        built.append((document_type, include_required))
        return {"type": "object", "title": f"{document_type} from tags"}


@pytest.fixture(autouse=True)
def plugins(monkeypatch):
    from validation.util import reset_schemas

    monkeypatch.setattr("plugins.get_plugins", lambda: [Productions(), Tags()])
    built.clear()
    reset_schemas()
    yield
    reset_schemas()


def fake_request(method="POST", endpoint="elody.genericobject"):
    return patch("validation.util.request", method=method, endpoint=endpoint)


class TestGetSchema:
    def test_returns_the_same_object_for_the_same_variant(self):
        from validation.util import get_schema

        schema = get_schema("production", include_required=True)

        assert schema is get_schema("production", include_required=True)
        assert built == [("production", True)]

    def test_full_and_partial_variants_differ_in_required_properties(self):
        from validation.util import get_schema

        assert get_schema("production", include_required=True)["required"] == ["title"]
        assert get_schema("production", include_required=False)["required"] == []

    @pytest.mark.parametrize(
        ("method", "endpoint", "expects_required"),
        [
            ("POST", "elody.genericobject", True),
            ("PUT", "elody.genericobject", True),
            ("PATCH", "elody.genericobject", False),
            ("PUT", "elody.elodydocumentrelations", False),
        ],
    )
    def test_picks_the_variant_from_the_request(
        self, method, endpoint, expects_required
    ):
        from validation.util import get_schema

        with fake_request(method, endpoint):
            schema = get_schema("production")

        assert schema is get_schema("production", include_required=expects_required)

    def test_the_first_plugin_with_a_schema_wins(self):
        from validation.util import get_schema

        assert get_schema("tag", include_required=True)["title"] == "tag from tags"
        assert "title" in get_schema("production", include_required=True)["properties"]

    @pytest.mark.parametrize("document_type", [None, "", "unknown"])
    def test_missing_or_unknown_document_types_are_bad_requests(self, document_type):
        from validation.util import get_schema

        with pytest.raises(BadRequest):
            get_schema(document_type, include_required=True)


class TestCompiledValidator:
    def test_compiles_each_schema_once(self):
        from validation.util import get_compiled_validator, get_schema

        schema = get_schema("production", include_required=True)

        assert get_compiled_validator(schema) is get_compiled_validator(schema)

    def test_raises_jsonschema_validation_errors(self):
        from validation.util import get_compiled_validator, get_schema

        validate = get_compiled_validator(
            get_schema("production", include_required=True)
        )

        with pytest.raises(ValidationError) as error:
            validate({"unknown": 1})
        assert error.value.schema_path


class TestWarmUp:
    def test_builds_both_variants_of_every_plugin_document_type(self):
        from validation.util import warm_up_validators

        warm_up_validators()

        assert sorted(built) == [
            ("production", False),
            ("production", True),
            ("tag", False),
            ("tag", True),
        ]

    def test_warmed_up_schemas_are_reused(self):
        from validation.util import get_schema, warm_up_validators

        warm_up_validators()
        get_schema("production", include_required=True)

        assert len(built) == 4
