import random

import pytest
from werkzeug.exceptions import BadRequest

from resources.base.merge_evaluation import (
    STATUS_INVALID,
    STATUS_UNKNOWN,
    STATUS_VALID,
    attach_merge_evaluation,
    evaluate_identifier_integrity,
    evaluate_merge_candidate,
    expected_generated_id,
    immutable_properties,
    survivor_evaluator,
)

CANONICAL = "NO-2IL216T37"
STALE = "NO-03K2TBT98"


def generate_id(prefix, seed=""):
    """A copy of VlaccConfiguration._generate_id."""
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    size = 11 - len(prefix)
    if seed:
        random.seed(seed.lower())
        return f"{prefix}-{''.join(random.choices(alphabet, k=size))}"
    return f"{prefix}-{''.join(random.choices(alphabet, k=size))}"


def nomen_preparer(post_body, **_):
    properties = post_body.get("properties", {})
    title = properties.get("title", {}).get("value", "")
    audience = properties.get("audience_type", {}).get("value", "")
    post_body["id"] = post_body.get("id") or generate_id("NO", f"{title}{audience}")
    post_body["identifiers"] = [post_body["id"]]
    return post_body


def unseeded_preparer(post_body, **_):
    post_body["id"] = post_body.get("id") or generate_id("W")
    return post_body


def echoing_preparer(post_body, **_):
    return post_body


def nomen(id, title, audience="volwassenen"):
    return {
        "_id": id,
        "id": id,
        "identifiers": [id],
        "type": "nomen",
        "schema": {"type": "vlacc", "version": 1},
        "properties": {
            "title": {"value": title},
            "audience_type": {"value": audience},
        },
    }


def configuration_with(**crud):
    class FakeConfiguration:
        def crud(self):
            return crud

    class FakeMapper:
        def get(self, _document_type):
            return FakeConfiguration()

    return FakeMapper


@pytest.fixture
def configuration(monkeypatch):
    def use(**crud):
        monkeypatch.setattr(
            "resources.base.merge_evaluation.get_object_configuration_mapper",
            configuration_with(**crud),
        )

    return use


class TestExpectedGeneratedId:
    def test_reproduces_the_id_the_current_seeding_produces(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        assert expected_generated_id(nomen(STALE, "Digitalisering")) == CANONICAL

    def test_ignores_the_stored_id_so_it_reports_what_the_content_deserves(
        self, configuration
    ):
        configuration(creation_preparer=nomen_preparer)

        assert expected_generated_id(nomen(STALE, "Digitalisering")) != STALE

    def test_is_blind_to_the_title_casing_the_seed_lowercases(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        assert expected_generated_id(nomen(STALE, "Digitalisering")) == (
            expected_generated_id(nomen(CANONICAL, "digitalisering"))
        )

    def test_reports_nothing_when_the_generator_is_not_seeded_on_content(
        self, configuration
    ):
        configuration(creation_preparer=unseeded_preparer)

        assert expected_generated_id(nomen("W-1", "irrelevant")) is None

    def test_reports_nothing_when_the_configuration_generates_no_id(
        self, configuration
    ):
        configuration(creation_preparer=echoing_preparer)

        assert expected_generated_id(nomen(STALE, "Digitalisering")) is None

    def test_leaves_the_document_it_was_given_untouched(self, configuration):
        configuration(creation_preparer=nomen_preparer)
        document = nomen(STALE, "Digitalisering")

        expected_generated_id(document)

        assert document == nomen(STALE, "Digitalisering")


class TestEvaluateIdentifierIntegrity:
    def test_a_document_holding_its_generated_id_is_valid(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        evaluation = evaluate_identifier_integrity(nomen(CANONICAL, "digitalisering"))

        assert evaluation["status"] == STATUS_VALID
        assert evaluation["details"]["expected_id"] == CANONICAL

    def test_a_document_holding_a_stale_id_is_invalid(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        evaluation = evaluate_identifier_integrity(nomen(STALE, "Digitalisering"))

        assert evaluation["status"] == STATUS_INVALID

    def test_an_invalid_document_names_the_id_it_should_have_had(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        evaluation = evaluate_identifier_integrity(nomen(STALE, "Digitalisering"))

        assert evaluation["details"]["expected_id"] == CANONICAL

    def test_an_unseeded_id_is_unknown_rather_than_invalid(self, configuration):
        configuration(creation_preparer=unseeded_preparer)

        evaluation = evaluate_identifier_integrity(nomen("W-1", "irrelevant"))

        assert evaluation["status"] == STATUS_UNKNOWN

    def test_an_unknown_verdict_claims_no_expected_id(self, configuration):
        configuration(creation_preparer=unseeded_preparer)

        evaluation = evaluate_identifier_integrity(nomen("W-1", "irrelevant"))

        assert "expected_id" not in evaluation["details"]

    def test_only_a_valid_document_scores(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        assert evaluate_identifier_integrity(nomen(CANONICAL, "digitalisering"))[
            "score"
        ] > evaluate_identifier_integrity(nomen(STALE, "Digitalisering"))["score"]

    def test_an_unknown_verdict_does_not_score(self, configuration):
        configuration(creation_preparer=unseeded_preparer)

        assert evaluate_identifier_integrity(nomen("W-1", "irrelevant"))["score"] == 0


class TestSurvivorEvaluator:
    def test_identifier_integrity_needs_no_client_declaration(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        assert survivor_evaluator("nomen", "identifierIntegrity") is (
            evaluate_identifier_integrity
        )

    def test_a_client_declared_strategy_is_available(self, configuration):
        def prefers_everything(**_):
            return {"status": STATUS_VALID, "score": 5, "details": {}}

        configuration(merge_survivor_evaluators={"custom": prefers_everything})

        assert survivor_evaluator("nomen", "custom") is prefers_everything

    def test_a_client_may_replace_a_builtin_strategy(self, configuration):
        def stricter(**_):
            return {"status": STATUS_INVALID, "score": 0, "details": {}}

        configuration(merge_survivor_evaluators={"identifierIntegrity": stricter})

        assert survivor_evaluator("nomen", "identifierIntegrity") is stricter

    def test_an_unknown_strategy_is_refused(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        with pytest.raises(NotImplementedError, match="nonsense"):
            survivor_evaluator("nomen", "nonsense")

    def test_a_configuration_without_evaluators_still_offers_the_builtins(
        self, configuration
    ):
        configuration(collection="entities_actual")

        assert survivor_evaluator("nomen", "identifierIntegrity") is (
            evaluate_identifier_integrity
        )


class TestEvaluateMergeCandidate:
    def test_reports_which_strategy_produced_the_verdict(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        evaluation = evaluate_merge_candidate(
            nomen(CANONICAL, "digitalisering"), "identifierIntegrity"
        )

        assert evaluation["strategy"] == "identifierIntegrity"
        assert evaluation["status"] == STATUS_VALID

    def test_hands_the_document_to_a_client_declared_evaluator(self, configuration):
        seen = {}

        def recording(document, **kwargs):
            seen.update(document=document, kwargs=kwargs)
            return {"status": STATUS_VALID, "score": 1, "details": {}}

        configuration(merge_survivor_evaluators={"custom": recording})
        document = nomen(CANONICAL, "digitalisering")

        evaluate_merge_candidate(document, "custom")

        assert seen["document"] == document
        assert seen["kwargs"]["strategy"] == "custom"

    def test_an_unknown_strategy_is_refused(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        with pytest.raises(NotImplementedError):
            evaluate_merge_candidate(nomen(CANONICAL, "digitalisering"), "nonsense")


class TestAttachMergeEvaluation:
    def test_attaches_the_verdict_when_a_strategy_is_asked_for(self, configuration):
        configuration(creation_preparer=nomen_preparer)
        document = nomen(STALE, "Digitalisering")

        data = attach_merge_evaluation(
            {"id": STALE, "type": "nomen"}, document, "identifierIntegrity"
        )

        assert data["merge_evaluation"]["status"] == STATUS_INVALID

    def test_leaves_a_csv_or_rdf_payload_alone(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        payload = attach_merge_evaluation(
            "id,title\r\nNO-1,Digitalisering\r\n",
            nomen(STALE, "Digitalisering"),
            "identifierIntegrity",
        )

        assert payload == "id,title\r\nNO-1,Digitalisering\r\n"

    def test_an_unknown_strategy_is_a_bad_request_rather_than_a_crash(
        self, configuration
    ):
        configuration(creation_preparer=nomen_preparer)

        with pytest.raises(BadRequest, match="nonsense"):
            attach_merge_evaluation(
                {"id": STALE, "type": "nomen"}, nomen(STALE, "Digitalisering"), "nonsense"
            )


class TestImmutableProperties:
    def test_reports_what_the_configuration_declares(self, configuration):
        configuration(
            immutable_properties=lambda **_: [
                {"key": "title", "identity_value": "digitalisering"},
            ]
        )

        assert immutable_properties(nomen(STALE, "Digitalisering")) == [
            {"key": "title", "identity_value": "digitalisering"},
        ]

    def test_is_empty_when_a_configuration_declares_nothing(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        assert immutable_properties(nomen(STALE, "Digitalisering")) == []

    def test_hands_the_document_to_the_declaration(self, configuration):
        """The identity value depends on the stored value, not just the type."""
        seen = {}

        def record(document_type, document, **_):
            seen.update(document_type=document_type, document=document)
            return []

        configuration(immutable_properties=record)
        document = nomen(STALE, "Digitalisering")
        immutable_properties(document)

        assert seen["document_type"] == "nomen"
        assert seen["document"] == document

    def test_normalises_a_bare_key_into_the_declared_shape(self, configuration):
        """A property with no identity value bears no identity, so it may move."""
        configuration(immutable_properties=lambda **_: ["internal_memo"])

        assert immutable_properties(nomen(STALE, "Digitalisering")) == [
            {"key": "internal_memo", "identity_value": None},
        ]

    def test_fills_in_a_missing_identity_value(self, configuration):
        configuration(immutable_properties=lambda **_: [{"key": "internal_memo"}])

        assert immutable_properties(nomen(STALE, "Digitalisering")) == [
            {"key": "internal_memo", "identity_value": None},
        ]


class TestAttachedImmutableFields:
    def test_travels_alongside_the_verdict(self, configuration):
        configuration(
            creation_preparer=nomen_preparer,
            immutable_properties=lambda **_: [
                {"key": "title", "identity_value": "digitalisering"},
            ],
        )

        data = attach_merge_evaluation(
            {"id": STALE, "type": "nomen"},
            nomen(STALE, "Digitalisering"),
            "identifierIntegrity",
        )

        assert data["merge_evaluation"]["immutable_fields"] == [
            {"key": "title", "identity_value": "digitalisering"},
        ]

    def test_is_reported_even_when_nothing_is_immutable(self, configuration):
        configuration(creation_preparer=nomen_preparer)

        data = attach_merge_evaluation(
            {"id": STALE, "type": "nomen"},
            nomen(STALE, "Digitalisering"),
            "identifierIntegrity",
        )

        assert data["merge_evaluation"]["immutable_fields"] == []

    def test_does_not_let_a_strategy_overwrite_it(self, configuration):
        def sneaky(**_):
            return {
                "status": STATUS_VALID,
                "score": 1,
                "details": {},
                "immutable_fields": ["nonsense"],
            }

        configuration(
            merge_survivor_evaluators={"custom": sneaky},
            immutable_properties=lambda **_: [{"key": "title", "identity_value": "x"}],
        )

        evaluation = evaluate_merge_candidate(nomen(STALE, "Digitalisering"), "custom")

        assert evaluation["immutable_fields"] == [
            {"key": "title", "identity_value": "x"},
        ]
