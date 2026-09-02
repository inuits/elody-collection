import pytest
from werkzeug.exceptions import BadRequest, Conflict

from resources.base.merge import (
    assert_mergeable,
    count_inbound_references,
    find_documents_referencing,
    inbound_reference_sources,
    repoint_inbound_references,
)

VICTIM = "PERS-victim"
SURVIVOR = "PERS-survivor"

AUTHORS = "properties.ref_authors.value"
SUBJECTS = "properties.ref_subjects.value"

SOURCES = [
    ("entities", AUTHORS),
    ("bibliographic_entities", AUTHORS),
    ("bibliographic_entities", SUBJECTS),
]


class TestAssertMergeable:
    def test_accepts_two_distinct_entities_of_one_type(self):
        assert_mergeable({"id": "A", "type": "person"}, {"id": "B", "type": "person"})

    def test_rejects_merging_an_entity_into_itself(self):
        with pytest.raises(BadRequest):
            assert_mergeable(
                {"id": "A", "type": "person"}, {"id": "A", "type": "person"}
            )

    def test_rejects_a_mixed_type_pair(self):
        with pytest.raises(Conflict):
            assert_mergeable({"id": "A", "type": "person"}, {"id": "B", "type": "work"})

    def test_rejects_two_subtypes_of_one_abstract_type(self):
        """Subtypes differ even when the abstract type matches, and their
        schemas are not interchangeable."""
        with pytest.raises(Conflict):
            assert_mergeable(
                {"id": "W-1", "type": "work_map"}, {"id": "W-2", "type": "work_music"}
            )

    def test_names_both_types_so_the_user_can_see_the_mismatch(self):
        with pytest.raises(Conflict, match="person.*work|work.*person"):
            assert_mergeable({"id": "A", "type": "person"}, {"id": "B", "type": "work"})


class FakeCollection:
    def __init__(self, documents):
        self.documents = documents

    def find(self, query):
        ((field, wanted),) = query.items()
        return [
            document
            for document in self.documents
            if wanted in _value_at(document, field)
        ]

    def distinct(self, key, query):
        return [document[key] for document in self.find(query)]


def _value_at(document, field):
    _, property, _ = field.split(".")
    value = document.get("properties", {}).get(property, {}).get("value")
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


class FakeStorage:
    def __init__(self, documents_per_collection=None):
        documents_per_collection = documents_per_collection or {}
        self.db = {
            collection: FakeCollection(documents_per_collection.get(collection, []))
            for collection in ("entities", "bibliographic_entities")
        }
        self.patches = []

    def patch_item_from_collection_v2(self, collection, item, content, spec):
        self.patches.append((collection, item, content, spec))


def fake_repointer(*, document, victim_id, survivor_id, **_):
    properties = {
        key: {
            "value": [
                survivor_id if id == victim_id else id for id in property["value"]
            ]
        }
        for key, property in document.get("properties", {}).items()
    }
    if properties == document.get("properties"):
        return None
    return {"properties": properties, "type": document["type"]}


def configuration_with(**crud):
    class FakeConfiguration:
        def crud(self):
            return crud

    class FakeMapper:
        def get(self, _document_type):
            return FakeConfiguration()

    return FakeMapper


@pytest.fixture
def declared_shape(monkeypatch):
    monkeypatch.setattr(
        "resources.base.merge.get_object_configuration_mapper",
        configuration_with(
            inbound_reference_sources=lambda **_: SOURCES,
            reference_repointer=fake_repointer,
        ),
    )


@pytest.fixture
def undeclared_shape(monkeypatch):
    monkeypatch.setattr(
        "resources.base.merge.get_object_configuration_mapper",
        configuration_with(collection="entities"),
    )


def work_referencing(victim_id, id="W-1", property="ref_authors"):
    return {
        "_id": id,
        "id": id,
        "type": "work_music",
        "schema": {"type": "vlacc", "version": 1},
        "properties": {property: {"value": [victim_id]}},
    }


@pytest.mark.usefixtures("declared_shape")
class TestInboundReferenceSources:
    def test_reads_the_sources_the_configuration_declares(self):
        assert inbound_reference_sources("person") == SOURCES


@pytest.mark.usefixtures("undeclared_shape")
class TestAnUndeclaredReferenceShape:
    def test_refuses_to_list_sources(self):
        with pytest.raises(NotImplementedError):
            inbound_reference_sources("person")

    def test_refuses_to_count(self):
        with pytest.raises(NotImplementedError):
            count_inbound_references(FakeStorage(), VICTIM, "person")

    def test_refuses_to_repoint(self):
        storage = FakeStorage({"bibliographic_entities": [work_referencing(VICTIM)]})

        with pytest.raises(NotImplementedError):
            repoint_inbound_references(storage, VICTIM, SURVIVOR, "person")

        assert storage.patches == []


@pytest.mark.usefixtures("declared_shape")
class TestFindDocumentsReferencing:
    def test_searches_every_declared_source(self):
        storage = FakeStorage(
            {
                "entities": [work_referencing(VICTIM, "PERS-1")],
                "bibliographic_entities": [work_referencing(VICTIM, "W-2")],
            }
        )

        found = list(find_documents_referencing(storage, VICTIM, "person"))

        assert sorted(document["id"] for _, document in found) == ["PERS-1", "W-2"]

    def test_reports_which_collection_each_document_came_from(self):
        storage = FakeStorage({"bibliographic_entities": [work_referencing(VICTIM)]})

        ((collection, _),) = find_documents_referencing(storage, VICTIM, "person")

        assert collection == "bibliographic_entities"

    def test_yields_a_document_once_when_two_properties_reference_the_entity(self):
        work = work_referencing(VICTIM)
        work["properties"]["ref_subjects"] = {"value": [VICTIM]}
        storage = FakeStorage({"bibliographic_entities": [work]})

        assert len(list(find_documents_referencing(storage, VICTIM, "person"))) == 1

    def test_finds_nothing_when_no_document_references_the_entity(self):
        storage = FakeStorage(
            {"bibliographic_entities": [work_referencing("PERS-other")]}
        )

        assert list(find_documents_referencing(storage, VICTIM, "person")) == []


@pytest.mark.usefixtures("declared_shape")
class TestCountInboundReferences:
    def test_counts_across_all_sources(self):
        storage = FakeStorage(
            {
                "entities": [work_referencing(VICTIM, "PERS-1")],
                "bibliographic_entities": [
                    work_referencing(VICTIM, "W-2"),
                    work_referencing(VICTIM, "W-3"),
                ],
            }
        )

        assert count_inbound_references(storage, VICTIM, "person") == 3

    def test_counts_a_document_once_when_it_references_twice(self):
        work = work_referencing(VICTIM)
        work["properties"]["ref_subjects"] = {"value": [VICTIM]}
        storage = FakeStorage({"bibliographic_entities": [work]})

        assert count_inbound_references(storage, VICTIM, "person") == 1

    def test_is_zero_when_nothing_points_at_the_entity(self):
        assert count_inbound_references(FakeStorage(), VICTIM, "person") == 0


@pytest.mark.usefixtures("declared_shape")
class TestRepointInboundReferences:
    def test_writes_what_the_client_rewriter_returns(self):
        storage = FakeStorage({"bibliographic_entities": [work_referencing(VICTIM)]})

        repointed = repoint_inbound_references(storage, VICTIM, SURVIVOR, "person")

        assert repointed == 1
        collection, item, content, spec = storage.patches[0]
        assert (collection, item["id"], spec) == (
            "bibliographic_entities",
            "W-1",
            "vlacc",
        )
        assert content["properties"]["ref_authors"]["value"] == [SURVIVOR]

    def test_patches_in_the_shape_the_document_is_stored_in(self):
        work = work_referencing(VICTIM)
        work["schema"] = {"type": "some_client", "version": 2}
        storage = FakeStorage({"bibliographic_entities": [work]})

        repoint_inbound_references(storage, VICTIM, SURVIVOR, "person")

        assert storage.patches[0][3] == "some_client"

    def test_skips_a_document_the_rewriter_declines(self):
        untouched = {
            "_id": "W-9",
            "id": "W-9",
            "type": "work_music",
            "schema": {"type": "vlacc", "version": 1},
            "properties": {},
        }
        storage = FakeStorage({"bibliographic_entities": [untouched]})
        storage.db["bibliographic_entities"].find = lambda _query: [untouched]

        assert repoint_inbound_references(storage, VICTIM, SURVIVOR, "person") == 0
        assert storage.patches == []

    def test_writes_nothing_when_there_is_nothing_to_repoint(self):
        storage = FakeStorage()

        assert repoint_inbound_references(storage, VICTIM, SURVIVOR, "person") == 0
        assert storage.patches == []

    def test_refuses_to_merge_an_entity_into_itself(self):
        storage = FakeStorage({"bibliographic_entities": [work_referencing(VICTIM)]})

        with pytest.raises(ValueError):
            repoint_inbound_references(storage, VICTIM, VICTIM, "person")

        assert storage.patches == []
