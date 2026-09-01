import pytest
from werkzeug.exceptions import BadRequest, Conflict

from resources.base.merge import assert_mergeable


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
