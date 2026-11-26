from filters_v2.helpers.base_helper import has_selection_filter_with_multiple_values

KEY = ["podiumnet:1|properties.is_venue.value"]


class TestHasSelectionFilterWithMultipleValues:
    def test_boolean_value_is_a_single_value(self):
        assert not has_selection_filter_with_multiple_values(
            [{"type": "selection", "key": KEY, "value": True, "match_exact": True}]
        )

    def test_number_value_is_a_single_value(self):
        assert not has_selection_filter_with_multiple_values(
            [{"type": "selection", "key": KEY, "value": 3, "match_exact": True}]
        )

    def test_list_with_several_values_is_multiple(self):
        assert has_selection_filter_with_multiple_values(
            [{"type": "selection", "key": KEY, "value": ["a", "b"]}]
        )
