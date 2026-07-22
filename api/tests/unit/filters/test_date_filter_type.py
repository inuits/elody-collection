"""
A bare date value (yyyy-mm-dd, no time-of-day) must match the whole
calendar day, not require an exact-instant equality no user can produce.
Run from the api/ dir:

    pytest tests/unit/filters/test_date_filter_type.py -vv
"""

from filters_v2.types.base_filter_type_query_generator import _as_day_range


class TestAsDayRange:
    def test_bare_date_becomes_day_range(self):
        day_range = _as_day_range("2026-07-14")
        assert day_range == {
            "min": "2026-07-14T00:00:00",
            "max": "2026-07-14T23:59:59.999999",
        }

    def test_full_timestamp_is_not_a_day_range(self):
        assert _as_day_range("2026-07-14T08:58:23.306000+00:00") is None

    def test_midnight_timestamp_is_not_a_day_range(self):
        # A bare date is the only explicit "date-only" signal - a real
        # instant that happens to land on midnight is still an instant.
        assert _as_day_range("2026-07-14T00:00:00+02:00") is None

    def test_non_string_value_is_not_a_day_range(self):
        assert _as_day_range({"min": "a", "max": "b"}) is None

    def test_unparsable_value_is_not_a_day_range(self):
        assert _as_day_range("*") is None
