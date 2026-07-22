from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Type

from filters_v2.matchers.matchers import BaseMatcher


def _as_day_range(value) -> dict | None:
    """A bare date string (yyyy-mm-dd, no time-of-day) means "this calendar
    day", not "this exact instant" - the frontend only ever sends a bare
    date when its time picker is hidden, so this is an explicit signal
    rather than a guess."""
    if not isinstance(value, str):
        return None
    try:
        day = date.fromisoformat(value)
    except ValueError:
        return None
    day_start = datetime.combine(day, datetime.min.time())
    day_end = day_start + timedelta(days=1, microseconds=-1)
    return {"min": day_start.isoformat(), "max": day_end.isoformat()}


class BaseFilterTypeQueryGenerator(ABC):
    @abstractmethod
    def generate_query_for_text_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            match_exact=filter_criteria.get("match_exact"),
            match_not=filter_criteria.get("match_not"),
            inner_exact_matches=filter_criteria.get("inner_exact_matches", {}),
            regex=filter_criteria.get("regex", False),
            regex_options=filter_criteria.get("regex_options", ""),
        )

    @abstractmethod
    def generate_query_for_date_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        day_range = _as_day_range(filter_criteria["value"])
        if day_range:
            return self._apply_matchers(
                matchers,
                filter_criteria["key"],
                day_range,
                is_datetime_value=True,
            )
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            match_exact=True,
            is_datetime_value=True,
        )

    @abstractmethod
    def generate_query_for_number_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            match_exact=True,
            aggregation=filter_criteria.get("aggregation", ""),
        )

    @abstractmethod
    def generate_query_for_selection_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            match_exact=filter_criteria.get("match_exact"),
            match_not=filter_criteria.get("match_not"),
            inner_exact_matches=filter_criteria.get("inner_exact_matches", {}),
        )

    @abstractmethod
    def generate_query_for_boolean_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            match_exact=True,
        )

    @abstractmethod
    def generate_query_for_geo_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
        )

    @abstractmethod
    def generate_query_for_type_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            "type",
            filter_criteria["value"],
            match_exact=True,
        )

    def _apply_matchers(
        self,
        matchers: dict[str, Type[BaseMatcher]],
        key: str | list[str],
        value,
        **kwargs,
    ):
        for matcher in matchers.values():
            result = matcher().match(key, value, **kwargs)
            if result:
                return result
