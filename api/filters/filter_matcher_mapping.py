from abc import ABC
from filters.matchers.matchers import (
    IdMatcher,
    ExactMatcher,
    ContainsMatcher,
    MinMatcher,
    MaxMatcher,
    MinIncludedMatcher,
    MaxIncludedMatcher,
    InBetweenMatcher,
    AnyMatcher,
    NoneMatcher,
)


class FilterMatcherMapping(ABC):
    mapping = {
        "id": {
            "id": IdMatcher,
            "exact": ExactMatcher,
            "contains": ContainsMatcher,
        },
        "text": {
            "any": AnyMatcher,
            "none": NoneMatcher,
            "exact": ExactMatcher,
            "contains": ContainsMatcher,
        },
        "date": {
            "any": AnyMatcher,
            "none": NoneMatcher,
            "exact": ExactMatcher,
            "min": MinMatcher,
            "max": MaxMatcher,
            "in_between": InBetweenMatcher,
        },
        "number": {
            "any": AnyMatcher,
            "none": NoneMatcher,
            "exact": ExactMatcher,
            "min_included": MinIncludedMatcher,
            "max_included": MaxIncludedMatcher,
            "in_between": InBetweenMatcher,
        },
        "selection": {
            "any": AnyMatcher,
            "none": NoneMatcher,
            "exact": ExactMatcher,
        },
        "boolean": {
            "exact": ExactMatcher,
        },
        "relation": {
            "any": AnyMatcher,
        },
    }
