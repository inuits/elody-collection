from abc import ABC
from filters_v2.matchers.matchers import (
    AnyMatcher,
    ContainsMatcher,
    ExactMatcher,
    GeoMatcher,
    InBetweenMatcher,
    MaxIncludedMatcher,
    MinIncludedMatcher,
    NoneMatcher,
)


class FilterMatcherMapping(ABC):
    mapping = {
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
            "min_included": MinIncludedMatcher,
            "max_included": MaxIncludedMatcher,
            "in_between": InBetweenMatcher,
        },
        "number": {
            "exact": ExactMatcher,
            "min_included": MinIncludedMatcher,
            "max_included": MaxIncludedMatcher,
            "in_between": InBetweenMatcher,
        },
        "selection": {
            "any": AnyMatcher,
            "none": NoneMatcher,
            "exact": ExactMatcher,
            "contains": ContainsMatcher,
        },
        "boolean": {
            "any": AnyMatcher,
            "none": NoneMatcher,
            "exact": ExactMatcher,
        },
        "geo": {"geo": GeoMatcher},
        "type": {
            "any": AnyMatcher,
            "none": NoneMatcher,
            "exact": ExactMatcher,
        },
    }
