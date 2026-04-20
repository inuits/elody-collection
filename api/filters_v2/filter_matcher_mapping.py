from abc import ABC
from filters_v2.matchers.matchers import (
    AnyMatcher,
    ContainsMatcher,
    ContainsNotMatcher,
    ExactMatcher,
    GeoMatcher,
    InBetweenMatcher,
    MaxIncludedMatcher,
    MinIncludedMatcher,
    NoneMatcher,
    RegexMatcher,
)


class FilterMatcherMapping(ABC):
    mapping = {
        "text": {
            "any": AnyMatcher,
            "none": NoneMatcher,
            "exact": ExactMatcher,
            "contains": ContainsMatcher,
            "contains_not": ContainsNotMatcher,
            "regex": RegexMatcher,
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
            "contains_not": ContainsNotMatcher,
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
