from abc import ABC
from filters_v2.matchers.matchers import (
    ExactMatcher,
    ContainsMatcher,
    MinIncludedMatcher,
    MaxIncludedMatcher,
    InBetweenMatcher,
    AnyMatcher,
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
        "type": {
            "any": AnyMatcher,
            "none": NoneMatcher,
            "exact": ExactMatcher,
        },
    }
