from __future__ import annotations

from typing import Any, Literal, NotRequired, TypedDict, Unpack

# A plain key ("properties.title.value"), optionally prefixed with "?" to also
# match entities where the field is absent, or a list of schema-specific keys
# ("elody:1|properties.title.value").
KeyType = str | list[str]

Lookup = TypedDict(
    "Lookup",
    {
        "from": str,
        "local_field": str,
        "foreign_field": str,
        "as": str,
        "preserve_null_and_empty_arrays": NotRequired[bool],
    },
)


class Facet(TypedDict):
    key: str
    lookups: NotRequired[list[Lookup]]


# ---------------------------------------------------------------------------
# Keyword arguments accepted by the builders. Each filter type only gets the
# modifiers its query generator actually reads; anything else would be
# silently ignored by the engine.
# ---------------------------------------------------------------------------


class CommonKwargs(TypedDict, total=False):
    """Modifiers honoured for every filter type."""

    operator: Literal["and", "or"]
    lookup: Lookup
    distinct_by: str
    facets: list[Facet]


class StringKwargs(CommonKwargs, total=False):
    """Modifiers for text and selection filters."""

    inner_exact_matches: dict[str, Any]


class NumberKwargs(CommonKwargs, total=False):
    """Modifiers for number filters."""

    aggregation: Literal["size"]
    # With `lookup` + `aggregation`: use a plain $lookup instead of the
    # array-safe pipeline lookup (faster, but no array/scalar fallbacks).
    simple: bool


# ---------------------------------------------------------------------------
# Filter-type shapes (one per filter type)
# ---------------------------------------------------------------------------


class _Common(TypedDict, total=False):
    operator: Literal["and", "or"]
    lookup: Lookup
    distinct_by: str
    facets: list[Facet]


class _StringCommon(_Common, total=False):
    match_exact: bool
    match_not: bool
    inner_exact_matches: dict[str, Any]


class TextFilterDict(_StringCommon):
    type: Literal["text"]
    key: KeyType
    value: str
    regex: NotRequired[bool]
    regex_options: NotRequired[str]


class NumberRangeDict(TypedDict, total=False):
    min: float
    max: float
    included: bool


class NumberFilterDict(_Common):
    type: Literal["number"]
    key: KeyType
    value: float | int | NumberRangeDict
    aggregation: NotRequired[Literal["size"]]
    simple: NotRequired[bool]


class DateRangeDict(TypedDict, total=False):
    min: str
    max: str
    included: bool


class DateFilterDict(_Common):
    type: Literal["date"]
    key: KeyType
    value: str | DateRangeDict


class SelectionFilterDict(_StringCommon):
    type: Literal["selection"]
    key: KeyType
    value: str | list[str]


class BooleanFilterDict(_Common):
    type: Literal["boolean"]
    key: KeyType
    value: bool


class GeoFilterDict(_Common):
    type: Literal["geo"]
    key: KeyType
    value: dict[str, Any]  # GeoJSON geometry
    bucket: NotRequired[int]


class TypeFilterDict(_Common):
    type: Literal["type"]
    value: str


Criterion = (
    TextFilterDict
    | NumberFilterDict
    | DateFilterDict
    | SelectionFilterDict
    | BooleanFilterDict
    | GeoFilterDict
    | TypeFilterDict
)


def _reject_wildcard_or_empty(value: str, builder: str) -> None:
    # "*" and "" are routed to the any/none matchers before `match_not` is
    # considered, which would turn a "does not contain" into its opposite.
    if value.strip() in ("*", ""):
        raise ValueError(
            f"{builder}() does not accept {value!r}; use the *_any/*_none builders"
        )


# ---------------------------------------------------------------------------
# text
# ---------------------------------------------------------------------------


def text_any(key: KeyType, **common: Unpack[StringKwargs]) -> TextFilterDict:
    """Field has a non-empty value."""
    return {"type": "text", "key": key, "value": "*", **common}


def text_none(key: KeyType, **common: Unpack[CommonKwargs]) -> TextFilterDict:
    """Field is empty or absent."""
    return {"type": "text", "key": key, "value": "", **common}


def text_exact(
    key: KeyType, value: str, **common: Unpack[StringKwargs]
) -> TextFilterDict:
    """Exact (case-sensitive) match. Note that "*" and "" are still treated
    as any/none."""
    return {"type": "text", "key": key, "value": value, "match_exact": True, **common}


def text_contains(
    key: KeyType, value: str, **common: Unpack[StringKwargs]
) -> TextFilterDict:
    """Case-insensitive match on a stripped value. `*` acts as a wildcard and
    `^` / `$` as anchors; other regex characters are escaped. "*" alone means
    any, "" means none."""
    return {"type": "text", "key": key, "value": value, **common}


def text_contains_not(
    key: KeyType, value: str, **common: Unpack[StringKwargs]
) -> TextFilterDict:
    """Field does NOT contain the value (case-insensitive, same wildcard and
    anchor rules as `text_contains`). Raises ValueError for "*" or ""."""
    _reject_wildcard_or_empty(value, "text_contains_not")
    return {"type": "text", "key": key, "value": value, "match_not": True, **common}


def text_regex(
    key: KeyType, pattern: str, options: str = "", **common: Unpack[StringKwargs]
) -> TextFilterDict:
    """Regular expression match, passed to Mongo as-is. `options`, e.g. "i"
    for case-insensitive."""
    return {
        "type": "text",
        "key": key,
        "value": pattern,
        "regex": True,
        "regex_options": options,
        **common,
    }


# ---------------------------------------------------------------------------
# date
#
# Values are ISO 8601 strings. A bare date ("2022-04-14") is only expanded to
# the whole calendar day by `date_exact`; as a range bound it means midnight,
# so `date_max_included(k, "2022-04-14")` excludes the rest of that day.
# ---------------------------------------------------------------------------


def date_any(key: KeyType, **common: Unpack[CommonKwargs]) -> DateFilterDict:
    """Field has a non-empty value."""
    return {"type": "date", "key": key, "value": "*", **common}


def date_none(key: KeyType, **common: Unpack[CommonKwargs]) -> DateFilterDict:
    """Field is empty or absent."""
    return {"type": "date", "key": key, "value": "", **common}


def date_exact(
    key: KeyType, value: str, **common: Unpack[CommonKwargs]
) -> DateFilterDict:
    """`value` is a full timestamp ("2022-04-14T15:00:00") or a bare date
    ("2022-04-14"), interpreted as that whole calendar day."""
    return {"type": "date", "key": key, "value": value, **common}


def date_min_included(
    key: KeyType, value_min: str, **common: Unpack[CommonKwargs]
) -> DateFilterDict:
    """Value is >= min (inclusive). A bare date means midnight."""
    return {
        "type": "date",
        "key": key,
        "value": {"min": value_min, "included": True},
        **common,
    }


def date_max_included(
    key: KeyType, value_max: str, **common: Unpack[CommonKwargs]
) -> DateFilterDict:
    """Value is <= max (inclusive). A bare date means midnight, so pass
    "2022-04-14T23:59:59.999999" to include the whole day."""
    return {
        "type": "date",
        "key": key,
        "value": {"max": value_max, "included": True},
        **common,
    }


def date_in_between(
    key: KeyType, value_min: str, value_max: str, **common: Unpack[CommonKwargs]
) -> DateFilterDict:
    """Value is between min and max (both inclusive). Bare dates mean
    midnight."""
    return {
        "type": "date",
        "key": key,
        "value": {"min": value_min, "max": value_max},
        **common,
    }


# ---------------------------------------------------------------------------
# number
# ---------------------------------------------------------------------------


def number_exact(
    key: KeyType, value: float, **common: Unpack[NumberKwargs]
) -> NumberFilterDict:
    """Value is exactly equal. With `aggregation="size"`, compares the
    length of the array at `key`."""
    return {"type": "number", "key": key, "value": value, **common}


def number_min_included(
    key: KeyType, value_min: float, **common: Unpack[NumberKwargs]
) -> NumberFilterDict:
    """Value is >= min (inclusive)."""
    return {
        "type": "number",
        "key": key,
        "value": {"min": value_min, "included": True},
        **common,
    }


def number_max_included(
    key: KeyType, value_max: float, **common: Unpack[NumberKwargs]
) -> NumberFilterDict:
    """Value is <= max (inclusive)."""
    return {
        "type": "number",
        "key": key,
        "value": {"max": value_max, "included": True},
        **common,
    }


def number_in_between(
    key: KeyType, value_min: float, value_max: float, **common: Unpack[NumberKwargs]
) -> NumberFilterDict:
    """Value is between min and max (both inclusive)."""
    return {
        "type": "number",
        "key": key,
        "value": {"min": value_min, "max": value_max},
        **common,
    }


# ---------------------------------------------------------------------------
# selection
# ---------------------------------------------------------------------------


def selection_any(key: KeyType, **common: Unpack[StringKwargs]) -> SelectionFilterDict:
    """Field has a non-empty value."""
    return {"type": "selection", "key": key, "value": "*", **common}


def selection_none(key: KeyType, **common: Unpack[CommonKwargs]) -> SelectionFilterDict:
    """Field is empty or absent."""
    return {"type": "selection", "key": key, "value": "", **common}


def selection_exact(
    key: KeyType, values: str | list[str], **common: Unpack[StringKwargs]
) -> SelectionFilterDict:
    """Field equals any of the given values. Also the recommended matcher
    for IDs/identifiers — a list of values is an OR of exact matches. A
    single string is wrapped in a list."""
    return {
        "type": "selection",
        "key": key,
        "value": [values] if isinstance(values, str) else values,
        "match_exact": True,
        **common,
    }


def selection_contains(
    key: KeyType, value: str, **common: Unpack[StringKwargs]
) -> SelectionFilterDict:
    """Case-insensitive match, same wildcard and anchor rules as
    `text_contains`."""
    return {"type": "selection", "key": key, "value": value, **common}


def selection_contains_not(
    key: KeyType, value: str, **common: Unpack[StringKwargs]
) -> SelectionFilterDict:
    """Field does NOT contain the value (case-insensitive). Raises ValueError
    for "*" or ""."""
    _reject_wildcard_or_empty(value, "selection_contains_not")
    return {
        "type": "selection",
        "key": key,
        "value": value,
        "match_not": True,
        **common,
    }


# ---------------------------------------------------------------------------
# boolean
# ---------------------------------------------------------------------------


def boolean_exact(
    key: KeyType, value: bool, **common: Unpack[CommonKwargs]
) -> BooleanFilterDict:
    """Field equals value exactly. A `False` value is treated as optional
    automatically by the API (also matches entities where the field is
    absent); in that case `operator` is overridden."""
    return {"type": "boolean", "key": key, "value": value, **common}


# ---------------------------------------------------------------------------
# geo
# ---------------------------------------------------------------------------


def geo(
    key: KeyType,
    geometry: dict[str, Any],
    bucket: int | None = None,
    **common: Unpack[CommonKwargs],
) -> GeoFilterDict:
    """Matches entities whose point lies inside the GeoJSON geometry.
    `geo` is the only matcher for this filter type, so there's no
    per-matcher suffix.

    `bucket` clusters the results for a map: the polygon's longitude span is
    split into `bucket` columns (rows are scaled by latitude), and each grid
    cell returns its first entity with a `bucket_count` and a `location`
    point at the cell's average position. Requires a Polygon geometry; sort,
    skip and limit are not applied."""
    result: GeoFilterDict = {"type": "geo", "key": key, "value": geometry, **common}
    if bucket is not None:
        result["bucket"] = bucket
    return result


# ---------------------------------------------------------------------------
# type
# ---------------------------------------------------------------------------


def type_exact(value: str, **common: Unpack[CommonKwargs]) -> TypeFilterDict:
    """Restrict the search to one entity type and select its object
    configuration. Without this or a `selection_exact("type", ...)`
    criterion the collection's configuration is used, and endpoints that
    require a type return 400."""
    return {"type": "type", "value": value, **common}
