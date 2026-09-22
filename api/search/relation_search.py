"""Relation-aware simple search.

A search token matches an entity's own metadata *or* one of its relations, and
tokens are ANDed. The relations are read exactly as they are stored — reference
ids — so nothing is copied into the search index and no value ever goes stale.

Both sides are expressed in a single Typesense ``filter_by``, which matches
tokens on text fields (``field:oorlog``) as well as membership on the reference
arrays (``field:[id1,id2]``). ``q`` cannot be OR-ed against ``filter_by``, so
putting the text side in the filter too is what makes one query enough.
"""

from re import findall

_TOKEN_PATTERN = r'"[^"]+"|\S+'

MAX_FILTER_OPS = 100


def tokenize(query):
    """Split a query into tokens, keeping a double-quoted phrase as one token."""
    if not query:
        return []
    tokens = [token.strip('"').strip() for token in findall(_TOKEN_PATTERN, query)]
    return [token for token in tokens if token]


def escape_filter_value(value):
    """Backtick-quote a value for ``filter_by``.

    Backticks are the only quoting ``filter_by`` offers, and a value cannot
    contain one, so an embedded backtick is dropped rather than escaped.
    """
    return "`" + value.replace("`", "") + "`"


def usable_fields(keys, available_fields):
    """Flatten schema-prefixed keys and keep only those the collection holds.

    ``filter_by`` rejects the whole query when it names a field the collection
    does not have, and a configured key that has never carried a value never
    locks a type in an ``auto`` schema. Order is preserved.
    """
    fields = []
    for key in keys:
        field = key.split("|")[-1].replace(".", "_")
        if field in available_fields and field not in fields:
            fields.append(field)
    return fields


def token_clause(token, text_fields, relation_fields, related_entity_ids):
    """Build ``(own metadata OR relation)`` for one token.

    ``related_entity_ids`` is empty when the token resolves to no related entity, or
    to so many that it no longer discriminates; the clause then stays text-only
    instead of widening the query.
    """
    value = escape_filter_value(token)
    branches = [f"{field}:{value}" for field in text_fields]
    if related_entity_ids:
        ids = "[" + ",".join(related_entity_ids) + "]"
        branches += [f"{field}:{ids}" for field in relation_fields]
    if not branches:
        return ""
    return "(" + " || ".join(branches) + ")"


def build_filter_by(clauses, base_filter=None):
    """AND the per-token clauses, behind an optional base filter (e.g. type)."""
    parts = [clause for clause in clauses if clause]
    if base_filter:
        parts.insert(0, base_filter)
    return " && ".join(parts)


def build_eval_sort(text_clauses):
    """Rank documents matching on their own fields above relation-only matches.

    ``q`` is ``*`` for these searches, so Typesense has no text relevance of its
    own to rank by.
    """
    if not text_clauses:
        return None
    return f"_eval({' || '.join(text_clauses)}):desc"


def filter_operations(comparison_count_, has_base_filter=False):
    """Operations Typesense counts for a filter of this many comparisons.

    Every field comparison and every boolean operator counts towards
    ``--filter-by-max-ops``, which gives ``2n - 1`` for ``n`` comparisons joined
    by ``||`` and ``&&``. A base filter is one more comparison.
    """
    total = comparison_count_ + (1 if has_base_filter else 0)
    return max(0, 2 * total - 1)


def fits_operation_budget(
    comparison_count_, has_base_filter=False, max_ops=MAX_FILTER_OPS
):
    """Whether a filter of this size is accepted by the server.

    At the default ``--filter-by-max-ops=100`` that is 50 comparisons, or 49
    alongside a base filter. Raising the server flag raises this ceiling.
    """
    return filter_operations(comparison_count_, has_base_filter) <= max_ops


def comparison_count(
    token_count, text_field_count, relation_field_count, resolved_tokens=0
):
    """Comparisons a relation-aware filter needs.

    Every token is compared against every text field; only a token that
    resolved to related entities also costs the relation fields.
    """
    return token_count * text_field_count + resolved_tokens * relation_field_count


def build_relation_search_params(
    value,
    text_keys,
    relation_keys,
    available_fields,
    resolve_ids,
    base_filter=None,
    max_ops=MAX_FILTER_OPS,
):
    """Typesense parameters for a relation-aware simple search.

    ``resolve_ids`` maps one token to the identifiers of the related entities
    whose label matches it: a list, ``[]`` when it matches none, or ``None``
    when it matches too many to discriminate. A token of the last kind says
    nothing about relations, and must not silently become a strict requirement
    on the entity's own metadata either, so it is left out of the AND.

    Returns ``None`` when the search cannot be served this way — nothing to
    search, no configured key present in the collection, every token
    undiscriminating, or a filter larger than the server accepts — and the
    caller then keeps the ordinary ``q`` based search.
    """
    tokens = tokenize(value)
    text_fields = usable_fields(text_keys, available_fields)
    relation_fields = usable_fields(relation_keys, available_fields)
    if not tokens or not (text_fields or relation_fields):
        return None

    active = []
    for token in tokens:
        ids = resolve_ids(token)
        if ids is not None:
            active.append((token, ids))
    if not active:
        return None

    resolved = sum(1 for _, ids in active if ids)
    if not fits_operation_budget(
        comparison_count(len(active), len(text_fields), len(relation_fields), resolved),
        has_base_filter=bool(base_filter),
        max_ops=max_ops,
    ):
        return None

    clauses = [
        token_clause(token, text_fields, relation_fields, ids)
        for token, ids in active
    ]
    text_clauses = [
        f"{field}:{escape_filter_value(token)}"
        for token, _ in active
        for field in text_fields
    ]
    return {
        "q": "*",
        "query_by": ",".join(text_fields) or "type",
        "filter_by": build_filter_by(clauses, base_filter=base_filter),
        "sort_by": build_eval_sort(text_clauses),
    }
