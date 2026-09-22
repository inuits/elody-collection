"""A simple-search token matches the entity's own metadata OR one of its
relations, and tokens are ANDed.

Relations are read as stored (reference ids), so nothing is copied into the
index. Run from the api/ dir:

    pytest tests/unit/search/test_relation_search.py -vv
"""

from search.relation_search import (
    MAX_FILTER_OPS,
    build_relation_search_params,
    build_eval_sort,
    build_filter_by,
    comparison_count,
    escape_filter_value,
    filter_operations,
    fits_operation_budget,
    token_clause,
    tokenize,
    usable_fields,
)

TEXT = ["properties_title_value", "properties_summary_value"]
RELATIONS = ["properties_ref_authors_value", "properties_ref_subjects_value"]


class TestTokenize:
    def test_splits_on_whitespace(self):
        assert tokenize("oorlogsliteratuur oekraine") == [
            "oorlogsliteratuur",
            "oekraine",
        ]

    def test_keeps_a_quoted_phrase_as_one_token(self):
        assert tokenize('"Thurman, Uma" oorlog') == ["Thurman, Uma", "oorlog"]

    def test_drops_the_quotes_from_a_phrase(self):
        assert tokenize('"de oorlog"') == ["de oorlog"]

    def test_ignores_empty_input(self):
        assert tokenize("   ") == []
        assert tokenize("") == []

    def test_collapses_repeated_whitespace(self):
        assert tokenize("a   b") == ["a", "b"]


class TestEscapeFilterValue:
    def test_wraps_in_backticks(self):
        assert escape_filter_value("oorlog") == "`oorlog`"

    def test_a_comma_is_safe_inside_backticks(self):
        assert escape_filter_value("Thurman, Uma") == "`Thurman, Uma`"

    def test_strips_backticks_from_the_value(self):
        assert escape_filter_value("a`b") == "`ab`"


class TestUsableFields:
    def test_flattens_schema_prefixed_keys(self):
        assert usable_fields(
            ["vlacc:1|properties.title.value"], {"properties_title_value"}
        ) == ["properties_title_value"]

    def test_drops_a_key_the_collection_does_not_have(self):
        # filter_by errors on an unknown field, so an unlocked key must not
        # reach the query.
        assert usable_fields(
            ["vlacc:1|properties.title.value", "vlacc:1|properties.ghost.value"],
            {"properties_title_value"},
        ) == ["properties_title_value"]

    def test_keeps_the_configured_order_and_deduplicates(self):
        assert usable_fields(
            ["a.b", "c.d", "a.b"], {"a_b", "c_d"}
        ) == ["a_b", "c_d"]


class TestTokenClause:
    def test_ors_the_token_across_every_text_field(self):
        clause = token_clause("oorlog", TEXT, RELATIONS, [])

        assert clause == (
            "(properties_title_value:`oorlog` || properties_summary_value:`oorlog`)"
        )

    def test_adds_a_relation_branch_per_relation_field(self):
        clause = token_clause("rowling", TEXT, RELATIONS, ["PERS-1", "PERS-2"])

        assert "properties_ref_authors_value:[PERS-1,PERS-2]" in clause
        assert "properties_ref_subjects_value:[PERS-1,PERS-2]" in clause
        assert "properties_title_value:`rowling`" in clause

    def test_without_related_entities_it_stays_text_only(self):
        # A token that resolves to nothing, or to too many related entities to be
        # discriminating, must not widen the query.
        assert "ref_" not in token_clause("zzz", TEXT, RELATIONS, [])

    def test_returns_empty_when_no_field_is_usable(self):
        assert token_clause("oorlog", [], [], []) == ""


class TestBuildFilterBy:
    def test_ands_the_token_clauses(self):
        assert build_filter_by(["(a:`x`)", "(b:`y`)"]) == "(a:`x`) && (b:`y`)"

    def test_prepends_a_base_filter(self):
        assert build_filter_by(["(a:`x`)"], base_filter="type:[work]") == (
            "type:[work] && (a:`x`)"
        )

    def test_a_base_filter_alone_survives(self):
        assert build_filter_by([], base_filter="type:[work]") == "type:[work]"

    def test_no_clauses_and_no_base_filter_is_empty(self):
        assert build_filter_by([]) == ""

    def test_skips_empty_clauses(self):
        assert build_filter_by(["", "(b:`y`)"]) == "(b:`y`)"


class TestBuildEvalSort:
    def test_ranks_own_field_matches_above_relation_only_matches(self):
        sort = build_eval_sort(["properties_title_value:`oorlog`"])

        assert sort == "_eval(properties_title_value:`oorlog`):desc"

    def test_ors_the_text_matches_of_every_token(self):
        sort = build_eval_sort(["a:`x`", "b:`y`"])

        assert sort == "_eval(a:`x` || b:`y`):desc"

    def test_no_text_match_means_no_sort(self):
        assert build_eval_sort([]) is None


class TestOperationBudget:
    """Typesense counts every comparison AND every boolean operator against
    ``--filter-by-max-ops`` (default 100), so a filter is capped at 50 field
    comparisons — 49 when a base filter takes one of them. Verified against
    Typesense 27.1.
    """

    def test_operations_are_two_per_comparison_minus_one(self):
        assert filter_operations(1) == 1
        assert filter_operations(50) == 99

    def test_a_base_filter_costs_one_more_comparison(self):
        assert filter_operations(49, has_base_filter=True) == 99

    def test_fifty_comparisons_is_the_ceiling_without_a_base_filter(self):
        assert fits_operation_budget(50)
        assert not fits_operation_budget(51)

    def test_forty_nine_is_the_ceiling_with_a_base_filter(self):
        assert fits_operation_budget(49, has_base_filter=True)
        assert not fits_operation_budget(50, has_base_filter=True)

    def test_a_raised_server_limit_allows_more(self):
        assert fits_operation_budget(200, max_ops=1000)

    def test_zero_comparisons_never_exceeds(self):
        assert filter_operations(0) == 0
        assert fits_operation_budget(0)

    def test_counts_text_fields_for_every_token(self):
        assert comparison_count(2, text_field_count=7, relation_field_count=10) == 14

    def test_counts_relation_fields_only_for_resolved_tokens(self):
        assert (
            comparison_count(
                2, text_field_count=7, relation_field_count=10, resolved_tokens=1
            )
            == 24
        )

    def test_default_matches_the_typesense_default(self):
        assert MAX_FILTER_OPS == 100


TEXT_KEYS = ["vlacc:1|properties.title.value", "vlacc:1|properties.summary.value"]
RELATION_KEYS = ["vlacc:1|properties.ref_authors.value"]
AVAILABLE = {
    "properties_title_value",
    "properties_summary_value",
    "properties_ref_authors_value",
}


def _resolver(mapping):
    # None marks a token that matched too many related entities to discriminate;
    # [] marks one that matched none.
    return lambda token: mapping.get(token, [])


class TestBuildRelationSearchParams:
    def test_one_token_matches_own_metadata_or_a_relation(self):
        params = build_relation_search_params(
            "rowling", TEXT_KEYS, RELATION_KEYS, AVAILABLE,
            _resolver({"rowling": ["PERS-1"]}),
        )

        assert params["q"] == "*"
        assert params["filter_by"] == (
            "(properties_title_value:`rowling` "
            "|| properties_summary_value:`rowling` "
            "|| properties_ref_authors_value:[PERS-1])"
        )

    def test_tokens_are_anded(self):
        params = build_relation_search_params(
            "oorlogsliteratuur oekraine", TEXT_KEYS, RELATION_KEYS, AVAILABLE,
            _resolver({"oorlogsliteratuur": ["G-1"], "oekraine": ["N-1"]}),
        )

        assert params["filter_by"].count(" && ") == 1
        assert "properties_ref_authors_value:[G-1]" in params["filter_by"]
        assert "properties_ref_authors_value:[N-1]" in params["filter_by"]

    def test_ranks_own_metadata_matches_first(self):
        params = build_relation_search_params(
            "rowling", TEXT_KEYS, RELATION_KEYS, AVAILABLE,
            _resolver({"rowling": ["PERS-1"]}),
        )

        assert params["sort_by"] == (
            "_eval(properties_title_value:`rowling` "
            "|| properties_summary_value:`rowling`):desc"
        )

    def test_a_base_filter_is_kept(self):
        params = build_relation_search_params(
            "rowling", TEXT_KEYS, RELATION_KEYS, AVAILABLE,
            _resolver({}), base_filter="type:[work_word]",
        )

        assert params["filter_by"].startswith("type:[work_word] && ")

    def test_an_unresolved_token_stays_text_only(self):
        params = build_relation_search_params(
            "zzz", TEXT_KEYS, RELATION_KEYS, AVAILABLE, _resolver({}),
        )

        assert "ref_authors" not in params["filter_by"]

    def test_a_key_missing_from_the_collection_is_skipped(self):
        params = build_relation_search_params(
            "rowling",
            TEXT_KEYS + ["vlacc:1|properties.ghost.value"],
            RELATION_KEYS, AVAILABLE, _resolver({}),
        )

        assert "ghost" not in params["filter_by"]

    def test_an_empty_query_cannot_be_served(self):
        assert build_relation_search_params(
            "  ", TEXT_KEYS, RELATION_KEYS, AVAILABLE, _resolver({})
        ) is None

    def test_no_usable_field_cannot_be_served(self):
        assert build_relation_search_params(
            "rowling", TEXT_KEYS, RELATION_KEYS, set(), _resolver({})
        ) is None

    def test_exceeding_the_operation_budget_cannot_be_served(self):
        # The caller must fall back to the q-based search rather than send a
        # filter the server will reject outright.
        many = [f"vlacc:1|properties.f{i}.value" for i in range(40)]
        available = {f"properties_f{i}_value" for i in range(40)}

        assert build_relation_search_params(
            "one two", many, [], available, _resolver({})
        ) is None

    def test_query_by_is_required_even_though_q_is_a_wildcard(self):
        params = build_relation_search_params(
            "rowling", TEXT_KEYS, RELATION_KEYS, AVAILABLE, _resolver({}),
        )

        assert params["query_by"]


class TestCappedTokens:
    """A token matching too many related entities says nothing about relations,
    so it must not silently turn into a strict requirement on the entity's own
    metadata either — it drops out of the AND entirely.
    """

    def test_a_capped_token_drops_out_of_the_and(self):
        params = build_relation_search_params(
            "radcliffe daniel", TEXT_KEYS, RELATION_KEYS, AVAILABLE,
            _resolver({"radcliffe": ["PERS-1"], "daniel": None}),
        )

        assert "daniel" not in params["filter_by"]
        assert " && " not in params["filter_by"]

    def test_the_remaining_token_still_constrains(self):
        params = build_relation_search_params(
            "radcliffe daniel", TEXT_KEYS, RELATION_KEYS, AVAILABLE,
            _resolver({"radcliffe": ["PERS-1"], "daniel": None}),
        )

        assert "properties_ref_authors_value:[PERS-1]" in params["filter_by"]
        assert "properties_title_value:`radcliffe`" in params["filter_by"]

    def test_a_token_matching_nothing_stays_text_only(self):
        # Distinct from a capped token: a rare word should still be searched on
        # the entity's own fields.
        params = build_relation_search_params(
            "zzznotexist", TEXT_KEYS, RELATION_KEYS, AVAILABLE,
            _resolver({"zzznotexist": []}),
        )

        assert "properties_title_value:`zzznotexist`" in params["filter_by"]
        assert "ref_authors" not in params["filter_by"]

    def test_every_token_capped_cannot_be_served(self):
        # Nothing would constrain the query, so the caller falls back to the
        # ordinary q search instead of returning the whole collection.
        assert build_relation_search_params(
            "de het een", TEXT_KEYS, RELATION_KEYS, AVAILABLE,
            _resolver({"de": None, "het": None, "een": None}),
        ) is None

    def test_a_capped_token_is_left_out_of_the_ranking(self):
        params = build_relation_search_params(
            "radcliffe daniel", TEXT_KEYS, RELATION_KEYS, AVAILABLE,
            _resolver({"radcliffe": ["PERS-1"], "daniel": None}),
        )

        assert "daniel" not in params["sort_by"]

    def test_a_capped_token_costs_no_operations(self):
        # 40 text fields x 2 tokens would exceed the budget; with one token
        # capped only the other is built, so it fits.
        many = [f"vlacc:1|properties.f{i}.value" for i in range(40)]
        available = {f"properties_f{i}_value" for i in range(40)}

        assert build_relation_search_params(
            "one two", many, [], available, _resolver({"two": None}),
        ) is not None
