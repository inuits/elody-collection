from re import escape
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def contains_matcher():
    with patch("filters_v2.matchers.matchers.getenv", return_value="mongo"):
        from filters_v2.matchers.matchers import ContainsMatcher

        matcher = ContainsMatcher()
        matcher.matcher_engine = MagicMock()
        matcher.matcher_engine.contains.return_value = {"mocked": True}
        yield matcher


class TestContainsMatcherMatchAllWords:
    def test_single_word(self, contains_matcher):
        result = contains_matcher.match("field", "herman", match_all_words=True)
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", escape("herman"), {}
        )
        assert result == {"mocked": True}

    def test_multiple_words(self, contains_matcher):
        result = contains_matcher.match(
            "field", "herman brusselmans", match_all_words=True
        )
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", "(?=.*herman)(?=.*brusselmans)", {}
        )
        assert result == {"mocked": True}

    def test_multiple_words_with_special_chars(self, contains_matcher):
        result = contains_matcher.match(
            "field", "herman (test)", match_all_words=True
        )
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", f"(?=.*{escape('herman')})(?=.*{escape('(test)')})", {}
        )
        assert result == {"mocked": True}

    def test_three_words(self, contains_matcher):
        result = contains_matcher.match(
            "field", "een twee drie", match_all_words=True
        )
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", "(?=.*een)(?=.*twee)(?=.*drie)", {}
        )
        assert result == {"mocked": True}


class TestContainsMatcherDefault:
    def test_single_word_default(self, contains_matcher):
        result = contains_matcher.match("field", "herman")
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", "herman", {}
        )
        assert result == {"mocked": True}

    def test_multiple_words_without_match_all_words(self, contains_matcher):
        result = contains_matcher.match(
            "field", "herman brusselmans", match_all_words=False
        )
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", escape("herman brusselmans"), {}
        )
        assert result == {"mocked": True}

    def test_wildcard_star(self, contains_matcher):
        result = contains_matcher.match("field", "test*value")
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", "test.*value", {}
        )
        assert result == {"mocked": True}

    def test_wildcard_caret(self, contains_matcher):
        result = contains_matcher.match("field", "^test")
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", "^test", {}
        )
        assert result == {"mocked": True}

    def test_wildcard_dollar(self, contains_matcher):
        result = contains_matcher.match("field", "test$")
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", "test$", {}
        )
        assert result == {"mocked": True}

    def test_special_chars_escaped(self, contains_matcher):
        result = contains_matcher.match("field", "test(value)")
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", escape("test(value)"), {}
        )
        assert result == {"mocked": True}

    def test_inner_exact_matches_passed(self, contains_matcher):
        inner = {"nested": "value"}
        result = contains_matcher.match(
            "field", "test", inner_exact_matches=inner
        )
        contains_matcher.matcher_engine.contains.assert_called_once_with(
            "field", "test", inner
        )
        assert result == {"mocked": True}


class TestContainsMatcherSkipConditions:
    def test_list_key_returns_none(self, contains_matcher):
        result = contains_matcher.match(["key"], "value")
        assert result is None
        contains_matcher.matcher_engine.contains.assert_not_called()

    def test_match_exact_returns_none(self, contains_matcher):
        result = contains_matcher.match("field", "value", match_exact=True)
        assert result is None
        contains_matcher.matcher_engine.contains.assert_not_called()

    def test_regex_returns_none(self, contains_matcher):
        result = contains_matcher.match("field", "value", regex=True)
        assert result is None
        contains_matcher.matcher_engine.contains.assert_not_called()
