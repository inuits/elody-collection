import pytest

from scripts.migrate_filesize_to_bytes import build_update, parse_filesize


class TestParseFilesize:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("512 B", 512),
            ("1024 B", 1024),
            ("685.95 KB", 702413),
            ("1.5 MB", 1572864),
            ("2.0 GB", 2147483648),
            ("1 TB", 1099511627776),
            ("1 PB", 1125899906842624),
            (" 12.5 kb ", 12800),
            ("3", 3),
        ],
    )
    def test_converts_human_readable_strings_to_bytes(self, value, expected):
        assert parse_filesize(value) == expected

    @pytest.mark.parametrize("value", ["", "abc", "12 XB", "MB", "-5 KB"])
    def test_returns_none_for_unparseable_strings(self, value):
        assert parse_filesize(value) is None


class TestBuildUpdate:
    def test_converts_root_filesize(self):
        assert build_update({"filesize": "1 KB"}) == {"filesize": 1024}

    def test_converts_metadata_filesize(self):
        document = {"metadata": {"filesize": "1 KB", "title": "x"}}
        assert build_update(document) == {"metadata.filesize": 1024}

    def test_leaves_numeric_filesizes_alone(self):
        assert build_update({"filesize": 1024, "metadata": {"filesize": 2048}}) == {}

    def test_skips_unparseable_filesizes(self):
        assert build_update({"filesize": "garbage"}) == {}

    def test_ignores_list_style_metadata(self):
        assert build_update({"metadata": [{"key": "filesize", "value": "1 KB"}]}) == {}
