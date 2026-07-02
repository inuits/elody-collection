import pytest


@pytest.fixture
def resource():
    from resources.base_filter_resource import BaseFilterResource

    return object.__new__(BaseFilterResource)


def _items(count, results_length):
    return {
        "count": count,
        "results": [{"_id": str(i)} for i in range(results_length)],
    }


class TestAddPaginationLinks:
    # Neutral collection name so _inject_api_urls_into_entities is not hit.
    collection = "things"

    def test_next_link_on_intermediate_page(self, resource):
        items = resource._add_pagination_links(
            _items(640, 20), 0, 20, self.collection
        )
        assert items["next"] == f"/{self.collection}/filter?skip=20&limit=20"

    def test_no_next_link_on_exact_last_page(self, resource):
        # A full page alone must not imply a next page when the count is exact.
        items = resource._add_pagination_links(
            _items(640, 20), 620, 20, self.collection
        )
        assert "next" not in items

    def test_previous_link_only_when_skipped(self, resource):
        items = resource._add_pagination_links(
            _items(640, 20), 40, 20, self.collection
        )
        assert items["previous"] == f"/{self.collection}/filter?skip=20&limit=20"

        items = resource._add_pagination_links(
            _items(640, 20), 0, 20, self.collection
        )
        assert "previous" not in items


class TestAddPaginationLinksCappedCount:
    """collection-api caps the filter count at LISTING_COUNT_CAP and returns
    cap + 1 as a sentinel (see MongoFilters.__count). Past the cap the count no
    longer tells whether more results exist — a full page does."""

    collection = "things"

    def test_next_link_survives_past_the_cap_on_a_full_page(self, resource):
        # sentinel 1001, client at offset 1000: results continue, so must next
        items = resource._add_pagination_links(
            _items(1001, 20), 1000, 20, self.collection
        )
        assert items["next"] == f"/{self.collection}/filter?skip=1020&limit=20"

    def test_no_next_link_on_a_short_page_past_the_cap(self, resource):
        items = resource._add_pagination_links(
            _items(1001, 7), 1980, 20, self.collection
        )
        assert "next" not in items

    def test_no_next_link_on_an_empty_page_past_the_cap(self, resource):
        items = resource._add_pagination_links(
            _items(1001, 0), 2000, 20, self.collection
        )
        assert "next" not in items

    def test_counts_are_exact_when_the_cap_is_disabled(self, resource, monkeypatch):
        import resources.base_filter_resource as bfr

        monkeypatch.setattr(bfr, "LISTING_COUNT_CAP", 0)
        # with the cap disabled 1200 is an exact count: full last page, no next
        items = resource._add_pagination_links(
            _items(1200, 20), 1180, 20, self.collection
        )
        assert "next" not in items
