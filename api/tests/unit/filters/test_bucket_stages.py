"""
Geo bucketing must group on the field the geo filter targets, not on a
fixed top-level `location`, so clients that store their point elsewhere
(e.g. inside a properties value list) get real buckets.
Run from the api/ dir:

    pytest tests/unit/filters/test_bucket_stages.py -vv
"""

from filters_v2.helpers.mongo_helper import get_bucket_stages, has_bucket_filter

POLYGON = {
    "type": "Polygon",
    "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
}


def geo_filter(key, bucket="10"):
    return {"type": "geo", "key": key, "value": POLYGON, "bucket": bucket}


def bucket_point_expression(key):
    group_stages, _ = get_bucket_stages(geo_filter(key))
    return group_stages[0]["$addFields"]["_bucket_point"]


class TestBucketPoint:
    def test_point_is_read_from_the_filter_key(self):
        coordinates = "$properties.gps_coordinates.geojson.coordinates"
        first_element = {"$arrayElemAt": [coordinates, 0]}
        assert bucket_point_expression("properties.gps_coordinates.geojson") == {
            "$cond": [{"$isArray": first_element}, first_element, coordinates]
        }

    def test_flat_location_key_still_resolves_to_its_coordinates(self):
        expression = bucket_point_expression("location")
        assert expression["$cond"][2] == "$location.coordinates"


class TestBucketStages:
    def test_group_uses_the_bucket_point(self):
        group_stages, _ = get_bucket_stages(geo_filter("location"))
        grid = group_stages[1]["$group"]["_id"]
        assert grid["grid_x"]["$floor"]["$divide"][0] == {
            "$arrayElemAt": ["$_bucket_point", 0]
        }
        assert grid["grid_y"]["$floor"]["$divide"][0] == {
            "$arrayElemAt": ["$_bucket_point", 1]
        }

    def test_helper_field_is_stripped_from_the_output(self):
        _, replace_root_stages = get_bucket_stages(geo_filter("location"))
        assert replace_root_stages[-1] == {"$project": {"_bucket_point": 0}}

    def test_output_keeps_bucket_count_and_centroid(self):
        _, replace_root_stages = get_bucket_stages(geo_filter("location"))
        merged = replace_root_stages[0]["$replaceRoot"]["newRoot"]["$mergeObjects"]
        assert merged[1] == {
            "bucket_count": "$count",
            "location": {"type": "Point", "coordinates": ["$avg_lng", "$avg_lat"]},
        }


class TestHasBucketFilter:
    def test_geo_filter_without_bucket_is_ignored(self):
        assert has_bucket_filter([geo_filter("location", bucket=None)]) is None

    def test_geo_filter_with_bucket_is_returned(self):
        bucketed = geo_filter("location")
        assert has_bucket_filter([{"type": "text"}, bucketed]) is bucketed
