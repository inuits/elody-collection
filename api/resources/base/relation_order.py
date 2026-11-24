import csv
import io
from collections import Counter, defaultdict

from serialization.case_converter import camel_to_snake

ORDER_KEY = "order"


def _order_of(relation):
    for item in relation.get("metadata") or []:
        value = item.get("value")
        is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
        if item.get("key") == ORDER_KEY and is_number:
            return value
    return None


def sort_by_order(relations):
    ordered = [relation for relation in relations if _order_of(relation) is not None]
    unordered = [relation for relation in relations if _order_of(relation) is None]
    return sorted(ordered, key=_order_of) + unordered


def _with_order(relation, position):
    metadata = [
        item for item in relation.get("metadata") or [] if item.get("key") != ORDER_KEY
    ]
    return {**relation, "metadata": metadata + [{"key": ORDER_KEY, "value": position}]}


def is_relation_type(relation, relation_type):
    return camel_to_snake(relation.get("type") or "") == camel_to_snake(relation_type)


def apply_order(relations, relation_type, ordered_keys):
    ordered_relations = [
        relation for relation in relations if is_relation_type(relation, relation_type)
    ]
    stored_keys = Counter(relation["key"] for relation in ordered_relations)
    if Counter(ordered_keys) != stored_keys:
        raise ValueError("The order must list every related item exactly once")

    relations_by_key = defaultdict(list)
    for relation in ordered_relations:
        relations_by_key[relation["key"]].append(relation)
    reordered = [
        _with_order(relations_by_key[key].pop(0), position)
        for position, key in enumerate(ordered_keys, start=1)
    ]
    other_relations = [
        relation
        for relation in relations
        if not is_relation_type(relation, relation_type)
    ]
    return other_relations + reordered


def order_rows_csv(rows):
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", "type"])
    writer.writerows(rows)
    return output.getvalue()


def parse_order_csv(text):
    return [row["id"] for row in csv.DictReader(io.StringIO(text))]
