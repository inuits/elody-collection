#!/usr/bin/env python
"""Add configured facet_fields to existing Typesense collections in place.

Avoids a full reindex: Typesense re-indexes ONLY the changed fields over the
documents it already stores — no reimport from Mongo. Use this after adding
fields to a config's ``typesense.facet_fields`` so their distinct_by dropdowns
are served from facets instead of a Mongo $group scan.

Idempotent: skips fields that are already faceted. Preserves each field's
existing attributes (type, infix, optional, sort, ...) and only flips facet on,
so capabilities like infix search are not lost. Waits out Typesense's
"another update in progress" while a prior field re-indexes.

Run inside a collection-api container, from the api/ directory:

    python -m scripts.sync_typesense_facets             # apply
    python -m scripts.sync_typesense_facets --dry-run   # show what would change
"""

import argparse
import sys
from time import sleep

# Field attributes worth carrying over when re-declaring a field as a facet.
_PRESERVED_ATTRS = (
    "type",
    "optional",
    "infix",
    "sort",
    "locale",
    "stem",
    "index",
    "store",
)


def collect_facet_config():
    """Return {collection: set(flat_field_name)} from every enabled typesense config."""
    from configuration import get_object_configuration_mapper, init_mappers

    init_mappers()
    mapper = get_object_configuration_mapper().get_all()
    if not mapper:
        sys.exit(
            "No ObjectConfigurations registered — run from api/ inside a "
            "collection-api container."
        )

    collections = {}
    for ConfigClass in mapper.values():
        try:
            typesense = ConfigClass().crud().get("typesense") or {}
        except Exception:
            continue
        if not typesense.get("enabled"):
            continue
        collection = typesense.get("collection", "entities")
        for field in typesense.get("facet_fields", []):
            collections.setdefault(collection, set()).add(field.replace(".", "_"))
    return collections


def build_change(field, current):
    """Schema-update payload that makes ``field`` a facet, preserving its attrs."""
    if current is None:
        # Field not in the schema yet (no document carries it) — declare fresh.
        new = {"name": field, "type": "string", "facet": True, "optional": True}
        return {"fields": [new]}
    new = {key: current[key] for key in _PRESERVED_ATTRS if key in current}
    new["name"] = field
    new["facet"] = True
    # Drop the existing declaration first; Typesense requires this to change facet.
    return {"fields": [{"name": field, "drop": True}, new]}


def apply_change(client, collection, field, change, attempts=120):
    """Apply a schema update, waiting out an in-progress re-index on the collection."""
    for _ in range(attempts):
        try:
            client.collections[collection].update(change)
            print(f"  [set]  {collection}.{field} -> facet")
            return True
        except Exception as error:
            if "in progress" in str(error):
                sleep(3)
                continue
            print(f"  [err]  {collection}.{field}: {str(error)[:160]}")
            return False
    print(f"  [err]  {collection}.{field}: timed out waiting for prior update")
    return False


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the fields that would be faceted without changing anything.",
    )
    args = parser.parse_args(argv)

    from search.typesense_client import get_typesense_client

    client = get_typesense_client()
    if not client:
        sys.exit("No Typesense client available (TYPESENSE_API_KEY set?).")

    config = collect_facet_config()
    if not config:
        sys.exit("No typesense.facet_fields configured for any object configuration.")

    changed = 0
    for collection in sorted(config):
        try:
            schema = client.collections[collection].retrieve()
        except Exception as error:
            print(f"  {collection}: cannot retrieve schema ({error}) — skipped")
            continue
        fields = {f["name"]: f for f in schema["fields"]}
        for field in sorted(config[collection]):
            current = fields.get(field)
            if current and current.get("facet"):
                print(f"  [ok]   {collection}.{field} already faceted")
                continue
            change = build_change(field, current)
            if args.dry_run:
                print(f"  [dry]  {collection}.{field} would be faceted")
                continue
            if apply_change(client, collection, field, change):
                changed += 1

    if not args.dry_run:
        print(
            f"\n{changed} field(s) faceted. Typesense re-indexes them in the background."
        )


if __name__ == "__main__":
    main()
