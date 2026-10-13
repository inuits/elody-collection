#!/usr/bin/env python
"""Apply configured facet_fields and sort_fields to existing Typesense collections in place.

Avoids a full reindex: Typesense re-indexes ONLY the changed fields over the
documents it already stores — no reimport from Mongo. Use this after adding
fields to a config's ``typesense.facet_fields`` (distinct_by dropdowns served
from facets instead of a Mongo $group scan) or ``typesense.sort_fields`` (lists
ordered by Typesense instead of losing their order).

Idempotent: skips fields that already carry their flags. Preserves each field's
existing attributes (type, infix, optional, facet, sort, ...) and only flips the
configured flags on, so capabilities like infix search are not lost. Waits out
Typesense's "another update in progress" while a prior field re-indexes.

Run inside a collection-api container, from the api/ directory:

    python -m scripts.sync_typesense_facets             # apply
    python -m scripts.sync_typesense_facets --dry-run   # show what would change
"""

import argparse
import sys
from os import environ
from time import sleep

# Field attributes worth carrying over when re-declaring a field.
_PRESERVED_ATTRS = (
    "type",
    "facet",
    "optional",
    "infix",
    "sort",
    "locale",
    "stem",
    "index",
    "store",
)


def merge_field_flags(typesense_configs):
    """Return {collection: {flat_field_name: flags}} from enabled typesense configs."""
    collections = {}
    for typesense in typesense_configs:
        if not typesense.get("enabled"):
            continue
        fields = collections.setdefault(typesense.get("collection", "entities"), {})
        for flag, key in (("facet", "facet_fields"), ("sort", "sort_fields")):
            for field in typesense.get(key, []):
                fields.setdefault(field.replace(".", "_"), {})[flag] = True
    return collections


def collect_field_config():
    """Return {collection: {flat_field_name: flags}} from every enabled typesense config."""
    from configuration import get_object_configuration_mapper, init_mappers

    init_mappers()
    mapper = get_object_configuration_mapper().get_all()
    if not mapper:
        sys.exit(
            "No ObjectConfigurations registered — run from api/ inside a "
            "collection-api container."
        )

    typesense_configs = []
    for ConfigClass in mapper.values():
        try:
            typesense_configs.append(ConfigClass().crud().get("typesense") or {})
        except Exception:
            continue
    return merge_field_flags(typesense_configs)


def field_is_up_to_date(current, flags):
    """Whether the existing field declaration already carries every flag."""
    return current is not None and all(current.get(flag) for flag in flags)


def build_change(field, current, flags=None):
    """Schema-update payload that sets ``flags`` on ``field``, preserving its attrs.

    Returns ``None`` when the change is impossible: Typesense cannot sort on an
    array field.
    """
    flags = flags or {"facet": True}
    if current is None:
        # Field not in the schema yet (no document carries it) — declare fresh.
        new = {"name": field, "type": "string", **flags, "optional": True}
        return {"fields": [new]}
    if flags.get("sort") and current.get("type", "").endswith("[]"):
        return None
    new = {key: current[key] for key in _PRESERVED_ATTRS if key in current}
    new["name"] = field
    new.update(flags)
    # Drop the existing declaration first; Typesense requires this to change a flag.
    return {"fields": [{"name": field, "drop": True}, new]}


def build_collection_change(schema_fields, field_flags):
    """One alter payload for every field of a collection that needs a flag.

    Typesense walks every document once per alter, so the fields are combined
    into a single alter instead of one alter per field. Returns ``(change,
    report)``; ``change`` is ``None`` when nothing needs to change.
    """
    drops, adds = [], []
    report = {"change": [], "ok": [], "skip": []}
    for field, flags in sorted(field_flags.items()):
        current = schema_fields.get(field)
        if field_is_up_to_date(current, flags):
            report["ok"].append(field)
            continue
        change = build_change(field, current, flags)
        if change is None:
            report["skip"].append(field)
            continue
        report["change"].append(field)
        *dropped, added = change["fields"]
        drops.extend(dropped)
        adds.append(added)
    if not adds:
        return None, report
    return {"fields": drops + adds}, report


def apply_change(client, collection, change, attempts=120):
    """Apply a schema update, waiting out an in-progress re-index on the collection."""
    for _ in range(attempts):
        try:
            client.collections[collection].update(change)
            return True
        except Exception as error:
            if "in progress" in str(error):
                sleep(3)
                continue
            print(f"  [err]  {collection}: {str(error)[:160]}")
            return False
    print(f"  [err]  {collection}: timed out waiting for prior update")
    return False


def wait_until_applied(client, collection, field_flags, interval=10, attempts=360):
    """Poll the schema until every field carries its flags; return those that don't.

    The alter keeps running on the server even when the HTTP request that
    started it gives up, so the schema is the only reliable outcome.
    """
    pending = sorted(field_flags)
    for attempt in range(attempts):
        fields = {
            f["name"]: f for f in client.collections[collection].retrieve()["fields"]
        }
        pending = [
            field
            for field, flags in sorted(field_flags.items())
            if not field_is_up_to_date(fields.get(field), flags)
        ]
        if not pending or attempt == attempts - 1:
            break
        sleep(interval)
    return pending


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the fields that would change without changing anything.",
    )
    args = parser.parse_args(argv)

    # An alter walks every document of the collection and takes minutes; the
    # default request timeout would make the client resend it while it runs.
    environ.setdefault("TYPESENSE_CONNECTION_TIMEOUT_SECONDS", "3600")
    from search.typesense_client import get_typesense_client

    client = get_typesense_client()
    if not client:
        sys.exit("No Typesense client available (TYPESENSE_API_KEY set?).")

    config = collect_field_config()
    if not config:
        sys.exit(
            "No typesense.facet_fields or sort_fields configured for any object "
            "configuration."
        )

    changed = 0
    for collection in sorted(config):
        try:
            schema = client.collections[collection].retrieve()
        except Exception as error:
            print(f"  {collection}: cannot retrieve schema ({error}) — skipped")
            continue
        fields = {f["name"]: f for f in schema["fields"]}
        change, report = build_collection_change(fields, config[collection])
        for field in report["ok"]:
            print(f"  [ok]   {collection}.{field} already up to date")
        for field in report["skip"]:
            print(f"  [skip] {collection}.{field} is an array, cannot sort")
        for field in report["change"]:
            flags = ", ".join(config[collection][field])
            verb = "would get" if args.dry_run else "gets"
            print(f"  [{'dry' if args.dry_run else 'set'}]  {collection}.{field} {verb} {flags}")
        if change is None or args.dry_run:
            continue
        print(
            f"  Altering {collection}: {len(report['change'])} field(s) in one pass...",
            flush=True,
        )
        apply_change(client, collection, change)
        pending = wait_until_applied(
            client,
            collection,
            {field: config[collection][field] for field in report["change"]},
        )
        for field in pending:
            print(f"  [err]  {collection}.{field} did not get its flags")
        changed += len(report["change"]) - len(pending)

    if not args.dry_run:
        print(
            f"\n{changed} field(s) updated. Typesense re-indexes them in the background."
        )


if __name__ == "__main__":
    main()
