#!/usr/bin/env python
import argparse
import re

from pymongo import UpdateOne

SUFFIXES = ["B", "KB", "MB", "GB", "TB", "PB"]
FILESIZE_PATTERN = re.compile(r"^(\d*\.?\d+)\s*([a-zA-Z]*)$")
STRING_FILESIZE_QUERY = {
    "$or": [
        {"filesize": {"$type": "string"}},
        {"metadata.filesize": {"$type": "string"}},
    ]
}


def parse_filesize(value):
    match = FILESIZE_PATTERN.match(value.strip())
    if not match:
        return None
    suffix = match.group(2).upper() or "B"
    if suffix not in SUFFIXES:
        return None
    return round(float(match.group(1)) * 1024 ** SUFFIXES.index(suffix))


def build_update(document):
    candidates = {"filesize": document.get("filesize")}
    if isinstance(document.get("metadata"), dict):
        candidates["metadata.filesize"] = document["metadata"].get("filesize")

    update = {}
    for path, value in candidates.items():
        if isinstance(value, str) and (size := parse_filesize(value)) is not None:
            update[path] = size
    return update


def main():
    parser = argparse.ArgumentParser(
        description="Convert string mediafile filesizes to bytes"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the changes; without it this is a dry run",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    from configuration import get_object_configuration_mapper, init_mappers
    from storage.storagemanager import StorageManager

    init_mappers()
    collection_name = (
        get_object_configuration_mapper().get("mediafile").crud()["collection"]
    )
    collection = StorageManager().get_db_engine().db[collection_name]

    matched, converted, unparseable, operations = 0, 0, [], []
    for document in collection.find(
        STRING_FILESIZE_QUERY, {"filesize": 1, "metadata.filesize": 1}
    ):
        matched += 1
        update = build_update(document)
        if not update:
            unparseable.append(document["_id"])
            continue
        converted += 1
        if not args.apply:
            continue
        operations.append(UpdateOne({"_id": document["_id"]}, {"$set": update}))
        if len(operations) >= args.batch_size:
            collection.bulk_write(operations, ordered=False)
            operations = []
    if args.apply and operations:
        collection.bulk_write(operations, ordered=False)

    mode = "converted" if args.apply else "would convert (dry run, pass --apply)"
    print(f"{collection_name}: {matched} with a string filesize, {mode} {converted}")
    if unparseable:
        print(f"{len(unparseable)} unparseable, left as is: {unparseable[:20]}")


if __name__ == "__main__":
    main()
