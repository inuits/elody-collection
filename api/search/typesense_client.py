import threading
from os import getenv

from logging_elody.log import log

_client = None
_initialized = False
_ensured_collections = set()
_lock = threading.Lock()


def get_typesense_client():
    global _client, _initialized
    if _initialized:
        return _client

    with _lock:
        if _initialized:
            return _client

        _initialized = True
        api_key = getenv("TYPESENSE_API_KEY")
        if not api_key:
            return None

        try:
            import typesense

            _client = typesense.Client(
                {
                    "api_key": api_key,
                    "nodes": [
                        {
                            "host": getenv(
                                "TYPESENSE_SERVER_HOST",
                                getenv("TYPESENSE_HOST", "typesense"),
                            ),
                            "port": getenv(
                                "TYPESENSE_SERVER_PORT",
                                getenv("TYPESENSE_PORT", "8108"),
                            ),
                            "protocol": "http",
                        }
                    ],
                    "connection_timeout_seconds": 2,
                }
            )
        except Exception as e:
            log.warning(f"Failed to initialize Typesense client: {e}")
            _client = None

    return _client


def ensure_collection(collection, facet_fields=None):
    """Ensure a Typesense collection exists with auto schema detection."""
    if collection in _ensured_collections:
        return
    client = get_typesense_client()
    if not client:
        return

    with _lock:
        if collection in _ensured_collections:
            return
        try:
            client.collections[collection].retrieve()
        except Exception:
            try:
                fields = [{"name": ".*", "type": "auto"}]
                for field_path in facet_fields or []:
                    flat_key = field_path.replace(".", "_")
                    fields.append({"name": flat_key, "type": "auto", "facet": True})
                client.collections.create(
                    {
                        "name": collection,
                        "fields": fields,
                    }
                )
                log.info(
                    f"Created Typesense collection '{collection}' with auto schema"
                )
            except Exception as e:
                log.warning(
                    f"Failed to create Typesense collection '{collection}': {e}"
                )
                return
        _ensured_collections.add(collection)


def _transform_facets(facet_counts):
    """Transform Typesense facet_counts into MongoDB-style facet format."""
    result = []
    for facet in facet_counts:
        field = facet["field_name"]
        entries = [{"_id": c["value"], "count": c["count"]} for c in facet["counts"]]
        result.append({field: entries})
    return result


def search(
    collection,
    query,
    query_by,
    filter_by=None,
    per_page=250,
    page=1,
    offset=None,
    facet_by=None,
    group_by=None,
):
    client = get_typesense_client()
    if not client:
        return None

    try:
        search_params = {
            "q": query,
            "query_by": query_by,
            "per_page": per_page,
        }
        if offset is not None:
            search_params["offset"] = offset
        else:
            search_params["page"] = page
        if filter_by:
            search_params["filter_by"] = filter_by
        if facet_by:
            search_params["facet_by"] = facet_by
        if group_by:
            search_params["group_by"] = group_by
            search_params["group_limit"] = 1

        result = client.collections[collection].documents.search(search_params)
        if group_by and "grouped_hits" in result:
            ids = [
                g["hits"][0]["document"]["_id"]
                for g in result["grouped_hits"]
                if g.get("hits")
            ]
        else:
            ids = [hit["document"]["_id"] for hit in result["hits"]]
        response = {"ids": ids, "count": result["found"]}
        if facet_by:
            response["facets"] = _transform_facets(result.get("facet_counts", []))
        return response
    except Exception as e:
        if "Could not find a field named" in str(e) and query_by:
            missing = str(e).split("`")[1] if "`" in str(e) else ""
            filtered = ",".join(f for f in query_by.split(",") if f != missing)
            if filtered:
                log.warning(
                    f"Retrying Typesense search without missing field '{missing}'"
                )
                return search(
                    collection,
                    query,
                    filtered,
                    filter_by,
                    per_page,
                    page,
                    offset,
                    facet_by,
                    group_by,
                )
        log.warning(f"Typesense search failed, falling back to MongoDB: {e}")
        return None


def search_all_ids(collection, query, query_by, filter_by=None, group_by=None):
    """Fetch all matching IDs from Typesense by paginating through results."""
    client = get_typesense_client()
    if not client:
        return None

    try:
        all_ids = []
        page = 1
        per_page = 250
        total = None

        while True:
            search_params = {
                "q": query,
                "query_by": query_by,
                "per_page": per_page,
                "page": page,
            }
            if filter_by:
                search_params["filter_by"] = filter_by
            if group_by:
                search_params["group_by"] = group_by
                search_params["group_limit"] = 1

            result = client.collections[collection].documents.search(search_params)
            if total is None:
                total = result["found"]

            if group_by and "grouped_hits" in result:
                ids = [
                    g["hits"][0]["document"]["_id"]
                    for g in result["grouped_hits"]
                    if g.get("hits")
                ]
            else:
                ids = [hit["document"]["_id"] for hit in result["hits"]]
            all_ids.extend(ids)

            if len(all_ids) >= total or len(ids) < per_page:
                break
            page += 1

        return {"ids": all_ids, "count": total}
    except Exception as e:
        if "Could not find a field named" in str(e) and query_by:
            missing = str(e).split("`")[1] if "`" in str(e) else ""
            filtered = ",".join(f for f in query_by.split(",") if f != missing)
            if filtered:
                log.warning(
                    f"Retrying search_all_ids without missing field '{missing}'"
                )
                return search_all_ids(collection, query, filtered, filter_by, group_by)
        log.warning(f"Typesense search_all_ids failed, falling back to MongoDB: {e}")
        return None


def build_type_filter(type_values):
    """Build Typesense filter_by string for type values."""
    if not type_values:
        return None
    if len(type_values) == 1:
        return f"type:={type_values[0]}"
    return f"type:[{','.join(type_values)}]"


def _quote_ts_value(value):
    """Quote a single Typesense filter_by value using backticks when needed."""
    s = value if isinstance(value, str) else str(value)
    needs_quote = any(c in s for c in (" ", ",", "(", ")", "[", "]", "`", "&", ":"))
    if needs_quote:
        return "`" + s.replace("`", "\\`") + "`"
    return s


def build_filter_by(type_values, exact_match_filters=None):
    """Build combined filter_by for type filter + exact-match field filters.

    exact_match_filters is a list of (flat_key, value_or_list) tuples. flat_key
    may be a single field (str) or a list of fields. A list expresses an OR
    across those fields for the same value(s), e.g. matching a title against
    both `properties_name_value` and `properties_title_value`.
    """
    parts = []
    type_clause = build_type_filter(type_values)
    if type_clause:
        parts.append(type_clause)
    for flat_key, values in exact_match_filters or []:
        if isinstance(values, list):
            values = [v for v in values if v not in (None, "")]
            if not values:
                continue
            quoted = [_quote_ts_value(v) for v in values]
            value_clause = quoted[0] if len(quoted) == 1 else f"[{','.join(quoted)}]"
        elif values not in (None, ""):
            value_clause = _quote_ts_value(values)
        else:
            continue
        keys = flat_key if isinstance(flat_key, list) else [flat_key]
        key_clauses = [f"{k}:={value_clause}" for k in keys]
        if len(key_clauses) == 1:
            parts.append(key_clauses[0])
        else:
            parts.append("(" + " || ".join(key_clauses) + ")")
    return " && ".join(parts) if parts else None


def upsert_document(collection, doc):
    client = get_typesense_client()
    if not client:
        return

    try:
        ensure_collection(collection)
        client.collections[collection].documents.upsert(doc)
    except Exception as e:
        log.warning(f"Typesense upsert failed for doc {doc.get('id')}: {e}")


def delete_document(collection, doc_id):
    client = get_typesense_client()
    if not client:
        return

    try:
        client.collections[collection].documents[doc_id].delete()
    except Exception as e:
        log.warning(f"Typesense delete failed for doc {doc_id}: {e}")


def get_nested_value(obj, path):
    keys = path.split(".") if isinstance(path, str) else list(path)
    return _descend(obj, keys)


def _descend(obj, keys):
    if not keys:
        return obj
    if obj is None:
        return None
    if isinstance(obj, list):
        if not obj:
            return None
        obj = obj[0]
    if isinstance(obj, dict):
        return _descend(obj.get(keys[0]), keys[1:])
    if isinstance(obj, list):
        collected = []
        for item in obj:
            v = _descend(item, keys)
            if v is None:
                continue
            if isinstance(v, list):
                collected.extend(x for x in v if x is not None)
            else:
                collected.append(v)
        return collected or None
    return None


def prepare_document_for_typesense(entity, search_fields, facet_fields=None):
    doc = {
        "id": entity["_id"],
        "_id": entity["_id"],
        "type": entity.get("type", ""),
    }
    all_fields = set(search_fields)
    if facet_fields:
        all_fields.update(facet_fields)
    for field_path in all_fields:
        value = get_nested_value(entity, field_path)
        if value is None:
            continue
        flat_key = field_path.replace(".", "_")
        if isinstance(value, list):
            doc[flat_key] = [v if isinstance(v, str) else str(v) for v in value]
        elif isinstance(value, str):
            doc[flat_key] = value
        else:
            doc[flat_key] = str(value)
    return doc


def _relation_hops(relation):
    """Normalize a denormalization relation to a list of hops.

    Supports a single-hop shorthand (``ref`` + ``source_collection``) and an
    explicit multi-hop ``path`` (a list of ``{ref, collection}`` steps) for
    chains such as manifestation -> expression -> work -> author. Returns an
    empty list when the relation is incomplete.
    """
    if relation.get("path"):
        return [
            {"ref": hop.get("ref"), "collection": hop.get("collection", "entities")}
            for hop in relation["path"]
        ]
    if relation.get("ref"):
        return [
            {
                "ref": relation["ref"],
                "collection": relation.get("source_collection", "entities"),
            }
        ]
    return []


def resolve_denormalized_fields(entity, denormalized_relations, storage):
    """Resolve reference fields to denormalized text on the Typesense document.

    WEMI bibliographic data spreads the dimensions a cataloger searches on
    (title, author, SISO) across separate entities linked by reference ids, so
    no single document holds them all. This copies the referenced entities' text
    onto the document at index time — the source (Mongo) is never modified, only
    the Typesense document gains the extra searchable fields.

    Each relation in `denormalized_relations` resolves a reference chain to a
    text field. A single-hop relation::

        {
            "ref": "properties.ref_authors.value",   # path to the reference id(s)
            "source_collection": "entities",          # collection holding the target
            "target_field": "properties.name.value",  # path to the text on the target
            "as": "author_names",                     # flat key written to the document
        }

    A multi-hop relation walks a `path` of ``{ref, collection}`` steps, e.g. for
    a manifestation reaching its work's authors::

        {
            "path": [
                {"ref": "properties.ref_expressions.value", "collection": "..."},
                {"ref": "properties.ref_work.value", "collection": "..."},
                {"ref": "properties.ref_authors.value", "collection": "entities"},
            ],
            "target_field": "properties.name.value",
            "as": "author_names",
        }

    A reference value may be a single id or a list, and may hold an `_id` or any
    of the target's `identifiers` (`get_item_from_collection_by_id` resolves
    both). Returns a dict of ``{flat_key: [values]}`` to merge into the document.
    Links that cannot be resolved are skipped so a broken chain never blocks
    indexing.
    """
    extra = {}
    for relation in denormalized_relations or []:
        target_field = relation.get("target_field")
        flat_key = relation.get("as")
        hops = _relation_hops(relation)
        if not (target_field and flat_key and hops):
            continue
        current = [entity]
        for hop in hops:
            ref_path = hop["ref"]
            collection = hop["collection"]
            if not ref_path:
                current = []
                break
            resolved = []
            for item in current:
                ref_ids = get_nested_value(item, ref_path)
                if not ref_ids:
                    continue
                for ref_id in ref_ids if isinstance(ref_ids, list) else [ref_ids]:
                    if not ref_id:
                        continue
                    try:
                        target = storage.get_item_from_collection_by_id(
                            collection, ref_id
                        )
                    except Exception as e:
                        log.warning(
                            f"Denormalization lookup failed for '{ref_id}' in "
                            f"'{collection}': {e}"
                        )
                        continue
                    if target:
                        resolved.append(target)
            current = resolved
            if not current:
                break
        values = []
        for item in current:
            value = get_nested_value(item, target_field)
            if value is None:
                continue
            for entry in value if isinstance(value, list) else [value]:
                text = entry if isinstance(entry, str) else str(entry)
                if text and text not in values:
                    values.append(text)
        if values:
            extra[flat_key] = values
    return extra
