import threading
from os import getenv
from time import sleep

from logging_elody.log import log

_client = None
_initialized = False
_ensured_collections = set()
_field_types_cache = {}
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
                    "connection_timeout_seconds": int(
                        getenv("TYPESENSE_CONNECTION_TIMEOUT_SECONDS", 5)
                    ),
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
                _field_types_cache.pop(collection, None)
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


def group_values(collection, field, filter_by=None, max_groups=250):
    """Return [(value, representative_id), ...] for a faceted field, or None.

    Populates filter dropdowns: a group_by search returns one representative
    document id per distinct value of the field, in one Typesense call. The id
    lets the caller hydrate the representative document by _id (indexed) instead
    of querying Mongo by the (unindexed) field value. Requires a facet field.
    """
    client = get_typesense_client()
    if not client:
        return None
    try:
        params = {
            "q": "*",
            "query_by": "type",
            "per_page": max_groups,
            "group_by": field,
            "group_limit": 1,
        }
        if filter_by:
            params["filter_by"] = filter_by
        result = client.collections[collection].documents.search(params)
        pairs = []
        for group in result.get("grouped_hits", []):
            keys = group.get("group_key") or []
            hits = group.get("hits") or []
            if keys and hits:
                pairs.append((keys[0], hits[0]["document"]["_id"]))
        return pairs
    except Exception as e:
        log.warning(f"Typesense group_by on '{field}' failed: {e}")
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


def upsert_document(collection, doc, max_attempts=10):
    """Upsert a document into Typesense, retrying transient failures.

    Returns True on success, False otherwise. A bounded retry with backoff
    smooths over transient hiccups (e.g. short connection timeouts); when all
    attempts fail the failure is logged at ERROR level (not silently swallowed)
    so the missed index is visible and recoverable instead of lost.
    """
    client = get_typesense_client()
    if not client:
        return False

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            ensure_collection(collection)
            client.collections[collection].documents.upsert(doc)
            return True
        except Exception as e:
            last_error = e
            if attempt < max_attempts:
                sleep(min(0.5 * 2 ** (attempt - 1), 2))
    log.error(
        f"Typesense upsert failed for doc {doc.get('id')} in '{collection}' "
        f"after {max_attempts} attempts: {last_error}"
    )
    return False


def delete_document(collection, doc_id, max_attempts=3):
    """Delete a document from Typesense, retrying transient failures.

    Returns True on success, False otherwise. A document that is already absent
    counts as success (delete is idempotent — the desired end state is reached).
    When all attempts fail the failure is logged at ERROR level so the stale
    document is visible and recoverable instead of silently lingering.
    """
    client = get_typesense_client()
    if not client:
        return False

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            client.collections[collection].documents[doc_id].delete()
            return True
        except Exception as e:
            if type(e).__name__ == "ObjectNotFound":
                return True
            last_error = e
            if attempt < max_attempts:
                sleep(min(0.5 * 2 ** (attempt - 1), 2))
    log.error(
        f"Typesense delete failed for doc {doc_id} in '{collection}' "
        f"after {max_attempts} attempts: {last_error}"
    )
    return False


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


def get_collection_field_types(collection):
    """Return ``{flat_field_name: type}`` for the concretely-typed fields of a
    Typesense collection.

    Wildcard (``.*``) and ``auto`` fields are omitted: they impose no cardinality
    constraint, so values for them never need coercion. Cached per collection and
    invalidated when the collection is (re)created in :func:`ensure_collection`.
    Returns an empty dict when Typesense is unavailable or the collection does not
    exist yet, in which case callers leave values untouched.
    """
    if collection in _field_types_cache:
        return _field_types_cache[collection]
    client = get_typesense_client()
    if not client:
        return {}
    try:
        schema = client.collections[collection].retrieve()
    except Exception:
        return {}
    field_types = {
        field["name"]: field["type"]
        for field in schema.get("fields", [])
        if field.get("name")
        and field["name"] != ".*"
        and field.get("type") not in (None, "auto")
    }
    with _lock:
        _field_types_cache[collection] = field_types
    return field_types


def _coerce_value_to_field_type(value, field_type):
    """Coerce ``value`` to match an existing Typesense field's cardinality.

    Typesense locks a field's type from the first value indexed into it. A field
    that is scalar for one entity type (e.g. ``properties.code.value`` is a single
    string on works/manifestations) but multi-valued for another (a list on zizo)
    would otherwise be rejected, failing the *whole* document. Collapsing arrays
    into a scalar field (single element unwrapped, multiple joined) and wrapping
    scalars into an array field keeps every document indexable regardless of which
    entity type set the field's type first.
    """
    if field_type.endswith("[]"):
        if isinstance(value, list):
            return [v if isinstance(v, str) else str(v) for v in value]
        return [value if isinstance(value, str) else str(value)]
    if isinstance(value, list):
        parts = [v if isinstance(v, str) else str(v) for v in value]
        return parts[0] if len(parts) == 1 else " ".join(parts)
    return value if isinstance(value, str) else str(value)


def prepare_document_for_typesense(
    entity, search_fields, facet_fields=None, field_types=None
):
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
        field_type = (field_types or {}).get(flat_key)
        if field_type:
            doc[flat_key] = _coerce_value_to_field_type(value, field_type)
        elif isinstance(value, list):
            doc[flat_key] = [v if isinstance(v, str) else str(v) for v in value]
        elif isinstance(value, str):
            doc[flat_key] = value
        else:
            doc[flat_key] = str(value)
    return doc
