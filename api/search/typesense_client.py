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
                            "host": getenv("TYPESENSE_HOST", "typesense"),
                            "port": getenv("TYPESENSE_PORT", "8108"),
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


def ensure_collection(collection):
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
                client.collections.create(
                    {
                        "name": collection,
                        "fields": [{"name": ".*", "type": "auto"}],
                    }
                )
                log.info(f"Created Typesense collection '{collection}' with auto schema")
            except Exception as e:
                log.warning(f"Failed to create Typesense collection '{collection}': {e}")
                return
        _ensured_collections.add(collection)


def search(collection, query, query_by, filter_by=None, per_page=250, page=1, offset=None):
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

        result = client.collections[collection].documents.search(search_params)
        ids = [hit["document"]["_id"] for hit in result["hits"]]
        return {"ids": ids, "count": result["found"]}
    except Exception as e:
        log.warning(f"Typesense search failed, falling back to MongoDB: {e}")
        return None


def search_all_ids(collection, query, query_by, filter_by=None):
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

            result = client.collections[collection].documents.search(search_params)
            if total is None:
                total = result["found"]

            ids = [hit["document"]["_id"] for hit in result["hits"]]
            all_ids.extend(ids)

            if len(all_ids) >= total or len(ids) < per_page:
                break
            page += 1

        return {"ids": all_ids, "count": total}
    except Exception as e:
        log.warning(f"Typesense search_all_ids failed, falling back to MongoDB: {e}")
        return None


def build_type_filter(type_values):
    """Build Typesense filter_by string for type values."""
    if not type_values:
        return None
    if len(type_values) == 1:
        return f"type:={type_values[0]}"
    return f"type:[{','.join(type_values)}]"


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
    keys = path.split(".")
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return None
    return obj


def prepare_document_for_typesense(entity, search_fields):
    doc = {
        "id": entity["_id"],
        "_id": entity["_id"],
        "type": entity.get("type", ""),
    }
    for field_path in search_fields:
        value = get_nested_value(entity, field_path)
        if value is not None:
            flat_key = field_path.replace(".", "_")
            doc[flat_key] = str(value) if not isinstance(value, str) else value
    return doc
