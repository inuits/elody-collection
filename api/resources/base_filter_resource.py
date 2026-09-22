from configuration import get_object_configuration_mapper
from filters_v2.filter_manager import FilterManager as FilterManagerV2
from filters_v2.helpers.base_helper import get_distinct_by
from filters_v2.mongo_filters import LISTING_COUNT_CAP
from flask import after_this_request, request
from flask_restful import abort
from logging_elody.log import log
from resources.base_resource import BaseResource
from search.relation_search import (
    MAX_FILTER_OPS,
    build_relation_search_params,
)
from search.typesense_client import (
    build_filter_by,
    ensure_collection as typesense_ensure_collection,
    get_collection_field_types,
    group_values as typesense_group_values,
    multi_search_all_ids,
    multi_search_one,
    resolve_related_entity_ids,
    search as typesense_search,
    search_all_ids as typesense_search_all_ids,
)
from storage.storagemanager import StorageManager
from tracing import get_tracer

tracer = get_tracer()


class BaseFilterResource(BaseResource):
    def __init__(self):
        super().__init__()
        self.filter_engine_v2 = FilterManagerV2().get_filter_engine()

    def _classify_filters_for_typesense(self, query):
        """Split query filters into text, type, exact-match, and remaining categories."""
        text_filters = []
        type_filter_values = []
        exact_match_filters = []
        remaining_filters = []
        for f in query:
            is_text_search = (
                not f.get("match_exact")
                and not f.get("match_not")
                and not f.get("regex")
                and f.get("value")
                and isinstance(f.get("value"), str)
            )
            if f.get("type") == "text" and is_text_search:
                text_filters.append(f)
            elif f.get("type") == "selection" and is_text_search:
                text_filters.append(f)
            elif f.get("type") == "type":
                val = f.get("value")
                if isinstance(val, list):
                    type_filter_values.extend(val)
                elif val:
                    type_filter_values.append(val)
            elif f.get("type") == "selection" and f.get("key") == "type":
                val = f.get("value")
                if isinstance(val, list):
                    type_filter_values.extend(val)
                elif val:
                    type_filter_values.append(val)
            elif (
                f.get("type") == "selection"
                and f.get("match_exact")
                and f.get("value")
                and f.get("value") != "*"
            ):
                exact_match_filters.append(f)
            else:
                remaining_filters.append(f)
        return text_filters, type_filter_values, exact_match_filters, remaining_filters

    def _resolve_mongo_collections(self, type_filter_values, default_collection):
        """Map entity types to their MongoDB collections."""
        collections = set()
        if type_filter_values:
            mapper = get_object_configuration_mapper()
            for t in type_filter_values:
                try:
                    collections.add(mapper.get(t).crud().get("collection"))
                except Exception:
                    collections.add(default_collection)
        return collections or {default_collection}

    def _resolve_lookup_via_typesense(self, filter_obj, typesense_config):
        """Resolve a relation filter by searching the related collection in Typesense.

        Handles both:
        - Filters with explicit lookup object
        - Filters on ref_*.key fields (auto-detected as relation lookups)

        Finds matching entities, fetches their identifiers from MongoDB,
        and returns an ID-based selection filter on the .value field.
        Returns None if Typesense can't resolve it.
        """
        lookup = filter_obj.get("lookup", {})
        search_value = filter_obj.get("value", "")
        if not search_value or not isinstance(search_value, str):
            return None

        # Determine local_field from lookup or by inferring from key pattern
        key = filter_obj.get("key", "")
        if isinstance(key, list):
            key = key[0].split("|")[-1] if key else ""

        if lookup:
            local_field = lookup.get("local_field", "")
        elif ".ref_" in key and key.endswith(".key"):
            # Auto-detect: properties.ref_authors.key → properties.ref_authors.value
            local_field = key.rsplit(".key", 1)[0] + ".value"
        else:
            return None

        ts_collection = typesense_config.get("collection", "entities")
        search_fields = typesense_config.get("search_fields", [])
        query_by = ",".join(field.replace(".", "_") for field in search_fields)
        if not query_by:
            return None

        ts_result = typesense_search_all_ids(ts_collection, search_value, query_by)
        if not ts_result or not ts_result.get("ids"):
            return None

        matched_ids = ts_result["ids"]
        from_collection = lookup.get("from", "entities_actual")
        storage = StorageManager().get_db_engine()
        all_identifiers = []
        for doc in storage.db[from_collection].find(
            {"_id": {"$in": matched_ids}}, {"identifiers": 1}
        ):
            all_identifiers.extend(doc.get("identifiers", [doc["_id"]]))

        if not all_identifiers:
            return None

        return {
            "type": "selection",
            "key": local_field,
            "value": all_identifiers,
            "match_exact": True,
        }

    def _resolve_source_relation_lookups(self, query):
        """Rewrite a relation lookup that should be evaluated against the *source*
        entity's own relation values into an indexed selection, avoiding a
        collection-wide ``$lookup``.

        A filter whose ``lookup.resolve_to_source_ids`` is truthy is treated as
        follows: the filter value is the source entity id(s); the source entity is
        read once from ``lookup.from``, and the ids stored at ``lookup.foreign_field``
        become the selection values on ``lookup.local_field`` (default ``id``). This
        replaces an O(collection) reverse self-join with a single indexed read plus
        an indexed selection. Purely config-driven — no entity-type or client
        specifics.

        When a lookup is resolved, any ``type`` filter is dropped: the marker
        denotes a symmetric self-relation, so both the forward (``id`` selection)
        and reverse (relation-value selection) branches already match only the
        relevant entities, making the broad ``type`` filter redundant. Keeping it
        lets the planner satisfy an ``order_by`` via the ``type``-prefixed sort
        index, scanning the whole collection instead of the selective relation
        index; dropping it keeps the query on the selective index.

        Returns ``(query, did_resolve)``.
        """
        if not isinstance(query, list):
            return query, False

        did_resolve = False
        resolved_query = []
        storage = None
        for filter_criteria in query:
            lookup = (
                filter_criteria.get("lookup")
                if isinstance(filter_criteria, dict)
                else None
            )
            if not (lookup and lookup.get("resolve_to_source_ids")):
                resolved_query.append(filter_criteria)
                continue

            did_resolve = True
            source_ids = filter_criteria.get("value")
            source_ids = source_ids if isinstance(source_ids, list) else [source_ids]
            source_ids = [source_id for source_id in source_ids if source_id]
            from_collection = lookup.get("from")
            foreign_field = lookup.get("foreign_field", "")
            local_field = lookup.get("local_field") or "id"

            # Preserve the original key's schema prefix (e.g. "vlacc:1|") so the
            # rewritten selection lands in the same matcher bucket as its OR
            # siblings — otherwise the union with a schema-specific sibling breaks.
            original_key = filter_criteria.get("key")
            if (
                isinstance(original_key, list)
                and original_key
                and "|" in original_key[0]
            ):
                schema_prefix = original_key[0].split("|", 1)[0]
                resolved_key = [f"{schema_prefix}|{local_field}"]
            else:
                resolved_key = local_field

            related_ids = []
            if source_ids and from_collection and foreign_field:
                if storage is None:
                    storage = StorageManager().get_db_engine()
                for document in storage.db[from_collection].find(
                    {
                        "$or": [
                            {"_id": {"$in": source_ids}},
                            {"identifiers": {"$in": source_ids}},
                        ]
                    },
                    {foreign_field: 1},
                ):
                    related_ids.extend(
                        self._extract_nested_values(document, foreign_field)
                    )

            seen = set()
            related_ids = [
                related_id
                for related_id in related_ids
                if not (related_id in seen or seen.add(related_id))
            ]
            resolved_query.append(
                {
                    "type": "selection",
                    "key": resolved_key,
                    "value": related_ids,
                    "match_exact": True,
                    "operator": filter_criteria.get("operator", "or"),
                }
            )

        if did_resolve:
            resolved_query = [
                filter_criteria
                for filter_criteria in resolved_query
                if not self._is_type_filter(filter_criteria)
            ]

        return resolved_query, did_resolve

    @staticmethod
    def _is_type_filter(filter_criteria):
        if not isinstance(filter_criteria, dict):
            return False
        if filter_criteria.get("type") == "type":
            return True
        return (
            filter_criteria.get("type") == "selection"
            and filter_criteria.get("key") == "type"
        )

    @staticmethod
    def _extract_nested_values(document, dotted_path):
        """Collect scalar values at a dotted path within a document, descending into
        any lists encountered along the way (e.g. ``properties.ref_x.value``)."""
        nodes = [document]
        for part in dotted_path.split("."):
            next_nodes = []
            for node in nodes:
                if isinstance(node, dict) and part in node:
                    value = node[part]
                    next_nodes.extend(value if isinstance(value, list) else [value])
            nodes = next_nodes
        return [node for node in nodes if isinstance(node, (str, int))]

    def _build_typesense_query(
        self,
        text_filters,
        type_filter_values,
        typesense_config,
        exact_match_filters=None,
    ):
        """Build Typesense search parameters from classified filters."""
        ts_collection = typesense_config.get("collection", "entities")
        typesense_ensure_collection(
            ts_collection,
            facet_fields=typesense_config.get("facet_fields", []),
            infix_fields=typesense_config.get("infix_fields", []),
        )
        search_fields = typesense_config.get("search_fields", [])
        filter_keys = []
        distinct_keys = []
        for f in text_filters:
            key = f.get("key", "")
            keys = key if isinstance(key, list) else [key]
            for k in keys:
                bare_key = k.split("|")[-1]
                if bare_key:
                    filter_keys.append(bare_key)
            distinct_by = f.get("distinct_by")
            if distinct_by:
                distinct_keys.append(distinct_by)
        if filter_keys:
            query_by = ",".join(dict.fromkeys(k.replace(".", "_") for k in filter_keys))
        else:
            query_by = ",".join(field.replace(".", "_") for field in search_fields)
        search_terms = " ".join(
            f.get("value", "") for f in text_filters if f.get("value")
        )
        if not search_terms:
            search_terms = "*"
        filter_by = build_filter_by(type_filter_values, exact_match_filters or [])
        group_by = distinct_keys[0].replace(".", "_") if distinct_keys else None
        infix_fields = typesense_config.get("infix_fields", [])
        search_options = self._build_search_options(typesense_config)
        return (
            ts_collection,
            query_by,
            search_terms,
            filter_by,
            group_by,
            infix_fields,
            search_options,
        )

    def _relation_aware_search(
        self,
        text_filters,
        type_filter_values,
        typesense_config,
        exact_match_filters,
        has_remaining,
        skip,
        limit,
    ):
        """Resolve a simple search that matches a token against the entity's own
        metadata *or* one of its relations, with the tokens ANDed.

        Both sides live in one ``filter_by``: ``q`` cannot be OR-ed against a
        filter, so the text side has to sit in the filter as well. Relations are
        read as stored reference ids, so nothing is denormalized into the index.

        Returns ``None`` whenever the search cannot be served this way — no
        relation keys configured, a key the collection does not hold, a filter
        past the server's operation budget, or simply no hits — and the caller
        then runs the ordinary ``q`` based search unchanged.
        """
        relation_filter = next(
            (f for f in text_filters if f.get("relation_keys")), None
        )
        if relation_filter is None or len(text_filters) != 1:
            return None

        ts_collection = typesense_config.get("collection", "entities")
        related_entity_types = typesense_config.get("related_entity_types") or []
        related_entity_fields = typesense_config.get("related_entity_label_fields") or []
        if not related_entity_types or not related_entity_fields:
            return None

        keys = relation_filter.get("key") or []
        keys = keys if isinstance(keys, list) else [keys]
        cap = typesense_config.get("related_entity_match_cap", 500)
        query_by = ",".join(f.replace(".", "_") for f in related_entity_fields)

        params = build_relation_search_params(
            relation_filter.get("value", ""),
            keys,
            relation_filter["relation_keys"],
            set(get_collection_field_types(ts_collection)),
            lambda token: resolve_related_entity_ids(
                ts_collection, token, query_by, related_entity_types, cap=cap
            ),
            base_filter=build_filter_by(type_filter_values, exact_match_filters or []),
            max_ops=typesense_config.get("filter_by_max_ops", MAX_FILTER_OPS),
        )
        if params is None:
            return None

        if has_remaining:
            # The other filters still run in MongoDB against these ids, so a
            # single page would silently truncate them.
            result = multi_search_all_ids(ts_collection, params)
            if not result or not result["ids"]:
                return None
            return result

        page = multi_search_one(
            ts_collection,
            {**params, "per_page": limit, "offset": skip, "include_fields": "_id"},
        )
        if not page or not page.get("hits"):
            return None
        return {
            "ids": [hit["document"]["_id"] for hit in page["hits"]],
            "count": page.get("found", 0),
        }

    @staticmethod
    def _build_search_options(typesense_config):
        """Collect the optional typo / token-drop tuning from the config.

        ``no_typo_fields`` lists fields that must never fuzzy-match (identifiers);
        ``drop_tokens_threshold`` = 0 keeps AND semantics for multi-word queries
        instead of letting Typesense drop words until something matches.
        """
        options = {}
        if typesense_config.get("no_typo_fields"):
            options["no_typo_fields"] = typesense_config["no_typo_fields"]
        if typesense_config.get("drop_tokens_threshold") is not None:
            options["drop_tokens_threshold"] = typesense_config["drop_tokens_threshold"]
        return options

    def _execute_typesense_search(
        self,
        ts_collection,
        search_terms,
        query_by,
        filter_by,
        has_remaining,
        skip,
        limit,
        facet_by=None,
        group_by=None,
        infix_fields=None,
        search_options=None,
    ):
        """Execute Typesense search with fallback. Returns None if unavailable."""
        search_options = search_options or {}
        tuning = {
            "no_typo_fields": search_options.get("no_typo_fields"),
            "drop_tokens_threshold": search_options.get("drop_tokens_threshold"),
        }
        if has_remaining:
            return typesense_search_all_ids(
                ts_collection,
                search_terms,
                query_by,
                filter_by=filter_by,
                group_by=group_by,
                infix_fields=infix_fields,
                **tuning,
            )
        return typesense_search(
            ts_collection,
            search_terms,
            query_by,
            filter_by=filter_by,
            per_page=limit,
            offset=skip,
            facet_by=facet_by,
            group_by=group_by,
            infix_fields=infix_fields,
            **tuning,
        )

    def _fetch_documents_from_mongo(self, matching_ids, collections):
        """Fetch documents from multiple MongoDB collections, ordered by Typesense relevance."""
        storage = StorageManager().get_db_engine()
        id_query = {
            "$or": [
                {"_id": {"$in": matching_ids}},
                {"identifiers": {"$in": matching_ids}},
            ]
        }
        documents = []
        for col in collections:
            documents.extend(storage.db[col].find(id_query))
        id_order = {doc_id: i for i, doc_id in enumerate(matching_ids)}
        documents.sort(key=lambda doc: id_order.get(doc["_id"], float("inf")))
        return [storage._prepare_mongo_document(doc, True) for doc in documents]

    def _add_pagination_links(self, items, skip, limit, collection):
        """Add next/previous pagination links to response."""
        count = items.get("count", 0)
        # A count above LISTING_COUNT_CAP is a sentinel, not the real total
        # (see MongoFilters.__count): past the cap it can no longer tell
        # whether more results exist, but a full page can.
        count_is_capped = LISTING_COUNT_CAP > 0 and count > LISTING_COUNT_CAP
        has_next = skip + limit < count or (
            count_is_capped and len(items.get("results", [])) == limit
        )
        if has_next:
            items["next"] = f"/{collection}/filter?skip={skip + limit}&limit={limit}"
        if skip > 0:
            items["previous"] = (
                f"/{collection}/filter?skip={max(0, skip - limit)}&limit={limit}"
            )
        if collection in [
            "entities",
            "mediafiles",
            "entities_actual",
            "bibliographic_entities_actual",
        ]:
            items["results"] = self._inject_api_urls_into_entities(items["results"])
        return items

    def _add_cors_headers(self):
        if request:

            @after_this_request
            def add_header(response):
                response.headers["Access-Control-Allow-Origin"] = "*"
                return response

    def _execute_advanced_search_with_query(
        self, query, collection="entities", order_by=None, asc=True
    ):
        from filters.filter_manager import FilterManager

        self.filter_engine = FilterManager().get_filter_engine()

        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)

        self._add_cors_headers()

        if not self.filter_engine:
            abort(500, message="Failed to init search engine")
        self.validate_advanced_query_syntax(query)

        items = self.filter_engine.filter(query, skip, limit, collection, order_by, asc)
        return self._add_pagination_links(items, skip, limit, collection)

    @tracer.start_as_current_span(
        "base.BaseFilterResource._execute_advanced_search_with_query_v2"
    )
    def _execute_advanced_search_with_query_v2(
        self, query, collection="entities", *, skip=None, limit=None
    ):
        order_by = request.args.get("order_by", None) if request else None
        asc = bool(request.args.get("asc", 1, int)) if request else 1
        exact_count = (
            bool(request.args.get("exact_count", 0, int)) if request else False
        )
        if request:
            skip = skip if skip is not None else request.args.get("skip", 0, int)
            limit = limit if limit is not None else request.args.get("limit", 20, int)
        else:
            skip = skip if skip else 0
            limit = limit if limit else 20

        self._add_cors_headers()

        if not self.filter_engine_v2:
            abort(500, message="Failed to init search engine")

        items = self.filter_engine_v2.filter(
            query, skip, limit, collection, order_by, asc, exact_count=exact_count
        )
        return self._add_pagination_links(items, skip, limit, collection)

    @tracer.start_as_current_span(
        "base.BaseFilterResource._execute_typesense_accelerated_search"
    )
    def _execute_typesense_accelerated_search(
        self, query, collection, typesense_config
    ):
        if not typesense_config.get("enabled"):
            return self._execute_advanced_search_with_query_v2(query, collection)

        skip = request.args.get("skip", 0, int) if request else 0
        limit = request.args.get("limit", 20, int) if request else 20
        order_by = request.args.get("order_by", None) if request else None
        asc = bool(request.args.get("asc", 1, int)) if request else 1
        exact_count = (
            bool(request.args.get("exact_count", 0, int)) if request else False
        )

        # Filter-dropdown options (distinct_by) on a faceted field come from a
        # Typesense facet instead of a Mongo $group scan over the whole collection.
        distinct_by = get_distinct_by(query)
        if distinct_by and distinct_by in typesense_config.get("facet_fields", []):
            return self._execute_typesense_distinct_options(
                query, collection, typesense_config, distinct_by, skip, limit, asc
            )

        text_filters, type_filter_values, exact_match_filters, remaining_filters = (
            self._classify_filters_for_typesense(query)
        )

        # Resolve lookup/relation filters via Typesense before main search
        resolved_text_filters = []
        has_resolved_lookups = False
        for f in text_filters:
            key = f.get("key", "")
            if isinstance(key, list):
                key = key[0].split("|")[-1] if key else ""
            is_relation_filter = f.get("lookup") or (
                ".ref_" in key and key.endswith(".key")
            )
            if is_relation_filter:
                resolved = self._resolve_lookup_via_typesense(f, typesense_config)
                if resolved:
                    remaining_filters.append(resolved)
                    has_resolved_lookups = True
                else:
                    remaining_filters.append(f)
            else:
                resolved_text_filters.append(f)
        text_filters = resolved_text_filters

        search_fields = set(typesense_config.get("search_fields", []))
        ts_text_filters = []
        for f in text_filters:
            key = f.get("key", "")
            keys = key if isinstance(key, list) else [key]
            bare_keys = [k.split("|")[-1] for k in keys]
            # Resolve via Typesense only when every key is indexed; otherwise a
            # dropped OR branch would silently narrow results, so defer the whole
            # filter to the MongoDB engine which handles multi-key OR correctly.
            if bare_keys and all(k in search_fields for k in bare_keys):
                ts_text_filters.append(f)
            else:
                if f.get("operator") == "or":
                    log.warning(
                        f"Text filter on non-indexed field(s) '{bare_keys}' with operator 'or' "
                        f"dropped — OR semantics not supported as "
                        f"remaining_filter. Add to search_fields config to fix."
                    )
                else:
                    remaining_filters.append(f)
        text_filters = ts_text_filters

        ts_exact_match_filters = []
        for f in exact_match_filters:
            key = f.get("key", "")
            keys = key if isinstance(key, list) else [key]
            bare_keys = [k.split("|")[-1] for k in keys]
            # Resolve via Typesense only when every key is indexed; otherwise a
            # dropped OR branch would silently narrow results, so defer the whole
            # filter to the MongoDB engine which handles multi-key OR correctly.
            if bare_keys and all(k in search_fields for k in bare_keys):
                flat_keys = [k.replace(".", "_") for k in bare_keys]
                ts_exact_match_filters.append(
                    (flat_keys if len(flat_keys) > 1 else flat_keys[0], f.get("value"))
                )
            else:
                remaining_filters.append(f)

        if not text_filters and not ts_exact_match_filters:
            if has_resolved_lookups:
                type_filters = [
                    f
                    for f in query
                    if f.get("type") == "type"
                    or (f.get("type") == "selection" and f.get("key") == "type")
                ]
                resolved_query = type_filters + remaining_filters
                mongo_collections = self._resolve_mongo_collections(
                    type_filter_values, collection
                )
                target_collection = next(iter(mongo_collections), collection)
                return self._execute_advanced_search_with_query_v2(
                    resolved_query, target_collection
                )
            return self._execute_advanced_search_with_query_v2(query, collection)
        ts_result = self._relation_aware_search(
            text_filters,
            type_filter_values,
            typesense_config,
            ts_exact_match_filters,
            bool(remaining_filters),
            skip,
            limit,
        )
        if ts_result is None:
            (
                ts_collection,
                query_by,
                search_terms,
                filter_by,
                group_by,
                infix_fields,
                search_options,
            ) = self._build_typesense_query(
                text_filters,
                type_filter_values,
                typesense_config,
                exact_match_filters=ts_exact_match_filters,
            )

            facet_fields = typesense_config.get("facet_fields", [])
            facet_by = (
                ",".join(f.replace(".", "_") for f in facet_fields)
                if facet_fields
                else None
            )

            ts_result = self._execute_typesense_search(
                ts_collection,
                search_terms,
                query_by,
                filter_by,
                bool(remaining_filters),
                skip,
                limit,
                facet_by=facet_by,
                group_by=group_by,
                infix_fields=infix_fields,
                search_options=search_options,
            )
        if ts_result is None:
            log.info("Typesense unavailable, falling back to MongoDB")
            return self._execute_advanced_search_with_query_v2(query, collection)

        matching_ids, total_count = ts_result["ids"], ts_result["count"]
        highlights = ts_result.get("highlights")

        if not matching_ids:
            return {"results": [], "count": total_count, "skip": skip, "limit": limit}

        if remaining_filters:
            remaining_filters.append(
                {
                    "type": "selection",
                    "key": "_id",
                    "value": matching_ids,
                    "match_exact": True,
                }
            )
            items = self.filter_engine_v2.filter(
                remaining_filters,
                skip,
                limit,
                collection,
                order_by,
                asc,
                exact_count=exact_count,
            )
            if highlights:
                items["highlights"] = highlights
        else:
            mongo_collections = self._resolve_mongo_collections(
                type_filter_values, collection
            )
            results = self._fetch_documents_from_mongo(matching_ids, mongo_collections)
            items = {
                "results": results,
                "count": total_count,
                "skip": skip,
                "limit": limit,
            }
            if "facets" in ts_result:
                items["facets"] = ts_result["facets"]
            if highlights:
                items["highlights"] = highlights

        self._add_cors_headers()
        return self._add_pagination_links(items, skip, limit, collection)

    @tracer.start_as_current_span(
        "base.BaseFilterResource._execute_typesense_distinct_options"
    )
    def _execute_typesense_distinct_options(
        self, query, collection, typesense_config, distinct_by, skip, limit, asc
    ):
        """Resolve a distinct_by dropdown via a Typesense group_by instead of $group.

        A distinct_by request wants one entry per distinct value of the field (a
        filter dropdown). Computing it with a Mongo $group scans the whole
        collection. For a faceted field, a group_by search returns one
        representative document id per distinct value in one Typesense call; those
        ids are sorted by value, paginated, and hydrated from Mongo BY _id (which
        is indexed) — not by the field value (which is not), keeping both the
        lookup and the entity-shaped response cheap.

        Falls back to the Mongo engine when the query carries filters Typesense
        cannot express (so the values would be inaccurate) or when Typesense is
        unavailable.
        """
        text_filters, type_filter_values, exact_match_filters, remaining_filters = (
            self._classify_filters_for_typesense(query)
        )
        # Anything Typesense can't express would skew the values -> use Mongo.
        if remaining_filters or exact_match_filters:
            return self._execute_advanced_search_with_query_v2(query, collection)

        ts_collection = typesense_config.get("collection", collection)
        flat_field = distinct_by.replace(".", "_")
        filter_by = build_filter_by(type_filter_values, [])

        pairs = typesense_group_values(ts_collection, flat_field, filter_by)
        if pairs is None:
            return self._execute_advanced_search_with_query_v2(query, collection)

        pairs.sort(key=lambda value_id: value_id[0], reverse=not asc)
        total = len(pairs)
        page_ids = [doc_id for _value, doc_id in pairs[skip : skip + limit]]

        mongo_collections = self._resolve_mongo_collections(
            type_filter_values, collection
        )
        results = self._fetch_documents_from_mongo(page_ids, mongo_collections)

        items = {"results": results, "count": total, "skip": skip, "limit": limit}
        self._add_cors_headers()
        return self._add_pagination_links(items, skip, limit, collection)

    def _execute_advanced_search_with_saved_search(
        self, id, collection="entities", order_by=None, asc=True
    ):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        return self._execute_advanced_search_with_query(
            saved_search["definition"], collection, order_by, asc
        )

    def validate_advanced_query_syntax(self, queries):
        if not isinstance(queries, list):
            abort(
                400,
                message="Filter not passed as an array",
            )
        for query in queries:
            if query.get("type") == "MinMaxInput":
                if "min" not in query["value"] and "max" not in query["value"]:
                    abort(
                        400,
                        message="MinMaxfilter must specify min and/or max value, none are specified",
                    )
                if (
                    "min" in query["value"]
                    and "max" in query["value"]
                    and query["value"]["min"] > query["value"]["max"]
                ):
                    abort(
                        400,
                        message="Min-value can not be bigger than max-value in MinMaxfilter",
                    )
