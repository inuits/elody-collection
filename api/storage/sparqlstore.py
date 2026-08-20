"""A read-through storage engine backed by a SPARQL endpoint.

Types served by this engine are never copied into the database: every request
queries the endpoint, so the store stays the single source of truth.

Two shapes of resource are served, and a collection picks one in its own
configuration:

* **Property-scoped** (the default). A resource is one subject and its
  properties, and many of them share a named graph. This is the read-through
  case -- alerts written by a pipeline -- and it stays read-only.
* **Graph-scoped** (`"graph_per_item": True`). A resource *is* a named graph,
  `<graph>/<id>`, and may be an arbitrary sub-graph: several subjects, blank
  nodes, whole shape descriptions. A pipeline definition is one of these. The
  whole graph is handed to the collection's serializer, and because a graph is
  also the unit the Graph Store Protocol addresses, these can be **written**:
  one PUT replaces a document, one DELETE removes it.

Writing is still deliberately narrow. There is no generic elody-to-RDF
mapping, so a writable collection is one whose serializer answers
`from_elody_to_sparql` with a turtle document; an empty answer means "this
document does not belong in the store" and withdraws it. Sub-document edits
(metadata, relations) are read-modify-write on top of that, which is honest for
a store with no transactions and no locking -- the last writer wins, exactly as
the Graph Store Protocol says it does.

The engine carries no vocabulary of its own. It learns the endpoint, the named
graph and which properties identify and order a resource from the collection's
own object configuration:

    crud()["sparql"] = {
        "endpoint": "http://triplestore:3030/alerts/sparql",
        "graph": "http://mu.semte.ch/graphs/errors",
        "target_class": "http://open-services.net/ns/core#Error",
        "identifier_predicate": "http://mu.semte.ch/vocabularies/core/uuid",
        "sort_predicate": "http://purl.org/dc/terms/created",   # optional
        # graph-scoped, writable collections add:
        "graph_per_item": True,          # <graph>/<id> is one document
        "gsp_endpoint": ".../store/data",  # Graph Store Protocol, for writes
        "user": "...", "password": "...",  # basic auth, if writes are guarded
    }

Each subject it finds is handed to that configuration's serializer as
`{"iri": ..., "properties": {predicate: [value, ...]}}` -- or, graph-scoped, as
`{"iri": ..., "graph": <rdflib Graph>, "graph_name": ...}` -- so the
RDF-to-entity mapping stays with the client that owns the vocabulary.
"""

import json
import re
from copy import deepcopy
from datetime import datetime, timezone

import requests
from configuration import get_object_configuration_mapper
from logging_elody.log import log
from policy_factory import get_user_context
from rabbit import get_rabbit
from rdflib import Graph, RDF, URIRef
from serialization.serialize import serialize
from storage.genericstore import GenericStorageManager


class ExternalStorageError(Exception):
    """The store refused or could not take a write.

    Raised rather than swallowed: a write that did not happen must not answer
    the request as though it had, or a caller is shown a saved document the store
    never received.
    """

TIMEOUT = 30

WRITE_OK = (200, 201, 204)

# Identifiers are interpolated into a query string, so they are held to a
# conservative character set first: letters, digits and the punctuation that
# appears in UUIDs and slugs. Anything else -- quotes, braces, backslashes,
# angle brackets, whitespace -- could terminate the literal and start a new
# clause, so it is refused rather than escaped.
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:@-]{1,255}$")


class SparqlStorageManager(GenericStorageManager):
    def get_items_from_collection(
        self,
        collection,
        skip=0,
        limit=20,
        fields=None,
        filters=None,
        sort=None,
        asc=True,
    ):
        empty = {"results": [], "count": 0, "limit": limit, "skip": skip}
        config = self._sparql_config(collection)
        if not self._is_usable(config, collection):
            return empty

        identifiers = self._requested_identifiers(filters)
        if identifiers == []:
            # An explicit "these ids" filter that names none of them.
            return empty

        if self._is_graph_scoped(config):
            return self._graph_page(
                collection, config, skip, limit, identifiers, asc
            )

        count = self._count(config, identifiers)
        # Order is decided here and nowhere else: a CONSTRUCT returns a graph,
        # which is a set of triples with no order at all, so the sequence has
        # to be carried over from the query that chose the page.
        iris = self._page_subjects(config, skip, limit, identifiers, asc)
        if iris is None:
            return empty
        if not iris:
            return {**empty, "count": count if count is not None else 0}

        graph = self._construct(config, self._properties_query(config, iris))
        if graph is None:
            return empty

        results = self._to_documents(graph, collection, config, order=iris)
        return {
            "results": results,
            # A CONSTRUCT that returned nothing still has a real total behind
            # it; only fall back to the page size when the count query failed.
            "count": count if count is not None else len(results),
            "limit": limit,
            "skip": skip,
        }

    def get_item_from_collection_by_id(self, collection, id):
        config = self._sparql_config(collection)
        if not self._is_usable(config, collection):
            return {}
        if not id or not SAFE_IDENTIFIER.match(str(id)):
            log.debug(f"Refusing unsafe SPARQL identifier for {collection}: {id!r}")
            return {}

        if self._is_graph_scoped(config):
            return self._graph_document(collection, config, str(id))

        graph = self._construct(config, self._item_query(config, str(id)))
        if graph is None:
            return {}
        documents = self._to_documents(graph, collection, config)
        return documents[0] if documents else {}

    # -- graph-scoped documents -------------------------------------------

    @staticmethod
    def _is_graph_scoped(config) -> bool:
        return bool(config.get("graph_per_item"))

    def _item_graph(self, config, id) -> str | None:
        """The named graph one document lives in."""
        base = str(config.get("graph") or "").rstrip("/")
        if not base or not SAFE_IDENTIFIER.match(str(id)):
            return None
        return f"{base}/{id}"

    def _graph_document(self, collection, config, id) -> dict:
        """One document, read as a whole named graph.

        The graph is addressed directly rather than searched for, because the
        id is what names it. A document whose graph is empty, or that no longer
        declares the target class, is simply not there.
        """
        graph_name = self._item_graph(config, id)
        if not graph_name:
            return {}
        graph = self._construct(config, self._graph_query(graph_name))
        if graph is None or len(graph) == 0:
            return {}
        subject = self._subject_of(graph, config)
        if subject is None:
            return {}
        return self._to_document(collection, {
            "iri": str(subject),
            "graph": graph,
            "graph_name": graph_name,
        })

    def _graph_page(self, collection, config, skip, limit, identifiers, asc) -> dict:
        """A page of graph-scoped documents.

        Each document is fetched with its own CONSTRUCT: merging several named
        graphs into one result would fuse their blank nodes, and a definition
        is mostly blank nodes.
        """
        empty = {"results": [], "count": 0, "limit": limit, "skip": skip}
        graphs = self._document_graphs(config, identifiers, asc)
        if graphs is None:
            return empty

        page = graphs[int(skip) : int(skip) + int(limit)] if limit else graphs[skip:]
        results = []
        for graph_name, iri in page:
            graph = self._construct(config, self._graph_query(graph_name))
            if graph is None or len(graph) == 0:
                continue
            document = self._to_document(
                collection,
                {"iri": iri, "graph": graph, "graph_name": graph_name},
            )
            if document:
                results.append(document)
        return {
            "results": results,
            "count": len(graphs),
            "limit": limit,
            "skip": skip,
        }

    def _document_graphs(self, config, identifiers, asc) -> list | None:
        """`(graph, subject)` for every document of this collection, in order.

        Paging happens on this list rather than in the query: a page has to be
        a page of documents, and only the caller knows how many graphs one
        document is (one).
        """
        body = self._ask(
            config,
            self._graph_list_query(config, identifiers, asc),
            "application/sparql-results+json",
        )
        if body is None:
            return None
        try:
            bindings = json.loads(body)["results"]["bindings"]
        except Exception as error:
            log.warning(f"SPARQL graph list could not be read: {error}")
            return None

        graphs = []
        for binding in bindings:
            graph_name = binding.get("g", {}).get("value")
            iri = binding.get("s", {}).get("value")
            if not graph_name or self._unsafe_iri(graph_name):
                continue
            if (graph_name, iri) not in graphs:
                graphs.append((graph_name, iri))
        return graphs

    def _graph_list_query(self, config, identifiers, asc) -> str:
        """Which graphs hold a document, ordered.

        Restricted to graphs under the configured base, so a store shared with
        other data -- the alert graph sits in the same dataset -- does not leak
        into the collection.
        """
        base = str(config.get("graph") or "").rstrip("/") + "/"
        sort_predicate = config.get("sort_predicate")
        if sort_predicate and not self._unsafe_iri(sort_predicate):
            sort_pattern = f"      OPTIONAL {{ ?s <{sort_predicate}> ?sort }}\n"
            projection = "?g ?s ?sort"
            order_by = f"ORDER BY {'ASC' if asc else 'DESC'}(?sort) ?g"
        else:
            sort_pattern = ""
            projection = "?g ?s"
            order_by = "ORDER BY ?g"

        return (
            f"SELECT DISTINCT {projection}\n"
            "WHERE {\n"
            "  GRAPH ?g {\n"
            f"    ?s a <{config['target_class']}> .\n"
            f"{self._values_clause(config, identifiers)}"
            f"{sort_pattern}"
            "  }\n"
            f'  FILTER(STRSTARTS(STR(?g), "{base}"))\n'
            "}\n"
            f"{order_by}"
        )

    @staticmethod
    def _graph_query(graph_name) -> str:
        return (
            "CONSTRUCT { ?s ?p ?o }\n"
            f"WHERE {{ GRAPH <{graph_name}> {{ ?s ?p ?o }} }}"
        )

    def _subject_of(self, graph, config):
        """The subject a graph-scoped document is about."""
        target_class = URIRef(config["target_class"])
        return next(graph.subjects(RDF.type, target_class), None)

    def _to_document(self, collection, payload) -> dict:
        try:
            return serialize(
                payload, type=collection, to_format="elody", from_format="sparql"
            ) or {}
        except Exception as error:
            log.warning(
                f"Could not map graph {payload.get('graph_name')} into an entity: "
                f"{error}"
            )
            return {}

    # -- configuration ----------------------------------------------------

    def _sparql_config(self, collection) -> dict:
        try:
            return get_object_configuration_mapper().get(collection).crud()["sparql"]
        except (KeyError, AttributeError):
            return {}

    def _is_usable(self, config, collection) -> bool:
        missing = [
            key
            for key in ("endpoint", "graph", "target_class", "identifier_predicate")
            if not config.get(key)
        ]
        if missing:
            log.debug(
                f"Collection {collection} has no usable sparql configuration, "
                f"missing: {', '.join(missing) or 'everything'}"
            )
            return False
        return not any(self._unsafe_iri(config[key]) for key in ("graph", "target_class"))

    @staticmethod
    def _unsafe_iri(iri) -> bool:
        return bool(re.search(r"[\s<>\"{}|\\^`]", str(iri)))

    @staticmethod
    def _requested_identifiers(filters):
        """The `ids` restriction, if the collection's filter serializer set one."""
        if isinstance(filters, dict) and "ids" in filters:
            return [
                str(id) for id in (filters["ids"] or []) if SAFE_IDENTIFIER.match(str(id))
            ]
        return None

    # -- queries ----------------------------------------------------------

    def _values_clause(self, config, identifiers) -> str:
        if not identifiers:
            return ""
        literals = " ".join(f'"{id}"' for id in identifiers)
        return (
            f"    ?s <{config['identifier_predicate']}> ?requested .\n"
            f"    VALUES ?requested {{ {literals} }}\n"
        )

    def _count_query(self, config, identifiers) -> str:
        return (
            "SELECT (COUNT(DISTINCT ?s) AS ?count)\n"
            "WHERE {\n"
            f"  GRAPH <{config['graph']}> {{\n"
            f"    ?s a <{config['target_class']}> .\n"
            f"{self._values_clause(config, identifiers)}"
            "  }\n"
            "}"
        )

    def _page_query(self, config, skip, limit, identifiers, asc) -> str:
        """Which subjects are on this page, in order.

        Selecting subjects rather than triples is what keeps a resource whole:
        LIMIT on a CONSTRUCT would bound triples and cut the last one in half.
        The sort property is OPTIONAL so a resource lacking it still appears,
        and is projected because ORDER BY may only use projected variables.
        """
        sort_predicate = config.get("sort_predicate")
        if sort_predicate and not self._unsafe_iri(sort_predicate):
            sort_pattern = f"    OPTIONAL {{ ?s <{sort_predicate}> ?sort }}\n"
            projection = "?s ?sort"
            order_by = f"ORDER BY {'ASC' if asc else 'DESC'}(?sort) ?s"
        else:
            sort_pattern = ""
            projection = "?s"
            order_by = "ORDER BY ?s"

        return (
            f"SELECT {projection}\n"
            "WHERE {\n"
            f"  GRAPH <{config['graph']}> {{\n"
            f"    ?s a <{config['target_class']}> .\n"
            f"{self._values_clause(config, identifiers)}"
            f"{sort_pattern}"
            "  }\n"
            "}\n"
            f"{order_by}\n"
            f"OFFSET {int(skip)} LIMIT {int(limit)}"
        )

    def _properties_query(self, config, iris) -> str:
        """Every property of exactly these subjects."""
        values = " ".join(f"<{iri}>" for iri in iris)
        return (
            "CONSTRUCT { ?s ?p ?o }\n"
            "WHERE {\n"
            f"  VALUES ?s {{ {values} }}\n"
            f"  GRAPH <{config['graph']}> {{ ?s ?p ?o }}\n"
            "}"
        )

    def _item_query(self, config, id) -> str:
        return (
            "CONSTRUCT { ?s ?p ?o }\n"
            "WHERE {\n"
            f"  GRAPH <{config['graph']}> {{\n"
            f"    ?s a <{config['target_class']}> ;\n"
            f"       <{config['identifier_predicate']}> ?id ;\n"
            "       ?p ?o .\n"
            f'    FILTER(str(?id) = "{id}")\n'
            "  }\n"
            "}"
        )

    # -- writing ----------------------------------------------------------

    def _is_writable(self, config) -> bool:
        return self._is_graph_scoped(config) and bool(config.get("gsp_endpoint"))

    def _refuse_write(self, collection, config, operation) -> None:
        reason = (
            "it is not graph-scoped"
            if not self._is_graph_scoped(config)
            else "no gsp_endpoint is configured"
        )
        raise NotImplementedError(
            f"Collection {collection} cannot be written to over SPARQL "
            f"({operation}): {reason}."
        )

    def save_item_to_collection(
        self, collection, content, only_return_id=False, create_sortable_metadata=True
    ):
        content = dict(content or {})
        if not content.get("_id"):
            content["_id"] = self._get_autogenerated_id_for_item(content)
        content["identifiers"] = self._get_autogenerated_identifiers_for_item(content)
        self._store(collection, content["_id"], content)
        if only_return_id:
            return content["_id"]
        # read back rather than echo: what the store holds is what every other
        # reader will see, and it may hold less than was sent
        return self.get_item_from_collection_by_id(collection, content["_id"]) or content

    def update_item_from_collection(
        self, collection, id, content, create_sortable_metadata=True
    ):
        self._store(collection, id, content)
        return self.get_item_from_collection_by_id(collection, id) or content

    def patch_item_from_collection(
        self, collection, id, content, create_sortable_metadata=True
    ) -> dict:
        """Read, replace the keys given, write the whole document back.

        The unit the store can write is a graph, so there is no partial update
        to be had: `patch` differs from `update` only in that it starts from
        what is already there. Everything the framework builds on top of this
        -- metadata patches, relation patches, deleting one key -- is the same
        read-modify-write, which is why they need no implementation of their own.
        """
        document = self.get_item_from_collection_by_id(collection, id)
        if not document:
            return {}
        document = {**document, **(content or {})}
        self._store(collection, id, document)
        return self.get_item_from_collection_by_id(collection, id) or document

    def delete_item_from_collection(self, collection, id):
        config = self._sparql_config(collection)
        if not self._is_writable(config):
            self._refuse_write(collection, config, "delete")
        graph = self._item_graph(config, id)
        if graph:
            self._delete_graph(config, graph)

    def delete_item(self, item):
        """The v2 delete, which is handed the document rather than an id."""
        collection = self._collection_of(item)
        if collection:
            self.delete_item_from_collection(collection, item.get("_id"))

    # -- the v2 write family ----------------------------------------------
    #
    # These are the paths the Elody document resources use, and what they add
    # over their v1 counterparts is the configuration's crud hooks: a document
    # is patched, stamped and validated by its own configuration rather than by
    # the engine. That part is storage-agnostic, so it happens here too.
    #
    # What is deliberately left out is everything that is a property of a
    # database: no history collection (versioning a definition is out of scope,
    # and the store has no second collection to put one in), and no etag
    # precondition -- the Graph Store Protocol replaces a graph wholesale, so
    # the last writer wins and pretending otherwise would be a lie.

    def save_item_to_collection_v2(
        self, collection, items, *, is_history=False, run_post_crud_hook=True
    ):
        if is_history:
            # There is no history collection to write to; a definition is
            # versioned by whatever the store itself keeps, if anything.
            return items if isinstance(items, list) else items
        documents = items if isinstance(items, list) else [items]
        saved = []
        for document in documents:
            config = self._configuration_of(document)
            timestamp = datetime.now(timezone.utc)
            document = config.crud()["pre_crud_hook"](
                crud="create", timestamp=timestamp, document=document
            )
            if not document.get("_id"):
                document["_id"] = self._get_autogenerated_id_for_item(document)
            document["identifiers"] = self._get_autogenerated_identifiers_for_item(
                document
            )
            self._store(collection, document["_id"], document)
            if run_post_crud_hook:
                self._run_post_crud_hook(config, "create", document)
            saved.append(
                self.get_item_from_collection_by_id(collection, document["_id"])
                or document
            )
        return saved if isinstance(items, list) else saved[0]

    def put_item_from_collection(
        self, collection, item, content, spec, *, run_post_crud_hook=True, patched_item={}
    ):
        return self._write_v2(
            collection,
            item,
            content,
            overwrite=True,
            patched_item=patched_item,
            run_post_crud_hook=run_post_crud_hook,
        )

    def patch_item_from_collection_v2(
        self, collection, item, content, spec, *, run_post_crud_hook=True, patched_item={}
    ):
        return self._write_v2(
            collection,
            item,
            content,
            overwrite=False,
            patched_item=patched_item,
            run_post_crud_hook=run_post_crud_hook,
        )

    def _write_v2(
        self, collection, item, content, *, overwrite, patched_item, run_post_crud_hook
    ):
        """One update, patched and stamped by the document's own configuration.

        `overwrite` is what separates a PUT from a PATCH, and it is the
        configuration's patcher that acts on it -- the same call the database
        engine makes, so a form save behaves identically whichever store is
        behind it.
        """
        config = self._configuration_of(item)
        collection = collection or config.crud().get("collection")
        timestamp = datetime.now(timezone.utc)
        unpatched_item = deepcopy(item)

        document = patched_item or config.crud()["document_content_patcher"](
            document=deepcopy(item),
            content=content,
            crud="update",
            timestamp=timestamp,
            overwrite=overwrite,
        )
        if not patched_item and not config.crud()["content_changes_checker"](
            document=document, unpatched_document=unpatched_item
        ):
            return item
        document = config.crud()["pre_crud_hook"](
            crud="update",
            timestamp=timestamp,
            document=document,
            unpatched_document=unpatched_item,
        )
        self._store(collection, document.get("_id") or item.get("_id"), document)
        if run_post_crud_hook:
            self._run_post_crud_hook(
                config, "update", document, content=content,
                unpatched_document=unpatched_item,
            )
        return (
            self.get_item_from_collection_by_id(
                collection, document.get("_id") or item.get("_id")
            )
            or document
        )

    def delete_item(self, item):
        """The v2 delete, which is handed the document rather than an id."""
        config = self._configuration_of(item)
        collection = config.crud().get("collection")
        timestamp = datetime.now(timezone.utc)
        document = config.crud()["pre_crud_hook"](
            crud="delete", timestamp=timestamp, document=item
        )
        self.delete_item_from_collection(collection, (document or item).get("_id"))
        self._run_post_crud_hook(config, "delete", document or item)

    @staticmethod
    def _configuration_of(document):
        return get_object_configuration_mapper().get((document or {}).get("type"))

    def _run_post_crud_hook(self, config, crud, document, **kwargs):
        """Call the post hook with what a hook is entitled to expect.

        `get_rabbit` and `get_user_context` are passed the same way the database
        engine passes them: as the callables themselves, so a hook that has no
        use for the message queue never opens a connection to it.
        """
        config.crud()["post_crud_hook"](
            crud=crud,
            document=document,
            storage=self,
            get_user_context=get_user_context,
            get_rabbit=get_rabbit,
            **kwargs,
        )

    def add_sub_item_to_collection_item(self, collection, id, sub_item, content):
        existing = self.get_collection_item_sub_item(collection, id, sub_item) or []
        merged = list(existing)
        for entry in content or []:
            if entry not in merged:
                merged.append(entry)
        self.patch_item_from_collection(collection, id, {sub_item: merged})
        return content

    def add_relations_to_collection_item(
        self, collection, id, relations, parent=True, dst_collection=None
    ):
        """Append relations to a document.

        No inverse relation is written on the other end, unlike the database
        engine: the other end of a relation in a store like this is typically
        described somewhere else entirely, and a document names what it refers
        to without the referent having to agree.
        """
        relations = [relation for relation in relations if relation.get("type")]
        self.add_sub_item_to_collection_item(collection, id, "relations", relations)
        return relations

    def get_collection_item_relations(
        self, collection, id, include_sub_relations=False, exclude=None, order=True
    ):
        """The relations of a document, in the order the store gave them.

        No `order` metadata is sorted on, unlike the database engine: the order
        of a graph-scoped document is whatever its serializer reconstructed from
        the graph, and re-sorting here would overrule that with a field the
        store does not necessarily carry.
        """
        return self.get_collection_item_sub_item(collection, id, "relations") or []

    def get_collection_item_mediafiles(self, collection, id, *args, **kwargs):
        """None: a resource in a triple store has no uploaded files.

        Answered rather than left to the base class's `pass`, because the
        entity-detail path iterates the result and `None` is not iterable.
        """
        return []

    def collection_item_has_relation(self, collection, id, relation_type):
        return any(
            relation.get("type") == relation_type
            for relation in self.get_collection_item_relations(collection, id)
        )

    def patch_collection_item_relations(self, collection, id, content, parent=True):
        """Replace the relations named in `content`, keep the rest.

        Same rule as the database engine: a relation is identified by its key,
        so patching one rewires it rather than adding a second copy.
        """
        keys = {entry.get("key") for entry in content or []}
        existing = self.get_collection_item_sub_item(collection, id, "relations") or []
        kept = [entry for entry in existing if entry.get("key") not in keys]
        self.patch_item_from_collection(
            collection, id, {"relations": [*kept, *(content or [])]}
        )
        return content

    def update_collection_item_relations(self, collection, id, content, parent=True):
        self.patch_item_from_collection(collection, id, {"relations": content or []})
        return content

    def delete_collection_item_relations(self, collection, id, content, parent=True):
        dropped = {
            (entry.get("key"), entry.get("type")) for entry in content or []
        }
        existing = self.get_collection_item_sub_item(collection, id, "relations") or []
        self.patch_item_from_collection(
            collection,
            id,
            {
                "relations": [
                    entry
                    for entry in existing
                    if (entry.get("key"), entry.get("type")) not in dropped
                ]
            },
        )

    def _collection_of(self, item):
        try:
            return (
                get_object_configuration_mapper()
                .get(item.get("type"))
                .crud()
                .get("collection")
            )
        except Exception:
            return None

    def _store(self, collection, id, document) -> None:
        """Put one document into the store, or withdraw it.

        The turtle comes from the collection's own serializer -- there is no
        generic elody-to-RDF mapping, and inventing one here would put the
        vocabulary in the wrong place. An empty document means the serializer
        decided this one does not belong in the store, which is a withdrawal
        rather than an error: it lets a serializer refuse to store a document it
        considers unfit to publish, and have the previous version withdrawn
        rather than left behind.
        """
        config = self._sparql_config(collection)
        if not self._is_writable(config):
            self._refuse_write(collection, config, "write")
        graph = self._item_graph(config, id)
        if not graph:
            raise ValueError(f"Refusing unsafe SPARQL identifier: {id!r}")

        turtle = serialize(
            document, type=collection, to_format="sparql", from_format="elody"
        )
        if not turtle:
            self._delete_graph(config, graph)
            return
        self._put_graph(config, graph, turtle)

    # -- transport --------------------------------------------------------

    def _ask(self, config, query, accept):
        try:
            response = requests.post(
                config["endpoint"],
                data={"query": query},
                headers={"Accept": accept},
                timeout=TIMEOUT,
            )
        except requests.exceptions.RequestException as error:
            log.warning(f"SPARQL endpoint {config['endpoint']} unreachable: {error}")
            return None
        if response.status_code != 200:
            log.warning(
                f"SPARQL endpoint {config['endpoint']} answered "
                f"{response.status_code}: {response.text[:200]}"
            )
            return None
        return response.text

    @staticmethod
    def _auth(config):
        user = str(config.get("user") or "").strip()
        if not user:
            return None
        return (user, config.get("password") or "")

    def _put_graph(self, config, graph, turtle) -> bool:
        return self._write(
            config,
            "put",
            graph,
            data=turtle.encode("utf-8"),
            headers={"Content-Type": "text/turtle; charset=utf-8"},
        )

    def _delete_graph(self, config, graph) -> bool:
        # A graph that was never written is already in the state asked for,
        # which the Graph Store Protocol reports as a 404.
        return self._write(config, "delete", graph, accept_missing=True)

    def _write(self, config, method, graph, *, accept_missing=False, **kwargs) -> bool:
        endpoint = config["gsp_endpoint"]
        try:
            response = getattr(requests, method)(
                endpoint,
                params={"graph": graph},
                auth=self._auth(config),
                timeout=TIMEOUT,
                **kwargs,
            )
        except requests.exceptions.RequestException as error:
            raise ExternalStorageError(
                f"Triple store {endpoint} unreachable: {error}"
            ) from error

        if response.status_code in WRITE_OK:
            return True
        if accept_missing and response.status_code == 404:
            return True
        raise ExternalStorageError(
            f"Triple store {endpoint} answered {response.status_code} for "
            f"{method.upper()} <{graph}>: {response.text[:200]}"
        )

    def _construct(self, config, query) -> Graph | None:
        body = self._ask(config, query, "text/turtle")
        if body is None:
            return None
        graph = Graph()
        try:
            graph.parse(data=body, format="turtle")
        except Exception as error:
            log.warning(f"SPARQL endpoint returned unparseable Turtle: {error}")
            return None
        return graph

    def _page_subjects(self, config, skip, limit, identifiers, asc) -> list[str] | None:
        """The subject IRIs on this page, in the order the endpoint returned them."""
        body = self._ask(
            config,
            self._page_query(config, skip, limit, identifiers, asc),
            "application/sparql-results+json",
        )
        if body is None:
            return None
        try:
            bindings = json.loads(body)["results"]["bindings"]
        except Exception as error:
            log.warning(f"SPARQL subject page could not be read: {error}")
            return None

        iris = []
        for binding in bindings:
            iri = binding.get("s", {}).get("value")
            # A resource with a repeated sort property would appear twice;
            # keep the first occurrence so the page stays the promised length.
            if iri and iri not in iris and not self._unsafe_iri(iri):
                iris.append(iri)
        return iris

    def _count(self, config, identifiers) -> int | None:
        body = self._ask(
            config, self._count_query(config, identifiers), "application/sparql-results+json"
        )
        if body is None:
            return None
        try:
            bindings = json.loads(body)["results"]["bindings"]
            return int(bindings[0]["count"]["value"]) if bindings else 0
        except Exception as error:
            log.warning(f"SPARQL count could not be read: {error}")
            return None

    # -- mapping ----------------------------------------------------------

    def _to_documents(self, graph: Graph, collection, config, order=None) -> list[dict]:
        """Group the graph by subject and let the collection's serializer map it.

        `order` carries the sequence chosen by the paging query, since the graph
        itself has none. Subjects the graph holds but the order does not are
        appended, so nothing is silently lost.
        """
        identifier_predicate = URIRef(config["identifier_predicate"])
        present = set(graph.subjects())
        if order:
            ordered = [URIRef(iri) for iri in order if URIRef(iri) in present]
            ordered += sorted(present - set(ordered), key=str)
        else:
            ordered = sorted(present, key=str)

        documents = []
        for subject in ordered:
            properties: dict[str, list[str]] = {}
            for predicate, object in graph.predicate_objects(subject):
                properties.setdefault(str(predicate), []).append(str(object))
            if not properties.get(str(identifier_predicate)):
                # Without the identifying property there is nothing to address
                # the resource by, so it cannot become an entity.
                log.debug(f"Skipping {subject}: no <{identifier_predicate}>")
                continue
            try:
                documents.append(
                    serialize(
                        {"iri": str(subject), "properties": properties},
                        type=collection,
                        to_format="elody",
                        from_format="sparql",
                    )
                )
            except Exception as error:
                log.warning(f"Could not map {subject} into an entity: {error}")
        return documents
