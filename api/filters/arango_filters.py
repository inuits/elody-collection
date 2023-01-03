import app

from storage.arangostore import ArangoStorageManager


class ArangoFilters(ArangoStorageManager):
    def filter(self, output_type, body, skip, limit, collection="entities"):
        aql = self.__generate_aql_query(body, collection)
        bind = {"skip": skip, "limit": limit}
        app.logger.info(aql)
        results = self.db.AQLQuery(aql, rawResults=True, fullCount=True, bindVars=bind)
        return results

    def __generate_aql_query(self, queries, collection="entities"):
        multi_select_exceptions = ["rights", "source", "publication_status"]
        counter = 0
        aql = ""
        prev_collection = collection
        types_for_next_filter = None
        if collection == "entities":
            types_for_next_filter = ["asset"]
        ignore_previous_results = True
        no_explicit_filters = True
        type_query = None
        for query in queries:
            if query["type"] == "TextInput" and "value" in query:
                aql += self.__generate_text_input_query(
                    query, counter, prev_collection, types_for_next_filter
                )
                prev_collection = f"results{counter}"
                counter += 1
                ignore_previous_results = False
                no_explicit_filters = False
            elif query["type"] == "TextInput" and "item_types" in query:
                types_for_next_filter = query["item_types"]
                type_query = query
            elif query["type"] == "MultiSelectInput":
                if "item_types" in query and all(
                    item in multi_select_exceptions for item in query["item_types"]
                ):
                    aql += self.__text_input_filter_exception(
                        query, counter, prev_collection
                    )
                else:
                    aql += self.__generate_multi_select_input_query(
                        query,
                        counter,
                        prev_collection,
                        collection,
                        types_for_next_filter,
                        ignore_previous_results,
                    )
                prev_collection = f"results{counter}"
                counter += 1
                ignore_previous_results = False
                no_explicit_filters = False
            elif query["type"] == "MinMaxInput":
                if "relation_types" in query:
                    aql += self.__generate_min_max_relations_filter(
                        query,
                        counter,
                        prev_collection,
                        query["relation_types"],
                    )
                elif "metadata_field" in query:
                    aql += self.__generate_min_max_metadata_filter(
                        query,
                        counter,
                        prev_collection,
                        query["metadata_field"],
                        types_for_next_filter,
                    )
                else:
                    break
                prev_collection = f"results{counter}"
                counter += 1
                ignore_previous_results = False
                no_explicit_filters = False
        if no_explicit_filters and type_query:
            aql += self.__generate_text_input_query(
                type_query, counter, prev_collection
            )
            prev_collection = f"results{counter}"
        aql += f"""
            FOR result IN {prev_collection}
                LIMIT @skip, @limit
                {"RETURN result._key" if prev_collection in ["entities", "mediafiles"] else "RETURN DOCUMENT(result)._key"}
        """
        return aql

    def __get_text_input_metadata_filter(self, query):
        aql = ""
        if "label" in query:
            aql += f'FILTER LIKE(metadata.label, "{query["label"]}", true)\n'
        if "key" in query:
            aql += f'FILTER metadata.key == "{query["key"]}"\n'
        if "value" in query:
            aql += f'FILTER LIKE(metadata.value, "%{query["value"]}%", true)\n'
        if not aql:
            return aql
        return f"""
            FILTER e.metadata != null
            FOR metadata IN e.metadata
                {aql}
        """

    def __get_text_input_root_field_filter(self, query):
        if "key" not in query or "value" not in query:
            return ""
        field = query["key"]
        value = query["value"]
        return f"""
            FILTER LIKE(e.{field}, "%{value}%")
        """

    def __generate_text_input_query(
        self, query, counter, prev_collection, item_types=None
    ):
        root_fields = ["filename", "mimetype"]
        type_query = ""
        if "item_types" in query and len(query["item_types"]):
            type_query = f'FILTER e.type IN {query["item_types"]}'
        if item_types:
            type_query = f"FILTER e.type IN {item_types}"
        if "key" in query and query["key"] in root_fields:
            metadata_query = self.__get_text_input_root_field_filter(query)
        else:
            metadata_query = self.__get_text_input_metadata_filter(query)
        return f"""
            LET results{counter} = (
                {self.__get_prev_collection_loop(prev_collection)}
                    {type_query}
                    {metadata_query}
                    RETURN e._id
            )
        """

    def __get_multi_select_metadata_filter(
        self, query, prev_collection, type, ignore_previous_results
    ):
        aql = ""
        prev_collection_filter = ""
        type_filter = ""
        if "key" in query and query["key"]:
            aql += f'FILTER metadata.key == "{query["key"]}"\n'
        if "value" in query and len(query["value"]):
            aql += f'FILTER metadata.value IN {query["value"]}\n'
        if prev_collection != "entities" and not ignore_previous_results:
            prev_collection_filter = f"FILTER asset IN {prev_collection}"
        if ignore_previous_results:
            type_filter = f"FILTER asset.type IN {type}"
        if not aql:
            return aql
        return f"""
            FILTER e.metadata != null
                FOR metadata IN e.metadata
                    {aql}
                    FOR asset IN OUTBOUND e GRAPH assets OPTIONS {{order: 'bfs'}}
                        {type_filter}
                        {prev_collection_filter}
                        RETURN asset._id
        """

    def __generate_multi_select_input_query(
        self,
        query,
        counter,
        prev_collection,
        collection="entities",
        type=None,
        ignore_previous_results=False,
    ):
        type_query = ""
        if "item_types" in query and len(query["item_types"]):
            type_query = f'FILTER e.type IN {query["item_types"]}'
        if not type:
            type = ["asset"]
        metadata_filter = self.__get_multi_select_metadata_filter(
            query, prev_collection, type, ignore_previous_results
        )
        if not metadata_filter:
            metadata_filter = "RETURN e._id"
        return f"""
            LET results{counter} = (
                FOR e IN {collection}
                    {type_query}
                    {metadata_filter}
            )
        """

    def __text_input_filter_exception(self, query, counter, prev_collection):
        value_query = ""
        key_query = ""
        if "value" in query and len(query["value"]):
            value_query = f'FILTER metadata.value IN {query["value"]}'
        if "item_types" in query and len(query["item_types"]):
            key_query = f'FILTER metadata.key IN {query["item_types"]}'
        return f"""
            LET results{counter} = (
                {self.__get_prev_collection_loop(prev_collection)}
                    FILTER e.metadata != null
                    FOR metadata IN e.metadata
                        {key_query}
                        {value_query}
                        RETURN e._id
            )
        """

    def __generate_min_max_relations_filter(
        self, query, counter, prev_collection, relation_types
    ):
        relation_types = self.__map_relation_types(relation_types)
        min = query["value"].get("min", -1)
        max = query["value"].get("max", sys.maxsize)
        min_max_filter = self.__get_min_max_filter_query(
            relation_types, prev_collection, min, max
        )
        return f"""
            LET results{counter} = (
                {min_max_filter}
            )
        """

    def __get_min_max_filter_query(self, relation_types, prev_collection, min, max):
        aql = ""
        counter = 0
        for relation_type in relation_types:
            previous_item_filter = f"FILTER item{counter}._from == i{counter - 1}"
            previous_collection_filter = f"FILTER i{counter} IN {prev_collection}"
            aql += f"""
                FOR item{counter} IN {relation_type}
                    {previous_item_filter if counter else ""}
                    COLLECT i{counter} = item{counter}._from WITH COUNT INTO count
                    FILTER count >= {min} AND count <= {max}
                    {previous_collection_filter if prev_collection != "entities" else ""}
            """
            counter += 1
        aql += f"RETURN i{counter - 1}"
        return aql

    def __generate_min_max_metadata_filter(
        self, query, counter, prev_collection, metadata_field, item_types=None
    ):
        type_query = ""
        if item_types:
            type_query = f"FILTER e.type IN {item_types}"
        elif "item_types" in query and len(query["item_types"]):
            type_query = f'FILTER e.type IN {query["item_types"]}'
        min = query["value"].get("min", -1)
        max = query["value"].get("max", sys.maxsize)
        return f"""
            LET results{counter} = (
                {self.__get_prev_collection_loop(prev_collection)}
                    {type_query}
                    FILTER e.metadata != null
                    FOR metadata IN e.metadata
                        FILTER metadata.key == "{metadata_field}_float"
                        FILTER metadata.value >= {min} AND metadata.value <= {max}
                        RETURN e._id
            )
        """

    def __map_relation_types(self, relation_types):
        relation_types_map = {
            "mediafiles": "hasMediafile",
            "testimonies": "hasTestimony",
        }
        return [relation_types_map[x] for x in relation_types]

    def __get_prev_collection_loop(self, prev_collection):
        if prev_collection in ["entities", "mediafiles"]:
            return f"FOR e IN {prev_collection}"
        return f"""
            FOR e_id in {prev_collection}
                LET e = DOCUMENT(e_id)
        """

    # needs to be implemented again
    def __api_output(self, query_output, skip, limit, collection="entities"):
        results = list()
        ids = list(filter(lambda item: item is not None, query_output))
        if ids:
            filters = {"ids": ids}
            skip_relations = True
            results = self.get_entities(skip, limit, skip_relations, filters)
        count = query_output.extra["stats"]["fullCount"]
        api_output = {
            "count": count,
            "results": results,
            "limit": limit,
        }
        if skip + limit < count:
            api_output["next"] = f"/advanced-search?skip={skip + limit}&limit={limit}"
        if skip > 0:
            api_output[
                "previous"
            ] = f"/advanced-search?skip={max(0, skip - limit)}&limit={limit}"
        return api_output
