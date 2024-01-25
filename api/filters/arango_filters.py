from filters.types.filter_types import get_filter
from storage.arangostore import ArangoStorageManager

class ArangoFilters(ArangoStorageManager):
    def filter(self, body, skip, limit, collection="entities", order_by=None, asc=True):
        if not self.db:
            raise ValueError("DB is not initialized")

        aql = self.__generate_aql_query(body, collection, order_by, asc)
        bind = {"skip": skip, "limit": limit}
        
       # 1. list of ids
        results = self.db.aql.execute(aql, bind_vars=bind, full_count=True) 

        # 2. make ids object containing the list
        ids_list = list(results)
        
        filters = {"ids": ids_list}  # type: ignore

       # Create a mapping of IDs to their positions in the result set
        id_position_map = {str(doc): index for index, doc in enumerate(ids_list)}


        # 3. list of objects
        items = self.get_items_from_collection(collection, 0, limit, None, filters)

        # Reorder the items based on the order of IDs obtained
        items["results"] = sorted(items["results"], key=lambda x: id_position_map.get(x["_key"]))

        items["count"] = results.statistics()["fullCount"]  # type: ignore
        items["limit"] = limit

        if any(
            "provide_value_options_for_key" in value
            and value["provide_value_options_for_key"] == True
            for value in body
        ):
            options = []
            for result in items["results"]:
                for metadata in result["metadata"]:
                    if metadata["key"] == body[0]["key"]:
                        if isinstance(metadata["value"], list):
                            options.extend(metadata["value"])
                        else:
                            options.append(metadata["value"])
            items["results"] = [{"options": options}]

        return items

    def __generate_aql_query(
        self, filter_request_body, collection="entities", order_by=None, asc=True
    ):
        aql = ""
        result_sets = []
        counter = 0
        collection_or_result_set = collection

        for index, filter_criteria in enumerate(filter_request_body):
            
            filter = get_filter(filter_criteria["type"])
            generated_query = filter.generate_query(filter_criteria)
            if generated_query == "":
                raise ValueError("No matcher was able to handle filter request.")

            item_types = filter_criteria.get("item_types", [])
            result_set = f"results{counter}"

            # Determine whether to use collection or result set in the AQL query
            if filter_criteria.get("operator", "and") != "or" and index > 0:
                collection_or_result_set = f"results{counter - 1}"
            
            aql += f"""
                LET {result_set} = (
                    FOR doc IN {collection_or_result_set}
                        {
                            f'FILTER doc.type IN {item_types}'
                            if collection_or_result_set == "entities" and len(item_types) > 0
                            else ""
                        }
                        {generated_query}
                        RETURN doc
                )
            """
            result_sets.append(result_set)
            counter += 1

        # Determine the final result set based on the operator
        final_result = []
        if result_sets:
            final_result = result_sets[-1]
        
        if filter_request_body and filter_criteria.get("operator", "and") == "or":
            # If operator is OR, use UNION_DISTINCT for multiple result sets
            if len(result_sets) > 1:
                final_result = f"UNION_DISTINCT({', '.join(result_sets)})"
                
        aql += f"""
            FOR result IN {final_result if final_result else collection}
                LET sortField = FIRST(
                    FOR meta IN result.metadata 
                    FILTER meta.key == "{order_by}"
                    RETURN meta.value
                )
                {f'SORT sortField {"ASC" if asc else "DESC"}' if order_by else ""}
                LIMIT @skip, @limit
                RETURN result._key
        """
        
        return aql
