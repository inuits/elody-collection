from filters.types.filter_types import get_filter
from storage.arangostore import ArangoStorageManager
import app

class ArangoFilters(ArangoStorageManager):
    def filter(self, body, skip, limit, collection="entities", order_by=None, asc=True):
        if not self.db:
            raise ValueError("DB is not initialized")

        aql = self.__generate_aql_query(body, collection, order_by, asc)
        
        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG 1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 1

        # aql = """
        #  LET results0 = (
        #         FOR doc IN entities     
        #         FILTER (IS_ARRAY(doc.type) AND "asset" IN doc.type)
        #             OR (doc.type == "asset")
        #                     RETURN doc
        # )
        
        # FOR result IN results0
        #     LET sortField = FIRST(
        #         FOR meta IN result.metadata 
        #         FILTER meta.key == "title"
        #         RETURN meta.value
        #     )
            
        #     SORT sortField DESC
        #     LIMIT @skip, @limit
        #     RETURN result._key
        # """
                
        bind = {"skip": skip, "limit": limit}
        
        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG HARDCODED ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 1
        app.logger.error(f"{ aql }")

        results = self.db.aql.execute(aql, bind_vars=bind, full_count=True) 
        
        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG HARDCODED RESULTS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 1
        app.logger.error(f"{ results }")

        ids_list = list(results)
        
        filters = {"ids": ids_list}  # type: ignore

        id_position_map = {str(doc): index for index, doc in enumerate(ids_list)}

        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG 1.1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 1.1

        items = self.get_items_from_collection(collection, 0, limit, None, filters)

        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG 1.2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 1.2

        items["results"] = sorted(items["results"], key=lambda x: id_position_map.get(x["_key"]))

        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG 1.3 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 1.3

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

        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG 1.4 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 1.4

        return items

    def __generate_aql_query(
        self, filter_request_body, collection="entities", order_by=None, asc=True
    ):
        aql = ""
        result_sets = []
        counter = 0
        collection_or_result_set = collection
        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG 2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 2

        for index, filter_criteria in enumerate(filter_request_body):
            
            filter = get_filter(filter_criteria["type"])
            generated_query = filter.generate_query(filter_criteria)
            if generated_query == "":
                raise ValueError("No matcher was able to handle filter request.")

            item_types = filter_criteria.get("item_types", [])
            result_set = f"results{counter}"

            if filter_criteria.get("operator", "and") != "or" and index > 0:
                collection_or_result_set = f"results{counter - 1}"
            
            app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG 2.{index+1} ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 2.1, 2.2, ...

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

        final_result = []
        if result_sets:
            final_result = result_sets[-1]
        
        if filter_request_body and filter_criteria.get("operator", "and") == "or":
            if len(result_sets) > 1:
                final_result = f"UNION_DISTINCT({', '.join(result_sets)})"
                
        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG 3 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 3

        aql += f"""
            FOR result IN {final_result if final_result else collection}
                FILTER HAS(result, 'metadata')
                LET sortField = FIRST(
                    FOR meta IN result.metadata 
                    FILTER meta.key == "{order_by}"
                    RETURN meta
                )
                {f'SORT sortField {"ASC" if asc else "DESC"}' if order_by else ""}
                LIMIT @skip, @limit
                RETURN result._key
        """


        
        app.logger.error(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~ LOG 4 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  # Log 4
        app.logger.error(f"{aql}")
        
        return aql
