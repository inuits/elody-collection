from filters.types.filter_types import get_filter
from storage.arangostore import ArangoStorageManager


class ArangoFiltersNew(ArangoStorageManager):
    def filter(self, body, skip, limit, collection="entities"):
        if not self.db:
            raise ValueError("DB is not initialized")

        aql = self.__generate_aql_query(body, collection)
        bind = {"skip": skip, "limit": limit}
        results = self.db.aql.execute(aql, bind_vars=bind, full_count=True)
        filters = {"ids": list(results)}  # type: ignore

        items = self.get_items_from_collection(collection, 0, limit, None, filters)
        items["count"] = results.statistics()["fullCount"]  # type: ignore
        items["limit"] = limit
        return items

    def __generate_aql_query(self, filter_request_body, collection="entities"):
        aql = ""
        result_set = ""
        counter = 0

        for filter_criteria in filter_request_body:
            filter = get_filter(filter_criteria["type"])
            generated_query = filter.generate_query(filter_criteria)
            if generated_query == "":
                raise ValueError("No matcher was able to handle filter request.")

            aql += f"""
                LET results{counter} = (
                    FOR doc IN {collection}
                        {generated_query}
                        RETURN doc._id
                )
            """
            result_set = f"results{counter}"
            counter += 1

        aql += f"""
            FOR result IN {result_set}
                LIMIT @skip, @limit
                RETURN DOCUMENT(result)._key
        """
        return aql
