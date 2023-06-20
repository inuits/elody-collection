from filters.types.filter_types import get_filter
from storage.arangostore import ArangoStorageManager


class ArangoFilters(ArangoStorageManager):
    def filter(self, body, skip, limit, collection="entities", order_by=None, asc=True):
        if not self.db:
            raise ValueError("DB is not initialized")

        aql = self.__generate_aql_query(body, collection, order_by, asc)
        bind = {"skip": skip, "limit": limit}
        results = self.db.aql.execute(aql, bind_vars=bind, full_count=True)
        filters = {"ids": list(results)}  # type: ignore

        items = self.get_items_from_collection(collection, 0, limit, None, filters)
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
        result_set = ""
        counter = 0

        for filter_criteria in filter_request_body:
            filter = get_filter(filter_criteria["type"])
            generated_query = filter.generate_query(filter_criteria)
            if generated_query == "":
                raise ValueError("No matcher was able to handle filter request.")

            item_types = filter_criteria.get("item_types", [])
            aql += f"""
                LET results{counter} = (
                    FOR doc IN {collection}
                        {
                            f'FILTER doc.type IN {item_types}'
                            if collection == "entities" and len(item_types) > 0
                            else ""
                        }
                        {generated_query}
                        RETURN doc._id
                )
            """
            result_set = f"results{counter}"
            counter += 1

        aql += f"""
            FOR result IN {result_set}
                {f'SORT c.{order_by} {"ASC" if asc else "DESC"}' if order_by else ""}
                LIMIT @skip, @limit
                RETURN DOCUMENT(result)._key
        """
        return aql
