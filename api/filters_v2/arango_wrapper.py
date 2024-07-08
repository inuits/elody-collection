from filters_v2.mongo_filters import MongoFilters
from storage.arangostore import ArangoStorageManager


class ArangoWrapper(ArangoStorageManager):
    def filter(
        self,
        filter_request_body,
        skip,
        limit,
        collection="entities",
        order_by=None,
        asc=True,
    ):
        mongo_pipeline: list[dict] = MongoFilters().filter(
            filter_request_body, skip, limit, collection, order_by, asc, True
        )  # pyright: ignore

        aql = f"FOR document IN {collection}"
        for stage in mongo_pipeline:
            try:
                key = list(stage.keys())[0]
                handle = getattr(self, f"_handle_{key[1:]}_stage")
                aql = handle(stage[key], aql)
            except AttributeError:
                pass
        aql += "\nRETURN document"

        raise Exception(aql)

    def _handle_match_stage(self, match, aql, element_name="document"):
        index = 0
        get_filter_prefix = (
            lambda operator, index: f"\n{'FILTER' if index == 0 else operator}"
        )

        for key, value in match.items():
            if key == "$or":
                or_index = 0
                for or_match in match["$or"]:
                    for or_key, or_value in or_match.items():
                        if isinstance(or_value, dict) and or_value.get("$all"):
                            aql = self.__handle_object_lists(
                                or_key,
                                or_value,
                                element_name,
                                aql,
                                or_index,
                                "OR",
                                get_filter_prefix,
                            )
                        else:
                            aql += f"{get_filter_prefix('OR', index)} {self.__get_comparison(or_key, or_value, element_name)}"
                    or_index += 1
            elif isinstance(value, dict) and value.get("$all"):
                aql = self.__handle_object_lists(
                    key, value, element_name, aql, 0, "AND", get_filter_prefix
                )
            else:
                aql += f"{get_filter_prefix('AND', index)} {self.__get_comparison(key, value, element_name)}"
            index += 1

        return aql

    def __handle_object_lists(
        self, key, value, element_name, aql, index, operator, get_filter_prefix
    ):
        for elem_match in value["$all"]:
            aql += f"{get_filter_prefix(operator, index)} LENGTH("
            aql += f"\nFOR item IN {element_name}.{key}"
            aql += self._handle_match_stage(elem_match["$elemMatch"], "", "item")
            aql += "\nRETURN item"
            aql += "\n) > 0"
            index += 1
        return aql

    def __get_comparison(self, key, value, element_name):
        if isinstance(value, dict):
            value_key = list(value.keys())[0]
            if value_key == "$regex":
                return f"LOWER({element_name}.{key}) LIKE '%{value['$regex']}%'"
            elif value_key == "$in":
                comparison = ""
                for i in range(len(value["$in"])):
                    comparison += f"{' OR ' if i > 0 else ''}'{value['$in'][i]}' IN {element_name}.{key}"
                return f"({element_name}.{key} IN {value['$in']} OR ({comparison}))"
        elif isinstance(value, str):
            return f"LOWER({element_name}.{key}) == LOWER('{value}')"

        return f"{element_name}.{key} == '{value}'"
