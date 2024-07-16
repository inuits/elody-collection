from filters_v2.helpers.arango_helper import (
    get_comparison,
    handle_object_lists,
    parse_matcher_list,
)
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
            filter_request_body, skip, limit, collection, order_by, asc, True, False
        )  # pyright: ignore

        aql = f"FOR document IN {collection}"
        for stage in mongo_pipeline:
            try:
                key = list(stage.keys())[0]
                handle = getattr(self, f"_handle_{key[1:]}_stage")
                aql = handle(stage[key], aql)
            except AttributeError:
                pass

        if aql.find("COLLECT result") >= 0:
            aql += "\nRETURN result"
        else:
            aql += "\nRETURN document"

        raise Exception(aql)

    def _handle_match_stage(
        self, match, aql, *, element_name="document", operator="AND", index=0
    ):
        get_filter_prefix = (
            lambda operator, index: f"\n{'FILTER' if index == 0 else operator}"
        )

        for key, value in match.items():
            if key == "$or":
                aql, index = parse_matcher_list(
                    match["$or"],
                    element_name,
                    "OR",
                    aql,
                    index,
                    get_filter_prefix,
                    self._handle_match_stage,
                )
            elif key == "$nor":
                aql, index = parse_matcher_list(
                    match["$nor"],
                    element_name,
                    operator,
                    aql,
                    index,
                    get_filter_prefix,
                    self._handle_match_stage,
                    is_none_matcher=True,
                )
            elif isinstance(value, dict) and value.get("$all"):
                aql, index = handle_object_lists(
                    key,
                    value,
                    f"{element_name}.{key}",
                    aql,
                    index,
                    "AND",
                    get_filter_prefix,
                    self._handle_match_stage,
                )
            else:
                aql += f"{get_filter_prefix(operator, index)}{'' if operator.endswith('(') else ' '}{get_comparison(key, value, element_name)}"
                index += 1

        return aql

    def _handle_project_stage(self, project, aql):
        map = project["options"]["$concatArrays"][0]["$map"]
        object_list = map["input"]["$filter"]["input"][1:]
        item_key = map["input"]["$filter"]["cond"]["$eq"][0].split(".")[1]
        item_value = map["in"]["$cond"]["if"]["$isArray"].split(".")[1]
        value = map["input"]["$filter"]["cond"]["$eq"][1]

        aql += "\nLET options = ("
        aql += f"\nFOR item IN document.{object_list}"
        aql += f"\nFILTER item.{item_key} == '{value}'"
        aql += f"\nRETURN {{ label: item.{item_value}, value: item.{item_value} }}"
        aql += "\n)"
        aql += "\nFOR option IN UNIQUE(options)"
        aql += "\nFILTER option.label != null"
        aql += "\nCOLLECT result = option INTO groups"

        return aql
