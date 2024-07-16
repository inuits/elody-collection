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
        aql = self.__generate_query(collection, mongo_pipeline)
        return self.__execute_query(aql, collection, skip, limit)

    def __generate_query(self, collection, mongo_pipeline):
        aql = f"FOR document IN {collection}"
        for stage in mongo_pipeline:
            try:
                key = list(stage.keys())[0]
                handle = getattr(self, f"_handle_{key[1:]}_stage")
                aql = handle(stage[key], aql, mongo_pipeline=mongo_pipeline)
            except AttributeError:
                pass

        if aql.find("COLLECT result") >= 0:
            aql += "\nRETURN result"
        else:
            aql += "\nRETURN document"
        return aql

    def __execute_query(self, aql, collection, skip, limit):
        documents = self.db.aql.execute(aql, full_count=True)  # pyright: ignore
        items = {
            "results": [
                (
                    self.get_item_from_collection_by_id(collection, document["_id"])
                    if document.get("_id")
                    else document
                )
                for document in documents  # pyright: ignore
            ]
        }
        items["skip"] = skip
        items["limit"] = limit
        items["count"] = documents.statistics()["fullCount"]  # pyright: ignore
        return items

    def _handle_match_stage(
        self, match, aql, *, element_name="document", operator="AND", index=0, **_
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

    def _handle_skip_stage(self, skip, aql, mongo_pipeline):
        limit = [stage for stage in mongo_pipeline if stage.get("$limit") is not None][
            0
        ]["$limit"]
        if skip is None or limit is None:
            return aql

        aql += f"\nLIMIT {skip}, {limit}"
        return aql

    def _handle_project_stage(self, project, aql, **_):
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
