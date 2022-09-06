import string
import random

from storage.arangostore import ArangoStorageManager


class CoghentArangoStorageManager(ArangoStorageManager):
    def get_box_visits(self, skip, limit, item_type=None, ids=None):
        aql = f"""
            FOR c IN box_visits
            {"FILTER c._key IN @ids" if ids else ""}
            {f'FILTER c.type == "{item_type}"' if item_type else ""}
        """
        aql2 = """
            LET new_metadata = (
                FOR item,edge IN OUTBOUND c GRAPH 'assets'
                    FILTER edge._id NOT LIKE 'hasMediafile%'
                    LET relation = {'key': edge._to, 'type': edge.type}
                    RETURN HAS(edge, 'label') ? MERGE(relation, {'label': IS_NULL(edge.label.`@value`) ? edge.label : edge.label.`@value`}) : relation
            )
            LET all_metadata = {'metadata': APPEND(c.metadata, new_metadata)}
            LIMIT @skip, @limit
            RETURN MERGE(c, all_metadata)
        """
        bind = {"skip": skip, "limit": limit}
        if ids:
            bind["ids"] = ids
        results = self.db.AQLQuery(
            f"{aql}{aql2}", rawResults=True, bindVars=bind, fullCount=True
        )
        items = dict()
        items["count"] = results.extra["stats"]["fullCount"]
        items["results"] = list(results)
        if ids:
            items["results"] = [
                result_item
                for i in ids
                for result_item in items["results"]
                if result_item["_key"] == i
            ]
        return items

    def __generate_unique_code(self):
        codes = ["".join(random.choices(string.digits, k=8)) for i in range(5)]
        aql = """
            FOR bv IN @@collection
                FILTER bv.code IN @code_list
                RETURN bv.code
        """
        bind = {"@collection": "box_visits", "code_list": codes}
        results = list(self.db.AQLQuery(aql, rawResults=True, bindVars=bind))
        return next((x for x in codes if x not in results), None)

    def generate_box_visit_code(self):
        code = self.__generate_unique_code()
        while not code:
            code = self.__generate_unique_code()
        return code

    def get_sixth_collection_id(self):
        aql = f"""
            FOR e IN entities
                FILTER e.type == 'asset'
                FILTER e.object_id LIKE 'cogent:CG_%'
                RETURN TO_NUMBER(LAST(SPLIT(e.object_id, "_")))
        """
        used_ids = {*list(range(1000)), 100000}
        used_ids.update(list(self.db.AQLQuery(aql, rawResults=True)))
        return f"cogent:CG_{str(min(set(range(1, max(used_ids) + 1)) - used_ids)).rjust(5, '0')}"
