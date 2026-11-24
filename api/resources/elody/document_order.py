from flask import Response, g, request
from inuits_policy_based_auth import RequestContext

from policy_factory import authenticate
from resources.base.relation_order import (
    apply_order,
    is_relation_type,
    order_rows_csv,
    parse_order_csv,
    sort_by_order,
)
from resources.elody._blueprint import api
from resources.elody.document_relations import ElodyDocumentRelations
from serialization.serialize import serialize


class ElodyDocumentOrder(ElodyDocumentRelations):
    @authenticate(RequestContext(request))
    def get(self, id, spec):
        relation_type = request.args.get("relation_type")
        child_type = request.args.get("return_type", "")
        if not relation_type:
            return "relation_type is required", 400

        # Read as a normal document GET, so its read policy and key masking apply
        request.environ["HTTP_ACCEPT"] = "application/json"
        response = self.resource.get(id=id, spec=spec)
        if not isinstance(response, tuple):
            document = response.get_json()
        elif response[1] == 200 and isinstance(response[0], dict):
            document = response[0]
        else:
            return response
        relations = [
            relation
            for relation in document.get("relations", [])
            if is_relation_type(relation, relation_type)
        ]
        rows = [(relation["key"], child_type) for relation in sort_by_order(relations)]
        return Response(order_rows_csv(rows), mimetype="text/csv")

    @authenticate(RequestContext(request))
    def post(self, id, spec):
        relation_type = request.args.get("relation_type")
        if not relation_type:
            return "relation_type is required", 400

        document = self._check_if_collection_and_item_exists(None, id)
        document = serialize(document, type=document.get("type"), to_format=spec)
        ordered_keys = parse_order_csv(request.get_data(as_text=True))
        try:
            g.content = apply_order(
                document.get("relations", []), relation_type, ordered_keys
            )
        except ValueError as error:
            return str(error), 400
        return self.put(id=id, spec=spec, document=document)


def resource_rules():
    return [
        {
            "route": "/entities/<string:id>/order",
            "resource": ElodyDocumentOrder,
            "api": api,
        },
    ]
