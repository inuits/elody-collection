from flask import request
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
)


class KeyValueStore(GenericObject):
    @apply_policies(RequestContext(request))
    def post(self):
        content = request.get_json()
        return super().post(
            "key_value_store",
            content=content,
            type="key_value_store",
        )


class KeyValueStoreDetail(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        return super().get("key_value_store", id)

    @apply_policies(RequestContext(request))
    def put(self, id):
        content = request.get_json()
        return super().put(
            "key_value_store", id, content=content, type="key_value_store"
        )

    @apply_policies(RequestContext(request))
    def patch(self, id):
        content = request.get_json()
        return super().patch(
            "key_value_store", id, content=content, type="key_value_store"
        )

    @apply_policies(RequestContext(request))
    def delete(self, id):
        return super().delete("key_value_store", id)
