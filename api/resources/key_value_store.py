from app import policy_factory
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
)


class KeyValueStore(GenericObject):
    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        content = request.get_json()
        return super().post(
            "key_value_store",
            content=content,
            type="key_value_store",
        )


class KeyValueStoreDetail(GenericObjectDetail):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        return super().get("key_value_store", id)

    @policy_factory.authenticate(RequestContext(request))
    def put(self, id):
        content = request.get_json()
        return super().put(
            "key_value_store", id, content=content, type="key_value_store"
        )

    @policy_factory.authenticate(RequestContext(request))
    def patch(self, id):
        content = request.get_json()
        return super().patch(
            "key_value_store", id, content=content, type="key_value_store"
        )

    @policy_factory.authenticate(RequestContext(request))
    def delete(self, id):
        return super().delete("key_value_store", id)
