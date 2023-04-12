import os

from app import policy_factory
from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint, request
from flask_restful import abort, Api
from inuits_policy_based_auth import RequestContext
from resources.entity import (
    Entity,
    EntityDetail,
    EntityMetadata,
    EntityMetadataKey,
    EntityMediafiles,
    EntityMediafilesCreate,
    EntityRelations,
    EntityRelationsAll,
    EntitySetPrimaryMediafile,
    EntitySetPrimaryThumbnail,
)

api_bp = Blueprint("entity", __name__)
api = Api(api_bp)


class CoghentEntity(CoghentBaseResource, Entity):
    @policy_factory.apply_policies(
        RequestContext(request, ["read-entity", "read-entity-all"])
    )
    def get(self):
        return super().get()

    @policy_factory.apply_policies(RequestContext(request, ["create-entity"]))
    def post(self):
        return super().post()


class CoghentEntityDetail(CoghentBaseResource, EntityDetail):
    @policy_factory.apply_policies(
        RequestContext(request, ["read-entity-detail", "read-entity-detail-all"])
    )
    def get(self, id):
        return super().get(id)

    @policy_factory.apply_policies(RequestContext(request, ["update-entity"]))
    def put(self, id):
        return super().put(id)

    @policy_factory.apply_policies(RequestContext(request, ["patch-entity"]))
    def patch(self, id):
        return super().patch(id)

    @policy_factory.apply_policies(RequestContext(request, ["delete-entity"]))
    def delete(self, id):
        return super().delete(id)


class CoghentEntityMediafiles(CoghentBaseResource, EntityMediafiles):
    @policy_factory.apply_policies(
        RequestContext(
            request, ["read-entity-mediafiles", "read-entity-mediafiles-all"]
        )
    )
    def get(self, id):
        return super().get(id)

    @policy_factory.apply_policies(RequestContext(request, ["add-entity-mediafiles"]))
    def post(self, id):
        return super().post(id)


class CoghentEntityMediafilesCreate(CoghentBaseResource, EntityMediafilesCreate):
    @policy_factory.apply_policies(RequestContext(request, ["create-entity-mediafile"]))
    def post(self, id):
        return super().post(id)


class CoghentEntityMetadata(CoghentBaseResource, EntityMetadata):
    @policy_factory.apply_policies(
        RequestContext(request, ["read-entity-metadata", "read-entity-metadata-all"])
    )
    def get(self, id):
        return super().get(id)

    @policy_factory.apply_policies(RequestContext(request, ["add-entity-metadata"]))
    def post(self, id):
        return super().post(id)

    @policy_factory.apply_policies(RequestContext(request, ["update-entity-metadata"]))
    def put(self, id):
        return super().put(id)

    @policy_factory.apply_policies(RequestContext(request, ["patch-entity-metadata"]))
    def patch(self, id):
        return super().patch(id)


class CoghentEntityMetadataKey(CoghentBaseResource, EntityMetadataKey):
    @policy_factory.apply_policies(
        RequestContext(
            request, ["read-entity-metadata-key", "read-entity-metadata-key-all"]
        )
    )
    def get(self, id, key):
        return super().get(id, key)

    @policy_factory.apply_policies(
        RequestContext(request, ["delete-entity-metadata-key"])
    )
    def delete(self, id, key):
        return super().delete(id, key)


class CoghentEntityRelations(CoghentBaseResource, EntityRelations):
    @policy_factory.apply_policies(
        RequestContext(request, ["read-entity-relations", "read-entity-relations-all"])
    )
    def get(self, id):
        return super().get(id)

    @policy_factory.apply_policies(RequestContext(request, ["add-entity-relations"]))
    def post(self, id):
        return super().post(id)

    @policy_factory.apply_policies(RequestContext(request, ["update-entity-relations"]))
    def put(self, id):
        return super().put(id)

    @policy_factory.apply_policies(RequestContext(request, ["patch-entity-relations"]))
    def patch(self, id):
        return super().patch(id)

    @policy_factory.apply_policies(RequestContext(request, ["delete-entity-relations"]))
    def delete(self, id):
        return super().delete(id)


class CoghentEntityRelationsAll(CoghentBaseResource, EntityRelationsAll):
    @policy_factory.apply_policies(
        RequestContext(request, ["read-entity-relations", "read-entity-relations-all"])
    )
    def get(self, id):
        return super().get(id)


class CoghentEntitySetPrimaryMediafile(CoghentBaseResource, EntitySetPrimaryMediafile):
    @policy_factory.apply_policies(
        RequestContext(request, ["set-entity-primary-mediafile"])
    )
    def put(self, id, mediafile_id):
        return super().put(id, mediafile_id)


class CoghentEntitySetPrimaryThumbnail(CoghentBaseResource, EntitySetPrimaryThumbnail):
    @policy_factory.apply_policies(
        RequestContext(request, ["set-entity-primary-thumbnail"])
    )
    def put(self, id, mediafile_id):
        return super().put(id, mediafile_id)


class EntityPermissions(CoghentBaseResource):
    @policy_factory.apply_policies(RequestContext(request, ["get-entity-permissions"]))
    def get(self, id):
        return self._get_item_permissions(id, "entities")


class EntitySixthCollectionEntityId(CoghentBaseResource):
    @policy_factory.apply_policies(
        RequestContext(request, ["get-entity-sixth-collection-entity-id"])
    )
    def get(self):
        if entity_id := os.getenv("SIXTH_COLLECTION_ID"):
            return entity_id
        if entity_id := self.storage.get_collection_item_sub_item(
            "entities", "sixth_collection", "_id"
        ):
            return entity_id
        abort(404, message="Sixth collection entity could not be found")


class EntitySixthCollectionId(CoghentBaseResource):
    @policy_factory.apply_policies(
        RequestContext(request, ["get-entity-sixth-collection-id"])
    )
    def get(self):
        return self.storage.get_sixth_collection_id()


api.add_resource(CoghentEntity, "/entities")
api.add_resource(CoghentEntityDetail, "/entities/<string:id>")
api.add_resource(CoghentEntityMediafiles, "/entities/<string:id>/mediafiles")
api.add_resource(
    CoghentEntityMediafilesCreate, "/entities/<string:id>/mediafiles/create"
)
api.add_resource(CoghentEntityMetadata, "/entities/<string:id>/metadata")
api.add_resource(
    CoghentEntityMetadataKey, "/entities/<string:id>/metadata/<string:key>"
)
api.add_resource(CoghentEntityRelations, "/entities/<string:id>/relations")
api.add_resource(CoghentEntityRelationsAll, "/entities/<string:id>/relations/all")
api.add_resource(
    CoghentEntitySetPrimaryMediafile,
    "/entities/<string:id>/set_primary_mediafile/<string:mediafile_id>",
)
api.add_resource(
    CoghentEntitySetPrimaryThumbnail,
    "/entities/<string:id>/set_primary_thumbnail/<string:mediafile_id>",
)
api.add_resource(EntityPermissions, "/entities/<string:id>/permissions")
api.add_resource(EntitySixthCollectionEntityId, "/entities/sixthcollection/entity_id")
api.add_resource(EntitySixthCollectionId, "/entities/sixthcollection/id")
