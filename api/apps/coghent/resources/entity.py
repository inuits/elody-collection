import app
import os

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import abort, Api
from inuits_jwt_auth.authorization import current_token
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
    @app.require_oauth(permissions=["read-entity", "read-entity-all"])
    def get(self):
        return super().get()

    @app.require_oauth("create-entity")
    def post(self):
        return super().post()


class CoghentEntityDetail(CoghentBaseResource, EntityDetail):
    @app.require_oauth(permissions=["read-entity-detail", "read-entity-detail-all"])
    def get(self, id):
        return super().get(id)

    @app.require_oauth("update-entity")
    def put(self, id):
        return super().put(id)

    @app.require_oauth("patch-entity")
    def patch(self, id):
        return super().patch(id)

    @app.require_oauth("delete-entity")
    def delete(self, id):
        return super().delete(id)


class CoghentEntityMediafiles(CoghentBaseResource, EntityMediafiles):
    @app.require_oauth(
        permissions=["read-entity-mediafiles", "read-entity-mediafiles-all"]
    )
    def get(self, id):
        return super().get(id)

    @app.require_oauth("add-entity-mediafiles")
    def post(self, id):
        return super().post(id)


class CoghentEntityMediafilesCreate(CoghentBaseResource, EntityMediafilesCreate):
    @app.require_oauth("create-entity-mediafile")
    def post(self, id):
        return super().post(id)


class CoghentEntityMetadata(CoghentBaseResource, EntityMetadata):
    @app.require_oauth(permissions=["read-entity-metadata", "read-entity-metadata-all"])
    def get(self, id):
        return super().get(id)

    @app.require_oauth("add-entity-metadata")
    def post(self, id):
        return super().post(id)

    @app.require_oauth("update-entity-metadata")
    def put(self, id):
        entity = self._abort_if_item_doesnt_exist("entities", id)
        content = self._get_request_body()
        if (
            entity["type"] != "testimony"
            or not any(x["key"] == "likes" for x in content)
            or not app.require_oauth.check_permission("like-testimony")
        ):
            if self._only_own_items():
                self._abort_if_no_access(entity, current_token)
        metadata = self.storage.update_collection_item_sub_item(
            "entities", self._get_raw_id(entity), "metadata", content
        )
        self._signal_entity_changed(entity)
        return metadata, 201

    @app.require_oauth("patch-entity-metadata")
    def patch(self, id):
        return super().patch(id)


class CoghentEntityMetadataKey(CoghentBaseResource, EntityMetadataKey):
    @app.require_oauth(
        permissions=["read-entity-metadata-key", "read-entity-metadata-key-all"]
    )
    def get(self, id, key):
        return super().get(id, key)

    @app.require_oauth("delete-entity-metadata-key")
    def delete(self, id, key):
        return super().delete(id, key)


class CoghentEntityRelations(CoghentBaseResource, EntityRelations):
    @app.require_oauth(
        permissions=["read-entity-relations", "read-entity-relations-all"]
    )
    def get(self, id):
        return super().get(id)

    @app.require_oauth("add-entity-relations")
    def post(self, id):
        return super().post(id)

    @app.require_oauth("update-entity-relations")
    def put(self, id):
        return super().put(id)

    @app.require_oauth("patch-entity-relations")
    def patch(self, id):
        return super().patch(id)

    @app.require_oauth("delete-entity-relations")
    def delete(self, id):
        return super().delete(id)


class CoghentEntityRelationsAll(CoghentBaseResource, EntityRelationsAll):
    @app.require_oauth(
        permissions=["read-entity-relations", "read-entity-relations-all"]
    )
    def get(self, id):
        return super().get(id)


class CoghentEntitySetPrimaryMediafile(CoghentBaseResource, EntitySetPrimaryMediafile):
    @app.require_oauth("set-entity-primary-mediafile")
    def put(self, id, mediafile_id):
        return super().put(id, mediafile_id)


class CoghentEntitySetPrimaryThumbnail(CoghentBaseResource, EntitySetPrimaryThumbnail):
    @app.require_oauth("set-entity-primary-thumbnail")
    def put(self, id, mediafile_id):
        return super().put(id, mediafile_id)


class EntityPermissions(CoghentBaseResource):
    @app.require_oauth("get-entity-permissions")
    def get(self, id):
        return self._get_item_permissions(id, "entities")


class EntitySixthCollectionEntityId(CoghentBaseResource):
    @app.require_oauth("get-entity-sixth-collection-entity-id")
    def get(self):
        if entity_id := os.getenv("SIXTH_COLLECTION_ID"):
            return entity_id
        if entity_id := self.storage.get_collection_item_sub_item(
            "entities", "sixth_collection", "_id"
        ):
            return entity_id
        abort(404, message="Sixth collection entity could not be found")


class EntitySixthCollectionId(CoghentBaseResource):
    @app.require_oauth("get-entity-sixth-collection-id")
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
