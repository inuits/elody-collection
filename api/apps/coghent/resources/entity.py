import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import abort, Api
from resources.entity import (
    EntityDetail,
    EntitySetPrimaryMediafile,
    EntitySetPrimaryThumbnail,
    EntityMetadata,
    EntityMetadataKey,
    EntityMediafiles,
    EntityMediafilesCreate,
    EntityRelationsAll,
    EntityRelations,
)

api_bp = Blueprint("entity", __name__)
api = Api(api_bp)


class CoghentEntityDetail(CoghentBaseResource, EntityDetail):
    pass


class CoghentEntityMediafiles(CoghentBaseResource, EntityMediafiles):
    pass


class CoghentEntityMediafilesCreate(CoghentBaseResource, EntityMediafilesCreate):
    pass


class CoghentEntityMetadata(CoghentBaseResource, EntityMetadata):
    pass


class CoghentEntityMetadataKey(CoghentBaseResource, EntityMetadataKey):
    pass


class CoghentEntityRelations(CoghentBaseResource, EntityRelations):
    pass


class CoghentEntityRelationsAll(CoghentBaseResource, EntityRelationsAll):
    pass


class CoghentEntitySetPrimaryMediafile(CoghentBaseResource, EntitySetPrimaryMediafile):
    pass


class CoghentEntitySetPrimaryThumbnail(CoghentBaseResource, EntitySetPrimaryThumbnail):
    pass


class EntityPermissions(CoghentBaseResource):
    @app.require_oauth("get-entity-permissions")
    def get(self, id):
        return self._get_item_permissions(id, "entities")


class EntitySixthCollectionEntityId(CoghentBaseResource):
    @app.require_oauth("get-entity-sixth-collection-entity-id")
    def get(self):
        entity_id = self.storage.get_collection_item_sub_item(
            "entities", "sixth_collection", "_id"
        )
        if not entity_id:
            abort(404, message="Sixth collection entity could not be found")
        return entity_id


class EntitySixthCollectionId(CoghentBaseResource):
    @app.require_oauth("get-entity-sixth-collection-id")
    def get(self):
        return self.storage.get_sixth_collection_id()


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
