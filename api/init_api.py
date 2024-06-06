from configuration import get_route_mapper
from flask_restful import Api
from resources.batch import Batch
from resources.config import Config
from resources.entity import (
    Entity,
    EntityDetail,
    EntityMediafiles,
    EntityMediafilesCreate,
    EntityMetadata,
    EntityMetadataKey,
    EntityRelations,
    EntityRelationsAll,
    EntitySetPrimaryMediafile,
    EntitySetPrimaryThumbnail,
)
from resources.filter import (
    FilterEntities,
    FilterEntitiesBySavedSearchId,
    FilterEntitiesV2,
    FilterGenericObjects,
    FilterGenericObjectsBySavedSearchId,
    FilterGenericObjectsV2,
    FilterMatchers,
    FilterMediafiles,
    FilterMediafilesBySavedSearchId,
    FilterMediafilesV2,
)
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
    GenericObjectDetailV2,
    GenericObjectMetadata,
    GenericObjectMetadataKey,
    GenericObjectRelations,
    GenericObjectV2,
)
from resources.history import History
from resources.job import Job, JobDetail
from resources.key_value_store import KeyValueStore, KeyValueStoreDetail
from resources.mediafile import (
    Mediafile,
    MediafileAssets,
    MediafileCopyright,
    MediafileDerivatives,
    MediafileDetail,
    MediafileMetadata,
    MediafileParent,
)
from resources.saved_search import SavedSearch, SavedSearchDetail
from resources.spec import AsyncAPISpec, OpenAPISpec
from resources.tenant import Tenant
from resources.ticket import Ticket, TicketDetail


def init_api(app):
    api = Api(app)

    api.add_resource(
        AsyncAPISpec,
        get_route_mapper().get(
            AsyncAPISpec.__name__, "/spec/dams-collection-api-events.html"
        ),
    )
    api.add_resource(Batch, get_route_mapper().get(Batch.__name__, "/batch"))
    api.add_resource(Config, get_route_mapper().get(Config.__name__, "/config"))

    api.add_resource(Entity, get_route_mapper().get(Entity.__name__, "/entities"))
    api.add_resource(
        EntityDetail,
        get_route_mapper().get(EntityDetail.__name__, "/entities/<string:id>"),
    )
    api.add_resource(
        EntityMediafiles,
        get_route_mapper().get(
            EntityMediafiles.__name__, "/entities/<string:id>/mediafiles"
        ),
    )
    api.add_resource(
        EntityMediafilesCreate,
        get_route_mapper().get(
            EntityMediafilesCreate.__name__, "/entities/<string:id>/mediafiles/create"
        ),
    )
    api.add_resource(
        EntityMetadata,
        get_route_mapper().get(
            EntityMetadata.__name__, "/entities/<string:id>/metadata"
        ),
    )
    api.add_resource(
        EntityMetadataKey,
        get_route_mapper().get(
            EntityMetadataKey.__name__, "/entities/<string:id>/metadata/<string:key>"
        ),
    )
    api.add_resource(
        EntityRelations,
        get_route_mapper().get(
            EntityRelations.__name__, "/entities/<string:id>/relations"
        ),
    )
    api.add_resource(
        EntityRelationsAll,
        get_route_mapper().get(
            EntityRelationsAll.__name__, "/entities/<string:id>/relations/all"
        ),
    )
    api.add_resource(
        EntitySetPrimaryMediafile,
        get_route_mapper().get(
            EntitySetPrimaryMediafile.__name__,
            "/entities/<string:id>/set_primary_mediafile/<string:mediafile_id>",
        ),
    )
    api.add_resource(
        EntitySetPrimaryThumbnail,
        get_route_mapper().get(
            EntitySetPrimaryThumbnail.__name__,
            "/entities/<string:id>/set_primary_thumbnail/<string:mediafile_id>",
        ),
    )

    api.add_resource(
        FilterEntities,
        get_route_mapper().get(FilterEntities.__name__, "/entities/filter"),
    )
    api.add_resource(
        FilterEntitiesBySavedSearchId,
        get_route_mapper().get(
            FilterEntitiesBySavedSearchId.__name__, "/entities/filter/<string:id>"
        ),
    )
    api.add_resource(
        FilterEntitiesV2,
        get_route_mapper().get(FilterEntitiesV2.__name__, "/entities/filter_v2"),
    )
    api.add_resource(
        FilterGenericObjects,
        get_route_mapper().get(
            FilterGenericObjects.__name__, "/<string:collection>/filter"
        ),
    )
    api.add_resource(
        FilterGenericObjectsBySavedSearchId,
        get_route_mapper().get(
            FilterGenericObjectsBySavedSearchId.__name__,
            "/<string:collection>/filter/<string:id>",
        ),
    )
    api.add_resource(
        FilterGenericObjectsV2,
        get_route_mapper().get(
            FilterGenericObjectsV2.__name__, "/<string:collection>/filter_v2"
        ),
    )
    api.add_resource(
        FilterMatchers,
        get_route_mapper().get(FilterMatchers.__name__, "/filter/matchers"),
    )
    api.add_resource(
        FilterMediafiles,
        get_route_mapper().get(FilterMediafiles.__name__, "/mediafiles/filter"),
    )
    api.add_resource(
        FilterMediafilesBySavedSearchId,
        get_route_mapper().get(
            FilterMediafilesBySavedSearchId.__name__, "/mediafiles/filter/<string:id>"
        ),
    )
    api.add_resource(
        FilterMediafilesV2,
        get_route_mapper().get(FilterMediafilesV2.__name__, "/mediafiles/filter_v2"),
    )

    api.add_resource(
        GenericObject,
        get_route_mapper().get(GenericObject.__name__, "/<string:collection>"),
    )
    api.add_resource(
        GenericObjectDetail,
        get_route_mapper().get(
            GenericObjectDetail.__name__, "/<string:collection>/<string:id>"
        ),
    )
    api.add_resource(
        GenericObjectDetailV2,
        get_route_mapper().get(
            GenericObjectDetailV2.__name__, "/v2/<string:collection>/<string:id>"
        ),
    )
    api.add_resource(
        GenericObjectMetadata,
        get_route_mapper().get(
            GenericObjectMetadata.__name__, "/<string:collection>/<string:id>/metadata"
        ),
    )
    api.add_resource(
        GenericObjectMetadataKey,
        get_route_mapper().get(
            GenericObjectMetadataKey.__name__,
            "/<string:collection>/<string:id>/metadata/<string:key>",
        ),
    )
    api.add_resource(
        GenericObjectRelations,
        get_route_mapper().get(
            GenericObjectRelations.__name__,
            "/<string:collection>/<string:id>/relations",
        ),
    )
    api.add_resource(
        GenericObjectV2,
        get_route_mapper().get(GenericObjectV2.__name__, "/v2/<string:collection>"),
    )

    api.add_resource(
        History,
        get_route_mapper().get(
            History.__name__, "/history/<string:collection>/<string:id>"
        ),
    )

    api.add_resource(Job, get_route_mapper().get(Job.__name__, "/jobs"))
    api.add_resource(
        JobDetail, get_route_mapper().get(JobDetail.__name__, "/jobs/<string:id>")
    )

    api.add_resource(
        KeyValueStore,
        get_route_mapper().get(KeyValueStore.__name__, "/key_value_store"),
    )
    api.add_resource(
        KeyValueStoreDetail,
        get_route_mapper().get(
            KeyValueStoreDetail.__name__, "/key_value_store/<string:id>"
        ),
    )

    api.add_resource(
        Mediafile, get_route_mapper().get(Mediafile.__name__, "/mediafiles")
    )
    api.add_resource(
        MediafileAssets,
        get_route_mapper().get(
            MediafileAssets.__name__, "/mediafiles/<string:id>/assets"
        ),
    )
    api.add_resource(
        MediafileCopyright,
        get_route_mapper().get(
            MediafileCopyright.__name__, "/mediafiles/<string:id>/copyright"
        ),
    )
    api.add_resource(
        MediafileDerivatives,
        get_route_mapper().get(
            MediafileDerivatives.__name__, "/mediafiles/<string:id>/derivatives"
        ),
    )
    api.add_resource(
        MediafileDetail,
        get_route_mapper().get(MediafileDetail.__name__, "/mediafiles/<string:id>"),
    )
    api.add_resource(
        MediafileMetadata,
        get_route_mapper().get(
            MediafileMetadata.__name__, "/mediafiles/<string:id>/metadata"
        ),
    )
    api.add_resource(
        MediafileParent,
        get_route_mapper().get(
            MediafileParent.__name__, "/mediafiles/<string:id>/parent"
        ),
    )

    api.add_resource(
        OpenAPISpec,
        get_route_mapper().get(OpenAPISpec.__name__, "/spec/dams-collection-api.json"),
    )

    api.add_resource(
        SavedSearch, get_route_mapper().get(SavedSearch.__name__, "/saved_searches")
    )
    api.add_resource(
        SavedSearchDetail,
        get_route_mapper().get(
            SavedSearchDetail.__name__, "/saved_searches/<string:id>"
        ),
    )

    api.add_resource(Tenant, get_route_mapper().get(Tenant.__name__, "/tenants"))

    api.add_resource(Ticket, get_route_mapper().get(Ticket.__name__, "/tickets"))
    api.add_resource(
        TicketDetail,
        get_route_mapper().get(TicketDetail.__name__, "/tickets/<string:id>"),
    )
