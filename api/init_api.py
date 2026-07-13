from api_base import CustomApi
from configuration import get_route_mapper
from resources.batch import Batch
from resources.config import Config
from resources.filter import FilterMatchers
from resources.history import History
from resources.job import (
    AddDocumentToJob,
    FailJob,
    FinishJob,
    FinishJobWithWarning,
    InitJob,
    StartJob,
)
from resources.share_link import ShareLink, ShareLinkDetail
from resources.spec import AsyncAPISpec, OpenAPISpec
from resources.tenant import Tenant
from resources.ticket import Ticket, TicketDetail


def init_api(app):
    api = CustomApi(app)

    api.add_resource(
        AsyncAPISpec,
        get_route_mapper().get(
            AsyncAPISpec.__name__, "/spec/dams-collection-api-events.html"
        ),
    )
    api.add_resource(Batch, get_route_mapper().get(Batch.__name__, "/batch"))
    api.add_resource(Config, get_route_mapper().get(Config.__name__, "/config"))

    api.add_resource(
        FilterMatchers,
        get_route_mapper().get(FilterMatchers.__name__, "/filter/matchers"),
    )

    api.add_resource(
        History,
        get_route_mapper().get(
            History.__name__, "/history/<string:collection>/<string:id>"
        ),
    )

    api.add_resource(
        OpenAPISpec,
        get_route_mapper().get(OpenAPISpec.__name__, "/spec/dams-collection-api.json"),
    )

    api.add_resource(
        InitJob,
        get_route_mapper().get(InitJob.__name__, "/job/init"),
    )
    api.add_resource(
        StartJob,
        get_route_mapper().get(StartJob.__name__, "/job/start/<string:id>"),
    )
    api.add_resource(
        AddDocumentToJob,
        get_route_mapper().get(
            AddDocumentToJob.__name__, "/job/add_document/<string:id>"
        ),
    )
    api.add_resource(
        FinishJob,
        get_route_mapper().get(FinishJob.__name__, "/job/finish/<string:id>"),
    )
    api.add_resource(
        FailJob,
        get_route_mapper().get(FailJob.__name__, "/job/fail/<string:id>"),
    )
    api.add_resource(
        FinishJobWithWarning,
        get_route_mapper().get(FinishJobWithWarning.__name__, "/job/warn/<string:id>"),
    )
    api.add_resource(
        ShareLink, get_route_mapper().get(ShareLink.__name__, "/share_links")
    )
    api.add_resource(
        ShareLinkDetail,
        get_route_mapper().get(ShareLinkDetail.__name__, "/share_links/<string:id>"),
    )

    api.add_resource(Tenant, get_route_mapper().get(Tenant.__name__, "/tenants"))

    api.add_resource(Ticket, get_route_mapper().get(Ticket.__name__, "/tickets"))
    api.add_resource(
        TicketDetail,
        get_route_mapper().get(TicketDetail.__name__, "/tickets/<string:id>"),
    )
    return api
