from prometheus_flask_exporter import RESTfulPrometheusMetrics
from os import getenv


def init_metrics(app, api):
    metrics = RESTfulPrometheusMetrics.for_app_factory(
        api, defaults_prefix=getenv("JOB_NAME", "collection-api")
    )
    metrics.init_app(app)

    return metrics
