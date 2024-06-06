from configuration import get_route_mapper
from healthcheck import HealthCheck
from os import getenv


_health = HealthCheck()


def init_health_check(app, database_available, rabbit_available):
    global _health
    if getenv("HEALTH_CHECK_EXTERNAL_SERVICES", True) in ["True", "true", True]:
        _health.add_check(database_available)
        _health.add_check(rabbit_available)
    app.add_url_rule(
        get_route_mapper().get("health", "/health"),
        "healthcheck",
        view_func=lambda: _health.run(),
    )


def get_health():
    global _health
    return _health
