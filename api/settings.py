from collections.abc import Mapping
from os import environ
from secrets import token_hex

from logging_elody.log import log


def env_bool(
    env: Mapping[str, str],
    key: str,
    default: bool = False,
) -> bool:
    """Turn a boolean-like environment variable into an actual boolean."""

    val = env.get(key)

    if val is None:
        return default

    return val.strip().lower() in {"true", "1", "yes", "y", "t"}


def env_int(
    env: Mapping[str, str],
    key: str,
    default: int = 0,
) -> int:
    """Turn an int-like environment variable into an actual integer."""

    val = env.get(key)
    if val is None:
        return default

    try:
        return int(val)
    except ValueError:
        log.error(
            f"Environment variable {key} could not be narrowed to an int, found value was {val}"
        )
        raise


def env_list(
    env: Mapping[str, str], key: str, default: list | None = None, sep: str = ","
):
    if default is None:
        default = []

    val = env.get(key)
    if not val:
        return default

    return val.split(sep)


def load_config(env=environ) -> dict:
    return {
        "DB_ENGINE": env.get("DB_ENGINE", "mongo"),
        "MONGODB_HOSTS": env_list(env, "MONGODB_HOSTS", ["mongo"]),
        "STATIC_JWT": env.get("STATIC_JWT"),
        "GLITCH_TIP_ENABLED": env_bool(env, "GLITCH_TIP_ENABLED", False),
        "GLITCH_TIP_DSN": env.get("GLITCH_TIP_DSN"),
        "NOMAD_NAMESPACE": env.get("NOMAD_NAMESPACE"),
        "APPS_MANIFEST": env["APPS_MANIFEST"],
        "SECRET_KEY": env.get("SECRET_KEY", token_hex(16)),
        "LOAD_BACKGROUND_SCHEDULER": env_bool(env, "LOAD_BACKGROUND_SCHEDULER", False),
        "ENABLE_METRICS": env_bool(env, "ENABLE_METRICS", False),
        "DELETE_EMPTY_MEDIAFILES": env_bool(env, "DELETE_EMPTY_MEDIAFILES", False),
        "HARD_DELETE": env_bool(env, "HARD_DELETE", False),
        "DELETE_MEDIAFILES": env_bool(env, "DELETE_MEDIAFILES", False),
        "HEALTH_CHECK_EXTERNAL_SERVICES": env_bool(
            env, "HEALTH_CHECK_EXTERNAL_SERVICES", True
        ),
        "LISTING_COUNT_CAP": env_int(env, "LISTING_COUNT_CAP", 0),
        "LOG_LEVEL": env.get("LOG_LEVEL"),
        "LOKI_URL": env.get("LOKI_URL"),
        "NOMAD_GROUP_NAME": env.get("NOMAD_GROUP_NAME", "nomad_group_name"),
        "NOMAD_JOB_NAME": env_list(env, "NOMAD_JOB_NAME", ["env"], "-")[-1],
        "SERVICE_TYPE": env.get("SERVICE_TYPE", "api"),
        "SERVICE_TYPE_CATEGORY": env.get("SERVICE_TYPE_CATEGORY", "collection"),
        "LOKI_TENANT_ID": env.get("LOKI_TENANT_ID", "infra"),
        "ELODY_LD_CONTEXT": env.get("ELODY_LD_CONTEXT"),
        "ELODY_LD_BASE_URI": env.get("ELODY_LD_BASE_URI"),
        "DAMS_FRONTEND_URL": env.get("DAMS_FRONTEND_URL"),
        "AMQP_STORM_APSCHEDULER": env_int(env, "AMQP_STORM_APSCHEDULER", 1),
        "AMQP_MANAGER": env.get("AMQP_MANAGER", "amqpstorm_flask"),
        "AUTO_DELETE_EXCHANGE": env_bool(env, "AUTO_DELETE_EXCHANGE", False),
        "DURABLE_EXCHANGE": env_bool(env, "DURABLE_EXCHANGE", True),
        "PASSIVE_EXCHANGE": env_bool(env, "PASSIVE_EXCHANGE", False),
        "COLLECTION_API_URL": env["COLLECTION_API_URL"],
        "IMAGE_API_URL_EXT": env.get("IMAGE_API_URL_EXT"),
        "STORAGE_API_URL": env.get("STORAGE_API_URL"),
        "STORAGE_API_URL_EXT": env.get("STORAGE_API_URL_EXT"),
        "TENANT_DEFINING_TYPES": env_list(env, "TENANT_DEFINING_TYPES"),
        "AUTO_CREATE_TENANTS": env_bool(env, "AUTO_CREATE_TENANTS"),
        "TICKET_LIFESPAN": env_int(env, "TICKET_LIFESPAN", 3600),
        "TICKET_CLEANUP": env_int(env, "TICKET_CLEANUP", 86400),
        "MINIO_BUCKET": env.get("MINIO_BUCKET"),
        "ROUTING_KEY_PREFIX": env.get("ROUTING_KEY_PREFIX", "dams"),
        "QUEUE_PREFIX": env.get("QUEUE_PREFIX", "dams"),
        "QUEUE_TYPE": env.get("QUEUE_TYPE"),
        "TYPESENSE_API_KEY": env.get("TYPESENSE_API_KEY"),
        "TYPESENSE_SERVER_PORT": env.get(
            "TYPESENSE_SERVER_PORT", env.get("TYPSENSE_PORT", "8108")
        ),
        "TYPESENSE_SERVER_HOST": env.get(
            "TYPESENSE_SERVER_HOST", env.get("TYPSENSE_HOST", "typsense")
        ),
        "TYPESENSE_CONNECTION_TIMEOUT_SECONDS": env_int(
            env, "TYPESENSE_CONNECTION_TIMEOUT_SECONDS", 5
        ),
        "SMTP_SECURITY": env.get("SMTP_SECURITY"),
        "SMTP_USE_TLS": env_bool(env, "SMTP_USE_TLS", False),
        "SMTP_SERVER_HOST": env.get("SMTP_SERVER_HOST", "mailpit"),
        "SMTP_SERVER_PORT": env.get("SMTP_SERVER_PORT", "1205"),
        "SMTP_FROM_EMAIL": env.get("SMTP_FROM_EMAIL"),
        "SMTP_USERNAME": env.get("SMTP_USERNAME"),
        "SMTP_PASSWORD": env.get("SMTP_PASSWORD"),
        "SERVICE_CLIENT_ID": env.get("SERVICE_CLIENT_ID", "service_client"),
        "SERVICE_CLIENT_SECRET": env.get("SERVICE_CLIENT_SECRET"),
        "KEYCLOAK_BASE_URL": env.get("KEYCLOAK_BASE_URL"),
        "REALM_NAME": env.get("REALM_NAME"),
        "REDIRECT_URI": env.get("REDIRECT_URI"),
        "OAUTH_CLIENT_ID": env.get("OAUTH_CLIENT_ID"),
        "ARANGO_DB_NAME": env.get("ARANGO_DB_NAME"),
        "DEFAULT_GRAPH": env.get("DEFAULT_GRAPH", "assets"),
        "ARANGO_DB_HOST": env.get("ARANGO_DB_HOST"),
        "ARANGO_DB_USERNAME": env.get("ARANGO_DB_USERNAME"),
        "ARANGO_DB_PASSWORD": env.get("ARANGO_DB_PASSWORD"),
        "INSTRUMENT_MONGODB": env_bool(env, "INSTRUMENT_MONGODB", False),
        "MONGODB_DIRECT": env_int(env, "MONGODB_DIRECT", 0),
        "MONGODB_DB_NAME": env.get("MONGODB_DB_NAME", "dams"),
        "MONGODB_PORT": env_int(env, "MONGODB_PORT", 27017),
        "MONGODB_REPLICA_SET": env.get("MONGODB_REPLICA_SET"),
        "MONGODB_USERNAME": env.get("MONGODB_USERNAME"),
        "MONGODB_PASSWORD": env.get("MONGODB_PASSWORD"),
        "MONGODB_TLS": env_bool(env, "MONGODB_TLS", False),
        "MONGODB_AUTH_SOURCE": env.get("MONGODB_AUTH_SOURCE"),
        "MONGODB_CONNECTION_STRING": env.get("MONGODB_CONNECTION_STRING"),
        "MONGODB_ALLOW_DISK_USE": env_bool(env, "MONGODB_ALLOW_DISK_USE", False),
        "MONGODB_READ_PREFERENCE": env.get(
            "MONGODB_READ_PREFERENCE", "secondaryPreferred"
        ),
    }
