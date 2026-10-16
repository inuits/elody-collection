from collections.abc import Mapping
from logging import getLogger
from os import environ
from secrets import token_hex
from typing import TypedDict

log = getLogger(__name__)


class BaseConfig(TypedDict):
    DB_ENGINE: str
    MONGODB_HOSTS: list[str]
    STATIC_JWT: str | None
    GLITCH_TIP_ENABLED: bool
    GLITCH_TIP_DSN: str | None
    NOMAD_NAMESPACE: str | None
    APPS_MANIFEST: str
    SECRET_KEY: str
    LOAD_BACKGROUND_SCHEDULER: bool
    ENABLE_METRICS: bool
    DELETE_EMPTY_MEDIAFILES: bool
    HARD_DELETE: bool
    DELETE_MEDIAFILES: bool
    HEALTH_CHECK_EXTERNAL_SERVICES: bool
    LISTING_COUNT_CAP: int
    LOG_LEVEL: str
    LOKI_URL: str | None
    NOMAD_GROUP_NAME: str
    NOMAD_JOB_NAME: str
    SERVICE_TYPE: str
    SERVICE_TYPE_CATEGORY: str
    LOKI_TENANT_ID: str
    ELODY_LD_CONTEXT: str | None
    ELODY_LD_BASE_URI: str | None
    DAMS_FRONTEND_URL: str | None
    AMQP_STORM_APSCHEDULER: int
    AMQP_MANAGER: str
    AUTO_DELETE_EXCHANGE: bool
    DURABLE_EXCHANGE: bool
    PASSIVE_EXCHANGE: bool
    COLLECTION_API_URL: str
    IMAGE_API_URL_EXT: str | None
    STORAGE_API_URL: str | None
    STORAGE_API_URL_EXT: str | None
    TENANT_DEFINING_TYPES: list[str]
    AUTO_CREATE_TENANTS: bool
    TICKET_LIFESPAN: int
    TICKET_CLEANUP: int
    MINIO_BUCKET: str | None
    ROUTING_KEY_PREFIX: str
    QUEUE_PREFIX: str
    QUEUE_TYPE: str | None
    TYPESENSE_API_KEY: str | None
    TYPESENSE_SERVER_PORT: str
    TYPESENSE_SERVER_HOST: str
    TYPESENSE_CONNECTION_TIMEOUT_SECONDS: int
    SMTP_SECURITY: str | None
    SMTP_USE_TLS: bool
    SMTP_SERVER_HOST: str
    SMTP_SERVER_PORT: int
    SMTP_FROM_EMAIL: str | None
    SMTP_USERNAME: str | None
    SMTP_PASSWORD: str | None
    SERVICE_CLIENT_ID: str
    SERVICE_CLIENT_SECRET: str | None
    KEYCLOAK_BASE_URL: str | None
    REALM_NAME: str | None
    REDIRECT_URI: str | None
    OAUTH_CLIENT_ID: str | None
    ARANGO_DB_NAME: str | None
    DEFAULT_GRAPH: str
    ARANGO_DB_HOST: str | None
    ARANGO_DB_USERNAME: str | None
    ARANGO_DB_PASSWORD: str | None
    INSTRUMENT_MONGODB: bool
    MONGODB_DIRECT: int
    MONGODB_DB_NAME: str
    MONGODB_PORT: int
    MONGODB_REPLICA_SET: str | None
    MONGODB_USERNAME: str | None
    MONGODB_PASSWORD: str | None
    MONGODB_TLS: bool
    MONGODB_AUTH_SOURCE: str | None
    MONGODB_CONNECTION_STRING: str | None
    MONGODB_ALLOW_DISK_USE: bool
    MONGODB_READ_PREFERENCE: str
    MQ_EXCHANGE: str
    SUPER_ADMIN_ROLE: str


def load_config(env=environ) -> BaseConfig:
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
        "LOG_LEVEL": env.get("LOG_LEVEL", "info"),
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
            "TYPESENSE_SERVER_PORT", env.get("TYPESENSE_PORT", "8108")
        ),
        "TYPESENSE_SERVER_HOST": env.get(
            "TYPESENSE_SERVER_HOST", env.get("TYPESENSE_HOST", "typesense")
        ),
        "TYPESENSE_CONNECTION_TIMEOUT_SECONDS": env_int(
            env, "TYPESENSE_CONNECTION_TIMEOUT_SECONDS", 5
        ),
        "SMTP_SECURITY": env.get("SMTP_SECURITY"),
        "SMTP_USE_TLS": env_bool(env, "SMTP_USE_TLS", False),
        "SMTP_SERVER_HOST": env.get("SMTP_SERVER_HOST", "mailpit"),
        "SMTP_SERVER_PORT": env_int(env, "SMTP_SERVER_PORT", 1025),
        "SMTP_FROM_EMAIL": env.get("SMTP_FROM_EMAIL"),
        "SMTP_USERNAME": env.get("SMTP_USERNAME"),
        "SMTP_PASSWORD": env.get("SMTP_PASSWORD"),
        "SERVICE_CLIENT_ID": env.get("SERVICE_CLIENT_ID", "service_client"),
        "SERVICE_CLIENT_SECRET": env.get("SERVICE_CLIENT_SECRET"),
        "KEYCLOAK_BASE_URL": env.get("KEYCLOAK_BASE_URL"),
        "REALM_NAME": env.get("REALM_NAME"),
        "REDIRECT_URI": env.get("REDIRECT_URI", env.get("DAMS_FRONTEND_URL")),
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
        "MQ_EXCHANGE": env.get("MQ_EXCHANGE", "dams"),
        "SUPER_ADMIN_ROLE": env.get("SUPER_ADMIN_ROLE", "super_admin"),
    }


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
