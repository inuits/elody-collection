from collections.abc import Mapping
from logging import getLogger
from os import environ
from secrets import token_hex
from typing import TypedDict

log = getLogger(__name__)


class ConfigError(RuntimeError):
    """Raised when required environment variables are missing or invalid.

    The message only contains variable names, never their values, so secrets
    in a misnamed variable don't end up in logs.
    """

    def __init__(
        self,
        missing: list[str] | None = None,
        invalid: list[tuple[str, str]] | None = None,
    ):
        self.missing = missing or []
        self.invalid = invalid or []
        lines = []
        if self.missing:
            lines.append(
                "Missing required environment variables: "
                + ", ".join(sorted(self.missing))
            )
        if self.invalid:
            lines.append(
                "Invalid environment variables: "
                + ", ".join(
                    f"{key} (expected {expected})"
                    for key, expected in sorted(self.invalid)
                )
            )
        super().__init__("\n".join(lines))


class Env(Mapping[str, str]):
    """Read-only view on the environment that collects missing and invalid keys.

    build_config passes one Env to the base and every client load_config, then
    raises a single ConfigError listing every problem.
    """

    def __init__(self, source: Mapping[str, str]):
        self._source = source
        self.missing: list[str] = []
        self.invalid: list[tuple[str, str]] = []

    def __getitem__(self, key):
        return self._source[key]

    def __iter__(self):
        return iter(self._source)

    def __len__(self):
        return len(self._source)


class _Required:
    def __repr__(self):
        return "REQUIRED"


# Pass as the default of an env_* parser to mark a variable as required
REQUIRED = _Required()


class BaseConfig(TypedDict):
    # AMQP/RabbitMQ related envs
    AMQP_MANAGER: str
    AMQP_STORM_APSCHEDULER: int
    MQ_EXCHANGE: str
    AUTO_DELETE_EXCHANGE: bool
    DURABLE_EXCHANGE: bool
    PASSIVE_EXCHANGE: bool
    QUEUE_PREFIX: str
    QUEUE_TYPE: str | None
    ROUTING_KEY_PREFIX: str

    # LD related envs
    ELODY_LD_BASE_URI: str | None
    ELODY_LD_CONTEXT: str | None

    # Arango related envs
    ARANGO_DB_HOST: str | None
    ARANGO_DB_NAME: str | None
    ARANGO_DB_PASSWORD: str | None
    ARANGO_DB_USERNAME: str | None
    DEFAULT_GRAPH: str

    # MongoDB related envs
    MONGODB_ALLOW_DISK_USE: bool
    MONGODB_AUTH_SOURCE: str | None
    MONGODB_CONNECTION_STRING: str | None
    MONGODB_DB_NAME: str
    MONGODB_DIRECT: int
    MONGODB_HOSTS: list[str]
    MONGODB_PASSWORD: str | None
    MONGODB_PORT: int
    MONGODB_READ_PREFERENCE: str
    MONGODB_REPLICA_SET: str | None
    MONGODB_TLS: bool
    MONGODB_USERNAME: str | None

    # Service uri's
    COLLECTION_API_URL: str
    IMAGE_API_URL_EXT: str | None
    STORAGE_API_URL: str | None
    STORAGE_API_URL_EXT: str | None
    DAMS_FRONTEND_URL: str | None

    # Keycloak related envs
    KEYCLOAK_BASE_URL: str | None
    REALM_NAME: str | None
    OAUTH_CLIENT_ID: str | None
    SERVICE_CLIENT_ID: str
    SERVICE_CLIENT_SECRET: str | None

    # Monitoring/O11y
    ENABLE_METRICS: bool
    GLITCH_TIP_DSN: str | None
    GLITCH_TIP_ENABLED: bool
    HEALTH_CHECK_EXTERNAL_SERVICES: bool
    INSTRUMENT_MONGODB: bool
    LOG_LEVEL: str
    LOKI_TENANT_ID: str
    LOKI_URL: str | None
    NOMAD_GROUP_NAME: str
    NOMAD_JOB_NAME: str
    NOMAD_NAMESPACE: str | None
    SERVICE_TYPE: str
    SERVICE_TYPE_CATEGORY: str

    # Email related envs
    REDIRECT_URI: str | None
    SMTP_FROM_EMAIL: str | None
    SMTP_PASSWORD: str | None
    SMTP_SECURITY: str | None
    SMTP_SERVER_HOST: str
    SMTP_SERVER_PORT: int
    SMTP_USERNAME: str | None
    SMTP_USE_TLS: bool

    # app configuration envs
    APPS_MANIFEST: str
    DB_ENGINE: str
    LOAD_BACKGROUND_SCHEDULER: bool
    AUTO_CREATE_TENANTS: bool
    DELETE_EMPTY_MEDIAFILES: bool
    DELETE_MEDIAFILES: bool
    HARD_DELETE: bool
    LISTING_COUNT_CAP: int
    SECRET_KEY: str
    STATIC_JWT: str | None
    SUPER_ADMIN_ROLE: str
    TENANT_DEFINING_TYPES: list[str]
    TICKET_CLEANUP: int
    TICKET_LIFESPAN: int

    # S3
    MINIO_BUCKET: str | None

    # Typesense related envs
    TYPESENSE_API_KEY: str | None
    TYPESENSE_CONNECTION_TIMEOUT_SECONDS: int
    TYPESENSE_SERVER_HOST: str
    TYPESENSE_SERVER_PORT: str


def load_config(env=environ) -> BaseConfig:
    return {
        # AMQP/RabbitMQ related envs
        "AMQP_MANAGER": env.get("AMQP_MANAGER", "amqpstorm_flask"),
        "AMQP_STORM_APSCHEDULER": env_int(env, "AMQP_STORM_APSCHEDULER", 1),
        "MQ_EXCHANGE": env.get("MQ_EXCHANGE", "dams"),
        "AUTO_DELETE_EXCHANGE": env_bool(env, "AUTO_DELETE_EXCHANGE", False),
        "DURABLE_EXCHANGE": env_bool(env, "DURABLE_EXCHANGE", True),
        "PASSIVE_EXCHANGE": env_bool(env, "PASSIVE_EXCHANGE", False),
        "QUEUE_PREFIX": env.get("QUEUE_PREFIX", "dams"),
        "QUEUE_TYPE": env.get("QUEUE_TYPE"),
        "ROUTING_KEY_PREFIX": env.get("ROUTING_KEY_PREFIX", "dams"),
        # LD related envs
        "ELODY_LD_BASE_URI": env.get("ELODY_LD_BASE_URI"),
        "ELODY_LD_CONTEXT": env.get("ELODY_LD_CONTEXT"),
        # Arango related envs
        "ARANGO_DB_HOST": env.get("ARANGO_DB_HOST"),
        "ARANGO_DB_NAME": env.get("ARANGO_DB_NAME"),
        "ARANGO_DB_PASSWORD": env.get("ARANGO_DB_PASSWORD"),
        "ARANGO_DB_USERNAME": env.get("ARANGO_DB_USERNAME"),
        "DEFAULT_GRAPH": env.get("DEFAULT_GRAPH", "assets"),
        # MongoDB related envs
        "MONGODB_ALLOW_DISK_USE": env_bool(env, "MONGODB_ALLOW_DISK_USE", False),
        "MONGODB_AUTH_SOURCE": env.get("MONGODB_AUTH_SOURCE"),
        "MONGODB_CONNECTION_STRING": env.get("MONGODB_CONNECTION_STRING"),
        "MONGODB_DB_NAME": env.get("MONGODB_DB_NAME", "dams"),
        "MONGODB_DIRECT": env_int(env, "MONGODB_DIRECT", 0),
        "MONGODB_HOSTS": env_list(env, "MONGODB_HOSTS", ["mongo"]),
        "MONGODB_PASSWORD": env.get("MONGODB_PASSWORD"),
        "MONGODB_PORT": env_int(env, "MONGODB_PORT", 27017),
        "MONGODB_READ_PREFERENCE": env.get(
            "MONGODB_READ_PREFERENCE", "secondaryPreferred"
        ),
        "MONGODB_REPLICA_SET": env.get("MONGODB_REPLICA_SET"),
        "MONGODB_TLS": env_bool(env, "MONGODB_TLS", False),
        "MONGODB_USERNAME": env.get("MONGODB_USERNAME"),
        # Service uri's
        "COLLECTION_API_URL": env["COLLECTION_API_URL"],
        "IMAGE_API_URL_EXT": env.get("IMAGE_API_URL_EXT"),
        "STORAGE_API_URL": env.get("STORAGE_API_URL"),
        "STORAGE_API_URL_EXT": env.get("STORAGE_API_URL_EXT"),
        "DAMS_FRONTEND_URL": env.get("DAMS_FRONTEND_URL"),
        # Keycloak related envs
        "KEYCLOAK_BASE_URL": env.get("KEYCLOAK_BASE_URL"),
        "REALM_NAME": env.get("REALM_NAME"),
        "OAUTH_CLIENT_ID": env.get("OAUTH_CLIENT_ID"),
        "SERVICE_CLIENT_ID": env.get("SERVICE_CLIENT_ID", "service_client"),
        "SERVICE_CLIENT_SECRET": env.get("SERVICE_CLIENT_SECRET"),
        # Monitoring/O11y
        "ENABLE_METRICS": env_bool(env, "ENABLE_METRICS", False),
        "GLITCH_TIP_DSN": env.get("GLITCH_TIP_DSN"),
        "GLITCH_TIP_ENABLED": env_bool(env, "GLITCH_TIP_ENABLED", False),
        "HEALTH_CHECK_EXTERNAL_SERVICES": env_bool(
            env, "HEALTH_CHECK_EXTERNAL_SERVICES", True
        ),
        "INSTRUMENT_MONGODB": env_bool(env, "INSTRUMENT_MONGODB", False),
        "LOG_LEVEL": env.get("LOG_LEVEL", "info"),
        "LOKI_TENANT_ID": env.get("LOKI_TENANT_ID", "infra"),
        "LOKI_URL": env.get("LOKI_URL"),
        "NOMAD_GROUP_NAME": env.get("NOMAD_GROUP_NAME", "nomad_group_name"),
        "NOMAD_JOB_NAME": env_list(env, "NOMAD_JOB_NAME", ["env"], "-")[-1],
        "NOMAD_NAMESPACE": env.get("NOMAD_NAMESPACE"),
        "SERVICE_TYPE": env.get("SERVICE_TYPE", "api"),
        "SERVICE_TYPE_CATEGORY": env.get("SERVICE_TYPE_CATEGORY", "collection"),
        # Email related envs
        "REDIRECT_URI": env.get("REDIRECT_URI", env.get("DAMS_FRONTEND_URL")),
        "SMTP_FROM_EMAIL": env.get("SMTP_FROM_EMAIL"),
        "SMTP_PASSWORD": env.get("SMTP_PASSWORD"),
        "SMTP_SECURITY": env.get("SMTP_SECURITY"),
        "SMTP_SERVER_HOST": env.get("SMTP_SERVER_HOST", "mailpit"),
        "SMTP_SERVER_PORT": env_int(env, "SMTP_SERVER_PORT", 1025),
        "SMTP_USERNAME": env.get("SMTP_USERNAME"),
        "SMTP_USE_TLS": env_bool(env, "SMTP_USE_TLS", False),
        # app configuration envs
        "APPS_MANIFEST": env["APPS_MANIFEST"],
        "DB_ENGINE": env.get("DB_ENGINE", "mongo"),
        "LOAD_BACKGROUND_SCHEDULER": env_bool(env, "LOAD_BACKGROUND_SCHEDULER", False),
        "AUTO_CREATE_TENANTS": env_bool(env, "AUTO_CREATE_TENANTS"),
        "DELETE_EMPTY_MEDIAFILES": env_bool(env, "DELETE_EMPTY_MEDIAFILES", False),
        "DELETE_MEDIAFILES": env_bool(env, "DELETE_MEDIAFILES", False),
        "HARD_DELETE": env_bool(env, "HARD_DELETE", False),
        "LISTING_COUNT_CAP": env_int(env, "LISTING_COUNT_CAP", 0),
        "SECRET_KEY": env.get("SECRET_KEY", token_hex(16)),
        "STATIC_JWT": env.get("STATIC_JWT"),
        "SUPER_ADMIN_ROLE": env.get("SUPER_ADMIN_ROLE", "super_admin"),
        "TENANT_DEFINING_TYPES": env_list(env, "TENANT_DEFINING_TYPES"),
        "TICKET_CLEANUP": env_int(env, "TICKET_CLEANUP", 86400),
        "TICKET_LIFESPAN": env_int(env, "TICKET_LIFESPAN", 3600),
        # S3
        "MINIO_BUCKET": env.get("MINIO_BUCKET"),
        # Typesense related envs
        "TYPESENSE_API_KEY": env.get("TYPESENSE_API_KEY"),
        "TYPESENSE_CONNECTION_TIMEOUT_SECONDS": env_int(
            env, "TYPESENSE_CONNECTION_TIMEOUT_SECONDS", 5
        ),
        "TYPESENSE_SERVER_HOST": env.get(
            "TYPESENSE_SERVER_HOST", env.get("TYPESENSE_HOST", "typesense")
        ),
        "TYPESENSE_SERVER_PORT": env.get(
            "TYPESENSE_SERVER_PORT", env.get("TYPESENSE_PORT", "8108")
        ),
    }


_TRUE_VALUES = {"true", "1", "yes", "y", "t"}
_FALSE_VALUES = {"false", "0", "no", "n", "f"}


def _report_missing(env: Mapping[str, str], key: str) -> None:
    if isinstance(env, Env):
        env.missing.append(key)
    else:
        raise ConfigError(missing=[key])


def _report_invalid(env: Mapping[str, str], key: str, expected: str) -> None:
    log.error(f"Environment variable {key} could not be parsed as {expected}")
    if isinstance(env, Env):
        env.invalid.append((key, expected))
    else:
        raise ConfigError(invalid=[(key, expected)])


def _lookup(env: Mapping[str, str], key: str, default: object) -> str | None:
    """Return the raw value, or None when the parser should use a fallback.

    Optional variables only fall back when unset. Required variables also
    count as missing when empty, and are reported.
    """

    val = env.get(key)
    if not isinstance(default, _Required):
        return val

    if val is None or val.strip() == "":
        _report_missing(env, key)
        return None
    return val


def env_bool(
    env: Mapping[str, str],
    key: str,
    default: bool | _Required = False,
) -> bool:
    """Turn a boolean-like environment variable into an actual boolean.

    Required booleans are parsed strictly, so a typo like "ture" is reported
    instead of silently becoming False.
    """

    val = _lookup(env, key, default)
    if val is None:
        return default if isinstance(default, bool) else False

    normalized = val.strip().lower()
    if isinstance(default, _Required) and normalized not in (
        _TRUE_VALUES | _FALSE_VALUES
    ):
        _report_invalid(env, key, "bool")
        return False

    return normalized in _TRUE_VALUES


def env_int(
    env: Mapping[str, str],
    key: str,
    default: int | _Required = 0,
) -> int:
    """Turn an int-like environment variable into an actual integer."""

    fallback = default if isinstance(default, int) else 0
    val = _lookup(env, key, default)
    if val is None:
        return fallback

    try:
        return int(val)
    except ValueError:
        _report_invalid(env, key, "int")
        return fallback


def env_list(
    env: Mapping[str, str],
    key: str,
    default: list[str] | _Required | None = None,
    sep: str = ",",
) -> list[str]:
    """Split a separated environment variable into a list.

    Required lists drop blank items, and count as missing when nothing is left.
    """

    if isinstance(default, _Required):
        val = _lookup(env, key, default)
        if val is None:
            return []
        items = [item.strip() for item in val.split(sep) if item.strip()]
        if not items:
            _report_missing(env, key)
        return items

    if default is None:
        default = []

    val = env.get(key)
    if not val:
        return default

    return val.split(sep)


def env_str(
    env: Mapping[str, str],
    key: str,
    default: str | _Required,
) -> str:
    """Read a string environment variable that has a default or is required.

    Optional strings without a default keep using env.get(key).
    """

    val = _lookup(env, key, default)
    if val is None:
        return default if isinstance(default, str) else ""
    return val
