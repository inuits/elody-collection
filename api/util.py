from os import getenv

from logging_elody.log import log
from werkzeug.exceptions import BadRequest


def encode_content_type_header(content_type: str, fallback_content_type=""):
    if not content_type:
        if not fallback_content_type:
            raise BadRequest("No Content-Type provided")
        content_type = fallback_content_type
    return content_type.replace("/", "").replace(".", "").replace("-", "")


def get_boolean_env(key: str, default: bool = False) -> bool:
    """Turn a boolean-like environment variable into an actual boolean."""

    val = getenv(key)

    if val is None:
        return default

    return val.strip().lower() in {"true", "1", "yes", "y", "t"}


def get_int_env(key: str, default: int = 0) -> int:
    """Turn an int-like environment variable into an actual boolean."""

    val = getenv(key)
    if val is None:
        return default

    try:
        return int(val)
    except ValueError:
        log.error(
            f"Environment variable {key} could not be narrowed to an int, found value was {val}"
        )
        raise
