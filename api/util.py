from werkzeug.exceptions import BadRequest


def encode_content_type_header(content_type: str, fallback_content_type=""):
    if not content_type:
        if not fallback_content_type:
            raise BadRequest("No Content-Type provided")
        content_type = fallback_content_type
    return content_type.replace("/", "").replace(".", "").replace("-", "")
