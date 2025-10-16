from werkzeug.exceptions import BadRequest


def encode_content_type_header(content_type: str):
    if not content_type:
        raise BadRequest("No Content-Type provided")
    return content_type.replace("/", "").replace(".", "").replace("-", "")
