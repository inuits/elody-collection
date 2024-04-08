import app

from elody.csv import CSVSingleObject
from elody.validator import validate_json
from flask import request
from flask_restful import abort, request


def _get_content_according_content_type(request, object_type="entity"):
    content_type = request.content_type
    match content_type:
        case "application/json":
            return request.get_json()
        case "text/csv":
            csv = request.get_data(as_text=True)
            parsed_csv = CSVSingleObject(csv, object_type)
            if object_type in ["metadata", "relations"]:
                return getattr(parsed_csv, object_type)
            return parsed_csv.get_type(object_type)
        case _:
            return request.get_json()


class Validator:
    def validator(object_type=None):
        def decorator(func):
            def wrapper(*args, **kwargs):
                nonlocal object_type
                if object_type is None:
                    object_type = request.path.split("/")[1]
                content = _get_content_according_content_type(
                    request, object_type=object_type
                )
                if content is None:
                    raise ValueError("Content is missing in the request.")
                validation_method, validator = (
                    app.object_configuration_mapper.get(object_type).validation()
                )
                if validation_method == "schema":
                    validation_error = validate_json(content, validator)
                    if validation_error:
                        abort(
                            400,
                            message=f"{validation_error}",
                        )
                elif validation_method == "function":
                    validator()
                else:
                    raise Exception(f"Validation method: {validation_method} doesn't exist ")

                return func(*args, **kwargs)

            return wrapper

        return decorator
