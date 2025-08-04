from configuration import get_object_configuration_mapper
from elody.error_codes import ErrorCode, get_error_code, get_write
from jsonschema import ValidationError, validate as validate_json
from logging_elody.log import log
from resources.base_resource import BaseResource
from werkzeug.exceptions import BadRequest


class Validator(BaseResource):
    def validate_decorator(self, http_method, request):
        def decorator(function):
            def wrapper(*args, **kwargs):
                if request.args.get("soft", 0, int):
                    return function(*args, **kwargs)

                item = {}
                if http_method.lower() != "post":
                    item = self._check_if_collection_and_item_exists(
                        kwargs.get("collection"),
                        kwargs.get("id"),
                        is_validating_content=True,
                    )

                content = self._get_content_according_content_type(
                    request=request,
                    content=kwargs.get("content"),
                    item=item,
                    spec=kwargs.get("spec", "elody"),
                    v2=True,
                )
                if not content:
                    raise ValueError(
                        f"{get_error_code(ErrorCode.CONTENT_NOT_FOUND, get_write())} - Content is missing in the request."
                    )
                if not content.get("type"):
                    raise BadRequest(
                        f"{get_error_code(ErrorCode.VALIDATION_ERROR, get_write())} - 'type' is a required property"
                    )

                strategy, validator = (
                    get_object_configuration_mapper().get(content["type"]).validation()
                )
                apply_strategy = getattr(self, f"_apply_{strategy}_strategy")
                apply_strategy(validator, content, http_method=http_method, item=item)
                return function(*args, **kwargs)

            return wrapper

        return decorator

    def _apply_function_strategy(self, validator, content, http_method, item, **_):
        try:
            validator(http_method.lower(), content, item)
        except BadRequest as bad_request:
            log.exception(
                f"{bad_request.__class__.__name__}: {bad_request}",
                content,
                exc_info=bad_request,
            )
            raise bad_request

    def _apply_schema_strategy(self, validator, content, **_):
        try:
            validate_json(content, validator)
        except ValidationError as error:
            schema = validator
            error_type = error.schema_path.pop()
            if error_type == "oneOf":
                property = error.instance.get(  # pyright: ignore
                    "key", error.instance.get("type")  # pyright: ignore
                )
                for context in error.context:  # pyright: ignore
                    if context.schema.get("_property") == property:  # pyright: ignore
                        schema = context.schema
                        error_type = context.schema_path.pop()  # pyright: ignore
                        break
            else:
                for path in error.schema_path:
                    schema = schema[path]
            raise BadRequest(
                schema.get("_customAttributes", {})  # pyright: ignore
                .get("errorMessages", {})
                .get(error_type, error.message)
            )
