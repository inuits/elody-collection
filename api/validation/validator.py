from configuration import get_object_configuration_mapper
from elody.validator import validate_json
from flask_restful import abort
from logging_elody.log import log
from resources.base_resource import BaseResource


class Validator(BaseResource):
    def validate_dedecorator(self, request):
        def decorator(function):
            def wrapper(*args, **kwargs):
                if request.args.get("soft", 0, int):
                    return function(*args, **kwargs)

                http_method = request.method.lower()
                item = self._check_if_collection_and_item_exists(
                    kwargs.get("collection"), id, is_validating_content=True
                )
                content = self._get_content_according_content_type(
                    request,
                    content=kwargs.get("content"),
                    item={} if http_method == "post" else item,
                    spec=kwargs.get("spec", "elody"),
                    v2=True,
                )
                if not content:
                    raise ValueError("Content is missing in the request.")

                strategy, validator = (
                    get_object_configuration_mapper().get(content["type"]).validation()
                )
                apply_strategy = getattr(self, f"_apply_{strategy}_strategy")
                apply_strategy(validator, content, http_method=http_method)
                return function(*args, **kwargs)

            return wrapper

        return decorator

    def _apply_function_strategy(self, validator, content, http_method, **_):
        try:
            validator(http_method.lower(), content)
        except Exception as exception:
            log.exception(
                f"{exception.__class__.__name__}: {exception}",
                content,
                exc_info=exception,
            )
            abort(400, message=str(exception))

    def _apply_schema_strategy(self, validator, content, **_):
        validation_error = validate_json(content, validator)
        if validation_error:
            abort(400, message=f"{validation_error}")
