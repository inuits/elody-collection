from configuration import get_object_configuration_mapper
from elody.validator import validate_json
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
        except BadRequest as bad_request:
            log.exception(
                f"{bad_request.__class__.__name__}: {bad_request}",
                content,
                exc_info=bad_request,
            )
            raise bad_request

    def _apply_schema_strategy(self, validator, content, **_):
        validation_error = validate_json(content, validator)
        if validation_error:
            raise BadRequest(f"{validation_error}")
