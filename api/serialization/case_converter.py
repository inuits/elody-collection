import re


def snake_to_camel(snake_case_str):
    components = snake_case_str.split("_")
    camel_case_str = components[0] + "".join(
        component.title() for component in components[1:]
    )
    return camel_case_str


def camel_to_snake(camel_case_str):
    snake_case_str = re.sub(r"(?<!^)(?=[A-Z])", "_", camel_case_str).lower()
    return snake_case_str
