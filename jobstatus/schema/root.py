from ariadne import make_executable_schema, load_schema_from_path

# import mutation and query endpoints
from mutation.endpoint import custom_mutation
from query.endpoint import custom_query

# Schema definition
schema = make_executable_schema(
    # gets all schema definitions from the current folder and any other subsequent folders 'controller'
    # regardless where they are defined this method will fetch ans combine them all in one place.
    load_schema_from_path("./"),
    # Loads schema definition objects
    custom_query,
    custom_mutation,
)
