"""
Ths is a simple GraphQL And REST API view_controller.The main development part will focus on GraphQL implementation
@author - SK

Code styles : Python3 with elevated OOP standards.
"""
from ariadne import graphql_sync
from ariadne.constants import PLAYGROUND_HTML
from flask import request, jsonify
from configuration.config import jobs
from schema.root import schema


# Set a simple and sample REST entry pont
@jobs.route('/')
def index():
    server_name = 'Jobs-status API'
    return {'message': f'Welcome {server_name}'}  # return a simple welcome object


# set the graphQL test server  - this is only for testing only, in production this access to this  endpoint will be
# restricted. alternatively disable this endpoint to access the api via postman.
@jobs.route('/jobs-status', methods=['GET'])
def jobs_test_server():
    return PLAYGROUND_HTML, 200


# Main GraphQL Server
@jobs.route('/jobs-status', methods=['POST'])
def jobs_graphql_server():
    """

    @return:
    """
    # every request sent to the sever is converted to json
    data = request.get_json()
    # set the schema and data fetching mechanism.
    success, result = graphql_sync(
        schema,
        data,
        context_value=request,
        debug=jobs.debug
    )
    # set the status code in which 200 represents success and 200 a failed request.
    status_code = 200 if success else 400

    # convert the response to json -readable format using flask request module and send the
    # response to the use along with the status code
    return jsonify(result), status_code
