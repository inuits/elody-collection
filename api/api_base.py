import json

from app_context import g
from flask import make_response
from flask_restful import Api


def _string_responder(data, code, headers, mimetype):
    """Helper to generate a response for already-serialized string data."""
    resp = make_response(data, code)
    resp.mimetype = mimetype
    if headers:
        resp.headers.extend(headers)
    return resp


def output_ld_json(data, code, headers=None):
    dumped = data if isinstance(data, str) else json.dumps(data)
    return _string_responder(dumped, code, headers, "application/ld+json")


def output_n_triples(data, code, headers=None):
    return _string_responder(data, code, headers, "application/n-triples")


def output_rdf_xml(data, code, headers=None):
    return _string_responder(data, code, headers, "application/rdf+xml")


def output_csv(data, code, headers=None):
    return _string_responder(data, code, headers, "text/csv")


def output_turtle(data, code, headers=None):
    return _string_responder(data, code, headers, "text/turtle")


def output_uri_list(data, code, headers=None):
    body = g.get("text_uri_list") if g.get("text_uri_list") else data
    return _string_responder(body, code, headers, "text/uri-list")


class CustomApi(Api):
    """Custom Flask-RESTful Api that has the representations pre-mapped."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.representations["application/ld+json"] = output_ld_json
        self.representations["application/n-triples"] = output_n_triples
        self.representations["application/rdf+xml"] = output_rdf_xml
        self.representations["text/csv"] = output_csv
        self.representations["text/turtle"] = output_turtle
        self.representations["text/uri-list"] = output_uri_list
