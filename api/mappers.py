import csv
import io
import json
import uuid
import re

from elody.util import get_item_metadata_value
from rdflib import Graph
from serialization.serialize import serialize
from storage.storagemanager import StorageManager

# TODO Move to client specific codebase
general_excluded_fields = [
    "identifier",
    "type",
    "filename",
    "bibliographic_citation_overwrite",
    "dc_rights_overwrite",
    "brocade_archief",
    "copyright_color_calculation",
    "isshownat",
    "copyright_paid",
    "institution",
    "format",
    "date",
    "collectiontype",
    "copyright_object",
    "bibliographic_citation",
    "dc_rights",
    "language",
    "relation",
    "object_number",
    # All relation fields
    "has*",
    "is*",
    "belongsTo",
]

map_relation_to_name = {
    "hasPhotographer": "photographer",
    "hasLicense": "license",
    "hasTag": "tag",
    "hasKeyword": "keyword",
    "hasLanguage": "language",
    "hasAssetCategory": "assetCategory",
    "hasLocationType": "locationType",
    "hasMarineRegion": "marineRegion",
    "hasEvent": "event",
    "hasProject": "project",
    "hasPartner": "partner",
    "hasCreator": "person",
    "hasOwner": "person",
    "hasConfidentiality": "confidentiality",
    "hasSubtitle": "subtitle",
    "hasPerson": "person",
    "hasContentMapType": "contentMapType",
    "hasFormalMapType": "formalMapType",
    "hasFormalDocumentType": "formalDocumentType",
    "hasContentDocumentType": "contentDocumentType",
}

map_name_to_csv_value = {
    "photographer": "name",
    "license": "key",
    "tag": "name",
    "media_keyword": "title",
    "language": "title",
    "asset_category": "title",
    "location_type": "title",
    "marine_region": "title",
    "event": "title",
    "project": "title",
    "partner": "title",
    "mediafile_keyword": "title",
    "owner": "name",
    "person": "name",
    "confidentiality": "title",
    "subtitle": "title",
    "creator": "name",
    "content_map_type": "title",
    "formal_map_type": "title",
    "formalDocumentType": "title",
    "contentDocumentType": "title",
}


def can_append_key(key, fields, excluded_fields=[]):
    if key is None:
        return False
    allowed_keys = set(map_relation_to_name.keys())
    if key in excluded_fields:
        return False
    for field in excluded_fields:
        if field is None:
            continue
        if field.endswith("*") and key not in allowed_keys:
            if re.match(field.replace("*", ".*"), key):
                return False
    if not fields:
        return True
    return key in fields


def is_relation_field(field):
    if re.fullmatch("(has|is)([A-Z][a-z]+)+", field):
        return True
    return False


def csv_writer(header, rows):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def is_uuid(value):
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def map_data_according_to_accept_header(
    data,
    accept_header,
    data_type="metadata",
    fields=None,
    spec="elody",
    request_parameters={},
    exclude_non_editable_fields=False,
):
    to_format = serialize.get_format(spec, request_parameters)
    if spec != "elody":
        return __serialize_data_according_to_accept_header(
            data, data_type, to_format, accept_header
        )

    match accept_header:
        case "application/ld+json":
            return map_to_rdf_data(data, data_type, format="json-ld")
        case "application/n-triples":
            return map_to_rdf_data(data, data_type, format="nt")
        case "application/rdf+xml":
            return map_to_rdf_data(data, data_type, format="pretty-xml")
        case "text/csv":
            return map_to_csv(data, data_type, fields, exclude_non_editable_fields)
        case "text/turtle":
            return map_to_rdf_data(data, data_type, format="turtle")
        case _:
            return __serialize_data_according_to_accept_header(
                data, data_type, to_format, accept_header
            )


def map_data_to_ldjson(data, format):
    match format:
        case "application/ld+json":
            format = "json-ld"
        case "application/n-triples":
            format = "nt"
        case "application/rdf+xml":
            format = "xml"
        case "text/turtle":
            format = "turtle"
    graph = Graph()
    graph.parse(data=data, format=format)
    return graph.serialize(format="json-ld")


def map_objects_to_csv(entities, fields=None, exclude_non_editable_fields=False):
    storage = StorageManager().get_db_engine()
    keys = list()
    root_values = list()
    excluded_fields = []
    if exclude_non_editable_fields:
        excluded_fields = general_excluded_fields
    for entity in entities:
        values = list()
        for id in entity.get("identifiers", []):
            if not can_append_key("identifiers", fields, excluded_fields):
                values.append({})
                break
            if "identifiers" not in keys:
                keys.append("identifiers")
            values.append({0: id})
        if can_append_key("identifier", fields, excluded_fields):
            if "identifier" not in keys:
                keys.append("identifier")
            values[0][0] = entity.get("_id")
        if can_append_key("type", fields, excluded_fields):
            if "type" not in keys:
                keys.append("type")
            values[0][1] = entity.get("type")
        if can_append_key("filename", fields, excluded_fields):
            if "filename" not in keys:
                keys.append("filename")
            values[0][2] = entity.get("original_filename")
        for metadata in entity.get("metadata", []):
            key = metadata.get("key")
            if is_uuid(key):
                continue
            if not can_append_key(key, fields, excluded_fields):
                continue
            if key not in keys:
                keys.append(metadata.get("key"))
            values[0][keys.index(key)] = metadata.get("value")
        for relation in entity.get("relations", []):
            original_type = relation.get("type")
            type = None
            if not can_append_key(original_type, fields, excluded_fields):
                continue
            if original_type in map_relation_to_name:
                type = map_relation_to_name.get(original_type)
            else:
                type = original_type
            if type not in keys:
                keys.append(type)
            if type in map_name_to_csv_value:
                item = storage.get_item_from_collection_by_id(
                    "entities", relation.get("key")
                )
                if item:
                    values[0][keys.index(type)] = get_item_metadata_value(
                        item, map_name_to_csv_value.get(type)
                    )
            else:
                values[0][keys.index(type)] = relation.get("key")
        for i in range(len(keys)):
            if i not in values[0]:
                values[0][i] = None
        values[0] = dict(sorted(values[0].items()))
        root_values += [list(row.values()) for row in values]
    return csv_writer(keys, root_values)


def map_object_to_csv(entity, fields=None, exclude_non_editable_fields=False):
    keys = list()
    values = list()
    excluded_fields = []
    if exclude_non_editable_fields:
        excluded_fields = general_excluded_fields

    for id in entity.get("identifiers", []):
        if not can_append_key("identifiers", fields, excluded_fields):
            values.append([])
            break
        if "identifiers" not in keys:
            keys.append("identifiers")
        values.append([id])
    if can_append_key("type", fields, excluded_fields):
        keys.append("type")
        values[0].append(entity.get("type"))
    for metadata in entity.get("metadata", []):
        key = metadata.get("key")
        if is_uuid(key):
            continue
        if not can_append_key(key, fields, excluded_fields):
            continue
        keys.append(key)
        values[0].append(metadata.get("value"))
    for relation in entity.get("relations", []):
        type = relation.get("type")
        if not can_append_key(type, fields, excluded_fields):
            continue
        keys.append(type)
        values[0].append(relation.get("key"))
    return csv_writer(keys, values)


def cast_to_boolean(value):
    if value.lower() == "true":
        return True
    elif value.lower() == "false":
        return False
    return value


def map_entity_to_rdf_data(objects, format):
    graph = Graph()
    for object in objects:
        if "data" not in object:
            continue
        data = json.dumps(object["data"])
        graph.parse(data=data, format="json-ld")
    rdf_data = graph.serialize(format=format)
    return rdf_data


def map_metadata_to_csv(metadata, fields=None):
    keys = list()
    values = list()
    for field in metadata:
        key = field.get("key")
        if not can_append_key(key, fields):
            continue
        keys.append(key)
        values.append(field.get("value"))
    return csv_writer(keys, [values])


def map_to_csv(data, data_type, fields=None, exclude_non_editable_fields=False):
    match data_type:
        case "metadata":
            return map_metadata_to_csv(data, fields)
        case "entities":
            return map_objects_to_csv(
                data["results"], fields, exclude_non_editable_fields
            )
        case "entity":
            return map_object_to_csv(data, fields, exclude_non_editable_fields)
        case "mediafiles":
            return map_objects_to_csv(
                data["results"], fields, exclude_non_editable_fields
            )
        case _:
            return data


def map_to_rdf_data(data, data_type, format):
    match data_type:
        case "entity":
            return map_entity_to_rdf_data([data], format)
        case "entities":
            return map_entity_to_rdf_data(data.get("results"), format)


def __serialize_data_according_to_accept_header(
    data, data_type, to_format, accept_header
):
    if data_type == "entities":
        results = []
        for result in data["results"]:
            results.append(
                serialize(
                    result,
                    type=result.get("type"),
                    to_format=to_format,
                    accept_header=accept_header,
                    hide_storage_format=True,
                )
            )
        data["results"] = results
        return data
    return serialize(
        data,
        type=data.get("type") if data_type == "entity" else None,
        to_format=to_format,
        accept_header=accept_header,
        hide_storage_format=True,
    )
