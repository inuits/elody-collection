import csv
import io


def csv_writter(header, rows):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def map_data_according_to_accept_header(data, accept_header, data_type="metadata"):
    match accept_header:
        case "text/csv":
            return map_to_csv(data, data_type)
        case _:
            return data


def map_entity_to_csv(entity):
    keys = list()
    values = list()
    for id in entity.get("identifiers", []):
        if "identifier" not in keys:
            keys.append("identifier")
        values.append([id])
    keys.append("type")
    values[0].append(entity.get("type"))
    for metadata in entity.get("metadata", []):
        keys.append(metadata.get("key"))
        values[0].append(metadata.get("value"))
    for relation in entity.get("relations", []):
        keys.append(relation.get("label"))
        values[0].append(relation.get("key"))
    return csv_writter(keys, values)


def map_metadata_to_csv(metadata):
    keys = list()
    values = list()
    for field in metadata:
        keys.append(field.get("key"))
        values.append(field.get("value"))
    return csv_writter(keys, [values])


def map_to_csv(data, data_type):
    match data_type:
        case "metadata":
            return map_metadata_to_csv(data)
        case "entity":
            return map_entity_to_csv(data)
        case _:
            return data
